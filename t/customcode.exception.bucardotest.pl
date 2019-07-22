#! perl

## Sample exception handler
## For this example, we will resolve unique violations by keeping the most
## recent record with the conflicting value and deleting the other values
## and any records that references them. It uses a general purpose design,
## requiring only a single-column primary key and a single column
## timestamp so as to keep the record with the latest time. Feel free to
## adapt to your use cases.
##
## To add this script to a sync, assuming the setting of the variables in the
## next section, run this command, replacing `{sync}` with the name of the
## sync it applies to.
##
##     bucardo add customcode employee_subid_email_conflict \
##            whenrun=exception \
##            src_code=bucardo_unique_conflict_resolution.pl \
##            sync={sync} \
##            getdbh=1 \
##            relation=public.employee

use strict;
use warnings;

##############################################################################
# Configuration
##############################################################################
# Set these variables to specify the unique constraint conflict to resolve.
# Quote identifiers properly for inclusion in queries. The value of $columns
# should be a the exact query expression used to create the unique constraint
# or index, generally a comma-delimited list of one or more columns or function
# calls, function, such as `lower(email)`. Check the constrait expression as
# shown by Postgres itself to ensure an exact match.
my $schema   = 'public';
my $table    = 'employee';
my $pk_col   = 'id';
my $columns  = 'subid, lower(email)';
my $time_col = 'updated_at';

# If there are any tables with FK constraints pointing to records to be
# deleted, list them here and the script will delete them, first. List in
# the order to be deleted, account for additional foreign key constraints
# if necessary. Format as arrays: [$schema, $table, $fk_column].
my @cascade  = (
    [qw(public supplies employee_id)],
);

# Optionaly set $copy_to to table with identical columns to $table to store
# away deleted records for later evaluation or recovery.
my $copy_to  = 'employee_conflict';
##############################################################################
# End of Configuration
##############################################################################

my $info = shift;
return if $info->{schemaname} ne $schema || $info->{tablename} ne $table;

# Do nothing unless it's a unique constraint violation for the columns.
return if $info->{error_string} !~ /violates unique constraint/
       || $info->{error_string} !~ /DETAIL:\s+Key\s+\Q($columns)\E/;

# Grab all the primary keys involved in the sync.
my %pks = map { $_ => 1 } map { keys %{ $_ } } values %{ $info->{deltabin} };

# Very unlikely to happen, but check anyway.
unless (%pks) {
    $info->{warning} = 'No database records found!';
    return;
}

# Query each database for the PKs, unique expression value as a JSON array,
# and update time.
my $query = qq{
    SELECT $pk_col                               AS pkey,
           json_build_array($columns)            AS ukey,
           extract(epoch FROM $time_col)         AS utime,
           ?::TEXT                               AS db
      FROM $schema.$table
     WHERE $pk_col = ANY(?)
};

# We'll want one instance of each unique key.
my %rec_for;

# Always work the databases in the same order.
my $dbhs = $info->{dbh};
for my $db (sort keys %{ $dbhs }) {
    my $dbh = $dbhs->{$db};
    my $sth = $dbh->prepare($query);
    $sth->execute($db, [keys %pks]);

    while (my $curr_rec = $sth->fetchrow_hashref) {
        # First time seeing this key? All good, record and move on.
        my $prev_rec = $rec_for{ $curr_rec->{ukey} } || do {
            $rec_for{ $curr_rec->{ukey} } = $curr_rec;
            next;
        };

        # Unique key seen already. No conflict if the PK is the same.
        next if $curr_rec->{pkey} eq $prev_rec->{pkey};

        # Keep the record with the latest update time.
        my ($keep, $lose) = $curr_rec->{utime} > $prev_rec->{utime}
            ? ($curr_rec, $prev_rec)
            : ($prev_rec, $curr_rec);
        $rec_for{ $keep->{ukey} } = $keep;

        # Store away the older record in a separate table if we want to manually
        # check or recover deleted records later.
        $dbhs->{ $lose->{db} }->do(
            "INSERT INTO $copy_to SELECT * FROM employee WHERE id = ?",
            $lose->{pkey},
        ) if $copy_to;

        # Cascade delete the older record.
        $dbhs->{ $lose->{db} }->do(
            "DELETE FROM $_->[0].$_->[1] WHERE $_->[2] = ?",
            $lose->{pkey},
        ) for @cascade, [$schema, $table, $pk_col];

        # Log the resolution.
        $info->{message} .= qq{Unique conflict on $schema.$table($columns) for value \`$keep->{ukey}\` resolved by deleting $pk_col \`$lose->{pkey}\` from database $lose->{db} and keeping $pk_col \`$keep->{pkey}\` from database $keep->{db}};

        # Note: Don't commit, Bucard handles transactions.
    }
}

# Retry now that things are cleaned up!
$info->{retry} = 1;

return;
