#! perl

## Sample exception handler
## For this example, we will fix unique violations on an email column

#! perl

use strict;
use warnings;

##############################################################################
# Set these variables to speciy the unique constraint conflict to resolve.
my $schema   = 'public';
my $table    = 'employee';
my $pk_col   = 'id';
my $columns  = 'email';
my $time_col = 'updated_at';

# If there are any tables with FK constraints pointint to records to be
# deleted, list them here and the script will delete them, first. List in
# the order to be deleted. Format as arryays: [$schema, $table, $fk_column].
my @cascade  = (
    # qw(log employee id)],
);

##############################################################################

my $info = shift;

return if $info->{schemaname} ne $schema || $info->{tablename} ne $table;

# Do nothing unless it a unique constraint violation for the columns.
return if $info->{error_string} !~ /violates unique constraint/
       || $info->{error_string} !~ /DETAIL:\s+Key \($columns\)/;


# Grab all the primary keys involved in the sync
my %pks = map { $_ => 1 } map { keys %{ $_ } } values %{ $info->{deltabin} };

# Very unlikely to happen, but we will check anyway:
unless (%pks) {
    $info->{warning} = 'No database records found!';
    return;
}

# Query each database for the PKs, hashed unique key, and update time.
my $query = qq{
    SELECT $pk_col                               AS pkey,
           json_build_array($columns)            AS val,
           md5(json_build_array($columns)::TEXT) AS ukey,
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
        # This a new unique key? All is good, just move on
        my $prev_rec = $rec_for{ $curr_rec->{ukey} } || do {
            $rec_for{ $curr_rec->{ukey} } = $curr_rec;
            next;
        };

        # This unique key already exists. If the same PK, no problem
        next if $curr_rec->{pkey} eq $prev_rec->{pkey};

        # Keep the record with the latest update time.
        my ($keep, $lose) = $curr_rec->{utime} > $prev_rec->{utime}
            ? ($curr_rec, $prev_rec)
            : ($prev_rec, $curr_rec);
        $rec_for{ $keep->{ukey} } = $keep;

        # Store away the older record in a separate table
        $dbhs->{ $lose->{db} }->do(
            'INSERT INTO employee_conflict SELECT * FROM employee WHERE id = ?',
            $lose->{pkey},
        );

        # Cascade delete the older record.
        $dbhs->{ $lose->{db} }->do(
            "DELETE FROM $_->[0].$_->[1] WHERE $_->[2] = ?",
            $lose->{pkey},
        ) for @cascade, [$schema, $table, $pk_col];

        # Log the resolution.
        $info->{message} .= qq{Unique conflict on $schema.$table($columns) for value \`$keep->{val}\` resolved by deleting $pk_col \`$lose->{pkey}\` from database $lose->{db} and keeping $pk_col \`$keep->{pkey}\` from database $keep->{db}};

        # Note: we do not want to commit (and it is disallowed by DBIx::Safe)
    }
}

# Retry now that things are cleaned up!
$info->{retry} = 1;

return;
