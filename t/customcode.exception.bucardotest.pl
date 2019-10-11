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
##     bucardo add customcode employee_sub_email_key_conflict \
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
# Quote identifiers properly for inclusion in queries.
my $relation = 'public.employee'; # index schema
my $index    = 'sub_email_key';   # index name
my $pk_col   = 'id';              # index table primary key column
my $time_col = 'updated_at';      # last update time column

# If there are any tables with FK constraints pointing to records to be
# deleted, list them here and the script will delete them, first. List in
# the order to be deleted, account for additional foreign key constraints
# if necessary. Format as arrays: [$schema, $table, $fk_column].
my @cascade  = (
    [qw(public.supplies employee_id)],
);

# Optionaly set $copy_to to table with identical columns to $table to store
# away deleted records for later evaluation or recovery.
my $copy_to  = 'employee_conflict';
##############################################################################
# End of Configuration
##############################################################################

my $info = shift;
return if "$info->{schemaname}.$info->{tablename}" ne $relation;

# Do nothing unless it's a unique violation for the specified index.
return if $info->{error_string} !~ /violates unique constraint "\Q$index\E"/;

# Grab all the primary keys involved in the sync.
my %pks = map { $_ => 1 } map { keys %{ $_ } } values %{ $info->{deltabin} };

# Very unlikely to happen, but check anyway.
unless (%pks) {
    $info->{warning} = "Conflict detected on $relation but no records found!";
    return;
}

# Grab one of the database handles.
my $dbhs = $info->{dbh};
my ($dbh) = values %{ $dbhs };
unless ($dbh) {
    $info->{warning} = "No database handles found when trying to resolve $relation conflict. Did you specify `getdbh=1` when adding the custom code?";
    return;
}

# Retrieve the index expression and predicate.
my ($expr, $pred) = $dbh->selectrow_array(q{
    SELECT string_agg(expr, ', '), pred FROM (
        SELECT pg_catalog.pg_get_indexdef( ci.oid, s.i + 1, false) AS expr,
               pg_catalog.pg_get_expr(x.indpred, ct.oid) AS pred
          FROM pg_catalog.pg_index x
          JOIN pg_catalog.pg_class ct    ON ct.oid = x.indrelid
          JOIN pg_catalog.pg_class ci    ON ci.oid = x.indexrelid
          JOIN generate_series(0, current_setting('max_index_keys')::int - 1) s(i)
            ON x.indkey[s.i] IS NOT NULL
         WHERE ct.oid     = ?::regclass
           AND ci.relname = ?
           AND x.indisunique
           AND NOT x.indisprimary
         ORDER BY s.i
    ) AS tab
    GROUP BY pred
}, undef, undef, $relation, $index);

unless ($expr) {
    # Should not happen, but just to be safe.
    $info->{warning} = "Conflict detected on $relation but index $index not found!";
    return;
}

# Assemble the query for the PKs, unique expression value as a JSON array, and
# update time.
my $query = qq{
    SELECT $pk_col                               AS pkey,
           json_build_array($expr)               AS ukey,
           extract(epoch FROM $time_col)         AS utime,
           ?::TEXT                               AS db
      FROM $relation
     WHERE $pk_col = ANY(?)
};
$query .= "       AND $pred" if $pred;

# We'll want one instance of each unique key.
my %rec_for;

# Always work the databases in the same order.
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
            "INSERT INTO $copy_to SELECT * FROM $relation WHERE id = ?",
            $lose->{pkey},
        ) if $copy_to;

        # Cascade delete the older record.
        $dbhs->{ $lose->{db} }->do(
            "DELETE FROM $_->[0] WHERE $_->[1] = ?",
            $lose->{pkey},
        ) for @cascade, [$relation, $pk_col];

        # Log the resolution.
        $info->{message} .= qq{Unique conflict on $relation($expr) for value \`$keep->{ukey}\` resolved by deleting $pk_col \`$lose->{pkey}\` from database $lose->{db} and keeping $pk_col \`$keep->{pkey}\` from database $keep->{db}};

        # Note: Don't commit, Bucard handles transactions.
    }
}

# Retry now that things are cleaned up!
$info->{retry} = 1;

return;
