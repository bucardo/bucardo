#! perl

## Sample exception handler
## For this example, we will fix unique violations on an email column

use strict;
use warnings;
use Data::Dumper;

my $info = shift;

## Do nothing unless this is the exact error we were designed to handle
return if $info->{error_string} !~ /violates unique constraint "employee_email_key"/o;

## Grab all the primary keys involved in the sync
my %pk;
for my $dbname ( keys %{ $info->{deltabin} }) {
    for my $pkey (keys %{ $info->{deltabin}{$dbname} }) {
        $pk{$pkey}++;
    }
}

## Very unlikely to happen, but we will check anyway:
if (! keys %pk) {
    $info->{warning} = 'No database records found!';
    return;
}

## We need to get information from every database on each involved record
my $SQL = sprintf 'SELECT id,email FROM employee WHERE id IN (%s)',
    (join ',' => sort keys %pk);

## Emails must be unique, so each must be associated with only one primary key (id)
my %emailpk;

## This is in the preferred order of databases
## Thus, any "conflicts" means A > B > C
for my $db (qw/ A B C /) {
    my $dbh = $info->{dbh}{$db};
    my $sth = $dbh->prepare($SQL);
    $sth->execute();
    my $rows = $sth->fetchall_arrayref();
    for my $row (@$rows) {
        my ($id,$email) = @$row;

        ## This a new email? All is good, just move on
        if (! exists $emailpk{$email}) {
            $emailpk{$email} = [$id, $db];
            next;
        }

        ## This email already exists. If the same PK, no problem
        my ($oldid,$olddb) = @{ $emailpk{$email} };
        if ($oldid == $id) {
            next;
        }

        ## We have the same email with different PKs! Time to get busy
        $info->{message} .= "Found problem with email $email. ";
        $info->{message} .= "Exists as PK $oldid on db $olddb, but as PK $id on $db!";

        ## Store it away in a separate table
        my $SQL = 'INSERT INTO employee_conflict SELECT * FROM employee WHERE id = ?';
        $sth = $dbh->prepare($SQL);
        $sth->execute($id);

        ## Now delete it from this database!
        $SQL = 'DELETE FROM employee WHERE id = ?';
        $sth = $dbh->prepare($SQL);
        $sth->execute($id);

        ## Note: we do not want to commit (and it is disallowed by DBIx::Safe)
    }
}

## Let's retry now that things are cleaned up!
$info->{retry} = 1;

return;
