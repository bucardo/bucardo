#!perl

## Test pushdelta functionality

use 5.008003;
use strict;
use warnings;
use Data::Dumper;
use lib 't','.';
use DBD::Pg;
use Test::More tests => 194;

use BucardoTesting;
my $bct = BucardoTesting->new() or BAIL_OUT "Creation of BucardoTesting object failed\n";
$location = 'pushdelta';

use vars qw/$SQL $sth $t $i $result $count %sql %val %pkey/;

pass("*** Beginning 'pushdelta' tests");

## Prepare a clean Bucardo database on A
my $dbhA = $bct->blank_database('A');
my $dbhX = $bct->setup_bucardo(A => $dbhA);

## Server A is the master, the rest are slaves
my $dbhB = $bct->blank_database('B');
my $dbhC = $bct->blank_database('C');

## Tell Bucardo about these databases
$bct->add_test_databases('A B C');

## Create a herd for 'A' and add all test tables to it
$bct->add_test_tables_to_herd('A', 'testherd1');

## Create a new sync to pushdelta from A to B
$t=q{Add sync works};
$i = $bct->ctl("add sync pushdeltatest source=testherd1 type=pushdelta targetdb=B");
like($i, qr{Added sync}, $t);

$dbhX->do('LISTEN bucardo_syncdone_pushdeltatest');
$dbhX->commit();

$bct->restart_bucardo($dbhX);

sub test_empty_drop {
    my ($table, $dbh) = @_;
    my $DROPSQL = 'SELECT * FROM droptest';
    my $line = (caller)[2];
    $t=qq{ Triggers and rules did NOT fire on remote table $table};
    $result = [];
    bc_deeply($result, $dbhB, $DROPSQL, $t, $line);
}

## Test unique index violation problems
## Test a deletion
for my $table (sort keys %tabletype) {
    my $pkeyname = $table =~ /test5/ ? q{"id space"} : 'id';
    my $type = $tabletype{$table};
    my $val1 = $val{$type}{1};
    my $val2 = $val{$type}{2};
    my $val3 = $val{$type}{3};
    $SQL = "INSERT INTO $table($pkeyname, inty, data1, email) VALUES (?,?,?,?)";
    $sth = $dbhA->prepare($SQL);
    $sth->execute($val1, 1, 1, 'moe');
    $sth->execute($val2, 2, 2, 'larry');
    $sth->execute($val3, 3, 3, 'curly');
}
$dbhA->commit();

$bct->ctl("kick pushdeltatest 0");
wait_for_notice($dbhX, 'bucardo_syncdone_pushdeltatest', 5);

$SQL = 'SELECT * FROM bucardo_test1';
my $info = $dbhB->selectall_arrayref($SQL);

## Switch things up to try and trick the unique index
SQL);
$sth->execute('larrytemp', 2);
$sth->execute('larry', 1);
$sth->execute('moe', 3);
$sth->execute('curly', 2);
$dbhA->commit();

$bct->ctl("kick pushdeltatest 0");
wait_for_notice($dbhX, 'bucardo_syncdone_pushdeltatest', 5);

## We want 1 2 3 to be larry, curly, moe
$SQL = 'SELECT id, email FROM bucardo_test1 ORDER BY id';
$t='Pushdelta handled a unique index without any problems';
$result = [[1,'larry'],[2,'curly'],[3,'moe']];
bc_deeply($result, $dbhB, $SQL, $t);

## Sequence testing

$dbhA->do("SELECT setval('bucardo_test_seq1', 123)");
$dbhA->commit();

$bct->ctl("kick pushdeltatest 0");
wait_for_notice($dbhX, 'bucardo_syncdone_pushdeltatest', 5);

$SQL = q{SELECT nextval('bucardo_test_seq1')};
$t='Pushdelta replicated a sequence properly';
$result = [[123+1]];
bc_deeply($result, $dbhB, $SQL, $t);

$dbhA->do("SELECT setval('bucardo_test_seq1', 223, false)");
$dbhA->commit();

$bct->ctl("kick pushdeltatest 0");
wait_for_notice($dbhX, 'bucardo_syncdone_pushdeltatest', 5);

$SQL = q{SELECT nextval('bucardo_test_seq1')};
$t='Pushdelta replicated a sequence properly with a false setval';
$result = [[223]];
bc_deeply($result, $dbhB, $SQL, $t);

$dbhA->do("SELECT setval('bucardo_test_seq1', 345, true)");
$dbhA->commit();

$bct->ctl("kick pushdeltatest 0");
wait_for_notice($dbhX, 'bucardo_syncdone_pushdeltatest', 5);

$SQL = q{SELECT nextval('bucardo_test_seq1')};
$t='Pushdelta replicated a sequence properly with a true setval';
$result = [[345+1]];
bc_deeply($result, $dbhB, $SQL, $t);

## Reset the tables
for my $table (sort keys %tabletype) {
    $dbhA->do("DELETE FROM $table");
}
$dbhA->do('DELETE FROM droptest');
$dbhA->commit();
$bct->ctl("kick pushdeltatest 0");
wait_for_notice($dbhX, 'bucardo_syncdone_pushdeltatest', 5);
$dbhB->do('DELETE FROM droptest');
$dbhB->commit();

## Prepare some insert statement handles, add a row to source database
for my $table (sort keys %tabletype) {

    my $type = $tabletype{$table};
    my $val1 = $val{$type}{1};
    my $val2 = $val{$type}{2};
    if (!defined $val1 or !defined $val2) {
        BAIL_OUT "Could not determine value for $table $type\n";
    }

    $pkey{$table} = $table =~ /test5/ ? q{"id space"} : 'id';

    $SQL = $table =~ /0/
        ? "INSERT INTO $table($pkey{$table}) VALUES (?)"
            : "INSERT INTO $table($pkey{$table},data1,inty) VALUES (?,'one',1)";
    $sql{insert}{$table} = $dbhA->prepare($SQL);
    if ($type eq 'BYTEA') {
        $sql{insert}{$table}->bind_param(1, undef, {pg_type => PG_BYTEA});
    }
    $val{$table} = $val1;

    $sql{insert}{$table}->execute($val{$table});

    ## Save for later
    $val{"2.$table"} = $val2;

}
$dbhA->commit();

## Verify triggers and rules on source database still fire
for my $table (sort keys %tabletype) {

    $t=q{ After insert, trigger and rule both populate droptest table };
    my $qtable = $dbhX->quote($table);
    my $LOCALDROPSQL = $table =~ /0/
        ? "SELECT type,0 FROM droptest WHERE name = $qtable ORDER BY 1,2"
            : "SELECT type,inty FROM droptest WHERE name = $qtable ORDER BY 1,2";
    my $tval = $table =~ /0/ ? 0 : 1;
    $result = [['rule',$tval],['trigger',$tval]];
    bc_deeply($result, $dbhA, $LOCALDROPSQL, $t);

    test_empty_drop($table,$dbhB);
}

## Make sure second database is still empty
for my $table (sort keys %tabletype) {
    $t=qq{ Second table $table still empty before kick };
    $sql{select}{$table} = "SELECT inty FROM $table ORDER BY $pkey{$table}";
    $table =~ /0/ and $sql{select}{$table} =~ s/inty/$pkey{$table}/;
    $result = [];
    bc_deeply($result, $dbhB, $sql{select}{$table}, $t);
}

## Kick the source database, replicate one row in each table
$bct->ctl("kick pushdeltatest 0");
wait_for_notice($dbhX, 'bucardo_syncdone_pushdeltatest', 5);
wait_for_notice($dbhX, 'bucardo_syncdone_pushdeltatest', 5);

## Make sure second database has the new rows, and that triggers and rules did not fire
for my $table (sort keys %tabletype) {
    $t=qq{ Second table $table got the pushdelta row};
    $result = [[1]];
    bc_deeply($result, $dbhB, $sql{select}{$table}, $t);

    test_empty_drop($table,$dbhB);
}
$bct->ctl("kick pushdeltatest 5");
wait_for_notice($dbhX, 'bucardo_syncdone_pushdeltatest', 5);

## Adding a new row should cause the sync to fire without waiting for a kick
for my $table (sort keys %tabletype) {
    ## Clear out any notices
    $dbhX->func('pg_notifies');
    $dbhX->commit();

    $dbhA->do("UPDATE $table SET inty = 2");
    $dbhA->commit();
    ## Hack.
    sleep 2;
    wait_for_notice($dbhX, 'bucardo_syncdone_pushdeltatest', 5);

    $t=qq{ Second table $table got the pushdelta row};
    $result = [[2]];
    bc_deeply($result, $dbhB, $sql{select}{$table}, $t);

    test_empty_drop($table,$dbhB);
}

## Add a new target database
$t=q{Add dbgroup works};
$i = $bct->ctl("add dbgroup tgroup B C");
like($i, qr{Added database group}, $t);

$t=q{Update sync works};
$dbhB->commit();
$i = $bct->ctl("update sync pushdeltatest targetgroup=tgroup");
like($i, qr{targetgroup : }, $t);

## Turn off the ping
$SQL = "UPDATE sync SET ping = FALSE";
$dbhX->do($SQL);

## Reload the sync
$dbhX->do("NOTIFY bucardo_reload_sync_pushdeltatest");
$dbhX->do('LISTEN bucardo_reloaded_sync_pushdeltatest');
$dbhX->commit();

for my $table (sort keys %tabletype) {
    $dbhA->do("UPDATE $table SET inty = 3");
}
$dbhA->commit();

for my $table (sort keys %tabletype) {
    $t=qq{ Second table $table did not change rows, not pinging};
    $result = [[2]];
    bc_deeply($result, $dbhB, $sql{select}{$table}, $t);
    $result = [];
    bc_deeply($result, $dbhC, $sql{select}{$table}, $t);

    test_empty_drop($table,$dbhB);
    test_empty_drop($table,$dbhC);
}

wait_for_notice($dbhX, 'bucardo_reloaded_sync_pushdeltatest', 10);

## Kick the source database, replicate one row in each table
$bct->ctl("kick pushdeltatest 5");
wait_for_notice($dbhX, 'bucardo_syncdone_pushdeltatest', 0);

for my $table (sort keys %tabletype) {
    $t=qq{ Second table $table did not change rows, not pinging};
    $result = [[3]];
    bc_deeply($result, $dbhB, $sql{select}{$table}, $t);
    bc_deeply($result, $dbhC, $sql{select}{$table}, $t);

    test_empty_drop($table,$dbhB);
    test_empty_drop($table,$dbhC);
}

## Make sure local changes stick
for my $table (sort keys %tabletype) {
    $dbhB->do("UPDATE $table SET inty = 4");
}
$dbhB->do('DELETE FROM droptest');
$dbhB->commit();

for my $table (sort keys %tabletype) {
    $sql{insert}{$table}->execute($val{"2.$table"});
}
$dbhA->commit();

$bct->ctl("kick pushdeltatest 0");
wait_for_notice($dbhX, 'bucardo_syncdone_pushdeltatest', 5);

for my $table (sort keys %tabletype) {
    $t=qq{ Second table $table did not get overwritten by pushdelta};
    $result = [[4],[1]];
    bc_deeply($result, $dbhB, $sql{select}{$table}, $t);
    $result = [[3],[1]];
    bc_deeply($result, $dbhC, $sql{select}{$table}, $t);

    test_empty_drop($table,$dbhB);
    test_empty_drop($table,$dbhC);
}

## Test a deletion
for my $table (sort keys %tabletype) {
    $dbhA->do("DELETE FROM $table");
}
$dbhA->commit();

$bct->ctl("kick pushdeltatest 0");
wait_for_notice($dbhX, 'bucardo_syncdone_pushdeltatest', 5);

for my $table (sort keys %tabletype) {
    $t=qq{ Second table $table got the delete};
    $result = [];
    bc_deeply($result, $dbhB, $sql{select}{$table}, $t);
    $result = [];
    bc_deeply($result, $dbhC, $sql{select}{$table}, $t);

    test_empty_drop($table,$dbhB);
    test_empty_drop($table,$dbhC);
}


exit;

END {
    $bct->stop_bucardo($dbhX);
    $dbhX->disconnect();
    $dbhA->disconnect();
    $dbhB->disconnect();
    $dbhC->disconnect();
}
