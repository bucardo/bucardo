#!perl

## Simple tests to allow for quick testing of various things

use 5.008003;
use strict;
use warnings;
use Data::Dumper;
use lib 't','.';
use DBD::Pg;
use Test::More 'no_plan';
use BucardoTesting;

my $bct = BucardoTesting->new() or BAIL_OUT "Creation of BucardoTesting object failed\n";

pass("*** Beginning 'add table' tests");

## Prepare a clean Bucardo database on A (master) and B (slave)
my $dbhA = $bct->blank_database('A');
my $dbhX = $bct->setup_bucardo(A => $dbhA);
my $dbhB = $bct->blank_database('B');

## Tell Bucardo about these databases
$bct->add_test_databases('A B');

## Create a herd for 'A' and add all test tables to it
$bct->add_test_tables_to_herd('A', 'testherd1');

## Create a new sync to pushdelta from A to B
my $t=q{Add sync works};
my $i = $bct->ctl("add sync simpletest2 source=testherd1 type=pushdelta targetdb=B");
like($i, qr{Added sync}, $t);

$bct->restart_bucardo($dbhX);
$dbhX->do('LISTEN bucardo_syncdone_simpletest2');
$dbhX->do('LISTEN bucardo_syncerror_simpletest2');
$dbhX->commit();

## Add a row to a table, make sure it gets pushed
$dbhA->do("INSERT INTO bucardo_test1(id,inty) VALUES (12,34)");
$dbhA->commit();

$bct->ctl("kick simpletest2 0");
wait_for_notice($dbhX, 'bucardo_syncdone_simpletest2', 5);

my $SQL = 'SELECT id,inty FROM bucardo_test1';
my $result = $dbhB->selectall_arrayref($SQL);
is_deeply($result, [[12,34]], $t);

$bct->stop_bucardo($dbhX);

for my $db ($dbhA, $dbhB) {
    $db->do('CREATE TABLE addtable (id INTEGER PRIMARY KEY, data integer)');
    $db->commit();
}

$i = $bct->ctl("add table addtable db=A standard_conflict=source herd=testherd1");
like($i, qr{Table added:}, 'Table added successfully');

## This line adds the triggers
$bct->ctl('validate sync simpletest2');

$dbhA->do('INSERT INTO addtable VALUES (1, 10)');
$dbhA->commit();

$bct->restart_bucardo($dbhX);
$bct->ctl("kick simpletest2 0");
wait_for_notice($dbhX, 'bucardo_syncdone_simpletest2', 5);

$SQL = 'SELECT id,data FROM addtable';
$result = $dbhB->selectall_arrayref($SQL);
my $result2 = $dbhA->selectall_arrayref($SQL);
is_deeply($result, $result2, 'Newly-added table is correctly replicated');

exit;

END {
	$bct->stop_bucardo($dbhX);
	$dbhX->disconnect();
	$dbhA->disconnect();
	$dbhB->disconnect();
}
