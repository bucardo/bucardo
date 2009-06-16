#!perl

# Test multi-column primary keys

use 5.008003;
use strict;
use warnings;
use Data::Dumper;
use lib 't','.';
use DBD::Pg;
use Test::More 'no_plan';

no warnings 'redefine';
use BucardoTesting;
use warnings;

my $bct = BucardoTesting->new() or BAIL_OUT "Creation of BucardoTesting object failed\n";

pass(q{*** Beginning 'multicol pk' tests});

## Prepare a clean Bucardo database on A
my $dbhA = $bct->blank_database('A');
my $dbhX = $bct->setup_bucardo(A => $dbhA);

## Server A is the master, the rest are slaves
my $dbhB = $bct->blank_database('B');

my $res = $dbhB->selectall_arrayref('SELECT * FROM bucardo_test_multicol');
is($#$res, -1, 'Slave test table is empty');
$dbhB->rollback();

## Tell Bucardo about these databases
$bct->add_test_databases('A B');

## Create a herd for 'A' and add all test tables to it
$bct->add_test_tables_to_herd('A', 'testherd1');

## Pushdelta sync from A to B
my $t=q{Add sync works};
print "Adding sync\n";
my $i = $bct->ctl("add sync multicolpushdeltatest source=testherd1 type=pushdelta targetdb=B");
print "Added sync: $i\n";
like($i, qr{Sync added:}, $t);

$bct->restart_bucardo($dbhX);

# Insert 5 rows and see if they replicate
$dbhA->do(q{INSERT INTO bucardo_test_multicol (id, id2, id3, data) VALUES (1, 1, 1, 'test')});
$dbhA->do(q{INSERT INTO bucardo_test_multicol (id, id2, id3, data) VALUES (1, 2, 1, 'test')});
$dbhA->do(q{INSERT INTO bucardo_test_multicol (id, id2, id3, data) VALUES (1, 3, 1, 'test')});
$dbhA->do(q{INSERT INTO bucardo_test_multicol (id, id2, id3, data) VALUES (1, 4, 1, 'test')});
$dbhA->do(q{INSERT INTO bucardo_test_multicol (id, id2, id3, data) VALUES (1, 5, 1, 'test')});
$dbhA->commit();

$dbhX->do(q{LISTEN bucardo_syncdone_multicolpushdeltatest});
$dbhX->commit();
$bct->ctl('kick multicolpushdeltatest 5');
eval {
    wait_for_notice($dbhX, 'bucardo_syncdone_multicolpushdeltatest', 5);
};
ok(! $@, 'Sync pushed');

$res = $dbhB->selectall_arrayref('SELECT id, id2, id3, data FROM bucardo_test_multicol');
is($#$res, 4, 'Slave table contains 5 rows');
$dbhB->rollback();

# Update a row and see if it replicates
$dbhA->do(q{UPDATE bucardo_test_multicol SET data = 'test2' WHERE id2 = 2});
$dbhA->commit();

$bct->ctl('kick multicolpushdeltatest 5');
eval {
    wait_for_notice($dbhX, 'bucardo_syncdone_multicolpushdeltatest', 5);
};
ok(! $@, 'Sync pushed');
$res = $dbhB->selectall_arrayref(q{SELECT * FROM bucardo_test_multicol WHERE id2 = 2 AND data = 'test'});
is($#$res, -1, 'Row successfully updated');
$dbhB->rollback();

# Remove a row and see if it replicates
$dbhA->do(q{DELETE FROM bucardo_test_multicol WHERE id2 = 1});
$dbhA->commit();

$bct->ctl('kick multicolpushdeltatest 5');
eval {
    wait_for_notice($dbhX, 'bucardo_syncdone_multicolpushdeltatest', 5);
};
ok(! $@, 'Sync pushed');
$res = $dbhB->selectall_arrayref('SELECT * FROM bucardo_test_multicol WHERE id2 = 1');
is($#$res, -1, 'Row successfully deleted');
$dbhB->rollback();

END {
    $bct->stop_bucardo($dbhX);
    $dbhX->disconnect();
    $dbhA->disconnect();
    $dbhB->disconnect();
}
