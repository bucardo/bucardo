#!perl

## Test customselect functionality

use 5.008003;
use strict;
use warnings;
use Data::Dumper;
use lib 't','.';
use DBD::Pg;
use Test::More tests => 12;

use BucardoTesting;
my $bct = BucardoTesting->new() or BAIL_OUT "Creation of BucardoTesting object failed\n";
$location = 'customselect';

pass("*** Beginning customselect tests");

## Prepare a clean Bucardo database on A
my $dbhA = $bct->blank_database('A');
my $dbhX = $bct->setup_bucardo(A => $dbhA);

## Server A is the master, the rest are slaves
my $dbhB = $bct->blank_database('B');

## Tell Bucardo about these databases
$bct->add_test_databases('A B');

## Create a herd for 'A' and add all test tables to it
$bct->add_test_tables_to_herd('A', 'testherd1');

## Create tables for this test
for my $dbh (($dbhA, $dbhB)) {
    for my $t (qw/csone csmulti/) {
        if (BucardoTesting::table_exists($dbh, $t)) {
            $dbh->do("DROP TABLE $t");
        }
    }
    $dbh->do(q{
        CREATE TABLE csone (
            id INTEGER PRIMARY KEY,
            field1 TEXT,
            field2 TEXT,
            field3 TEXT
        )});
    $dbh->do(q{
        CREATE TABLE csmulti (
            id1 INTEGER,
            id2 INTEGER,
            field1 TEXT,
            field2 TEXT,
            field3 TEXT,
            PRIMARY KEY (id1, id2)
        )});
    $dbh->commit;
}

my $i = $bct->ctl('add sync cs1 source=A type=fullcopy targetdb=B usecustomselect=true tables=csone');
like($i, qr{Added sync}, 'Added cs1 sync');
$dbhX->do(q{UPDATE goat SET customselect = $$select id, 'aaa'::text as field1, field2, field3 from csone$$ WHERE tablename = 'csone'});
$i = $bct->ctl('add sync cs2 source=A type=fullcopy targetdb=B usecustomselect=true tables=csmulti');
like($i, qr{Added sync}, 'Added cs2 sync');
$dbhX->do(q{UPDATE goat SET customselect = $$select id1, id2, 'aaa'::text as field1, field2, field3 from csmulti$$ WHERE tablename = 'csmulti'});
$dbhX->do(q{LISTEN bucardo_syncdone_cs1});
$dbhX->do(q{LISTEN bucardo_syncdone_cs2});
$dbhX->commit();

# Test that sync works
$dbhA->do(q{INSERT INTO csone (id, field1, field2, field3) VALUES (1, 'alpha',    'bravo',  'charlie')});
$dbhA->do(q{INSERT INTO csone (id, field1, field2, field3) VALUES (2, 'delta',    'echo',   'foxtrot')});
$dbhA->do(q{INSERT INTO csone (id, field1, field2, field3) VALUES (3, 'hotel',    'india',  'juliet')});
$dbhA->do(q{INSERT INTO csone (id, field1, field2, field3) VALUES (4, 'kilo',     'lima',   'mike')});
$dbhA->do(q{INSERT INTO csone (id, field1, field2, field3) VALUES (5, 'november', 'oscar',  'papa')});
$dbhA->do(q{INSERT INTO csone (id, field1, field2, field3) VALUES (6, 'romeo',    'sierra', 'tango')});
$dbhA->do(q{INSERT INTO csone (id, field1, field2, field3) VALUES (7, 'uniform',  'victor', 'whiskey')});
$dbhA->do(q{INSERT INTO csone (id, field1, field2, field3) VALUES (8, 'xray',     'yankee', 'zulu')});
$dbhA->commit();

$bct->restart_bucardo($dbhX);

$bct->ctl('kick cs1 0');
wait_for_notice($dbhX, 'bucardo_syncdone_cs1', 5);

my $aa = $dbhA->selectall_arrayref(q{SELECT id, 'aaa', field2, field3 FROM csone ORDER BY id});
my $bb = $dbhB->selectall_arrayref('SELECT * FROM csone ORDER BY id');
is_deeply($aa, $bb, 'Swap works on single-column primary key');

# Test that sync works
$dbhA->do(q{INSERT INTO csmulti (id1, id2, field1, field2, field3) VALUES (1, 9, 'alpha',    'bravo',  'charlie')});
$dbhA->do(q{INSERT INTO csmulti (id1, id2, field1, field2, field3) VALUES (2, 9, 'delta',    'echo',   'foxtrot')});
$dbhA->do(q{INSERT INTO csmulti (id1, id2, field1, field2, field3) VALUES (3, 9, 'hotel',    'india',  'juliet')});
$dbhA->do(q{INSERT INTO csmulti (id1, id2, field1, field2, field3) VALUES (4, 9, 'kilo',     'lima',   'mike')});
$dbhA->do(q{INSERT INTO csmulti (id1, id2, field1, field2, field3) VALUES (5, 9, 'november', 'oscar',  'papa')});
$dbhA->do(q{INSERT INTO csmulti (id1, id2, field1, field2, field3) VALUES (6, 9, 'romeo',    'sierra', 'tango')});
$dbhA->do(q{INSERT INTO csmulti (id1, id2, field1, field2, field3) VALUES (7, 9, 'uniform',  'victor', 'whiskey')});
$dbhA->do(q{INSERT INTO csmulti (id1, id2, field1, field2, field3) VALUES (8, 9, 'xray',     'yankee', 'zulu')});
$dbhA->commit();

$bct->restart_bucardo($dbhX);

$bct->ctl('kick cs2 0');
wait_for_notice($dbhX, 'bucardo_syncdone_cs2', 5);

$aa = $dbhA->selectall_arrayref(q{SELECT id1, id2, 'aaa', field2, field3 FROM csmulti ORDER BY id1, id2});
$bb = $dbhB->selectall_arrayref('SELECT * FROM csmulti ORDER BY id1, id2');
is_deeply($aa, $bb, 'Swap works on multi-column primary key');


## Test case where target table does not match the source

$dbhB->do(q{TRUNCATE TABLE csone});
$dbhA->do(q{DELETE FROM csone WHERE id <> 2});
$dbhA->commit;
for my $x (1..3) {
    $dbhB->do(qq{ALTER TABLE csone DROP COLUMN field$x});
}
$dbhB->do(q{ALTER TABLE csone ADD COLUMN f1 DATE});
$dbhB->do(q{ALTER TABLE csone ADD COLUMN f2 BIGINT});
$dbhB->commit();

$dbhX->do(q{UPDATE goat SET customselect = $$select id, '2008-01-01'::date as f1, 9999::bigint as f2 from csone$$ where tablename = 'csone'});
$dbhX->commit();

$bct->restart_bucardo($dbhX);

$bct->ctl('kick cs1 0');
wait_for_notice($dbhX, 'bucardo_syncdone_cs1', 5);

$aa = [[2,'2008-01-01',9999]];
$bb = $dbhB->selectall_arrayref('SELECT * FROM csone ORDER BY id');
is_deeply($aa, $bb, 'customselect works for target with different columns');

exit;

END {
    $bct->stop_bucardo($dbhX);
    $dbhX->disconnect();
    $dbhA->disconnect();
    $dbhB->disconnect();
}
