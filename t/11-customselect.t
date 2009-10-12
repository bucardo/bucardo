#!perl

## Test customselect functionality

use 5.008003;
use strict;
use warnings;
use Data::Dumper;
use lib 't','.';
use DBD::Pg;
use Test::More tests => 9;

use BucardoTesting;
my $bct = BucardoTesting->new() or BAIL_OUT "Creation of BucardoTesting object failed\n";
my $i;

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
	for my $t (qw/customselect csmulti/) {
		if (BucardoTesting::table_exists($dbh, $t)) {
			$dbh->do("DROP TABLE $t");
		}
    }
    $dbh->do(q{
        CREATE TABLE customselect (
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

$i = $bct->ctl('add sync customselectsync source=A type=fullcopy targetdb=B usecustomselect=true tables=customselect');
like($i, qr{Added sync}, 'Added customselect sync');
$dbhX->do(q{update goat set customselect = $$select id, 'aaa'::text as field1, field2, field3 from customselect$$ where tablename = 'customselect'});
$i = $bct->ctl('add sync csmulti source=A type=fullcopy targetdb=B usecustomselect=true tables=csmulti');
like($i, qr{Added sync}, 'Added multi-column primary key customselect sync');
$dbhX->do(q{update goat set customselect = $$select id1, id2, 'aaa'::text as field1, field2, field3 from csmulti$$ where tablename = 'csmulti'});
$dbhX->do(q{LISTEN bucardo_syncdone_customselectsync});
$dbhX->do(q{LISTEN bucardo_syncdone_csmulti});
$dbhX->commit();

# Test that sync works
$dbhA->do(q{INSERT INTO customselect (id, field1, field2, field3) VALUES (1, 'alpha',    'bravo',  'charlie')});
$dbhA->do(q{INSERT INTO customselect (id, field1, field2, field3) VALUES (2, 'delta',    'echo',   'foxtrot')});
$dbhA->do(q{INSERT INTO customselect (id, field1, field2, field3) VALUES (3, 'hotel',    'india',  'juliet')});
$dbhA->do(q{INSERT INTO customselect (id, field1, field2, field3) VALUES (4, 'kilo',     'lima',   'mike')});
$dbhA->do(q{INSERT INTO customselect (id, field1, field2, field3) VALUES (5, 'november', 'oscar',  'papa')});
$dbhA->do(q{INSERT INTO customselect (id, field1, field2, field3) VALUES (6, 'romeo',    'sierra', 'tango')});
$dbhA->do(q{INSERT INTO customselect (id, field1, field2, field3) VALUES (7, 'uniform',  'victor', 'whiskey')});
$dbhA->do(q{INSERT INTO customselect (id, field1, field2, field3) VALUES (8, 'xray',     'yankee', 'zulu')});
$dbhA->commit();

$bct->restart_bucardo($dbhX);

$bct->ctl('kick customselectsync 0');
wait_for_notice($dbhX, 'bucardo_syncdone_customselectsync', 5);

my $aa = $dbhA->selectall_arrayref(q{SELECT id, 'aaa', field2, field3 FROM customselect ORDER BY id});
my $bb = $dbhB->selectall_arrayref('SELECT * FROM customselect ORDER BY id');
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

$bct->ctl('kick csmulti 0');
wait_for_notice($dbhX, 'bucardo_syncdone_csmulti', 5);

$aa = $dbhA->selectall_arrayref(q{SELECT id1, id2, 'aaa', field2, field3 FROM csmulti ORDER BY id1, id2});
$bb = $dbhB->selectall_arrayref('SELECT * FROM csmulti ORDER BY id1, id2');
is_deeply($aa, $bb, 'Swap works on multi-column primary key');

exit;

END {
	$bct->stop_bucardo($dbhX);
	$dbhX->disconnect();
	$dbhA->disconnect();
	$dbhB->disconnect();
}
