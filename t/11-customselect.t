#!perl

## Test customselect functionality

use 5.008003;
use strict;
use warnings;
use Data::Dumper;
use lib 't','.';
use DBD::Pg;
use Test::More tests => 10;

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
	$dbh->do('DROP TABLE IF EXISTS customselect');
	$dbh->do('DROP TABLE IF EXISTS csmulti');
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

$bct->ctl('add herd herd1');
$bct->ctl('add table customselect db=A herd=herd1');
$i = $bct->ctl('add sync customselectsync source=herd1 type=fullcopy targetdb=B usecustomselect=true');
like($i, qr{Sync added:}, 'Added customselect sync');
$dbhX->do(q{update goat set customselect = $$select id, 'aaa' as field1, field2, field3 from customselect$$ where tablename = 'customselect'});
$bct->ctl('add herd herd2');
$bct->ctl('add table csmulti db=A herd=herd2');
$dbhX->do(q{update goat set customselect = $$select id1, id2, 'aaa' as field1, field2, field3 from csmulti$$ where tablename = 'csmulti'});
$i = $bct->ctl('add sync csmulti source=herd2 type=fullcopy targetdb=B usecustomselect=true');
like($i, qr{Sync added:}, 'Added multi-column primary key customselect sync');
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
$dbhX->do(q{LISTEN bucardo_syncdone_customselectsync});
$dbhA->commit();
$dbhX->commit();
$bct->restart_bucardo($dbhX);
$bct->ctl('kick customselectsync 0');
wait_for_notice($dbhX, 'bucardo_syncdone_customselectsync', 5);
my $a = $dbhA->selectall_arrayref(q{SELECT id, 'aaa', field2, field3 FROM customselect ORDER BY id});
my $b = $dbhB->selectall_arrayref('SELECT * FROM customselect ORDER BY id');
is_deeply($a, $b, 'Swap works on single-column primary key');
$dbhA->rollback();
$dbhB->rollback();
$bct->stop_bucardo($dbhX);

# Test that sync works
$dbhA->do(q{INSERT INTO csmulti (id1, id2, field1, field2, field3) VALUES (1, 9, 'alpha',    'bravo',  'charlie')});
$dbhA->do(q{INSERT INTO csmulti (id1, id2, field1, field2, field3) VALUES (2, 9, 'delta',    'echo',   'foxtrot')});
$dbhA->do(q{INSERT INTO csmulti (id1, id2, field1, field2, field3) VALUES (3, 9, 'hotel',    'india',  'juliet')});
$dbhA->do(q{INSERT INTO csmulti (id1, id2, field1, field2, field3) VALUES (4, 9, 'kilo',     'lima',   'mike')});
$dbhA->do(q{INSERT INTO csmulti (id1, id2, field1, field2, field3) VALUES (5, 9, 'november', 'oscar',  'papa')});
$dbhA->do(q{INSERT INTO csmulti (id1, id2, field1, field2, field3) VALUES (6, 9, 'romeo',    'sierra', 'tango')});
$dbhA->do(q{INSERT INTO csmulti (id1, id2, field1, field2, field3) VALUES (7, 9, 'uniform',  'victor', 'whiskey')});
$dbhA->do(q{INSERT INTO csmulti (id1, id2, field1, field2, field3) VALUES (8, 9, 'xray',     'yankee', 'zulu')});
$dbhX->do(q{LISTEN bucardo_syncdone_csmulti});
$dbhA->commit();
$dbhX->commit();
pass('Inserted data into master');
$bct->restart_bucardo($dbhX);

$bct->ctl('kick csmulti 0');
wait_for_notice($dbhX, 'bucardo_syncdone_csmulti', 5);
$a = $dbhA->selectall_arrayref(q{SELECT id1, id2, 'aaa', field2, field3 FROM csmulti ORDER BY id1, id2});
$b = $dbhB->selectall_arrayref('SELECT * FROM csmulti ORDER BY id1, id2');
is_deeply($a, $b, 'Swap works on multi-column primary key');
$dbhA->rollback();
$dbhB->rollback();
$bct->stop_bucardo($dbhX);

exit;

END {
	$bct->stop_bucardo($dbhX);
	$dbhX->disconnect();
	$dbhA->disconnect();
	$dbhB->disconnect();
}
