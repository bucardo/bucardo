## Test pushdelta functionality, with custom code

use 5.008003;
use strict;
use warnings;
use Data::Dumper;
use lib 't','.';
use DBD::Pg;
use Test::More tests => 14;

use BucardoTesting;
my $bct = BucardoTesting->new() or BAIL_OUT "Creation of BucardoTesting object failed\n";

pass("*** Beginning unique constraint tests");
#$bct->drop_database('all');

## Prepare a clean Bucardo database on A
my $dbhA = $bct->blank_database('A');
my $dbhX = $bct->setup_bucardo(A => $dbhA);

## Server A is the master, the rest are slaves
my $dbhB = $bct->blank_database('B');

## Tell Bucardo about these databases
$bct->add_test_databases('A B');

## Create a herd for 'A' and add all test tables to it
$bct->add_test_tables_to_herd('A', 'testherd1');

if (!BucardoTesting::table_exists($dbhA => 'uniq_test')) {
    ## Create tables for this test
    for my $dbh ($dbhA, $dbhB) {
        $dbh->do(q{
            CREATE TABLE uniq_test (
                id INTEGER PRIMARY KEY,
                field1 TEXT UNIQUE,
                field2 TEXT UNIQUE,
                field3 TEXT UNIQUE
            )});
        # mcpk == multi-column primary key
        $dbh->do(q{
            CREATE TABLE uniq_test_mcpk (
                id1 INTEGER,
                id2 INTEGER,
                id3 INTEGER,
                field1 TEXT UNIQUE,
                field2 TEXT UNIQUE,
                field3 TEXT UNIQUE,
                PRIMARY KEY (id1, id2, id3)
            )});
        $dbh->commit;
    }
}
else {
    $dbhA->do('TRUNCATE uniq_test');
    $dbhA->do('TRUNCATE uniq_test_mcpk');
    $dbhB->do('TRUNCATE uniq_test');
    $dbhB->do('TRUNCATE uniq_test_mcpk');
    $dbhX->do('TRUNCATE sync CASCADE');
    $dbhX->do('TRUNCATE herdmap CASCADE');
    $dbhX->do('TRUNCATE herd CASCADE');
    $dbhX->do('TRUNCATE goat CASCADE');
    $dbhX->commit();
    $dbhA->commit();
    $dbhB->commit();
}
$bct->ctl('add herd herd1');
$bct->ctl('add table uniq_test      db=A herd=herd1');
$bct->ctl('add table uniq_test_mcpk db=A herd=herd1');
$bct->ctl('add sync uniqsync source=herd1 type=pushdelta targetdb=B');
#$dbhX->do(q{update sync set disable_triggers = 'replica', disable_rules = 'replica'});
$dbhX->commit();

# Test that sync works
$dbhA->do(q{INSERT INTO uniq_test (id, field1, field2, field3) VALUES (1, 'alpha',    'bravo',  'charlie')});
$dbhA->do(q{INSERT INTO uniq_test (id, field1, field2, field3) VALUES (2, 'delta',    'echo',   'foxtrot')});
$dbhA->do(q{INSERT INTO uniq_test (id, field1, field2, field3) VALUES (3, 'hotel',    'india',  'juliet')});
$dbhA->do(q{INSERT INTO uniq_test (id, field1, field2, field3) VALUES (4, 'kilo',     'lima',   'mike')});
$dbhA->do(q{INSERT INTO uniq_test (id, field1, field2, field3) VALUES (5, 'november', 'oscar',  'papa')});
$dbhA->do(q{INSERT INTO uniq_test (id, field1, field2, field3) VALUES (6, 'romeo',    'sierra', 'tango')});
$dbhA->do(q{INSERT INTO uniq_test (id, field1, field2, field3) VALUES (7, 'uniform',  'victor', 'whiskey')});
$dbhA->do(q{INSERT INTO uniq_test (id, field1, field2, field3) VALUES (8, 'xray',     'yankee', 'zulu')});
$dbhA->do(q{INSERT INTO uniq_test_mcpk (id1, id2, id3, field1, field2, field3) VALUES (1, 1, 1, 'alpha',    'bravo',  'charlie')});
$dbhA->do(q{INSERT INTO uniq_test_mcpk (id1, id2, id3, field1, field2, field3) VALUES (2, 2, 2, 'delta',    'echo',   'foxtrot')});
$dbhA->do(q{INSERT INTO uniq_test_mcpk (id1, id2, id3, field1, field2, field3) VALUES (3, 3, 3, 'hotel',    'india',  'juliet')});
$dbhA->do(q{INSERT INTO uniq_test_mcpk (id1, id2, id3, field1, field2, field3) VALUES (4, 4, 4, 'kilo',     'lima',   'mike')});
$dbhA->do(q{INSERT INTO uniq_test_mcpk (id1, id2, id3, field1, field2, field3) VALUES (5, 5, 5, 'november', 'oscar',  'papa')});
$dbhA->do(q{INSERT INTO uniq_test_mcpk (id1, id2, id3, field1, field2, field3) VALUES (6, 6, 6, 'romeo',    'sierra', 'tango')});
$dbhA->do(q{INSERT INTO uniq_test_mcpk (id1, id2, id3, field1, field2, field3) VALUES (7, 7, 7, 'uniform',  'victor', 'whiskey')});
$dbhA->do(q{INSERT INTO uniq_test_mcpk (id1, id2, id3, field1, field2, field3) VALUES (8, 8, 8, 'xray',     'yankee', 'zulu')});
$dbhX->do(q{LISTEN bucardo_syncdone_uniqsync});
$dbhA->commit();
$dbhX->commit();
$bct->restart_bucardo($dbhX);
$bct->ctl('kick uniqsync 0');
wait_for_notice($dbhX, 'bucardo_syncdone_uniqsync', 5);
is_deeply($dbhA->selectall_arrayref('SELECT * FROM uniq_test ORDER BY id'), $dbhB->selectall_arrayref('SELECT * FROM uniq_test ORDER BY id'), 'Swap works on single-column primary key');
is_deeply($dbhA->selectall_arrayref('SELECT * FROM uniq_test_mcpk ORDER BY id1'), $dbhB->selectall_arrayref('SELECT * FROM uniq_test_mcpk ORDER BY id1'), 'Swap works on multi-column primary key');
$dbhA->rollback();
$dbhB->rollback();
$bct->stop_bucardo($dbhX);

# Single column primary key
# Update some stuff just to give Bucardo something to do
$dbhA->do(q{UPDATE uniq_test SET field1 = 'fred'   WHERE id = 3});
$dbhA->do(q{UPDATE uniq_test SET field1 = 'wilma'  WHERE id = 4});
$dbhA->do(q{UPDATE uniq_test SET field1 = 'barney' WHERE id = 5});
$dbhA->do(q{UPDATE uniq_test SET field1 = 'betty'  WHERE id = 6});
# Swap around rows
    # swap alpha and delta in field1
$dbhA->do(q{UPDATE uniq_test SET field1 = 'golf'  WHERE field1 = 'alpha'});
$dbhA->do(q{UPDATE uniq_test SET field1 = 'alpha' WHERE field1 = 'delta'});
$dbhA->do(q{UPDATE uniq_test SET field1 = 'delta' WHERE field1 = 'golf'});
    # Add some rows just for kicks, and delete some others
$dbhA->do(q{INSERT INTO uniq_test (id, field1, field2, field3) VALUES (19, 'x', 'y', 'z')});
$dbhA->do(q{INSERT INTO uniq_test (id, field1, field2, field3) VALUES (20, 'a', 'b', 'c')});
$dbhA->do(q{DELETE FROM uniq_test WHERE id IN (5, 6)});
    # swap echo and india in field2
$dbhA->do(q{UPDATE uniq_test SET field2 = 'golf'  WHERE field2 = 'echo'});
$dbhA->do(q{UPDATE uniq_test SET field2 = 'echo'  WHERE field2 = 'india'});
$dbhA->do(q{UPDATE uniq_test SET field2 = 'india' WHERE field2 = 'golf'});
    # swap juliet and mike in field3
$dbhA->do(q{UPDATE uniq_test SET field3 = 'golf'  WHERE field3 = 'mike'});
$dbhA->do(q{UPDATE uniq_test SET field3 = 'mike'  WHERE field3 = 'juliet'});
$dbhA->do(q{UPDATE uniq_test SET field3 = 'juliet' WHERE field3 = 'golf'});
# Update some more stuff
$dbhA->do(q{UPDATE uniq_test SET field1 = 'dino'    WHERE id = 7});
$dbhA->do(q{UPDATE uniq_test SET field1 = 'pebbles' WHERE id = 8});

$dbhA->commit();
$bct->restart_bucardo($dbhX);
$bct->ctl('kick uniqsync 0');
wait_for_notice($dbhX, 'bucardo_syncdone_uniqsync', 5);
is_deeply($dbhA->selectall_arrayref('SELECT * FROM uniq_test ORDER BY id'), $dbhB->selectall_arrayref('SELECT * FROM uniq_test ORDER BY id'), 'Swap works on single-column primary key with unique constraints');
$dbhA->rollback();
$dbhB->rollback();

# Multi-column primary key
    # Update some stuff just to give Bucardo something to do
$dbhA->do(q{UPDATE uniq_test_mcpk SET field1 = 'fred'   WHERE id1 = 3});
$dbhA->do(q{UPDATE uniq_test_mcpk SET field1 = 'wilma'  WHERE id1 = 4});
$dbhA->do(q{UPDATE uniq_test_mcpk SET field1 = 'barney' WHERE id1 = 5});
$dbhA->do(q{UPDATE uniq_test_mcpk SET field1 = 'betty'  WHERE id1 = 6});
    # Swap around rows
$dbhA->do(q{UPDATE uniq_test_mcpk SET field1 = 'golf'  WHERE field1 = 'alpha'});
$dbhA->do(q{UPDATE uniq_test_mcpk SET field1 = 'alpha' WHERE field1 = 'delta'});
$dbhA->do(q{UPDATE uniq_test_mcpk SET field1 = 'delta' WHERE field1 = 'golf'});
    # Add some rows just for kicks, and delete some others
$dbhA->do(q{INSERT INTO uniq_test_mcpk (id1, id2, id3, field1, field2, field3) VALUES (15, 15, 15, 'n', 'o', 'p')});
$dbhA->do(q{INSERT INTO uniq_test_mcpk (id1, id2, id3, field1, field2, field3) VALUES (16, 16, 16, 'r', 's', 't')});
$dbhA->do(q{INSERT INTO uniq_test_mcpk (id1, id2, id3, field1, field2, field3) VALUES (17, 17, 17, 'u', 'v', 'w')});
$dbhA->do(q{INSERT INTO uniq_test_mcpk (id1, id2, id3, field1, field2, field3) VALUES (18, 18, 18, 'x', 'y', 'z')});
$dbhA->do(q{DELETE FROM uniq_test_mcpk WHERE id1 IN (5, 6)});
    # Update some more stuff
$dbhA->do(q{UPDATE uniq_test_mcpk SET field1 = 'dino'    WHERE id1 = 7});
$dbhA->do(q{UPDATE uniq_test_mcpk SET field1 = 'pebbles' WHERE id1 = 8});

$dbhA->commit();
$bct->restart_bucardo($dbhX);
$bct->ctl('kick uniqsync 0');
wait_for_notice($dbhX, 'bucardo_syncdone_uniqsync', 5);
is_deeply($dbhA->selectall_arrayref('SELECT * FROM uniq_test_mcpk ORDER BY id1'), $dbhB->selectall_arrayref('SELECT * FROM uniq_test_mcpk ORDER BY id1'), 'Swap works on multi-column primary key with unique constraints');
$dbhA->rollback();
$dbhB->rollback();

# Try to replicate a row that will always fail to swap. 
$bct->stop_bucardo();
$dbhB->do(q{INSERT INTO uniq_test (id, field1, field2, field3) VALUES (123, 'peter', 'paul', 'mary')});
$dbhB->commit();
$dbhX->do(<<'END_CCINSERT'
INSERT INTO customcode
    (name, about, whenrun, getrows, src_code)
VALUES
    ('uniq_test_custcode', 'Custom code module for use with unique constraint tests',
    'exception', true,
$perl$
use Data::Dumper;
my ($args) = @_;
return if (exists $args->{dummy});
my $sourcedbh = $args->{sourcedbh};
$sourcedbh->do(q{INSERT INTO uniq_cc_flag VALUES ('T')});
$sourcedbh->do(q{DELETE FROM uniq_test WHERE id = 8});
$args->{runagain} = 1;
$perl$);
END_CCINSERT
);
$dbhX->do(q{INSERT INTO customcode_map (code, goat)
    SELECT customcode.id, goat.id FROM customcode, goat
    WHERE customcode.name = 'uniq_test_custcode' AND goat.tablename = 'uniq_test'});
$dbhX->commit();
if (BucardoTesting::table_exists($dbhA => 'uniq_cc_flag')) {
    $dbhA->do(q{DROP TABLE uniq_cc_flag});
}
$dbhA->do(q{CREATE TABLE uniq_cc_flag (a BOOLEAN)});
$dbhA->do(q{UPDATE uniq_test SET field1 = 'peter' WHERE id = 8});    # This conflicts with the row we stuck in $dbhB
$dbhA->commit();
$bct->restart_bucardo($dbhX);
$bct->ctl('kick uniqsync 0');
wait_for_notice($dbhX, 'bucardo_syncdone_uniqsync', 5);
is($dbhA->do('SELECT * FROM uniq_cc_flag'), 1, 'Custom code exception handler ran successfully');

exit;

END {
	$bct->stop_bucardo($dbhX);
	$dbhX->disconnect();
	$dbhA->disconnect();
	$dbhB->disconnect();
}
