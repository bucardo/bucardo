#!perl

## This test creates a star-like set of databases. A is the center; B and C are two spokes.
## The idea is to see if we can replicate everything everywhere with A
## replicating to all spokes, and each spoke replicating back to A with pushdelta
## turned on

use 5.008003;
use strict;
use warnings;
use Data::Dumper;
use lib 't','.';
use DBD::Pg;
use Test::More tests => 34;

use BucardoTesting;
my $bct = BucardoTesting->new() or BAIL_OUT "Creation of BucardoTesting object failed\n";

pass("*** Beginning star tests");
$bct->drop_database('all');

## Prepare a clean Bucardo database on A
my $dbhA = $bct->blank_database('A');
my $dbhX = $bct->setup_bucardo(A => $dbhA);

## Server A is the master, the rest are slaves
my $dbhB = $bct->blank_database('B');
my $dbhC = $bct->blank_database('C');

## Tell Bucardo about these databases
$bct->add_test_databases('A B C');

## Create some tables
if (!BucardoTesting::table_exists($dbhA => 'makedelta')) {
    for my $dbh ($dbhA, $dbhB, $dbhC) {
        $dbh->do(q{
            CREATE TABLE star_test (
                id INTEGER PRIMARY KEY,
                field1 INTEGER
            )});
        # mcpk == multi-column primary key
        $dbh->do(q{
            CREATE TABLE star_test_mcpk (
                id1 INTEGER,
                id2 INTEGER,
                id3 INTEGER,
                field1 INTEGER,
                PRIMARY KEY (id1, id2, id3)
            )});
        $dbh->commit;
    }
}
else {
    $dbhA->do('TRUNCATE star_test');
    $dbhA->do('TRUNCATE star_test_mcpk');
    $dbhB->do('TRUNCATE star_test');
    $dbhB->do('TRUNCATE star_test_mcpk');
    $dbhC->do('TRUNCATE star_test');
    $dbhC->do('TRUNCATE star_test_mcpk');
    $dbhX->do('TRUNCATE sync CASCADE');
    $dbhX->do('TRUNCATE herdmap CASCADE');
    $dbhX->do('TRUNCATE herd CASCADE');
    $dbhX->do('TRUNCATE goat CASCADE');
    $dbhX->commit();
    $dbhA->commit();
    $dbhB->commit();
    $dbhC->commit();
}
my $res;
my @dummy;

pass('Adding goats and herds');
for my $db (qw/A B C/) {
    ($res, @dummy) = split"\n", $bct->ctl("add herd herd$db");
    like($res, qr/Added herd/, $res);
    if ($db eq 'A') {
        ($res, @dummy) = split "\n", $bct->ctl("add table star_test db=$db herd=herd$db makedelta=true");
        like($res, qr/Added table/, $res);
        ($res, @dummy) = split "\n", $bct->ctl("add table star_test_mcpk db=$db herd=herd$db makedelta=true");
        like($res, qr/Added table/, $res);
    }
    else {
        ($res, @dummy) = split "\n", $bct->ctl("add table star_test db=$db herd=herd$db");
        like($res, qr/Added table/, $res);
        ($res, @dummy) = split "\n", $bct->ctl("add table star_test_mcpk db=$db herd=herd$db");
        like($res, qr/Added table/, $res);
    }
}

pass('Adding syncs');
$res = $bct->ctl('add sync star_a_b source=herdA type=pushdelta targetdb=B');
chomp $res;
like($res, qr/Added sync/, $res);
$res = $bct->ctl('add sync star_a_c source=herdA type=pushdelta targetdb=C');
chomp $res;
like($res, qr/Added sync/, $res);

$res = $bct->ctl('add sync star_b_a source=herdB type=pushdelta targetdb=A makedelta=true');
chomp $res;
like($res, qr/Added sync/, $res);
$res = $bct->ctl('add sync star_c_a source=herdC type=pushdelta targetdb=A makedelta=true');
chomp $res;
like($res, qr/Added sync/, $res);

$dbhX->commit();

# Test that sync works
$dbhX->do(q{LISTEN bucardo_syncdone_star_a_b});
$dbhX->do(q{LISTEN bucardo_syncdone_star_a_c});
$dbhX->do(q{LISTEN bucardo_syncdone_star_b_a});
$dbhX->do(q{LISTEN bucardo_syncdone_star_c_a});
$dbhX->commit();
$bct->restart_bucardo($dbhX);

pass('Trying to insert into A and replicate');
$dbhA->do(q{INSERT INTO star_test (id, field1) VALUES (1, 10)});
$dbhA->do(q{INSERT INTO star_test_mcpk (id1, id2, id3, field1) VALUES (1, 2, 3, 10)});
$dbhA->commit();
$bct->ctl('kick star_a_b 0');
wait_for_notice($dbhX, 'bucardo_syncdone_star_a_b', 5);
is_deeply($dbhA->selectall_arrayref('SELECT * FROM star_test ORDER BY id'),       $dbhB->selectall_arrayref('SELECT * FROM star_test ORDER BY id'),       'Sync from A to B works, single-column primary key');
is_deeply($dbhA->selectall_arrayref('SELECT * FROM star_test_mcpk ORDER BY id1'), $dbhB->selectall_arrayref('SELECT * FROM star_test_mcpk ORDER BY id1'), 'Sync from A to B works, multi-column primary key');
$bct->ctl('kick star_a_c 0');
wait_for_notice($dbhX, 'bucardo_syncdone_star_a_c', 5);
is_deeply($dbhA->selectall_arrayref('SELECT * FROM star_test ORDER BY id'),       $dbhC->selectall_arrayref('SELECT * FROM star_test ORDER BY id'),       'Sync from A to C works, single-column primary key');
is_deeply($dbhA->selectall_arrayref('SELECT * FROM star_test_mcpk ORDER BY id1'), $dbhC->selectall_arrayref('SELECT * FROM star_test_mcpk ORDER BY id1'), 'Sync from A to C works, multi-column primary key');
$dbhA->rollback();
$dbhB->rollback();
$dbhC->rollback();

pass('Trying to insert into B and replicate');
$dbhB->do(q{INSERT INTO star_test (id, field1) VALUES (2, 20)});
$dbhB->do(q{INSERT INTO star_test_mcpk (id1, id2, id3, field1) VALUES (2, 4, 6, 20)});
$dbhB->commit();
$bct->ctl('kick star_b_a 0');
wait_for_notice($dbhX, 'bucardo_syncdone_star_b_a', 5);
is_deeply($dbhA->selectall_arrayref('SELECT * FROM star_test ORDER BY id'),       $dbhB->selectall_arrayref('SELECT * FROM star_test ORDER BY id'),       'Sync from B to A works, single-column primary key');
is_deeply($dbhA->selectall_arrayref('SELECT * FROM star_test_mcpk ORDER BY id1'), $dbhB->selectall_arrayref('SELECT * FROM star_test_mcpk ORDER BY id1'), 'Sync from B to A works, multi-column primary key');
$bct->ctl('kick star_a_c 0');
wait_for_notice($dbhX, 'bucardo_syncdone_star_a_c', 5);
is_deeply($dbhB->selectall_arrayref('SELECT * FROM star_test ORDER BY id'),       $dbhC->selectall_arrayref('SELECT * FROM star_test ORDER BY id'),       'Sync from A to C works, single-column primary key');
is_deeply($dbhB->selectall_arrayref('SELECT * FROM star_test_mcpk ORDER BY id1'), $dbhC->selectall_arrayref('SELECT * FROM star_test_mcpk ORDER BY id1'), 'Sync from A to C works, multi-column primary key');
$dbhA->rollback();
$dbhB->rollback();
$dbhC->rollback();

pass('Trying to insert into C and replicate');
$dbhC->do(q{INSERT INTO star_test (id, field1) VALUES (3, 30)});
$dbhC->do(q{INSERT INTO star_test_mcpk (id1, id2, id3, field1) VALUES (3, 6, 9, 30)});
$dbhC->commit();
$bct->ctl('kick star_c_a 0');
wait_for_notice($dbhX, 'bucardo_syncdone_star_c_a', 5);
is_deeply($dbhA->selectall_arrayref('SELECT * FROM star_test ORDER BY id'),       $dbhC->selectall_arrayref('SELECT * FROM star_test ORDER BY id'),       'Sync from C to A works, single-column primary key');
is_deeply($dbhA->selectall_arrayref('SELECT * FROM star_test_mcpk ORDER BY id1'), $dbhC->selectall_arrayref('SELECT * FROM star_test_mcpk ORDER BY id1'), 'Sync from C to A works, multi-column primary key');
$bct->ctl('kick star_a_b 0');
wait_for_notice($dbhX, 'bucardo_syncdone_star_a_b', 5);
is_deeply($dbhB->selectall_arrayref('SELECT * FROM star_test ORDER BY id'),       $dbhC->selectall_arrayref('SELECT * FROM star_test ORDER BY id'),       'Sync from A to B works, single-column primary key');
is_deeply($dbhB->selectall_arrayref('SELECT * FROM star_test_mcpk ORDER BY id1'), $dbhC->selectall_arrayref('SELECT * FROM star_test_mcpk ORDER BY id1'), 'Sync from A to B works, multi-column primary key');
$dbhA->rollback();
$dbhB->rollback();
$dbhC->rollback();

$bct->stop_bucardo($dbhX);

exit;

END {
	$bct->stop_bucardo($dbhX);
	$dbhX->disconnect();
	$dbhA->disconnect();
	$dbhB->disconnect();
}
