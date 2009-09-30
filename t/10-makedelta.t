#!perl

## Test makedelta functionality

use 5.008003;
use strict;
use warnings;
use Data::Dumper;
use lib 't','.';
use DBD::Pg;
use Test::More tests => 15;

use BucardoTesting;
my $bct = BucardoTesting->new() or BAIL_OUT "Creation of BucardoTesting object failed\n";

pass("*** Beginning makedelta tests");
#$bct->drop_database('all');

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
            CREATE TABLE makedelta (
                id INTEGER PRIMARY KEY,
                field1 INTEGER
            )});
        # mcpk == multi-column primary key
        $dbh->do(q{
            CREATE TABLE makedelta_mcpk (
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
    $dbhA->do('TRUNCATE makedelta');
    $dbhA->do('TRUNCATE makedelta_mcpk');
    $dbhB->do('TRUNCATE makedelta');
    $dbhB->do('TRUNCATE makedelta_mcpk');
    $dbhC->do('TRUNCATE makedelta');
    $dbhC->do('TRUNCATE makedelta_mcpk');
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
($res, @dummy) = split "\n", $bct->ctl('add herd herdA');
like($res, qr/Herd added/, $res);
($res, @dummy) = split "\n", $bct->ctl('add table makedelta      db=A herd=herdA makedelta=true');
like($res, qr/Table added/, $res);
($res, @dummy) = split "\n", $bct->ctl('add table makedelta_mcpk db=A herd=herdA makedelta=true');
like($res, qr/Table added/, $res);
($res, @dummy) = split "\n", $bct->ctl('add herd herdB');
like($res, qr/Herd added/, $res);
($res, @dummy) = split "\n", $bct->ctl('add table makedelta      db=B herd=herdB');
like($res, qr/Table added/, $res);
($res, @dummy) = split "\n", $bct->ctl('add table makedelta_mcpk db=B herd=herdB');
like($res, qr/Table added/, $res);
$res = $bct->ctl('add sync makedeltasync_b source=herdB type=pushdelta targetdb=C');
like($res, qr/Added sync/, $res);
$res = $bct->ctl('add sync makedeltasync_a source=herdA type=pushdelta targetdb=B makedelta=true');
like($res, qr/Added sync/, $res);
$dbhX->commit();

# Test that sync works
$dbhA->do(q{INSERT INTO makedelta (id, field1) VALUES (1, 10)});
$dbhA->do(q{INSERT INTO makedelta_mcpk (id1, id2, id3, field1) VALUES (1, 2, 3, 10)});
$dbhX->do(q{LISTEN bucardo_syncdone_makedeltasync_a});
$dbhA->commit();
$dbhX->commit();
$bct->restart_bucardo($dbhX);
$bct->ctl('kick makedeltasync_a 0');
is_deeply($dbhA->selectall_arrayref('SELECT * FROM makedelta ORDER BY id'),       $dbhB->selectall_arrayref('SELECT * FROM makedelta ORDER BY id'),       'Sync from A to B works, single-column primary key');
is_deeply($dbhA->selectall_arrayref('SELECT * FROM makedelta_mcpk ORDER BY id1'), $dbhB->selectall_arrayref('SELECT * FROM makedelta_mcpk ORDER BY id1'), 'Sync from A to B works, multi-column primary key');
$dbhA->rollback();
$dbhB->rollback();

$dbhX->do(q{LISTEN bucardo_syncdone_makedeltasync_b});
$dbhX->commit();
# By this point, this sync will probably already have run, but we're kicking
# it anyway so we can listen for its results and make *sure* it ran
$bct->ctl('kick makedeltasync_b 0'); 
wait_for_notice($dbhX, 'bucardo_syncdone_makedeltasync_b', 5);
is_deeply($dbhA->selectall_arrayref('SELECT * FROM makedelta ORDER BY id'),       $dbhC->selectall_arrayref('SELECT * FROM makedelta ORDER BY id'),       'Makedelta-facilitated sync from B to C works, single-column primary key');
is_deeply($dbhA->selectall_arrayref('SELECT * FROM makedelta_mcpk ORDER BY id1'), $dbhC->selectall_arrayref('SELECT * FROM makedelta_mcpk ORDER BY id1'), 'Makedelta-facilitated sync from B to C works, multi-column primary key');
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
