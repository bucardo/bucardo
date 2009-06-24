#!perl

## Test truncate functionality
## Only for Postgres 8.4 and up

use 5.008003;
use strict;
use warnings;
use Data::Dumper;
use lib 't','.';
use DBD::Pg;
use Test::More;

use BucardoTesting;
my $bct = BucardoTesting->new() or BAIL_OUT "Creation of BucardoTesting object failed\n";
$location = 'truncate';

use vars qw/$SQL $sth $t $i $result $count/;

## Prepare a clean Bucardo database on A
my $dbhA = $bct->blank_database('A');

## Bail out if the version if not high enough
my $ver = $dbhA->{pg_server_version};
if ($dbhA->{pg_server_version} < 80400) {
	plan (skip_all =>  'Cannot test truncate triggers unless version 8.4 or greater');
}
plan tests => 13;

pass("*** Beginning 'truncate' tests");

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
$i = $bct->ctl("add sync truncatetest source=testherd1 type=pushdelta targetdb=B");
like($i, qr{Sync added:}, $t);

$dbhA->do('DELETE FROM bucardo.bucardo_truncate_trigger_log');
$dbhA->do('DELETE FROM bucardo.bucardo_truncate_trigger');

$bct->restart_bucardo($dbhX);

$dbhX->do('LISTEN bucardo_syncdone_truncatetest');
$dbhX->commit();

$t=q{A truncate populates the bucardo_truncate_trigger table};
$dbhA->do('TRUNCATE TABLE bucardo.bucardo_truncate_trigger');
$dbhA->do('TRUNCATE TABLE bucardo_test1');
$dbhA->commit();
$SQL = 'SELECT tname FROM bucardo.bucardo_truncate_trigger';
$sth = $dbhA->prepare($SQL);
$sth->execute();
$result = $sth->fetchall_arrayref({});
is_deeply($result, [{tname => 'bucardo_test1'}], $t);

wait_for_notice($dbhX, 'bucardo_syncdone_truncatetest', 5);

$t=q{Truncate trigger works when source is truncated};
$dbhA->do("INSERT into bucardo_test1(id,inty) VALUES (101,99)");
$dbhA->commit();
wait_for_notice($dbhX, 'bucardo_syncdone_truncatetest', 5);

my $CHECKSQL = 'SELECT id FROM bucardo_test1 ORDER BY id';
$result = [[101]];
bc_deeply($result, $dbhB, $CHECKSQL, $t);

$t=q{Truncate trigger works when source is truncated, and extra rows are added};
$dbhA->do('DELETE FROM bucardo.bucardo_delta');
$dbhA->do('TRUNCATE TABLE bucardo_test1');
$dbhA->do("INSERT into bucardo_test1(id,inty) VALUES (102,99)");
$dbhA->do("INSERT into bucardo_test1(id,inty) VALUES (103,99)");
$dbhA->commit();

wait_for_notice($dbhX, 'bucardo_syncdone_truncatetest', 5);

$result = [[102],[103]];
bc_deeply($result, $dbhB, $CHECKSQL, $t);

$t=q{Delta rows are removed after truncation};
$SQL = 'SELECT * FROM bucardo.bucardo_delta';
$result = $dbhA->selectall_arrayref($SQL);
is_deeply($result, [], $t);

$t=q{Truncate trigger works when source is truncated, but only for some tables in the sync};
$dbhA->do('TRUNCATE TABLE bucardo_test1');
$dbhA->do("INSERT into bucardo_test3(id,inty) VALUES (201,99)");
$dbhA->commit();

wait_for_notice($dbhX, 'bucardo_syncdone_truncatetest', 5);


$result = [];
bc_deeply($result, $dbhB, $CHECKSQL, $t);

(my $CHECKSQL2 = $CHECKSQL) =~ s/1/3/o;
$result = [[201]];
bc_deeply($result, $dbhB, $CHECKSQL2, $t);

$t=q{Delta rows are only removed for truncated tables};
$SQL = q{SELECT oid FROM pg_class WHERE relname = 'bucardo_test3'};
my $oid = $dbhA->selectall_arrayref($SQL)->[0][0];
$t=q{Delta rows are not removed after non-truncation};
$SQL = 'SELECT tablename FROM bucardo.bucardo_delta';
$result = $dbhA->selectall_arrayref($SQL);
is_deeply($result, [[$oid]], $t);

$t=q{Truncated tables in syncs enter into the bucardo_truncate_trigger_log table};
$SQL = q{SELECT tname,sync,targetdb FROM bucardo.bucardo_truncate_trigger_log};
$result = $dbhA->selectall_arrayref($SQL);
is_deeply($result,
[
	['bucardo_test1','truncatetest','B'],
	['bucardo_test1','truncatetest','B'],
	['bucardo_test1','truncatetest','B'],
]
, $t);

$dbhX->do('NOTIFY bucardo_kick_sync_truncatetest');
$dbhX->commit();
wait_for_notice($dbhX, 'bucardo_syncdone_truncatetest', 5);

$t=q{Once truncation has been processed, it does not occur again};
$SQL = q{SELECT tname,sync,targetdb FROM bucardo.bucardo_truncate_trigger_log};
$result = $dbhA->selectall_arrayref($SQL);
is_deeply($result,
[
	['bucardo_test1','truncatetest','B'],
	['bucardo_test1','truncatetest','B'],
	['bucardo_test1','truncatetest','B'],
]
, $t);

END {
	$bct->stop_bucardo($dbhX);
	$dbhX and $dbhX->disconnect();
	$dbhA and $dbhA->disconnect();
	$dbhB and $dbhB->disconnect();
	$dbhC and $dbhC->disconnect();
}
