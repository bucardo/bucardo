#!/usr/bin/perl -- -*-cperl-*-

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
$location = 'pushdelta';

use vars qw/$SQL $sth $t $i $result $count %sql %val %pkey/;

unlink "tmp/log.bucardo";

pass("*** Beginning 'simple' tests");

## Prepare a clean Bucardo database on A (master) and B (slave)
my $dbhA = $bct->blank_database('A');
my $dbhX = $bct->setup_bucardo(A => $dbhA);
my $dbhB = $bct->blank_database('B');

## Tell Bucardo about these databases
$bct->add_test_databases('A B');

## Create a herd for 'A' and add all test tables to it
$bct->add_test_tables_to_herd('A', 'testherd1');

## Create a new sync to pushdelta from A to B
$t=q{Add sync works};
$i = $bct->ctl("add sync simpletest source=testherd1 type=pushdelta targetdb=B");
like($i, qr{Sync added:}, $t);

$bct->restart_bucardo($dbhX);
$dbhX->do('LISTEN bucardo_syncdone_simpletest');
$dbhX->do('LISTEN bucardo_syncerror_simpletest');
$dbhX->commit();

## Add a row to a table, make sure it gets pushed
$dbhA->do("INSERT INTO bucardo_test1(id,inty) VALUES (12,34)");
$dbhA->commit();

$bct->ctl("kick simpletest 0");
wait_for_notice($dbhX, 'bucardo_syncdone_simpletest', 5);

$SQL = 'SELECT id,inty FROM bucardo_test1';
$result = $dbhB->selectall_arrayref($SQL);
diag Dumper $result;
is_deeply($result, [[12,34]], $t);

## Test mismatched rows - kicking the sync should fail, as it will be inactive
$dbhA->do("ALTER TABLE bucardo_test1 ADD newcol INT");
$dbhA->commit();

$bct->restart_bucardo($dbhX);
$dbhA->do("INSERT INTO bucardo_test1(id,inty) VALUES (44,55)");
$dbhA->commit();

$bct->ctl("kick simpletest 2");
wait_for_notice($dbhX, 'bucardo_syncerror_simpletest', 5);

## Add the same column to B, then try again
$dbhB->do("ALTER TABLE bucardo_test1 ADD newcol INT");
$dbhB->commit();

$bct->restart_bucardo($dbhX);
$bct->ctl("kick simpletest 2");
wait_for_notice($dbhX, 'bucardo_syncdone_simpletest', 5);

$SQL = 'SELECT id,inty FROM bucardo_test1';
$result = $dbhB->selectall_arrayref($SQL);
is_deeply($result, [[12,34],[44,55]], $t);

## Now we introduce a "hole" in the column numbers on A:
$dbhA->do("ALTER TABLE bucardo_test1 DROP COLUMN newcol");
$dbhA->do("ALTER TABLE bucardo_test1 ADD COLUMN newcol INT");
$dbhA->commit();

$bct->restart_bucardo($dbhX);
$bct->ctl("kick simpletest 2");
wait_for_notice($dbhX, 'bucardo_syncdone_simpletest', 5);

$SQL = 'SELECT id,inty FROM bucardo_test1';
$result = $dbhB->selectall_arrayref($SQL);
is_deeply($result, [[12,34],[44,55]], $t);

exit;
