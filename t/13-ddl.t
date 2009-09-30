#!perl

## Test DDL pushing

use 5.008003;
use strict;
use warnings;
use Data::Dumper;
use lib 't','.';
use DBD::Pg;
use Test::More 'no_plan';

use BucardoTesting;
my $bct = BucardoTesting->new() or BAIL_OUT "Creation of BucardoTesting object failed\n";
$location = 'ddl';

use vars qw/$SQL $sth $t $i $result $count %sql %val %pkey/;

pass("*** Beginning 'ddl' tests");

## Prepare a clean Bucardo database on A
my $dbhA = $bct->blank_database('A');
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
$i = $bct->ctl("add sync pushdeltatest source=testherd1 type=pushdelta targetdb=B");
like($i, qr{Added sync}, $t);

$dbhX->do('LISTEN bucardo_syncdone_pushdeltatest');
$dbhX->commit();

$bct->restart_bucardo($dbhX);

exit;

END {
	$bct->stop_bucardo($dbhX);
	$dbhX->disconnect();
	$dbhA->disconnect();
	$dbhB->disconnect();
	$dbhC->disconnect();
}
