#!perl

## Test pushdelta functionality

use 5.008003;
use strict;
use warnings;
use Data::Dumper;
use lib 't','.';
use DBD::Pg;
use Test::More tests => 194;

use BucardoTesting;
my $bct = BucardoTesting->new() or BAIL_OUT "Creation of BucardoTesting object failed\n";
$location = 'pushdelta';

use vars qw/$SQL $sth $t $i $result $count %sql %val %pkey/;

pass("*** Beginning 'pushdelta' tests");

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

## Create a new sync to pushdelta from A to B, and another from A to C
$t=q{Add sync works};
$i = $bct->ctl("add sync pushdeltatestB source=testherd1 type=pushdelta targetdb=B");
like($i, qr{Added sync}, $t);
$i = $bct->ctl("add sync pushdeltatestC source=testherd1 type=pushdelta targetdb=C");
like($i, qr{Added sync}, $t);

$dbhX->do('LISTEN "bucardo_syncdone_pushdeltatestB"');
$dbhX->do('LISTEN "bucardo_syncdone_pushdeltatestC"');
$dbhX->commit();

$bct->restart_bucardo($dbhX);

sub test_empty_drop {
	my ($table, $dbh) = @_;
	my $DROPSQL = 'SELECT * FROM droptest';
	my $line = (caller)[2];
	$t=qq{ Triggers and rules did NOT fire on remote table $table};
	$result = [];
	bc_deeply($result, $dbhB, $DROPSQL, $t, $line);
	bc_deeply($result, $dbhC, $DROPSQL, $t, $line);
}

## Test unique index violation problems
## Test a deletion
for my $table (sort keys %tabletype) {
	my $pkeyname = $table =~ /test5/ ? q{"id space"} : 'id';
	my $type = $tabletype{$table};
	my $val1 = $val{$type}{1};
	my $val2 = $val{$type}{2};
	my $val3 = $val{$type}{3};
	$SQL = "INSERT INTO $table($pkeyname, inty, data1, email) VALUES (?,?,?,?)";
	$sth = $dbhA->prepare($SQL);
	$sth->execute($val1, 1, 1, 'moe');
	$sth->execute($val2, 2, 2, 'larry');
	$sth->execute($val3, 3, 3, 'curly');
}
$dbhA->commit();

for my $db (qw/B C/) {
    $bct->ctl("kick pushdeltatest$db 0");
    wait_for_notice($dbhX, "bucardo_syncdone_pushdeltatest$db", 15);
}

$SQL = 'SELECT * FROM bucardo_test1';
my $info = $dbhB->selectall_arrayref($SQL);

## Switch things up to try and trick the unique index
my $table = 'bucardo_test1';
$SQL = "UPDATE $table SET email = ? WHERE id = ?";
$sth = $dbhA->prepare($SQL);
$sth->execute('larrytemp', 2);
$sth->execute('larry', 1);
$sth->execute('moe', 3);
$sth->execute('curly', 2);
$dbhA->commit();

for my $db (qw/B C/) {
    $bct->ctl("kick pushdeltatest$db 0");
    wait_for_notice($dbhX, "bucardo_syncdone_pushdeltatest$db", 5);
}

## We want 1 2 3 to be larry, curly, moe
$SQL = 'SELECT id, email FROM bucardo_test1 ORDER BY id';
$t='Pushdelta handled a unique index without any problems';
$result = [[1,'larry'],[2,'curly'],[3,'moe']];
bc_deeply($result, $dbhB, $SQL, $t);
bc_deeply($result, $dbhC, $SQL, $t);

## Sequence testing

$dbhA->do("SELECT setval('bucardo_test_seq1', 123)");
$dbhA->commit();

for my $db (qw/B C/) {
    $bct->ctl("kick pushdeltatest$db 0");
    wait_for_notice($dbhX, "bucardo_syncdone_pushdeltatest$db", 5);
}

$SQL = q{SELECT nextval('bucardo_test_seq1')};
$t='Pushdelta replicated a sequence properly';
$result = [[123+1]];
bc_deeply($result, $dbhB, $SQL, $t);
bc_deeply($result, $dbhC, $SQL, $t);

$dbhA->do("SELECT setval('bucardo_test_seq1', 223, false)");
$dbhA->commit();

for my $db (qw/B C/) {
    $bct->ctl("kick pushdeltatest$db 0");
    wait_for_notice($dbhX, "bucardo_syncdone_pushdeltatest$db", 5);
}

$SQL = q{SELECT nextval('bucardo_test_seq1')};
$t='Pushdelta replicated a sequence properly with a false setval';
$result = [[223]];
bc_deeply($result, $dbhB, $SQL, $t);
bc_deeply($result, $dbhC, $SQL, $t);

$dbhA->do("SELECT setval('bucardo_test_seq1', 345, true)");
$dbhA->commit();

for my $db (qw/B C/) {
    $bct->ctl("kick pushdeltatest$db 0");
    wait_for_notice($dbhX, "bucardo_syncdone_pushdeltatest$db", 5);
}

$SQL = q{SELECT nextval('bucardo_test_seq1')};
$t='Pushdelta replicated a sequence properly with a true setval';
$result = [[345+1]];
bc_deeply($result, $dbhB, $SQL, $t);
bc_deeply($result, $dbhC, $SQL, $t);

## Reset the tables
for my $table (sort keys %tabletype) {
	$dbhA->do("DELETE FROM $table");
}
$dbhA->do('DELETE FROM droptest');
$dbhA->commit();
for my $db (qw/B C/) {
    $bct->ctl("kick pushdeltatest$db 0");
    wait_for_notice($dbhX, "bucardo_syncdone_pushdeltatest$db", 5);
}
$dbhB->do('DELETE FROM droptest');
$dbhB->commit();
$dbhC->do('DELETE FROM droptest');
$dbhC->commit();

exit;

END {
	$bct->stop_bucardo($dbhX);
	$dbhX->disconnect();
	$dbhA->disconnect();
	$dbhB->disconnect();
	$dbhC->disconnect();
}
