#!perl

## Test fullcopy functionality

use 5.008003;
use strict;
use warnings;
use Data::Dumper;
use lib 't','.';
use DBD::Pg;
use Test::More tests => 131;

use BucardoTesting;
my $bct = BucardoTesting->new() or BAIL_OUT "Creation of BucardoTesting object failed\n";
$location = 'fullcopy';

use vars qw/$SQL $sth $t $i $result $count %sql %val %pkey/;

pass("*** Beginning 'fullcopy' tests");

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

## Create a new sync to fullcopy from A to B
$t=q{Add sync works};
$i = $bct->ctl("add sync fullcopytest source=testherd1 type=fullcopy targetdb=B");
like($i, qr{Added sync}, $t);

## Tell sync kids to stay alive
$dbhX->do(q{UPDATE bucardo.sync SET kidsalive = 't'});
$dbhX->commit();

$bct->restart_bucardo($dbhX);

$dbhX->do('LISTEN bucardo_syncdone_fullcopytest');
$dbhX->commit();

for my $table (sort keys %tabletype) {

	my $type = $tabletype{$table};
	my $val = $val{$type}{1};
	if (!defined $val) {
		BAIL_OUT "Could not determine value for $table $type\n";
	}

	$pkey{$table} = $table =~ /test5/ ? q{"id space"} : 'id';

	$SQL = $table =~ /0/
		? "INSERT INTO $table($pkey{$table}) VALUES (?)"
			: "INSERT INTO $table($pkey{$table},data1,inty) VALUES (?,'one',1)";
	$sql{insert}{$table} = $dbhA->prepare($SQL);
	if ($type eq 'BYTEA') {
		$sql{insert}{$table}->bind_param(1, undef, {pg_type => PG_BYTEA});
	}
	$val{$table} = $val;

	$sql{insert}{$table}->execute($val{$table});
}

$dbhA->commit();

sub test_empty_drop {
	my ($table, $dbh) = @_;
	my $DROPSQL = 'SELECT * FROM droptest';
	my $line = (caller)[2];
	$t=qq{ Triggers and rules did NOT fire on remote table $table};
	$result = [];
	bc_deeply($result, $dbhB, $DROPSQL, $t, $line);
}

for my $table (sort keys %tabletype) {
	$t=qq{ Second table $table still empty before commit };
	$SQL = $table =~ /0/
		? "SELECT $pkey{$table} FROM $table"
			: "SELECT $pkey{$table},data1 FROM $table";
	$result = [];
	bc_deeply($result, $dbhB, $SQL, $t);

	$t=q{ After insert, trigger and rule both populate droptest table };
	my $qtable = $dbhX->quote($table);
	my $LOCALDROPSQL = $table =~ /0/
		? "SELECT type,0 FROM droptest WHERE name = $qtable ORDER BY 1,2"
			: "SELECT type,inty FROM droptest WHERE name = $qtable ORDER BY 1,2";
	my $tval = $table =~ /0/ ? 0 : 1;
	$result = [['rule',$tval],['trigger',$tval]];
	bc_deeply($result, $dbhA, $LOCALDROPSQL, $t);

	test_empty_drop($table,$dbhB);
}

for my $table (sort keys %tabletype) {
	$t=qq{ Second table $table still empty before kick };
	$sql{select}{$table} = "SELECT inty FROM $table ORDER BY $pkey{$table}";
	$table =~ /0/ and $sql{select}{$table} =~ s/inty/$pkey{$table}/;
	$result = [];
	bc_deeply($result, $dbhB, $sql{select}{$table}, $t);
}

## Give the table some heft for speed tests
## $sth = $dbhA->prepare("INSERT INTO bucardo_test2(id,inty) VALUES(?,?)");
## for my $x (2..100000) {	$sth->execute($x,1000); }
## $dbhA->commit();

$bct->ctl("kick fullcopytest 0");
wait_for_notice($dbhX, 'bucardo_syncdone_fullcopytest', 5);

for my $table (sort keys %tabletype) {
	$t=qq{ Second table $table got the fullcopy row};
	$result = [[1]];
	bc_deeply($result, $dbhB, $sql{select}{$table}, $t);

	test_empty_drop($table,$dbhB);
}

for my $table (sort keys %tabletype) {
	## Make changes to B, have the sync blow them away
	$i = $dbhB->do("UPDATE $table SET inty = 99");
	$dbhB->do("DELETE FROM droptest");
	$dbhB->commit();
}

for my $table (sort keys %tabletype) {
	$t=qq{ Second table $table can be changed directly};
	$result = [[99]];
	bc_deeply($result, $dbhB, $sql{select}{$table}, $t);
}

$bct->ctl('kick fullcopytest 0');
wait_for_notice($dbhX, 'bucardo_syncdone_fullcopytest', 5);

for my $table (sort keys %tabletype) {
	$t=qq{ Second table $table loses local changes on fullcopy};
	$result = [[1]];
	bc_deeply($result, $dbhB, $sql{select}{$table}, $t);
}

## Sequence testing

$dbhA->do("SELECT setval('bucardo_test_seq1', 123)");
$dbhA->commit();

$bct->ctl("kick fullcopytest 0");
wait_for_notice($dbhX, 'bucardo_syncdone_fullcopytest', 5);

$SQL = q{SELECT nextval('bucardo_test_seq1')};
$t='Fullcopy replicated a sequence properly';
$result = [[123+1]];
bc_deeply($result, $dbhB, $SQL, $t);

$dbhA->do("SELECT setval('bucardo_test_seq1', 223, false)");
$dbhA->commit();

$bct->ctl("kick fullcopytest 0");
wait_for_notice($dbhX, 'bucardo_syncdone_fullcopytest', 5);

$SQL = q{SELECT nextval('bucardo_test_seq1')};
$t='Fullcopy replicated a sequence properly with a false setval';
$result = [[223]];
bc_deeply($result, $dbhB, $SQL, $t);

$dbhA->do("SELECT setval('bucardo_test_seq1', 345, true)");
$dbhA->commit();

$bct->ctl("kick fullcopytest 0");
wait_for_notice($dbhX, 'bucardo_syncdone_fullcopytest', 5);

$SQL = q{SELECT nextval('bucardo_test_seq1')};
$t='Fullcopy replicated a sequence properly with a true setval';
$result = [[345+1]];
bc_deeply($result, $dbhB, $SQL, $t);

## Add another slave
$t=q{Add dbgroup works};
$i = $bct->ctl("add dbgroup tgroup B C");
like($i, qr{Added database group}, $t);

$t=q{Alter sync works};
$dbhB->commit();
$i = $bct->ctl("alter sync fullcopytest targetgroup=tgroup");
like($i, qr{Sync updated}, $t);

$dbhX->do("NOTIFY bucardo_reload_sync_fullcopytest");
$dbhX->commit();

for my $table (sort keys %tabletype) {
	$dbhA->do("DELETE FROM $table");
}

$dbhA->commit();
# XXX - Hack
sleep 5;
$bct->ctl('kick fullcopytest 0');
wait_for_notice($dbhX, 'bucardo_syncdone_fullcopytest', 5);

for my $table (sort keys %tabletype) {
	$t=qq{ Second table $table was emptied out};
	$result = [];

	bc_deeply($result, $dbhB, $sql{select}{$table}, $t);

	$t=qq{ Third table $table begins empty};
	$result = [];
	bc_deeply($result, $dbhC, $sql{select}{$table}, $t);

	test_empty_drop($table,$dbhC);

	$sql{insert}{$table}->execute($val{$table});
}

$dbhA->commit();

$bct->ctl('kick fullcopytest 0');
wait_for_notice($dbhX, 'bucardo_syncdone_fullcopytest', 5);

for my $table (sort keys %tabletype) {
	$t=qq{ Second table $table got the fullcopy row};
	$result = [[1]];
	bc_deeply($result, $dbhB, $sql{select}{$table}, $t);

	$t=qq{ Third table $table got the fullcopy row};
	$result = [[1]];
	bc_deeply($result, $dbhC, $sql{select}{$table}, $t);
}

## Test out customselect - update just the id column
$dbhX->do(q{UPDATE goat SET customselect='SELECT '||replace(qpkey,'|',',')||' FROM '||tablename});
$dbhX->do(q{UPDATE sync SET usecustomselect = true});
$dbhX->do("NOTIFY bucardo_reload_sync_fullcopytest");
$dbhX->commit();

$dbhA->do("UPDATE bucardo_test1 SET id = id + 100, inty=inty + 100");
$dbhA->commit();

# XXX - Hack
sleep 5;
$bct->ctl('kick fullcopytest 0');
wait_for_notice($dbhX, 'bucardo_syncdone_fullcopytest', 5);

for my $table (sort keys %tabletype) {
	$t=qq{ Second table $table got the fullcopy row};
	$result = [[undef]];
	bc_deeply($result, $dbhB, $sql{select}{$table}, $t);

	$t=qq{ Third table $table got the fullcopy row};
	$result = [[undef]];
	bc_deeply($result, $dbhC, $sql{select}{$table}, $t);
}


KILLTEST: {
sleep 1;
}

## Kill the Postgres backend for one of the kids to see how it is handled
my $SQL = "SELECT * FROM bucardo.audit_pid WHERE target='B' ORDER BY id DESC LIMIT 1";
my $info = $dbhX->prepare($SQL);
$info->execute();
$info = $info->fetchall_arrayref({})->[0];
my $pid = $info->{'target_backend'};
my $kidid = $info->{'id'};
kill 15 => $pid;

$dbhA->do("UPDATE bucardo_test1 SET id = id + 100, inty=inty + 100");
$dbhA->commit();
$bct->ctl('kick fullcopytest 0');

$SQL = "SELECT * FROM bucardo.audit_pid WHERE id = ?";
$info = $dbhX->prepare($SQL);
$info->execute($kidid);
$info = $info->fetchall_arrayref({})->[0];

$t = 'Kid death was detected and entered in audit_pid table';
like ($info->{death}, qr{target error: 7}, $t);

sleep 2;
## Latest kid should have a life of 2
$SQL = "SELECT * FROM bucardo.audit_pid WHERE target='B' ORDER BY id DESC LIMIT 1";
$info = $dbhX->prepare($SQL);
$info->execute();
$info = $info->fetchall_arrayref({})->[0];
$t = 'Kid was resurrected by the controller after untimely death';
like ($info->{death}, qr{abnormally}, $t);

END {
	$bct->stop_bucardo($dbhX);
	$dbhX->disconnect();
	$dbhA->disconnect();
	$dbhB->disconnect();
	$dbhC->disconnect();
}

