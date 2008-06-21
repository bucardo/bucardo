#!/usr/bin/perl -- -*-cperl-*-

## Test pushdelta functionality

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

use vars qw/$SQL $sth $t $i $result $count/;

pass("*** Beginning 'pushdelta' tests");

## Start with a clean schema and databases

my $dbh = $bct->setup_database({db => 'bucardo', recreatedb => 0, recreateschema => 0});

my $dbhA = $bct->setup_database({db => 'A'});
my $dbhB = $bct->setup_database({db => 'B'});
my $dbhC = $bct->setup_database({db => 'C'});

$bct->scrub_bucardo_tables($dbh);
$bct->scrub_bucardo_target_tables($dbhA,$dbhB,$dbhC);

## Teach Bucardo about databases and tables
## TODO: Refactor this

$t=q{Add database works};
my $ctlargs = $bct->add_db_args('A');
$i = $bct->ctl("add database $ctlargs");
like($i, qr{Database added}, $t);

$ctlargs = $bct->add_db_args('B');
$i = $bct->ctl("add database $ctlargs");
like($i, qr{Database added}, $t);

$ctlargs = $bct->add_db_args('C');
$i = $bct->ctl("add database $ctlargs");
like($i, qr{Database added}, $t);

## Add a source herd
$t=q{Add herd works};
$i = $bct->ctl("add herd testherd1");
like($i, qr{Herd added}, $t);

## Add the test tables to the herd
$bct->add_test_tables_to_herd('A', 'testherd1');

$t=q{Add sync works};
$i = $bct->ctl("add sync pushdeltatest source=testherd1 type=pushdelta targetdb=B");
like($i, qr{Sync added:}, $t);

$bct->stop_bucardo($dbh);
$bct->start_bucardo($dbh);

## Check for our PIDs - from system, from q table, from logs

## Compare tables on A and B to make sure they are identical
for my $table (sort keys %tabletype) {
	compare_tables($table,$dbhA,$dbhB) or BAIL_OUT "Compare tables failed for $table\n";
}
pass 'Tables were identical before testing';

for my $table (sort keys %tabletype) {

	diag "Testing table $table\n";

	my $qtable = $dbh->quote($table);
	my $type = $tabletype{$table};
	my $val = $val{$type}{1};
	if (!defined $val) {
		BAIL_OUT "Could not determine value for $table $type\n";
	}

	$dbh->do("LISTEN bucardo_syncdone_pushdeltatest");
	$dbh->commit();

	my $pkey = $table =~ /test5/ ? q{"id space"} : 'id';

	$SQL = $table =~ /0/
		? "INSERT INTO $table($pkey) VALUES (?)"
		: "INSERT INTO $table($pkey,data1,inty) VALUES (?,'one',1)";
	$sth = $dbhA->prepare($SQL);
	if ($type eq 'BYTEA') {
		$sth->bind_param(1, undef, {pg_type => PG_BYTEA});
	}
	$sth->execute($val);
	$dbhA->commit;

	$t=qq{ Second table $table still empty before commit };
	$SQL = $table =~ /0/
		? "SELECT $pkey FROM $table"
		: "SELECT $pkey,data1 FROM $table";
	$result = [];
	bc_deeply($result, $dbhB, $SQL, $t);

	$t=q{ After insert, trigger and rule both populate droptest table };
	my $DROPSQL = $table =~ /0/
		? "SELECT type,0 FROM droptest WHERE name = $qtable ORDER BY 1,2"
		: "SELECT type,inty FROM droptest WHERE name = $qtable ORDER BY 1,2";
	my $tval = $table =~ /0/ ? 0 : 1;
	$result = [['rule',$tval],['trigger',$tval]];
	bc_deeply($result, $dbhA, $DROPSQL, $t);

	$t=q{ Table droptest is empty on remote database };
	$result = [];
	bc_deeply($result, $dbhB, $DROPSQL, $t);

	wait_for_notice($dbh, 'bucardo_syncdone_pushdeltatest');

	## Insert to A should be echoed to B, after a slight delay:
	$t=qq{ Second table $table got the pushdelta row};
	$SQL = $table =~ /0/
		? "SELECT $pkey,'one' FROM $table"
		: "SELECT $pkey,data1 FROM $table";
	$result = [[qq{$val},'one']];
	bc_deeply($result, $dbhB, $SQL, $t);

	$t=q{ Triggers and rules did not fire on remote table };
	$result = [];
	bc_deeply($result, $dbhB, $DROPSQL, $t);

	## Add a row to two, should not get removed or replicated
	my $rval = $val{$type}{9};
	$SQL = $table =~ /0/
		? "INSERT INTO $table($pkey) VALUES (?)"
		: "INSERT INTO $table($pkey,data1,inty) VALUES (?,'nine',9)";
	$sth = $dbhB->prepare($SQL);
	if ($type eq 'BYTEA') {
		$sth->bind_param(1, undef, {pg_type => PG_BYTEA});
	}
	$sth->execute($rval);
	$dbhB->commit;

	## Another source change, but with a different trigger drop method
	$SQL = "UPDATE sync SET disable_triggers = 'SQL'";
	$dbh->do($SQL);
	$dbh->do("NOTIFY bucardo_reload_sync_pushdeltattest");
	$dbh->commit();

	$val = $val{$type}{2};
	$SQL = $table =~ /0/
		? "INSERT INTO $table($pkey) VALUES (?)"
		: "INSERT INTO $table($pkey,data1,inty) VALUES (?,'two',2)";
	$sth = $dbhA->prepare($SQL);
	if ($type eq 'BYTEA') {
		$sth->bind_param(1, undef, {pg_type => PG_BYTEA});
	}
	$sth->execute($val);
	$dbhA->commit;

	$t=q{ After insert, trigger and rule both populate droptest table4 };
	$result = $table =~ /0/
		? [['rule',0],['rule',0],['trigger',0],['trigger',0]]
		: [['rule',1],['rule',2],['trigger',1],['trigger',2]];
	bc_deeply($result, $dbhA, $DROPSQL, $t);

	$t=q{ Table droptest has correct entries on remote database };
	my $ninezero = $table =~ /0/ ? 0 : 9;
	$result = [['rule',$ninezero],['trigger',$ninezero]];
	bc_deeply($result, $dbhB, $DROPSQL, $t);

	wait_for_notice($dbh, 'bucardo_syncdone_pushdeltatest');

	## Insert to A should be echoed to B, after a slight delay:
	$t=qq{ Second table $table got the pushdelta row};
	$SQL = $table =~ /0/
		? "SELECT $pkey FROM $table ORDER BY id"
		: "SELECT data1,inty FROM $table ORDER BY inty";
	$result = $table =~ /0/
		? [[1],[2],[9]]
		: [['one',1],['two',2],['nine',9]];
	bc_deeply($result, $dbhB, $SQL, $t);

	$t=q{ Triggers and rules did not fire on remote table };
	$result = [['rule',$ninezero],['trigger',$ninezero]];
	bc_deeply($result, $dbhB, $DROPSQL, $t);

	$t=q{ Source table did not get updated for pushdelta sync };
	my $col = $table =~ /0/ ? $pkey : 'inty';
	$SQL = "SELECT count(*) FROM $table WHERE $col = 9";
	$count = $dbhA->selectall_arrayref($SQL)->[0][0];
	is($count, 0, $t);

	## Now with many rows
	$SQL = $table =~ /0/
		? "INSERT INTO $table($pkey) VALUES (?)"
		: "INSERT INTO $table($pkey,data1,inty) VALUES (?,?,?)";
	$sth = $dbhA->prepare($SQL);
	for (3..6) {
		$val = $val{$type}{$_};
		$table =~ /0/ ? $sth->execute($val) : $sth->execute($val,'bob',$_);
	}
	$dbhA->commit;

	## Sanity check
	$t=qq{ Rows are not in target table before the kick for $table};
	$sth = $dbhB->prepare("SELECT 1 FROM $table WHERE $col BETWEEN 3 and 6");
	$count = $sth->execute();
	$sth->finish();
	is($count, '0E0', $t);

	wait_for_notice($dbh, 'bucardo_syncdone_pushdeltatest');

	$t=qq{ Second table $table got the pushdelta rows};
	$SQL = "SELECT $col FROM $table ORDER BY 1";
	$result = [['1'],['2'],['3'],['4'],['5'],['6'],['9']];
	bc_deeply($result, $dbhB, $SQL, $t);


	## Test of bytea columns
  SKIP: {
		$table =~ /0/ and skip 'Cannot test bytea on single-pkey table', 3;

		$SQL = "INSERT INTO $table($pkey,data1,inty,bite1) VALUES (?,?,?,?)";
		$sth = $dbhA->prepare($SQL);
		$val = $val{$type}{17};
		my $bite = 'FooBar';
		$sth->execute($val,'bob',17,$bite);
		$dbhA->commit;

		wait_for_notice($dbh, 'bucardo_syncdone_pushdeltatest');

		$t=qq{ Second table $table got the pushdelta rows with bytea column};
		$SQL = "SELECT bite1 FROM $table WHERE inty = 17";
		$result = [[$bite]];
		bc_deeply($result, $dbhB, $SQL, $t);

		## That was too easy, let's do some real bytea data

		$t=qq{ Second table $table got the pushdelta rows with null-containing bytea column};
		$val = $val{$type}{18};
		$bite = "Foo\0Bar";
		$sth->bind_param(4, undef, {pg_type => PG_BYTEA});
		$sth->execute($val,'bob',18,$bite);
		$dbhA->commit;

		wait_for_notice($dbh, 'bucardo_syncdone_pushdeltatest');

		$SQL = "SELECT bite1 FROM $table WHERE inty = 18";
		$result = [[$bite]];
		bc_deeply($result, $dbhB, $SQL, $t);

		## Now two bytea columns at once
		$SQL = "INSERT INTO $table($pkey,bite2,data1,inty,bite1) VALUES (?,?,?,?,?)";
		$sth = $dbhA->prepare($SQL);
		$val = $val{$type}{19};
		my ($bite1,$bite2) = ("over\0cycle", "foo\tbar\0\tbaz\0");
		$sth->bind_param(2, undef, {pg_type => PG_BYTEA});
		$sth->bind_param(5, undef, {pg_type => PG_BYTEA});
		$sth->execute($val,$bite2,'bob',19,$bite1);
		$dbhA->commit;

		wait_for_notice($dbh, 'bucardo_syncdone_pushdeltatest');

		$SQL = "SELECT bite1,bite2 FROM $table WHERE inty = 19";
		$result = [[$bite1,$bite2]];
		bc_deeply($result, $dbhB, $SQL, $t);

	}

	$dbhA->commit();
	$dbhB->commit();


} ## end each type of table


## Now connect to more than one target database at a time
$t=q{Remove sync works};
$i = $bct->ctl("remove sync pushdeltatest");
like($i, qr{Sync removed:}, $t);

## Add databases to a new group
$t=q{Add dbgroup works};
$i = $bct->ctl("add dbgroup testgroup B C=4");
like($i, qr{Group updated}, $t);

$t=q{Add sync works};
$i = $bct->ctl("add sync pushdeltatest source=testherd1 type=pushdelta targetgroup=testgroup");
like($i, qr{Sync added:}, $t);

## Make Bucardo reload the sync changes
$dbh->do('NOTIFY bucardo_mcp_reload');
$dbh->do('LISTEN bucardo_reloaded_mcp');
$dbh->commit();
wait_for_notice($dbh, 'bucardo_reloaded_mcp');

for my $table (sort keys %tabletype) {

	diag "Testing table $table\n";

	my $qtable = $dbh->quote($table);
	my $type = $tabletype{$table};
	my $val = $val{$type}{22};
	if (!defined $val) {
		BAIL_OUT "Could not determine value for $table $type\n";
	}

	$dbh->do("LISTEN bucardo_syncdone_pushdeltatest");
	$dbh->commit();

	my $pkey = $table =~ /test5/ ? q{"id space"} : 'id';

	$dbhA->do("DELETE FROM $table");

	$SQL = $table =~ /0/
		? "INSERT INTO $table($pkey) VALUES (?)"
		: "INSERT INTO $table($pkey,data1,inty) VALUES (?,'one',1)";
	$sth = $dbhA->prepare($SQL);
	if ($type eq 'BYTEA') {
		$sth->bind_param(1, undef, {pg_type => PG_BYTEA});
	}
	$sth->execute($val);
	$dbhA->commit;

	wait_for_notice($dbh, 'bucardo_syncdone_pushdeltatest');

	## Insert to A should be echoed to B and C, after a slight delay:
	$t=qq{ Database B table $table got the pushdelta row};
	$SQL = $table =~ /0/
		? "SELECT $pkey,'one' FROM $table"
		: "SELECT $pkey,data1 FROM $table";
	$result = [[qq{$val},'one']];
	bc_deeply($result, $dbhB, $SQL, $t);

	$t=qq{ Database C table $table got the pushdelta row};
	bc_deeply($result, $dbhC, $SQL, $t);

}

END {
	$bct->stop_bucardo();
}

$dbh->disconnect();
$dbhA->disconnect();
$dbhB->disconnect();
$dbhC->disconnect();

