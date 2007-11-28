#!perl

## Shared code for the bucardo tests.

use strict;
use warnings;
use Time::HiRes qw/sleep gettimeofday tv_interval/;

my $TEST_INFO_FILE = "t/bucardo.test.data";
my $TEST_SCHEMA = "bucardo_schema";

our $location;
my $testmsg  = ' ?';
my $testline = '?';
my $showline = 1;
my $showtime = 0;

## Used by the tt sub
my %timing;

## Sometimes, we want to stop as soon as we see an error
my $bail_on_error = $ENV{BUCARDO_TESTBAIL} || 0;
my $total_errors = 0;

my ($SQL,$sth,$count);
## Load the setup information from the test info file
-e $TEST_INFO_FILE or die qq{Must have a "$TEST_INFO_FILE" file for testing};
open my $bct, "<", $TEST_INFO_FILE or die qq{Could not open "$TEST_INFO_FILE": $!\n};
pass(" Opened configuration file");

our %bc;
while (<$bct>) {
	next unless /^\s*(\w\S+?):?\s+(.*?)\s*$/;
	$bc{$1} = $2; ## no critic
}
$bc{TESTPW}  ||= 'pie';
$bc{TESTPW1} ||= 'pie';
$bc{TESTPW2} ||= 'pie';
$bc{TESTPW3} ||= 'pie';
## Quick sanity check
for my $req (qw(DBNAME DBUSER TESTDB TESTBC)) {
	for my $suffix ('','1','2') {
		exists $bc{"$req$suffix"} or die qq{Required test arg "$req$suffix" not found in config file};
	}
}
if (
	($bc{DBHOST} eq $bc{DBHOST1} and $bc{DBPORT} == $bc{DBPORT1} and $bc{TESTDB} eq $bc{TESTDB1})
	or
	($bc{DBHOST} eq $bc{DBHOST2} and $bc{DBPORT} == $bc{DBPORT2} and $bc{TESTDB} eq $bc{TESTDB2})
	or
	($bc{DBHOST1} eq $bc{DBHOST2} and $bc{DBPORT1} == $bc{DBPORT2} and $bc{TESTDB1} eq $bc{TESTDB2})
	) {
	die qq{Test databases cannot be the same!};
}



sub setup_database {

	my $type = shift;
	my $arg = shift;

	my $suffix = ($type eq 'master') ? '' : $type;

	my $dbname   = $bc{"DBNAME$suffix"};
	my $dbuser   = $bc{"DBUSER$suffix"};
	my $dbpass   = $bc{"DBPASS$suffix"};
	my $dbhost   = $bc{"DBHOST$suffix"} || '';
	my $dbport   = $bc{"DBPORT$suffix"} || '';
	my $testdb   = $bc{"TESTDB$suffix"};
	my $testuser = $bc{"TESTBC$suffix"};
	my $testpass = $bc{"TESTPW$suffix"} || 'pie';

	my $dsn = "dbi:Pg:database=$dbname";
	length $dbhost and $dsn .= ";host=$dbhost";
	length $dbport and $dsn .= ";port=$dbport";
	my $dbh;
	eval {
		$dbh = DBI->connect($dsn, $dbuser, $dbpass,
							{AutoCommit=>0,RaiseError=>1,PrintError=>0});
	};
	$@ and die "Could not connect to the database (check your t/bucardo.test.data file): $@\n";
	pass(" Connected to the database");

	## Does the test user and test database exist?
	$SQL = "SELECT 1 FROM pg_catalog.pg_user WHERE usename = ?";
	$sth = $dbh->prepare($SQL);
	my $usercount = $sth->execute($testuser);
	$sth->finish();
	$usercount=0 if $usercount eq '0E0';

	$SQL = "SELECT 1 FROM pg_catalog.pg_database WHERE datname = ?";
	$sth = $dbh->prepare($SQL);
	my $dbcount = $sth->execute($testdb);
	$sth->finish();
	$dbcount=0 if $dbcount eq '0E0';

	if ($arg->{rebuild}) {

		if (!$ENV{BUCARDO_TEST_NUKE_OKAY} and ($dbcount or $usercount)) {
			diag (($dbcount and $usercount)
				  ? qq{\nOkay to drop user "$testuser" and database "$testdb"?}
				  : $usercount
				  ? qq{Okay to drop user "$testuser"?}
				  : qq{Okay to drop database "$testdb"?});
		}
		if ($dbcount or $usercount) {
			die("As you wish!") if !$ENV{BUCARDO_TEST_NUKE_OKAY} and <> !~ /^Y/i;
			if ($dbcount) {
				$dbh->{AutoCommit} = 1;
				$dbh->do("DROP DATABASE $testdb");
				$dbh->{AutoCommit} = 0;
				pass(qq{ Dropped database "$testdb"});
			}
			if ($usercount) {
				$dbh->do("DROP USER $testuser");
				pass(qq{ Dropped user "$testuser"});
				$dbh->commit;
			}
		}
		
		$SQL = "CREATE USER $testuser SUPERUSER PASSWORD '$testpass'";
		eval { $dbh->do($SQL); };
		$@ and die qq{Could not create test superuser "$testuser": $@\n};
		pass(qq{ Created test super user "$testuser"});
		
		$dbh->{AutoCommit} = 1;
		$SQL = "CREATE DATABASE $testdb OWNER $testuser";
		eval { $dbh->do($SQL); };
		$dbh->{AutoCommit} = 0;
		$@ and die qq{Could not create test database $testdb: $@\n};
		pass(qq{ Created test database "$testdb" owned by user "$testuser"});
	}

	## Reconnect as the test user in the test database
	$dbh->disconnect();
	$dsn =~ s/database=$dbname/database=$testdb/;
	eval {
		$dbh = DBI->connect($dsn, $testuser, $testpass,
							{AutoCommit=>0,RaiseError=>1,PrintError=>0});
	};
	$@ and die "Could not connect to database: $@\n";
	pass(qq{ Connected to the test database as the test user "$testuser"});

	if (!$arg->{checklang} and !$arg->{rebuild}) {
		pass(" Skipping language checks");
		pass(" Skipping user checks");
	}
	else {
		## Do we have the languages we need?
		$sth = $dbh->prepare("SELECT 1 FROM pg_catalog.pg_language WHERE lanname = ?");
		my @languages = ('plpgsql');
		if ($type eq 'master') {
			push @languages, 'plperl', 'plperlu';
		}
		for my $lan (@languages) {
			$count = $sth->execute($lan);
			$sth->finish();
			if ($count eq '0E0') {
				$dbh->do("CREATE LANGUAGE $lan");
				$count = $sth->execute($lan);
				$sth->finish();
				$count==1 or die ("Could not create language $lan");
				$dbh->commit();
			}
		}
		pass(" All needed languages are installed");

		$dbh->do("SET escape_string_warning = 'off'");
		$dbh->do("ALTER USER $testuser SET escape_string_warning = 'off'");
		$dbh->do("ALTER USER $testuser SET DateStyle ='ISO, YMD'");

		if ($type ne 'master') {
			$dbh->do("SET client_min_messages = 'warning'");
			$dbh->do("CREATE SCHEMA $TEST_SCHEMA") unless $ENV{BUCARDO_TEST_NOCREATEDB};
			$dbh->do("SET search_path = $TEST_SCHEMA");
			$dbh->do("ALTER USER $testuser SET search_path = $TEST_SCHEMA");
		}
	}

	if ($type eq 'master') {
		add_schema_to_database($dbh);
		$dbh->do("SET search_path = bucardo, public");
	}

	$dbh->commit();

	return $dbh;

} ## end of setup_database

sub add_schema_to_database {

	my $dbh = shift;

	## Bail if schema "bucardo" already exists
	my $SQL = "SELECT count(*) FROM pg_namespace WHERE nspname = 'bucardo'";
	my $count = $dbh->selectall_arrayref($SQL)->[0][0];
	return if $count == 1;


	## Create a new schema from the local file
	my $schema_file = 'bucardo.schema';
	-e $schema_file or BAIL_OUT qq{Cannot find the file "$schema_file"!};
	open my $fh, '<', $schema_file or BAIL_OUT qq{Could not open "$schema_file": $!\n};
	my $sql='';
	my (%copy,%copydata);
	my ($copy,$insidecopy) = (0,0);
	while (<$fh>) {
		next if /^\\[^\.]/; ## Avoid psql meta-commands at top of file
		if (1==$insidecopy) {
			$copy{$copy} .= $_;
			if (/;/) {
				$insidecopy = 2;
			}
		}
		elsif (2==$insidecopy) {
			if (/^\\\./) {
				$insidecopy = 0;
			}
			else {
				push @{$copydata{$copy}}, $_;
			}
		}
		elsif (/^\s*(COPY bucardo.*)/) {
			$copy{++$copy} = $1;
			$insidecopy = 1;
		}
		else {
			$sql .= $_;
		}
	}
	close $fh or die qq{Could not close "$schema_file": $!\n};

	$dbh->do("SET escape_string_warning = 'off'");
	
	$dbh->{pg_server_prepare} = 0;

	unless ($ENV{BUCARDO_TEST_NOCREATEDB}) {
		$dbh->do($sql);

		$count = 1;
		while ($count <= $copy) {
			$dbh->do($copy{$count});
			for my $copyline (@{$copydata{$count}}) {
				$dbh->pg_putline($copyline);
			}
			$dbh->pg_endcopy();
			$count++;
		}
	}
	
	$dbh->commit();

} ## end of add_schema_to_database
	
	
sub get_bc {
	return \%bc;
}


## no critic
{
	no warnings; ## Yes, we know they are being redefined!
	sub is_deeply {
		t($_[2],$_[3] || (caller)[2]);
		return if Test::More::is_deeply($_[0],$_[1],$testmsg);
		if ($bail_on_error > $total_errors++) {
			my $line = (caller)[2];
			my $time = time;
			diag("GOT: ".Dumper $_[0]);
			diag("EXPECTED: ".Dumper $_[1]);
			BAIL_OUT "Stopping on a failed 'is_deeply' test from line $line. Time: $time";
		}
	} ## end of is_deeply
	sub like {
		t($_[2],(caller)[2]);
		return if Test::More::like($_[0],$_[1],$testmsg);
		if ($bail_on_error > $total_errors++) {
			my $line = (caller)[2];
			my $time = time;
			diag("GOT: ".Dumper $_[0]);
			diag("EXPECTED: ".Dumper $_[1]);
			BAIL_OUT "Stopping on a failed 'like' test from line $line. Time: $time";
		}
	} ## end of like
	sub pass {
		t($_[0],$_[1]||(caller)[2]);
		Test::More::pass($testmsg);
	} ## end of pass
	sub is {
		t($_[2],(caller)[2]);
		return if Test::More::is($_[0],$_[1],$testmsg);
		if ($bail_on_error > $total_errors++) {
			my $line = (caller)[2];
			my $time = time;
			BAIL_OUT "Stopping on a failed 'is' test from line $line. Time: $time";
		}
	} ## end of is
	sub isa_ok {
		t("Object isa $_[1]",(caller)[2]);
		my ($name, $type, $msg) = ($_[0],$_[1]);
		if (ref $name and ref $name eq $type) {
			Test::More::pass($testmsg);
			return;
		}
		$bail_on_error > $total_errors++ and BAIL_OUT "Stopping on a failed test";
	} ## end of isa_ok
	sub ok {
		t($_[1]||$testmsg);
		return if Test::More::ok($_[0],$testmsg);
		if ($bail_on_error > $total_errors++) {
			my $line = (caller)[2];
			my $time = time;
			BAIL_OUT "Stopping on a failed 'ok' test from line $line. Time: $time";
		}
	} ## end of ok
}
## use critic
sub tt {
	## Simple timing routine. Call twice with the same arg, before and after
	my $name = shift or die qq{Need a name!\n};
	if (exists $timing{$name}) {
		my $newtime = tv_interval($timing{$name});
		warn "Timing for $name: $newtime\n";
		delete $timing{$name};
	}
	else {
		$timing{$name} = [gettimeofday];
	}
	return;
} ## end of tt

sub t {
	$testmsg = shift;
	$testline = shift || (caller)[2];
	$testmsg =~ s/^\s+//;
	if ($location) {
		$testmsg = "($location) $testmsg";
	}
	if ($showline) {
		$testmsg .= " [line: $testline]";
	}
	if ($showtime) {
		my $time = time;
		$testmsg .= " [time: $time]";
	}
	return;
} ## end of t


1;

