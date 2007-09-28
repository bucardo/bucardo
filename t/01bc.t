#!perl

use strict;
use warnings;
use Data::Dumper;
use DBI;
use IO::Handle;
use Test::More;
use Time::HiRes qw/sleep gettimeofday tv_interval/;
use Test::Dynamic '1.3.1';

## Running all the tests can take quite a while
## This allows us to only run a subset while debugging
our $TEST_METHODS       = 1;
our $TEST_CONFIG        = 1;
our $TEST_PURGE         = 1;
our $TEST_PUSHDELTA     = 1;
our $TEST_MAKEDELTA     = 1;
our $TEST_COPY          = 1;
our $TEST_SWAP          = 1;
our $TEST_CUSTOM_CODE   = 1;
our $TEST_PING          = 1;

our $TEST_RANDOM_SWAP   = 0;

## Count the number of tests
my $tests = Test::Dynamic::count_tests
	(
	 {
	  filehandle => \*DATA,
	  verbose    => 1,
	  local      => [qw(bc_deeply compare_tables)]
	  }
	 );

plan tests => $tests;

my $location = 'setup';
my $testmsg  = ' ?';
my $testline = '?';
my $showline = 0;
my $showtime = 0;

## Run perlcritic against the main source file, using custom rules
SKIP: {

	if (!$ENV{BUCARDO_TEST_CRITIC}) {
		skip 'Set BUCARDO_TEST_CRITIC to run Perl::Critic tests', 2;
	}

	eval {
		require Perl::Critic;
	};

	if ($@) {
		skip 'Module Perl::Critic not available', 2;
	}

	## Gotta have a profile
	my $PROFILE = '.perlcriticrc';
	if (! -e $PROFILE) {
		skip qq{Perl::Critic profile "$PROFILE" not found\n}, 2;
	}

	## Gotta have our code
	my $CODE = './Bucardo.pm';
	if (! -e $CODE) {
		skip qq{Perl::Critic cannot find "$CODE" to test with\n}, 2;
	}

	pass(" Running Perl::Critic on Bucardo.pm");
	my $critic = Perl::Critic->new(-profile => $PROFILE);
	my @problems = $critic->critique($CODE);
	is(@problems, 0, "Passed Perl::Critic run");

};

## Once we reach a certain point, we may need to shutdown our test Bucardo processes
our $need_shutdown = 0;

## Used by the tt sub
my %timing;

## To avoid stepping on other instance's toes
my $PIDDIR = "/tmp/bucardo_testing_$ENV{USER}";
mkdir $PIDDIR if ! -e $PIDDIR;
my $PIDFILE = "bucardo_testing.pid";
my $TEST_INFO_FILE = "t/bucardo.test.data";
my $TEST_SCHEMA = "bucardo_schema";
my $REASONFILE = "/tmp/bucardo_testing_reason_$ENV{USER}";
$ENV{BUCARDO_SENDMAIL_FILE} = 'bucardo_test.email';
$ENV{BUCARDO_NOSENDMAIL} = 1;
if (! exists $ENV{BUCARDO_TEST_NUKE_OKAY}) {
    $ENV{BUCARDO_TEST_NUKE_OKAY} = 1;
}

# Set a semi-unique name to make killing old tests easier
my $xname = "bctest_$ENV{USER}";

my $DEBUGDIR = ".";
-e $DEBUGDIR or mkdir $DEBUGDIR;

## Maximum time to wait for bucardo_ctl to return
my $ALARM_BUCARDO_CTL = 100;
## Maximum time to wait for a kid to appear via pg_listener
my $ALARM_WAIT4KID = 10;

## How long to wait for most syncs to take effect?
my $TIMEOUT_SYNCWAIT = 20;
## How long to sleep between checks for sync being done?
my $TIMEOUT_SLEEP = 0.1;
## How long to wait for a notice to be issued?
my $TIMEOUT_NOTICE = 10;

my $DEBUG = 0;

*STDOUT->autoflush(1);
*STDERR->autoflush(1);

use vars qw($masterdbh $SQL $sth $sth2 %sth $dbh1 $dbh2 $dbh3 $result $result2 $info $count);
use vars qw($type $now $now2 $val $val2 $t $got $expected);

## Sometimes, we want to stop as soon as we see an error
my $bail_on_error = $ENV{BUCARDO_TESTBAIL} || 0;
my $total_errors = 0;

eval { require Bucardo; };
$@ and BAIL_OUT qq{Could not load the Bucardo module: $@};
pass(" Bucardo module loaded");

## Load the setup information from the test info file
-e $TEST_INFO_FILE or BAIL_OUT qq{Must have a "$TEST_INFO_FILE" file for testing};
open my $bct, "<", $TEST_INFO_FILE or BAIL_OUT qq{Could not open "$TEST_INFO_FILE": $!\n};
pass(" Opened configuration file");

our %bc;
while (<$bct>) {
	next unless /^\s*(\w\S+?):?\s+(.*?)\s*$/;
	$bc{$1} = $2; ## no critic
}
$bc{TESTPW}  ||= 'pie';
$bc{TESTPW1} ||= 'pie';
$bc{TESTPW2} ||= 'pie';
## Quick sanity check
for my $req (qw(DBNAME DBUSER TESTDB TESTBC)) {
	for my $suffix ('','1','2') {
		exists $bc{"$req$suffix"} or BAIL_OUT qq{Required test arg "$req$suffix" not found in config file};
	}
}
if (
	($bc{DBHOST} eq $bc{DBHOST1} and $bc{DBPORT} == $bc{DBPORT1} and $bc{TESTDB} eq $bc{TESTDB1})
	or
	($bc{DBHOST} eq $bc{DBHOST2} and $bc{DBPORT} == $bc{DBPORT2} and $bc{TESTDB} eq $bc{TESTDB2})
	or
	($bc{DBHOST1} eq $bc{DBHOST2} and $bc{DBPORT1} == $bc{DBPORT2} and $bc{TESTDB1} eq $bc{TESTDB2})
	) {
	BAIL_OUT qq{Test databases cannot be the same!};
}

## We use alarms a bit so we can wait a certain amount of time
local $SIG{ALRM} = sub { die "Timed out"; };

## Shut down any existing tests
shutdown_bucardo();

## Connect to the main database and set things up
$masterdbh = setup_database('master');

## Same for our test databases
$dbh1 = setup_database(1);
$dbh2 = setup_database(2);
$dbh3 = setup_database(3);

my %dbmap = (
			 $masterdbh => 'master',
			 $dbh1      => 'one',
			 $dbh2      => 'two',
			 $dbh3      => 'three',
);

## Make sure the bucardo_ctl helper is running
bucardo_ctl("--help", 5);
pass(" Helper script bucardo.test.helper appears to be running");

if (!exists $ENV{BUCARDO_KEEP_OLD_DEBUG}) {
	my $dirh;
	opendir $dirh, $DEBUGDIR;
	for my $file (grep { -f "$DEBUGDIR/$_" and $_ =~ /^log\.bucardo(?:\....\.\d+(?:~\d+~)*)*$/ } readdir($dirh)) {
		unlink "$DEBUGDIR/$file";
	}
	closedir $dirh;
}

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

$masterdbh->do("SET escape_string_warning = 'off'");

$masterdbh->{pg_server_prepare} = 0;

; ## ENV_BUCARDO_TEST_NOCREATEDB TESTCOUNT - 16

unless ($ENV{BUCARDO_TEST_NOCREATEDB}) {
	$masterdbh->do($sql);

	$count = 1;
	while ($count <= $copy) {
		$masterdbh->do($copy{$count});
		for my $copyline (@{$copydata{$count}}) {
			$masterdbh->pg_putline($copyline);
		}
		$masterdbh->pg_endcopy();
		$count++;
	}
}

$masterdbh->commit();
pass(" Bucardo master schema was created");

## Set up the config table
$masterdbh->do("UPDATE bucardo.bucardo_config SET value='$PIDDIR' WHERE setting = 'piddir'");
$masterdbh->do("UPDATE bucardo.bucardo_config SET value='$PIDFILE' WHERE setting = 'pidfile'");
$masterdbh->do("UPDATE bucardo.bucardo_config SET value='$REASONFILE' WHERE setting = 'reason_file'");
$masterdbh->commit();

$masterdbh->do("ALTER USER $bc{TESTBC} SET search_path = bucardo, public");
$masterdbh->commit();

## Create a new Bucardo instance
my $bc;
eval {
	$bc = Bucardo->new
		({
		  dbhost      => $bc{DBHOST},
		  dbport      => $bc{DBPORT},
		  dbname      => $bc{TESTDB},
		  dbuser      => $bc{TESTBC},
		  dbpass      => $bc{TESTPW},
		  verbose     => 0,
		  debugsyslog => 0,
		  debugstderr => 0,
		  debugstdout => 0,
		  debugfile   => 1,
		  cleandebugs => 0,
		  debugsql    => 0,
		  bcquiet     => 1,
		  });
};
$@ and BAIL_OUT "Could not create Bucardo instance: $@";

isa_ok($bc, 'Bucardo');

$need_shutdown = 1;

$masterdbh->commit();

## Assign names to the databases, and set some common things for each one
my %db = (
		  $dbh1 => 'bctest1',
		  $dbh2 => 'bctest2',
		  $dbh3 => 'bctest3',
		  );

## Create our test tables, one for each major data type we handle
my %tabletype =
	(
	 'bucardo_test1' => 'INT',
	 'bucardo_test2' => 'TEXT',
	 'bucardo_test3' => 'DATE',
	 'bucardo_test4' => 'TIMESTAMP',
 );

my %table; ## This will hold the oids

## Used for rule testing
$SQL = qq{
	CREATE TABLE droptest (
		name TEXT NOT NULL,
		type TEXT NOT NULL,
		inty INTEGER NOT NULL
	)
};
unless ($ENV{BUCARDO_TEST_NOCREATEDB}) {
	$dbh1->do($SQL);
	$dbh2->do($SQL);
	$dbh3->do($SQL);
}

## Used for trigger testing
## no critic
$SQL = q{
	CREATE OR REPLACE FUNCTION trigger_test()
	RETURNS trigger
	LANGUAGE plpgsql
	AS $_$ BEGIN
		INSERT INTO droptest(name,type,inty) VALUES (TG_RELNAME, 'trigger', NEW.inty);
		RETURN NULL;
		END;
	$_$
};
## use critic

$dbh1->do($SQL);
$dbh2->do($SQL);
$dbh3->do($SQL);


for my $table (sort keys %tabletype) {
	$SQL = qq{
		CREATE TABLE $table (
			id    $tabletype{$table} NOT NULL PRIMARY KEY,
        	data1 TEXT                   NULL,
	        inty  SMALLINT               NULL,
    	    email TEXT                   NULL UNIQUE
		)
	};
	unless ($ENV{BUCARDO_TEST_NOCREATEDB}) {
		$dbh1->do($SQL);
		$dbh2->do($SQL);
		$dbh3->do($SQL);
	}

	## Create a trigger to test trigger supression during syncs
	$SQL = qq{
		CREATE TRIGGER bctrig_$table
		AFTER INSERT OR UPDATE ON $table
		FOR EACH ROW EXECUTE PROCEDURE trigger_test()
	};
	unless ($ENV{BUCARDO_TEST_NOCREATEDB}) {
		$dbh1->do($SQL);
		$dbh2->do($SQL);
		$dbh3->do($SQL);
	}

	## Create a rule to test rule supression during syncs
	$SQL = qq{
		CREATE OR REPLACE RULE bcrule_$table
		AS ON INSERT TO $table
		DO ALSO INSERT INTO droptest(name,type,inty) VALUES ('$table','rule',NEW.inty)
	};
	$dbh1->do($SQL);
	$dbh2->do($SQL);
	$dbh3->do($SQL);

	## Get the oid back out:
	$SQL = qq{
		SELECT c.oid
		FROM   pg_catalog.pg_class c, pg_catalog.pg_namespace n
		WHERE  c.relnamespace = n.oid
		AND    n.nspname = ?
		AND    relkind = 'r'
		AND    relname = ?
	};
	$sth = $dbh1->prepare($SQL);
	$count = $sth->execute($TEST_SCHEMA, $table);
	BAIL_OUT(qq{No oid for "$table"}) unless 1==$count;
	$table{$dbh1}{$table} = $sth->fetchall_arrayref()->[0][0];

	$sth = $dbh2->prepare($SQL);
	$count = $sth->execute($TEST_SCHEMA, $table);
	BAIL_OUT(qq{No oid for "$table"}) unless 1==$count;
	$table{$dbh2}{$table} = $sth->fetchall_arrayref()->[0][0];

	$sth = $dbh3->prepare($SQL);
	$count = $sth->execute($TEST_SCHEMA, $table);
	BAIL_OUT(qq{No oid for "$table"}) unless 1==$count;
	$table{$dbh3}{$table} = $sth->fetchall_arrayref()->[0][0];

} ## end creating each table
pass(" Create test tables on remote databases");

## We must commit as we will not be connecting from this session
$dbh1->commit();
$dbh2->commit();
$dbh3->commit();

## Prepare some test values for easy use
my %val;
for (1..30) {
	$val{INT}{$_} = $_;
	$val{TEXT}{$_} = "bc$_";
	$val{DATE}{$_} = sprintf "2001-10-%02d", $_;
	$val{TIMESTAMP}{$_} = $val{DATE}{$_} . " 12:34:56";
}

if ($ENV{BUCARDO_TEST_NOCREATEDB}) {
	$masterdbh->do("DELETE FROM q");
	$masterdbh->do("DELETE FROM sync");
	$masterdbh->do("DELETE FROM goat");
	$masterdbh->do("DELETE FROM db");
	$masterdbh->commit();
}


## Add in our test databases
$bc->database({
	name   => $db{$dbh1},
	dbhost => $bc{DBHOST1},
	dbport => $bc{DBPORT1},
	dbname => $bc{TESTDB1},
	dbuser => $bc{TESTBC1},
	dbpass => $bc{TESTPW1}
});

$bc->database({
	name   => $db{$dbh2},
	dbhost => $bc{DBHOST2},
	dbport => $bc{DBPORT2},
	dbname => $bc{TESTDB2},
	dbuser => $bc{TESTBC2},
	dbpass => $bc{TESTPW2}
});

$bc->database({
	name   => $db{$dbh3},
	dbhost => $bc{DBHOST3},
	dbport => $bc{DBPORT3},
	dbname => $bc{TESTDB3},
	dbuser => $bc{TESTBC3},
	dbpass => $bc{TESTPW3}
});

pass(" Added in databases");

## Add all the goats
my $herd1 = "bctestherd1";
my $herd2 = "bctestherd2";
for my $table (sort keys %tabletype) {
	for my $db (1..2) {
		$bc->goat
			({
			  db         => "bctest$db",
			  schemaname => $TEST_SCHEMA,
			  tablename  => $table,
			  herd       => "bctestherd$db",
			  pkey       => 'id',
			  pkeytype   => lc $tabletype{$table},
			  standard_conflict => 'source',
		  });
	}
}

if ($TEST_PING) { ## START_TEST_PING

	$location = 'ping';
	pass(" Begin TEST_PING section");

	ping_testing();

} ## STOP_TEST_PING


if ($TEST_METHODS) { ## START_TEST_METHODS

	## Test methods to change things in the Bucardo database

	$location = 'methods';
	pass(" Begin TEST_METHODS section");

	clean_all_tables();

	shutdown_bucardo();

	test_customcode_methods();

	test_database_methods();

	test_goat_methods();

	test_sync_methods();

} ## STOP_TEST_METHODS

if ($TEST_CONFIG) { ## START_TEST_CONFIG

	$location = 'config';
	pass(" Begin TEST_CONFIG section");

	shutdown_bucardo();

	test_config();

} ## STOP_TEST_CONFIG

if ($TEST_PURGE) { ## START_TEST_PURGE

	$location = 'purge';
	pass(" Begin TEST_PURGE section");

	shutdown_bucardo();

	clean_all_tables();

	test_purge();

} ## STOP_TEST_PURGE

if ($TEST_PUSHDELTA) { ## START_TEST_PUSHDELTA

	$location = 'pushdelta';
	pass(" Begin TEST_PUSHDELTA section");

	shutdown_bucardo();
	clean_all_tables();

	## Setup a pushdelta sync
	$bc->sync
		({
		  name             => 'pushdeltatest',
		  source           => 'bctestherd1',
		  targetdb         => 'bctest2',
		  synctype         => 'pushdelta',
	  });

	bucardo_ctl("start 'Start pushdelta testing'");
	wait4kid('bucardo_q_pushdeltatest_bctest2');
	pass(" Bucardo was started");

	for my $table (sort keys %tabletype) {
		basic_pushdelta_testing($table,$dbh1,$dbh2); ## TESTCOUNT * 4
	}

	pass(" Finished with pushdelta tests");

} ## STOP_TEST_PUSHDELTA

if ($TEST_MAKEDELTA) { ## START_TEST_MAKEDELTA

	$location = 'makedelta';
	pass(" Begin TEST_MAKEDELTA section");

	shutdown_bucardo();
	clean_all_tables();

	## Test makedelta column
	$bc->sync
		({
		  name      => 'makedeltatest',
		  source    => 'bctestherd1',
		  targetdb  => 'bctest2',
		  synctype  => 'swap',
          makedelta => 1,
	  });

	bucardo_ctl("start 'Start makedelta testing'");
	wait4kid('bucardo_q_makedeltatest_bctest2');
	pass(" Bucardo was started");

	for my $table (sort keys %tabletype) {
		makedelta_testing($table,$dbh1,$dbh2); ## TESTCOUNT * 4
	}

	pass(" Finished with makedelta tests");

} ## STOP_TEST_MAKEDELTA

if ($TEST_COPY) { ## START_TEST_COPY

	$location = 'copy';
	pass(" Begin TEST_COPY section");

	shutdown_bucardo();

	clean_all_tables();

	## Test full push
	$bc->sync
		({
		  name             => 'copytest',
		  source           => 'bctestherd1',
		  targetdb         => 'bctest2',
		  synctype         => 'fullcopy',
		  disable_triggers => 'replica',
		  disable_rules    => 'replica',
	  });

	bucardo_ctl("start 'Start fullcopy testing'");

	wait4kid('bucardo_q_copytest_bctest2');
	pass(" Bucardo was started");

	for my $table (sort keys %tabletype) {
		basic_copy_testing($table,$dbh1,$dbh2); ## TESTCOUNT * 4
	}

	analyze_after_copy('bucardo_test1',$dbh1,$dbh2);

	pass(" Finished with fullcopy tests");

} ## STOP_TEST_COPY

if ($TEST_SWAP) { ## START_TEST_SWAP

	$location = 'swap';
	pass(" Begin TEST_SWAP section");

	shutdown_bucardo();
	clean_all_tables();

	## Test swap
	$bc->sync
		({
		  name              => 'swaptest',
		  source            => 'bctestherd2',
		  targetdb          => 'bctest1',
		  synctype          => 'swap',
	  });
	bucardo_ctl("start 'Start swap testing'");
	wait4kid('bucardo_q_swaptest_bctest1');
	pass(" Bucardo was started");

	## Check that each table type populates bucardo_delta
	for my $table (sort keys %tabletype) {
		bucardo_delta_populate($table,$dbh1); ## TESTCOUNT * 4
		bucardo_delta_populate($table,$dbh2); ## TESTCOUNT * 4
	}
	$dbh1->rollback();
	$dbh2->rollback();

	## Test the swap sync method
	for my $table (sort keys %tabletype) {
		basic_swap_testing($table,$dbh1,$dbh2); ## TESTCOUNT * 4
		basic_swap_testing($table,$dbh2,$dbh1); ## TESTCOUNT * 4
	}

	pass(" Finished with swap tests");

} ## STOP_TEST_SWAP

if ($TEST_CUSTOM_CODE) { ## START_TEST_CUSTOM_CODE

	$location = 'customcode';
	pass(" Begin TEST_CUSTOM_CODE section");

	shutdown_bucardo();
	clean_all_tables(); ## TEST_CUSTOM_CODE

	## Test custom code
	$bc->sync
		({
		  name      => 'customcode',
		  source    => 'bctestherd1',
		  targetdb  => 'bctest2',
		  synctype  => 'swap', ## separate pushdelta later?
	  });

	bucardo_ctl("start 'Start customcode testing'");
	wait4kid('bucardo_q_customcode_bctest2');
	pass(" Bucardo was started");

	for my $table (sort keys %tabletype) {
		test_customcode($table,$dbh1,$dbh2); ## TESTCOUNT * 4
	}

	pass(" Finished with custom_code tests");

} ## STOP_TEST_CUSTOM_CODE

if ($TEST_RANDOM_SWAP) { ## START_TEST_RANDOM_SWAP

	$location = 'randomswap';
	pass(" Begin TEST_RANDOM_SWAP section");

	shutdown_bucardo();
	clean_all_tables();

	## Test swap
	$bc->sync
		({
		  name              => 'swaptest',
		  source            => 'bctestherd2',
		  targetdb          => 'bctest1',
		  synctype          => 'swap',
	  });
	bucardo_ctl("start 'Start random swap testing'");
	wait4kid('bucardo_q_swaptest_bctest1');
	pass(" Bucardo was started");

	for my $table (sort keys %tabletype) {
		random_swap_testing($table,$dbh1,$dbh2); ## TESTCOUNT * 4
	}

	pass(" Finished with random swap tests");

} ## STOP_TEST_RANDOM_SWAP

exit;

END {
	if ($need_shutdown) {
		diag "\nLeaving, shutting down any running processes";
		bucardo_ctl("stop 'Stop the testing'");

		if ($masterdbh) {
			$masterdbh->rollback();
			$masterdbh->disconnect();
		}
		$dbh1 and $dbh1->disconnect();
		$dbh2 and $dbh2->disconnect();
		system("/bin/rm -fr $PIDDIR/*.pid");
		system("touch $PIDDIR/fullstopbucardo");
	}

	## Kill our test program if running
	## TODO : Clean this up
	if ($^O !~ /Win/) {		
		for (split /\n/ => qx{/bin/ps w}) {
			next if /^\s*$$\s/;
			if (m{(\d+).*perl t/bucardo.test.helper\b}) {
				kill 15, $1;
				last;
			}
		}
	}
	exit;
}



sub setup_database {

	my $type = shift;
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
	$@ and BAIL_OUT "Could not connect to the database (check your t/bucardo.test.data file): $@\n";
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

	unless ($ENV{BUCARDO_TEST_NOCREATEDB}) {

		if (!$ENV{BUCARDO_TEST_NUKE_OKAY} and ($dbcount or $usercount)) {
			diag (($dbcount and $usercount)
				  ? qq{\nOkay to drop user "$testuser" and database "$testdb"?}
				  : $usercount
				  ? qq{Okay to drop user "$testuser"?}
				  : qq{Okay to drop database "$testdb"?});
		}
		if ($dbcount or $usercount) {
			BAIL_OUT("As you wish!") if !$ENV{BUCARDO_TEST_NUKE_OKAY} and <> !~ /^Y/i;
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
		
		$SQL = "CREATE USER $testuser SUPERUSER PASSWORD 'pie'";
		eval { $dbh->do($SQL); };
		$@ and BAIL_OUT qq{Could not create test superuser "$testuser": $@\n};
		pass(qq{ Created test super user "$testuser"});
		
		$dbh->{AutoCommit} = 1;
		$SQL = "CREATE DATABASE $testdb OWNER $testuser";
		eval { $dbh->do($SQL); };
		$dbh->{AutoCommit} = 0;
		$@ and BAIL_OUT qq{Could not create test database $testdb: $@\n};
		pass(qq{ Created test database "$testdb" owned by user "$testuser"});
	}

	## Reconnect as the test user in the test database
	$dbh->disconnect();
	$dsn =~ s/database=$dbname/database=$testdb/;
	eval {
		$dbh = DBI->connect($dsn, $testuser, $testpass,
							{AutoCommit=>0,RaiseError=>1,PrintError=>0});
	};
	$@ and BAIL_OUT "Could not connect to database: $@\n";
	pass(qq{ Connected to the test database as the test user "$testuser"});

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
			$count==1 or BAIL_OUT ("Could not create language $lan");
			$dbh->commit();
		}
	}
	pass(" All needed languages are installed");

	$dbh->do("SET escape_string_warning = 'off'");
	$dbh->do("ALTER USER $testuser SET escape_string_warning = 'off'");

	if ($type ne 'master') {
		$dbh->do("SET client_min_messages = 'warning'");
		$dbh->do("CREATE SCHEMA $TEST_SCHEMA") unless $ENV{BUCARDO_TEST_NOCREATEDB};
		$dbh->do("SET search_path = $TEST_SCHEMA");
		$dbh->do("ALTER USER $testuser SET search_path = $TEST_SCHEMA");
	}

	$dbh->commit();

	return $dbh;

} ## end of setup_database


sub shutdown_bucardo {

	my $STOPFILE = "$PIDDIR/fullstopbucardo";
	open my $stop, '>', $STOPFILE or die qq{Could not create "$STOPFILE": $!\n};
	print {$stop} "Stopped by $0 on " . (scalar localtime) . "\n";
	close $stop or warn qq{Could not close "$STOPFILE": $!\n};

	pass(" Existing Bucardo asked to shut down");

    ## Find a grep we can use.

    my $grep_path = $ENV{GREP_PATH}     ||
                    qx!which grep!      ||
                    qx!whereis -b grep!;
    if (defined $grep_path) {
        $grep_path =~ s!\Agrep: /!!;
        $grep_path =~ s!\n\z!!;
    }
    else {
        for my $path (grep { -x } qw!/bin/grep /usr/bin/grep /usr/local/bin/grep!) {
            $grep_path = $path;
            last;
        }
    }

    BAIL_OUT q!Cannot find a usable "grep"; set GREP_PATH!
        unless -x $grep_path;

	my $loop = 1;
	{
		my $res = qx{/bin/ps -Afwww | $grep_path Bucardo | $grep_path $xname | $grep_path -v grep}; ## no critic
		last if $res !~ /Bucardo/m;
		if ($loop++ > 10) {
			BAIL_OUT "Could not persuade existing Bucardo to shut down\n";
		}
		sleep 1;
		redo;
	}

	return;

} ## end of shutdown_bucardo


sub wait_until_true {

	my $xline = (caller)[2];
	my $dbh = shift or die "Need a database handle (from line $xline)\n";
	my $sql = shift or die "Need a SQL statement (from line $xline)\n";
	my $timeout = shift || $TIMEOUT_SYNCWAIT;
	my $sleep = shift || $TIMEOUT_SLEEP;
	my $type = shift || 'true';
	my $line = shift || $xline;

	alarm $timeout;
	$sth = $dbh->prepare($sql);
	eval {
	  W: {
			$count = $sth->execute();
			$sth->finish();
			$dbh->commit();
			last if ($type eq 'true' and $count >= 1) or ($type eq 'false' and $count < 1);
			sleep $sleep;
			redo;
		}
	};
	$count = alarm 0;
	return $count unless $@;
	my $db = $dbmap{$dbh} || '?';
	BAIL_OUT (qq{Gave up waiting for "$sql" on db "$db" to be $type: timed out at $timeout from line $line ($@)});
	return;

} ## end of wait_until_true


sub wait_until_false {

	my $xline = (caller)[2];
	my $dbh = shift or die "Need a database handle (from line $xline)\n";
	my $sql = shift or die "Need a SQL statement (from line $xline)\n";
	my $timeout = shift || $TIMEOUT_SYNCWAIT;
	my $sleep = shift || $TIMEOUT_SLEEP;
	return wait_until_true($dbh,$sql,$timeout,$sleep,'false',$xline);

} ## end of wait_until_false

sub clean_all_tables {

	## Reset out test databases to their original state
	## Empty out all data from the tables
	## Remove any triggers added
	## Drop the helper bucardo schema

	$masterdbh->rollback();
	$masterdbh->do("DELETE FROM sync"); ## Needs to go first due to trigger tek

	for my $dbh ($dbh1,$dbh2,$dbh3) {
		$dbh->rollback();
		for my $table (sort keys %tabletype) {
			$dbh->do("TRUNCATE TABLE $table");
			$SQL = "SELECT tgname FROM pg_trigger WHERE tgrelid = (SELECT oid FROM pg_class WHERE relname = '$table')";
			for (@{$dbh->selectall_arrayref($SQL)}) {
				next if $_->[0] =~ /^bctrig_/o;
				$dbh->do("DROP TRIGGER $_->[0] ON $table");
			}
		}
		## Nuke the entire bucardo schema if it exists
		if (object_count($dbh,'bucardo','schema','')) {
			$dbh->do("DROP SCHEMA bucardo CASCADE");
		}
		$dbh->do("TRUNCATE TABLE droptest");
		$dbh->commit();
	}
	$masterdbh->do("DELETE FROM q");
	$masterdbh->do("DELETE FROM audit_pid");
	$masterdbh->commit();
	pass(" Finished clean_all_tables");
	return;

} ## end of clean_all_tables

sub clean_swap_table {

	## Empty out swap table and associated tables on one or more databases
	my ($table,$dbs) = @_;
	for my $dbh (@{$dbs}) {
		$dbh->rollback;
	}
	for my $dbh (@{$dbs}) {
		my $oid = $table{$dbh}{$table};
		$dbh->do("DELETE FROM $table");
		$dbh->do("DELETE FROM bucardo.bucardo_track WHERE tablename = $oid");
		$dbh->do("DELETE FROM bucardo.bucardo_delta WHERE tablename = $oid");
		$dbh->do("DELETE FROM bucardo.bucardo_track");
		$dbh->do("DELETE FROM bucardo.bucardo_delta");
		$dbh->commit;
	}
	return;

} ## end of clean_swap_table


sub table_exists {

	my ($dbh,$table) = @_;
	my $schema = '';
	if ($table =~ /(\w+)\.(\w+)/) {
		($schema,$table) = ($1,$2);
	}
	$SQL = "SELECT count(*) FROM information_schema.tables WHERE table_name = ".$dbh->quote($table);
	if ($schema) {
		$SQL .= "AND table_schema = ".$dbh->quote($schema);
	}
	return $dbh->selectall_arrayref($SQL)->[0][0];

} ## end of table_exists


sub object_count {

	## See if an object exists in a database. Returns number of objects found, usually 0 or 1

	my ($dbh,$schema,$type,$name) = @_;

	if ('table' eq $type) {
		$SQL = "SELECT 1 FROM pg_class c, pg_namespace n WHERE c.relnamespace=n.oid".
			" AND n.nspname = ? AND c.relname = ?";
	}
	elsif ('function' eq $type) {
		$SQL = "SELECT 1 FROM pg_proc p, pg_namespace n WHERE p.pronamespace = n.oid".
			" AND n.nspname = ? AND p.proname = ?";
	}
	elsif ('schema' eq $type) {
		$SQL = "SELECT 1 FROM pg_namespace n WHERE nspname = ? AND 'null'<>?";
	}
	else {
		die "Invalid type: $type\n";
	}

	$sth = $dbh->prepare_cached($SQL);
	$count = $sth->execute($schema,$name);
	$sth->finish();
	return $count >= 1 ? $count : 0;

} ## end of object_count

sub bucardo_ctl {

	## Use a helper program to safely invoke bucardo_ctl with the given args

	my $command = shift;
	my $timeout = shift || $ALARM_BUCARDO_CTL;

	if ($command =~ /kick/io) {
		$dbh1->commit();
		$dbh2->commit();
		$dbh3->commit();
		$masterdbh->commit();
	}

	## diag "Starting bucardo_ctl with $command I am $$\n";
	my $controlfile = "bucardo_test_control";
	my $tmpfile = "$controlfile.tmp";
	open my $fh, ">", $tmpfile or die qq{Could not open "$tmpfile": $!\n};
	my $text = "$command --dbname=$bc{TESTDB} --dbuser=$bc{TESTBC} --dbpass=$bc{TESTPW} --ctlquiet ".
		"--debugstderr=0 --debugstdout=0 --debugfile=1 --verbose=1 --sendmail=0 ".
		"--cleandebugs=0 --debugdir=$DEBUGDIR ".
		"--extraname=$xname";
	$bc{DBPORT} and $text .= " --dbport=$bc{DBPORT}";
	$bc{DBHOST} and $text .= " --dbhost=$bc{DBHOST}";
	$DEBUG and diag "Called bucardo_ctl with: $text";
	print $fh "$text\n";
	close $fh or die qq{Could not close "$tmpfile": $!\n};
	rename $tmpfile, $controlfile or die qq{Could not rename $tmpfile to $controlfile\n};
	## Wait until it is deleted

	alarm $timeout;
	eval {
	  S: {
			last if ! -e $controlfile;
			sleep 0.1;
			redo;
		}
	};
	$count = alarm 0;

	if (!$@) {
		## diag "End bucardo_ctl with $command\n";
		return $count;
	}

	if ($@ =~ /Timed out/) {
		system("touch $PIDDIR/fullstopbucardo");
		BAIL_OUT("bucardo_ctl was not invoked: is the bucardo.test.helper file running? (command=$command)");
	}
	BAIL_OUT("bucardo_ctl gave an error: $@");

	return;

} ## end of bucardo_ctl


sub wait4kid {

	my $notice = shift;
	my $timeout = shift || $ALARM_WAIT4KID;

	$SQL = "SELECT 1 FROM pg_catalog.pg_listener WHERE relname = ?";
	my $listen = $masterdbh->prepare($SQL);
	alarm $timeout;
	eval {
	  S: {
			$count = $listen->execute($notice);
			$listen->finish();
			last if $count == 1;
			sleep 0.1;
			redo;
		}
	};
	$count = alarm 0;
	return $count unless $@;
	BAIL_OUT("Waited too long ($timeout) for kid to LISTEN to $notice");
	return;

} ## end of wait4kid

sub wait_for_notice {

	my $dbh = shift;
	my $text = shift;
	my $timeout = shift || $TIMEOUT_NOTICE;
	my $sleep = shift || $TIMEOUT_SLEEP;

	my $n;
	alarm $timeout;
	eval {
	  N: {
			while ($n = $dbh->func('pg_notifies')) {
				last N if $n->[0] eq $text;
			}
			sleep $sleep;
			redo;
		}
	};
	$count = alarm 0;
	return $count unless $@;
	my $line = (caller)[2];
	BAIL_OUT (qq{Gave up waiting for notice "$text": timed out at $timeout from line $line ($@)});
	return;

} ## end of wait_for_notice


sub now_time {
	my $dbh = shift;
	return $dbh->selectall_arrayref("SELECT now()")->[0][0];
} ## end of now_time


sub bc_deeply {

	my ($exp,$dbh,$sql,$msg) = @_;
	my ($line) = (caller)[2];
	$msg .= " (line $line)";

	local $Data::Dumper::Terse = 1;
	local $Data::Dumper::Indent = 0;

	my $got = $dbh->selectall_arrayref($sql);

	return is_deeply($got,$exp,$msg);

} ## end of bc_deeply


sub compare_tables {

	my ($table,$sdbh,$rdbh) = @_;

	my ($line) = (caller)[2];

	local $Data::Dumper::Terse = 1;
	local $Data::Dumper::Indent = 0;

	my $msg = " ($location)  Table $table is the same on both databases";
	$DEBUG and $msg .= " (line $line)";
	$SQL = "SELECT * FROM $table ORDER BY inty, id";
	my $uno = $sdbh->selectall_arrayref($SQL);
	my $dos = $rdbh->selectall_arrayref($SQL);
	if ((Dumper $uno) eq (Dumper $dos)) {
		pass($msg);
		return 1;
	}

	return is_deeply($uno,$dos,$msg);

} ## end of compare_tables


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
} ## end of t

## no critic
{
	no warnings; ## Yes, we know they are being redefined!
	sub is_deeply {
		t($_[2],(caller)[2]);
		return if Test::More::is_deeply($_[0],$_[1],$testmsg);
		if ($bail_on_error > $total_errors++) {
			my $line = (caller)[2];
			my $time = time;
			diag("GOT: ".Dumper $_[0]);
			diag("EXPECTED: ". Dumper $_[1]);
			BAIL_OUT "Stopping on a failed 'is_deeply' test from line $line. Time: $time";
		}
	} ## end of is_deeply
	sub like {
		t($_[2],(caller)[2]);
		return if Test::More::like($_[0],$_[1],$testmsg);
		if ($bail_on_error > $total_errors++) {
			my $line = (caller)[2];
			my $time = time;
			BAIL_OUT "Stopping on a failed 'like' test from line $line. Time: $time";
		}
	} ## end of like
	sub pass {
		t($_[0],(caller)[2]);
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

sub exitnow {
	$need_shutdown = 0;
	exit;
} ## end of exitnow



sub test_customcode_methods {

	## Test methods related to custom code: customcode, remove_customcode

	$location = 'customcode_methods';

	my ($code, $codeid);

	$t=q{ Method customcode fails if no arguments are given };
	eval { $bc->customcode(); };
	like($@, qr{must be a hashref}, $t);

	$t=q{ Method customcode fails if no 'src_code' argument given };
	eval { $bc->customcode({name => 'test'}); };
	like($@, qr{\QAttribute (src_code) is required}, $t);

	$t=q{ Method customcode fails if no 'name' argument given };
	eval { $bc->customcode({src_code => 'foo'}); };
	like($@, qr{\QAttribute (name) is required}, $t);

	$t=q{ Method customcode fails if no 'whenrun' argument given };
	eval { $bc->customcode({name => 'test', src_code => 'foo'}); };
	like($@, qr{\QAttribute (whenrun) is required}, $t);

	$t=q{ Method customcode fails if 'whenrun' is invalid };
	eval { $bc->customcode({name => 'test', src_code => 'foo', whenrun => 'invalid'}); };
	like($@, qr{violates check constraint "customcode_whenrun"}, $t);

	$t=q{ Method customcode works if given valid arguments };
	eval { $code = $bc->customcode({name => 'test', src_code => 'foo', whenrun => 'before_txn'}); };
	is($@, q{}, $t);

	$t=q{ Method customcode returns a hashref };
	is(ref $code, 'HASH', $t);

	$t=q{ Method customcode returns a hashref containing a numeric 'id' key };
	$codeid = $code->{id} || 0;
	like($codeid, qr{^\d$}, $t);

	$t=q{ Method customcode inserts to the database correctly };
	$SQL = "SELECT id, name, about, whenrun, src_code, getdbh, getrows FROM customcode";
	$sth = $masterdbh->prepare($SQL);
	$sth->execute();
	$got = $sth->fetchall_arrayref({});
	$expected = [
				 {
				  id       => $codeid,
				  name     => 'test',
				  about    => undef,
				  whenrun  => 'before_txn',
				  src_code => 'foo',
				  getdbh   => 1,
				  getrows  => 0,
				  }
				 ];
	is_deeply($got, $expected, $t);

	$t=q{ Method customcode fails if 'name' already exists };
	eval { $bc->customcode({name => 'test', src_code => 'foo', whenrun => 'before_txn'}); };
	like($@, qr{duplicate key}, $t);

	$t=q{ Method customcode inserts with optional attributes };
	eval { $code = $bc->customcode({name => 'test2', about=>'bz', src_code => 'foo',
									whenrun => 'after_txn', getdbh=>0, getrows=>1});
	};
	is($@, q{}, $t);

	$t=q{ Method customcode inserts to the database correctly };
	$sth->execute();
	$got = $sth->fetchall_arrayref({});
	push @$expected,
		{
		 id       => $code->{id},
		 name     => 'test2',
		 about    => 'bz',
		 whenrun  => 'after_txn',
		 getdbh   => 0,
		 getrows  => 1,
		 src_code => 'foo',
		 };
	is_deeply($got, $expected, $t);

	## Codes can be connected to syncs and goats via the customcode_map table

	$t=q{ Method customcode fails if 'id' is not numeric };
	eval { $bc->customcode({id => 'foobar'}); };
	like($@, qr{must be a number}, $t);

	$t=q{ Method customcode fails if 'id' given, but not 'goat' or 'sync' };
	eval { $bc->customcode({id => $codeid}); };
	like($@, qr{sync or goat}, $t);

	$t=q{ Method customcode fails if 'id' does not exist };
	eval { $bc->customcode({id => 42, sync => 'foo'}); };
	like($@, qr{\d does not exist}, $t);

	$t=q{ Method customcode fails if 'sync' does not exist };
	eval { $bc->customcode({id => $codeid, sync => 'foo'}); };
	like($@, qr{sync does not exist}, $t);

	$t=q{ Method customcode fails if 'goat' is not numeric };
	eval { $bc->customcode({id => $codeid, goat => 'foo'}); };
	like($@, qr{Invalid goat}, $t);

	$t=q{ Method customcode fails if 'goat' does not exist };
	eval { $bc->customcode({id => $codeid, goat => 999}); };
	like($@, qr{goat does not exist}, $t);

	$t=q{ Method customcode works if given a valid 'id' and 'goat' };
	eval { $bc->customcode({id => $codeid, goat => 1}); };
	is($@, q{}, $t);

	## Create a sync for us to use
	$bc->sync
		({
		  name      => 'cctest',
		  source    => 'bctestherd1',
		  targetdb  => 'bctest2',
		  synctype  => 'fullcopy',
	  });

	$t=q{ Method customcode works if given a valid 'id' and 'sync' };
	eval { $bc->customcode({id => $codeid, sync => 'cctest'}); };
	is($@, q{}, $t);

	$t=q{ Method customcode fails if 'id' and 'sync' already exist };
	eval { $bc->customcode({id => $codeid, sync => 'cctest'}); };
	like($@, qr{customcode_map_unique_sync}, $t);

	$t=q{ Method customcode fails if 'id' and 'goat' already exists };
	eval { $bc->customcode({id => $codeid, goat => 1}); };
	like($@, qr{customcode_map_unique_goat}, $t);

	$t=q{ Method customcode inserts to the database correctly };
	$SQL = "SELECT code,sync,goat,active,priority FROM customcode_map ORDER BY code";
	$sth = $masterdbh->prepare($SQL);
	$sth->execute();
	$got = $sth->fetchall_arrayref({});
	$expected = [
				 {code=>$codeid, goat=>1, sync=>undef,active=>1,priority=>0},
				 {code=>$codeid, goat=>undef, sync=>'cctest',active=>1,priority=>0},
				 ];
	is_deeply($got, $expected, $t);

	$t=q{ Method customcode works with optional attributes 'active' and 'priority' };
	eval { $bc->customcode({id => $codeid, goat => 2,active => 0, priority => 9}); };
	is($@, q{}, $t);

	$t=q{ Method customcode inserts to database correctly with optional attribs };
	$sth->execute();
	$got = $sth->fetchall_arrayref({});
	push @$expected, {code=>2, goat=>2, sync=>undef,active=>0,priority=>9};
	is_deeply($got, $expected, $t);

	## Codes can be unmapped

	$t=q{ Method remove_customcode fails if no arguments are given };
	eval { $bc->remove_customcode(); };
	like($@, qr{must be a hashref}, $t);

	$t=q{ Method remove_customcode fails if no 'id', 'name', or 'code' argument given };
	eval { $bc->remove_customcode({foo => 'bar'}); };
	like($@, qr{required argument}, $t);

	$t=q{ Method remove_customcode fails if 'code' is not numeric };
	eval { $bc->remove_customcode({code => 'foo'}); };
	like($@, qr{'code' is not numeric}, $t);

	$t=q{ Method remove_customcode fails if 'code' given, but not 'sync' or 'goat' };
	eval { $bc->remove_customcode({code => '123'}); };
	like($@, qr{sync or goat}, $t);

	$t=q{ Method remove_customcode fails if 'goat' is not numeric };
	eval { $bc->remove_customcode({code => $codeid, goat => 'foo'}); };
	like($@, qr{goat is not numeric}, $t);

	$t=q{ Method remove_customcode returns a 0 if 'code' is not valid };
	$count = $bc->remove_customcode({code => '123', goat => 1});
	is($count, 0, $t);

	$t=q{ Method remove_customcode returns 0 if 'sync' is not valid };
	$count = $bc->remove_customcode({code => $codeid, sync => 'nosuch'});
	is($count, 0, $t);

	$t=q{ Method remove_customcode returns 1 if given valid 'id' and 'goat' };
	$count = $bc->remove_customcode({code => $codeid, goat => 1});
	is($count, 1, $t);

	$t=q{ Method customcode deletes from customcode_map correctly };
	$sth->execute();
	$got = $sth->fetchall_arrayref({});
	shift @$expected;
	is_deeply($got, $expected, $t);

	$t=q{ Method remove_customcode returns 1 if given valid 'id' and 'sync' };
	$count = $bc->remove_customcode({code => $codeid, sync => 'cctest'});
	is($count, 1, $t);

	$t=q{ Method remove_customcode returns 0 if repeating previous deletion };
	$count = $bc->remove_customcode({code => $codeid, sync => 'cctest'});
	is($count, 0, $t);

	$t=q{ Method customcode deletes from customcode_map correctly. };
	$sth->execute();
	$got = $sth->fetchall_arrayref({});
	shift @$expected;
	is_deeply($got, $expected, $t);

	## Codes can be deleted

	$t=q{ Method remove_customcode fails if 'id' is not numeric };
	eval { $count = $bc->remove_customcode({id => 'foo'}); };
	like($@, qr{not numeric}, $t);

	$t=q{ Method remove_customcode returns 0 if 'id' does not match };
	$count = $bc->remove_customcode({id => 123});
	is($count, 0, $t);

	$t=q{ Method remove_customcode returns 0 if 'name' does not match };
	$count = $bc->remove_customcode({name => 'nosuch'});
	is($count, 0, $t);

	$t=q{ Method remove_customcode returns 1 if 'id' matches };
	$count = $bc->remove_customcode({id => $codeid});
	is($count, 1, $t);

	$t=q{ Method remove_customcode returns 1 if 'name' matches };
	$count = $bc->remove_customcode({name => 'test2'});
	is($count, 1, $t);

	$t=q{ Method customcode deletes from customcode correctly };
	$SQL = "SELECT * FROM customcode ORDER BY id";
	$sth = $masterdbh->prepare($SQL);
	$sth->execute();
	$got = $sth->fetchall_arrayref({});
	is_deeply($got, [], $t);

	return;

} ## end of test_customcode_methods


sub test_database_methods {

	## Test methods related to the db table

	$location = 'database_methods';

	$t=q{ Adding a database with a null name does not work };
	eval { $masterdbh->do(qq{INSERT INTO db(name) VALUES (NULL)}); };
	like($@, qr{violates not-null constraint}, $t);
	$masterdbh->rollback();

	$t=q{ Adding an invalid database to the db table fails };
	eval { $masterdbh->do(qq{INSERT INTO db(name,dbname,dbuser) VALUES ('bctest','nosuchdb!','no_such_user!')}); };
	like($@, qr{authentication failed|database .* does not exist|no password supplied|could not connect to server}, $t);
	$masterdbh->rollback();

	$t=q{ Dots not allowed in database names };
	eval { $masterdbh->do(qq{INSERT INTO db(name,dbname,dbuser) VALUES ('bct.dotted','aa','bb')}); };
	like($@, qr{db_name_sane}, $t);
	$masterdbh->rollback();

	return;

} ## end of test_database_methods


sub test_goat_methods {

	## Test methods related to the goat table

	$location = 'goat_methods';

	$SQL = qq{INSERT INTO goat(db,tablename,pkey,pkeytype) VALUES };

	$t=q{ Adding a goat with a non-existent database fails };
	eval { $masterdbh->do(qq{$SQL ('invalid','bucardo_test1','id','int')}); };
	like($@, qr{find a database}, $t);
	$masterdbh->rollback();

	$t=q{ Adding an goat with a null table fails };
	eval { $masterdbh->do(qq{$SQL ('bctest1',null,'id','int')}); };
	like($@, qr{not-null constraint}, $t);
	$masterdbh->rollback();
	
	$t=q{ Adding an goat with a no primary key type fails };
	eval { $masterdbh->do(qq{$SQL ('bctest1','bucardo_test1','notid',null)}); };
	like($@, qr{pkey_needs_type}, $t);
	$masterdbh->rollback();

	$t=q{ Adding an goat with an invalid primary key type fails };
	eval { $masterdbh->do(qq{$SQL ('bctest1','bucardo_test1','notid','money')}); };
	like($@, qr{pkeytype_check}, $t);
	$masterdbh->rollback();

	return;

} ## end of test_goat_methods


sub test_sync_methods {

	## Test methods related to the sync table

	$location = 'sync_methods';

	$SQL = qq{INSERT INTO sync(name,synctype,source,targetdb) VALUES };

	$t=q{ Adding invalid synctype to the sync table fails };
	eval { $masterdbh->do(qq{$SQL ('bct','foobar','bctesterd1','bctest2')}) };
	like($@, qr{sync_type}, $t);
	$masterdbh->rollback();
	
	$t=q{ Adding invalid source to the sync table fails };
	eval { $masterdbh->do(qq{$SQL ('bct','pushdelta','invalid','bctest2')}); };
	like($@, qr{sync_source_herd_fk}, $t);
	$masterdbh->rollback();

	$t=q{ Adding invalid targetdb to the sync table fails };
	eval { $masterdbh->do(qq{$SQL ('bct','fullcopy','bctestherd1','invalid')}); };
	like($@, qr{sync_targetdb_fk}, $t);
	$masterdbh->rollback();

	$t=q{ Adding targetgroup to a swap sync table fails };
	(my $si = $SQL) =~ s/targetdb/targetgroup/;
	eval { $masterdbh->do(qq{$si ('bct','swap','bctestherd1','invalid')}); };
	like($@, qr{sync_swap_nogroup}, $t);
	$masterdbh->rollback();

	$t=q{ Adding invalid targetgroup to the sync table fails };
	eval { $masterdbh->do(qq{$si ('bct','pushdelta','bctestherd1','invalid')}); };
	like($@, qr{sync_targetgroup_fk}, $t);
	$masterdbh->rollback();

	$masterdbh->do("DELETE FROM sync");
	$t=q{ Adding a duplicate row to the sync table fails };
	$masterdbh->do(qq{$SQL ('bct','fullcopy','bctestherd1','bctest2')});
	eval { $masterdbh->do(qq{$SQL ('bct','fullcopy','bctestherd1','bctest2')}); };
	like($@, qr{sync_name_pk}, $t);
	$masterdbh->rollback();

	$t=q{ One of targetdb or targetgroup must be not-null when adding to sync table };
	eval { $masterdbh->do(qq{$SQL ('bct','fullcopy','bctestherd1',NULL)}); };
	like($@, qr{sync_validtarget}, $t);
	$masterdbh->rollback();

	$t=q{ Dots not allowed in sync names };
	eval { $masterdbh->do(qq{$SQL ('bct.dotted','swap','bctestherd1','bctest2')}); };
	like($@, qr{sync_name_sane}, $t);
	$masterdbh->rollback();

	return;

} ## end of test_sync_methods


sub test_config {

	$location = 'bucardo_config';

	$t=q{ Values of bucardo_config from bucardo.schema are available from %config };
	my $val = $bc->get_config('kid_abort_limit');
	is($val, 3, $t);

	$t=q{ Values of bucardo_config from bucardo.schema are available from %config_about };
	$val = $bc->get_config_about('kid_abort_limit');
	is($val, 'How many times we will restore an aborted kid before giving up?', $t);
	
	$t=q{ Method set_config stores a new setting inside of %config };
	my $fingerprint = '2529 DF6A B8F7 9407 E944  45B4 BC9B 9067 1496 4AC8';
	$bc->set_config('pgp_fingerprint', $fingerprint);
	$val = $bc->get_config('pgp_fingerprint');
	is($val, $fingerprint, $t);

	$t=q{ Method set_config_about allows us set a new setting description };
	$bc->set_config_about('pgp_fingerprint', 'PGP fingerprint');
	$val = $bc->get_config_about('pgp_fingerprint');
	is($val, 'PGP fingerprint', $t);

	$t=q{ Method set_config allows us to change an existing value };
	$fingerprint =~ s/ //g;
	$bc->set_config('pgp_fingerprint', $fingerprint);
	$val = $bc->get_config('pgp_fingerprint');
	is($val, $fingerprint, $t);

	$t=q{ Method save_config writes an existing config to the database };
	$bc->store_config('pgp_fingerprint');
	$SQL = "SELECT value FROM bucardo_config WHERE setting = 'pgp_fingerprint'";
	$val = $masterdbh->selectall_arrayref($SQL)->[0][0];
	is($val, $fingerprint, $t);

	$t=q{ All configuration settings are forced to lowercase };
	$fingerprint = lc $fingerprint;
	$bc->set_config('PGP_Fingerprint', $fingerprint);
	$val = $bc->get_config('pgp_fingerprint');
	is($val, $fingerprint, $t);

	$t=q{ Setting a bucardo_config setting twice gives an error };
	$SQL = "INSERT INTO bucardo_config(setting,value) VALUES (?,?)";
	$sth = $masterdbh->prepare($SQL);
	$sth->execute('bctest_unique', 123);
	$masterdbh->commit();
	eval {
		$sth->execute('bctest_unique', 123);
	};
	like($@, qr{violates unique constraint}, $t);
	$masterdbh->rollback();

	$t=q{ A bucardo_config setting of 'sync' gives an error };
	eval {
		$sth->execute('sync', 123);
	};
	like($@, qr{Invalid setting name}, $t);
	$masterdbh->rollback();

	$t=q{ A bucardo_config setting of 'goat' gives an error };
	eval {
		$sth->execute('goat', 123);
	};
	like($@, qr{Invalid setting name}, $t);
	$masterdbh->rollback();

	$t=q{ Setting a bucardo_config type/name works };
	$SQL = "INSERT INTO bucardo_config(setting,value,type,name) VALUES (?,?,?,?)";
	$sth = $masterdbh->prepare($SQL);
	$sth->execute('bctest_unique', 123, 'sync', 'foo');

	$t=q{ Setting a bucardo_config type without a name gives an error };
	$sth = $masterdbh->prepare($SQL);
	eval {
		$sth->execute('bctest_unique', 123, 'sync', undef);
	};
	like($@, qr{provide a specific sync}, $t);
	$masterdbh->rollback();

	$t=q{ Setting a bucardo_config name without a type gives an error };
	$sth = $masterdbh->prepare($SQL);
	eval {
		$sth->execute('bctest_unique', 123, undef, 'foo');
	};
	like($@, qr{provide a type}, $t);
	$masterdbh->rollback();

	$t=q{ Setting bucardo_config.name to an invalid goat gives an error };

	$masterdbh->do("DELETE FROM sync");
	$masterdbh->commit();
	$t=q{ Running Bucardo process can be forced to reload conf from the database };
	$bc->sync
		({
		  name      => 'configtest',
		  source    => 'bctestherd1',
		  targetdb  => 'bctest2',
		  synctype  => 'pushdelta',
	  });

	bucardo_ctl("start 'Start config testing'");
	wait4kid('bucardo_q_configtest_bctest2');
	pass(" Bucardo was started");

	$SQL = "INSERT INTO bucardo_config(setting,value,about) VALUES (?,?,?)";
	$sth = $masterdbh->prepare($SQL);
	$sth->execute('bctesting','22','Testing config reload');
	$masterdbh->commit();
	bucardo_ctl("reload_config");
	## TODO: test actual results without adding overhead

	return;

} ## end of test_config


sub ping_testing {

	## Setup a pushdelta sync
	$bc->sync
		({
		  name             => 'pingtest',
		  source           => 'bctestherd1',
		  targetdb         => 'bctest2',
		  synctype         => 'pushdelta',
	  });

	$masterdbh->do("LISTEN bucardo_started");
	$masterdbh->commit();
	pass(" Waiting for bucardo to start up");
	bucardo_ctl("start 'Ping testing'");
	{
		last if $masterdbh->func('pg_notifies');
		sleep 0.1;
		redo;
	}
	pass("Bucardo started up for ping testing");

	$masterdbh->do("UNLISTEN *");
	$masterdbh->do("LISTEN bucardo_mcp_pong");
	$masterdbh->commit();
	$masterdbh->do("NOTIFY bucardo_mcp_ping");
	## This should return very quickly, but we'll give it 5 whole seconds
	my $found = 0;
	for (1..50) {
		$masterdbh->commit();
		if ($masterdbh->func('pg_notifies')) {
			$found = 1;
			last;
		}
		sleep 0.1;
	}
	is($found, 1, qq{MCP responds to bucardo_mcp_ping});

	# We will need the PID to test the CTL ping
	$SQL = "SELECT pid FROM audit_pid WHERE type='CTL' ORDER BY birthdate DESC LIMIT 1";
	my $pid = $masterdbh->selectall_arrayref($SQL)->[0][0];

	$masterdbh->do("LISTEN bucardo_ctl_${pid}_pong");
	$masterdbh->commit();
	$masterdbh->do("NOTIFY bucardo_ctl_${pid}_ping");
	$found = 0;
	for (1..50) {
		$masterdbh->commit();
		if ($masterdbh->func('pg_notifies')) {
			$found = 1;
			last;
		}
		sleep 0.1;
	}
	is($found, 1, qq{CTL responds to bucardo_ctl_<pid>_ping});

	$SQL = "SELECT pid FROM audit_pid WHERE type='KID' ORDER BY birthdate DESC LIMIT 1";
	$pid = $masterdbh->selectall_arrayref($SQL)->[0][0];

	$masterdbh->do("LISTEN bucardo_kid_${pid}_pong");
	$masterdbh->commit();
	$masterdbh->do("NOTIFY bucardo_kid_${pid}_ping");
	$found = 0;
	for (1..50) {
		$masterdbh->commit();
		if ($masterdbh->func('pg_notifies')) {
			$found = 1;
			last;
		}
		sleep 0.1;
	}
	is($found, 1, qq{KID responds to bucardo_kid_<pid>_ping});

	bucardo_ctl("stop 'Ping testing'");

} ## end of ping_testing



sub test_purge {

	## Test the bucardo_purge_delta function
	$location = 'purge';

	$dbh1->do("SET search_path = bucardo_schema, public");
	$dbh2->do("SET search_path = bucardo_schema, public");

	clean_all_tables();

	## A fullcopy sync should not create any of the above
	$bc->sync
		({
		  name             => 'purgetest1',
		  source           => 'bctestherd1',
		  targetdb         => 'bctest2',
		  synctype         => 'fullcopy',
	});

	$t=q{ Table bucardo_delta is not created on source database automatically for fullcopy};
	is (object_count($dbh1, 'bucardo', 'table', 'bucardo_delta'), 0, $t);
	$t=q{ Table bucardo_track is not created on source database automatically for fullcopy};
	is (object_count($dbh1, 'bucardo', 'table', 'bucardo_track'), 0, $t);
	$t=q{ Table bucardo_delta_targets is not created on source database automatically for fullcopy};
	is (object_count($dbh1, 'bucardo', 'table', 'bucardo_delta_targets'), 0, $t);
	$t=q{ Function bucardo_purge_delta() is not created on source database automatically for fullcopy};
	is (object_count($dbh1, 'bucardo', 'function', 'bucardo_purge_delta'), 0, $t);

	$t=q{ Table bucardo_delta is not created on remote database automatically for fullcopy};
	is (object_count($dbh2, 'bucardo', 'table', 'bucardo_delta'), 0, $t);
	$t=q{ Table bucardo_track is not created on remote database automatically for fullcopy};
	is (object_count($dbh2, 'bucardo', 'table', 'bucardo_track'), 0, $t);
	$t=q{ Table bucardo_delta_targets is not created on remote database automatically for fullcopy};
	is (object_count($dbh2, 'bucardo', 'table', 'bucardo_delta_targets'), 0, $t);
	$t=q{ Function bucardo_purge_delta() is not created on remote database automatically for fullcopy};
	is (object_count($dbh2, 'bucardo', 'function', 'bucardo_purge_delta'), 0, $t);

	$masterdbh->do("DELETE FROM sync");
	$masterdbh->commit();

	## A pushdelta should create things on one side only
	$bc->sync
		({
		  name             => 'purgetest2',
		  source           => 'bctestherd1',
		  targetdb         => 'bctest2',
		  synctype         => 'pushdelta',
	});

	$dbh1->do("SET search_path = bucardo_schema, bucardo, public");
	$dbh1->commit();

	$t=q{ Table bucardo_delta IS created on source database automatically for pushdelta};
	is (object_count($dbh1, 'bucardo', 'table', 'bucardo_delta'), 1, $t);
	$t=q{ Table bucardo_track IS created on source database automatically for pushdelta};
	is (object_count($dbh1, 'bucardo', 'table', 'bucardo_track'), 1, $t);
	$t=q{ Table bucardo_delta_targets IS created on source database automatically for pushdelta};
	is (object_count($dbh1, 'bucardo', 'table', 'bucardo_delta_targets'), 1, $t);
	$t=q{ Function bucardo_purge_delta() IS created on source database automatically for pushdelta};
	is (object_count($dbh1, 'bucardo', 'function', 'bucardo_purge_delta'), 1, $t);

	$t=q{ Table bucardo_delta is not created on remote database automatically for pushdelta};
	is (object_count($dbh2, 'bucardo', 'table', 'bucardo_delta'), 0, $t);
	$t=q{ Table bucardo_track is not created on remote database automatically for pushdelta};
	is (object_count($dbh2, 'bucardo', 'table', 'bucardo_track'), 0, $t);
	$t=q{ Table bucardo_delta_targets is not created on remote database automatically for pushdelta};
	is (object_count($dbh2, 'bucardo', 'table', 'bucardo_delta_targets'), 0, $t);
	$t=q{ Function bucardo_purge_delta() is not created on remote database automatically for pushdelta};
	is (object_count($dbh2, 'bucardo', 'function', 'bucardo_purge_delta'), 0, $t);

	$t=q{ Table bucardo_delta_targets is populated on source database at pushdelta sync creation};
	my $getoids = "SELECT relname, oid FROM pg_class WHERE relname ~ 'bucardo_test'";
	my %oid;
	for (@{$dbh1->selectall_arrayref($getoids)}) {
		$oid{source}{$_->[0]} = $_->[1];
	}
	$oid{sourceresult} = [
	   [$oid{source}{'bucardo_test1'},'bctest2'],
	   [$oid{source}{'bucardo_test2'},'bctest2'],
	   [$oid{source}{'bucardo_test3'},'bctest2'],
	   [$oid{source}{'bucardo_test4'},'bctest2'],
    ];
	my $view_targets = "SELECT * FROM bucardo_delta_targets";
	$got = $dbh1->selectall_arrayref($view_targets);
	is_deeply($got, $oid{sourceresult}, $t);

	$masterdbh->do("DELETE FROM bucardo.sync");
	$masterdbh->commit();

	$t=q{ Delete from sync removes the bucardo_delta_targets row};
	$view_targets = "SELECT * FROM bucardo_delta_targets";
	$got = $dbh1->selectall_arrayref($view_targets);
	is_deeply($got, [], $t);

	## Remove the old ones for a better test
	$dbh1->do("DROP TABLE bucardo_delta");
	$dbh1->do("DROP TABLE bucardo_delta_targets");
	$dbh1->do("DROP TABLE bucardo_track");
	$dbh1->do("DROP FUNCTION bucardo_purge_delta(interval)");
	$dbh1->commit();

	## A swap should create things on both sides
	$bc->sync
		({
		  name             => 'purgetest3',
		  source           => 'bctestherd1',
		  targetdb         => 'bctest2',
		  synctype         => 'swap',
	});

	$dbh2->do("SET search_path = bucardo_schema, bucardo, public");
	$dbh2->commit();

	$t=q{ Table bucardo_delta IS created on source database automatically for swap};
	is (object_count($dbh1, 'bucardo', 'table', 'bucardo_delta'), 1, $t);
	$t=q{ Table bucardo_track IS created on source database automatically for swap};
	is (object_count($dbh1, 'bucardo', 'table', 'bucardo_track'), 1, $t);
	$t=q{ Table bucardo_delta_targets IS created on source database automatically for swap};
	is (object_count($dbh1, 'bucardo', 'table', 'bucardo_delta_targets'), 1, $t);
	$t=q{ Function bucardo_purge_delta() IS created on source database automatically for swap};
	is (object_count($dbh1, 'bucardo', 'function', 'bucardo_purge_delta'), 1, $t);

	$t=q{ Table bucardo_delta IS created on remote database automatically for swap};
	is (object_count($dbh2, 'bucardo', 'table', 'bucardo_delta'), 1, $t);
	$t=q{ Table bucardo_track IS created on remote database automatically for swap};
	is (object_count($dbh2, 'bucardo', 'table', 'bucardo_track'), 1, $t);
	$t=q{ Table bucardo_delta_targets IS created on remote database automatically for swap};
	is (object_count($dbh2, 'bucardo', 'table', 'bucardo_delta_targets'), 1, $t);
	$t=q{ Function bucardo_purge_delta() IS created on remote database automatically for swap};
	is (object_count($dbh2, 'bucardo', 'function', 'bucardo_purge_delta'), 1, $t);


	$t=q{ Table bucardo_delta_targets is populated on source database at swap sync creation};
	$got = $dbh1->selectall_arrayref($view_targets);
	is_deeply($got, $oid{sourceresult}, $t);

	$t=q{ Table bucardo_delta_targets is populated on target database at swap sync creation};
	for (@{$dbh2->selectall_arrayref($getoids)}) {
		$oid{target}{$_->[0]} = $_->[1];
	}
	$oid{targetresult} = [
			   [$oid{target}{'bucardo_test1'},'bctest1'],
			   [$oid{target}{'bucardo_test2'},'bctest1'],
			   [$oid{target}{'bucardo_test3'},'bctest1'],
			   [$oid{target}{'bucardo_test4'},'bctest1'],
			   ];
	$got = $dbh2->selectall_arrayref($view_targets);
	is_deeply($got, $oid{targetresult}, $t);

	$masterdbh->do("DELETE FROM bucardo.sync");
	$masterdbh->commit();

	$t=q{ Delete from sync removes the bucardo_delta_targets row on source database};
	$got = $dbh1->selectall_arrayref($view_targets);
	is_deeply($got, [], $t);
	$t=q{ Delete from sync removes the bucardo_delta_targets row on target database};
	$got = $dbh2->selectall_arrayref($view_targets);
	is_deeply($got, [], $t);

	## Same, but test with a targetgroup

	$masterdbh->do("INSERT INTO dbgroup(name) VALUES ('bcgroup1')");
	$masterdbh->do("INSERT INTO dbmap(db,dbgroup) VALUES ('bctest2','bcgroup1')");
	$masterdbh->do("INSERT INTO dbmap(db,dbgroup) VALUES ('bctest3','bcgroup1')");
	$masterdbh->commit();

	## Remove the old ones for a better test
	$dbh1->do("DROP TABLE bucardo_delta");
	$dbh1->do("DROP TABLE bucardo_delta_targets");
	$dbh1->do("DROP TABLE bucardo_track");
	$dbh1->do("DROP FUNCTION bucardo_purge_delta(interval)");
	$dbh1->commit();

	## Remove the old ones for a better test
	$dbh2->do("DROP TABLE bucardo_delta");
	$dbh2->do("DROP TABLE bucardo_delta_targets");
	$dbh2->do("DROP TABLE bucardo_track");
	$dbh2->do("DROP FUNCTION bucardo_purge_delta(interval)");
	$dbh2->commit();

	$bc->sync
		({
		  name             => 'purgetest3',
		  source           => 'bctestherd1',
		  targetgroup      => 'bcgroup1',
		  synctype         => 'pushdelta',
	});

	$t=q{ Table bucardo_delta IS created on source database automatically for pushdelta};
	is (object_count($dbh1, 'bucardo', 'table', 'bucardo_delta'), 1, $t);
	$t=q{ Table bucardo_track IS created on source database automatically for pushdelta};
	is (object_count($dbh1, 'bucardo', 'table', 'bucardo_track'), 1, $t);
	$t=q{ Table bucardo_delta_targets IS created on source database automatically for pushdelta};
	is (object_count($dbh1, 'bucardo', 'table', 'bucardo_delta_targets'), 1, $t);
	$t=q{ Function bucardo_purge_delta() IS created on source database automatically for pushdelta};
	is (object_count($dbh1, 'bucardo', 'function', 'bucardo_purge_delta'), 1, $t);

	$t=q{ Table bucardo_delta is not created on first remote database automatically for pushdelta};
	is (object_count($dbh2, 'bucardo', 'table', 'bucardo_delta'), 0, $t);
	$t=q{ Table bucardo_track is not created on first remote database automatically for pushdelta};
	is (object_count($dbh2, 'bucardo', 'table', 'bucardo_track'), 0, $t);
	$t=q{ Table bucardo_delta_targets is not created on first remote database automatically for pushdelta};
	is (object_count($dbh2, 'bucardo', 'table', 'bucardo_delta_targets'), 0, $t);
	$t=q{ Function bucardo_purge_delta() is not created on first remote database automatically for pushdelta};
	is (object_count($dbh2, 'bucardo', 'function', 'bucardo_purge_delta'), 0, $t);

	$t=q{ Table bucardo_delta is not created on second remote database automatically for pushdelta};
	is (object_count($dbh3, 'bucardo', 'table', 'bucardo_delta'), 0, $t);
	$t=q{ Table bucardo_track is not created on second remote database automatically for pushdelta};
	is (object_count($dbh3, 'bucardo', 'table', 'bucardo_track'), 0, $t);
	$t=q{ Table bucardo_delta_targets is not created on second remote database automatically for pushdelta};
	is (object_count($dbh3, 'bucardo', 'table', 'bucardo_delta_targets'), 0, $t);
	$t=q{ Function bucardo_purge_delta() is not created on second remote database automatically for pushdelta};
	is (object_count($dbh3, 'bucardo', 'function', 'bucardo_purge_delta'), 0, $t);

	$t=q{ Table bucardo_delta_targets is populated on source database at pushdelta sync creation};
	push @{$oid{sourceresult}},
			   [$oid{source}{'bucardo_test1'},'bctest3'],
			   [$oid{source}{'bucardo_test2'},'bctest3'],
			   [$oid{source}{'bucardo_test3'},'bctest3'],
			   [$oid{source}{'bucardo_test4'},'bctest3'];
	$got = $dbh1->selectall_arrayref($view_targets);
	is_deeply($got, $oid{sourceresult}, $t);

	$masterdbh->do("DELETE FROM sync");
	$masterdbh->commit();

	$t=q{ Table bucardo_delta_targets is emptied out after sync is removed};
	$got = $dbh1->selectall_arrayref($view_targets);
	is_deeply($got, [], $t);

	## Start up the sync again for function testing
	$bc->sync
		({
		  name             => 'purgetest3',
		  source           => 'bctestherd1',
		  targetgroup      => 'bcgroup1',
		  synctype         => 'pushdelta',
	});

	## And kick it off
	bucardo_ctl("start 'Start purge testing'");

	$t=q{ Calling bucardo_purge_delta with no arguments fails};
	eval {
		$dbh1->do("SELECT bucardo.bucardo_purge_delta()");
	};
	like($@, qr{does not exist}, $t);
	$dbh1->rollback();

	$t=q{ Calling bucardo_purge_delta with text argument fails};
	eval {
		$dbh1->do("SELECT bucardo.bucardo_purge_delta('foobar')");
	};
	like($@, qr{invalid input syntax for type interval}, $t);
	$dbh1->rollback();

	$t=q{ Calling bucardo_purge_delta with valid interval argument works};
	eval {
		$got = $dbh1->selectall_arrayref("SELECT bucardo.bucardo_purge_delta('10 minutes'::interval)")->[0][0];
	};
	like($@, qr{}, $t);

	$t=q{ Calling bucardo_purge_delta returns the expected text string};
	is($got, "Rows deleted from bucardo_delta: 0 Rows deleted from bucardo_track: 0", $t);

	## Populate the table with a few rows
	$SQL = "INSERT INTO bucardo_test1(id,inty,data1) VALUES (?,?,?)";
	my $insert1 = $dbh1->prepare($SQL);
	my $insert2 = $dbh2->prepare($SQL);

	$insert1->execute(1,1,'purgetest1');
	$insert1->execute(2,2,'purgetest2');

	bucardo_ctl("kick purgetest3 0");

	my $viewdelta = "SELECT * FROM bucardo.bucardo_delta ORDER BY txntime, rowid";
	$val = $dbh1->selectall_arrayref($viewdelta);

	$t=q{ Calling bucardo_purge_delta respects time passed in};
	$SQL = "SELECT bucardo_purge_delta('1 hour'::interval)";
	$dbh1->do($SQL);
	$got = $dbh1->selectall_arrayref($viewdelta);
	is_deeply($got, $val, $t);

	## Insert some bogus entries directly into bucardo_delta and bucardo_track
	$dbh1->commit();
	$dbh1->do("INSERT INTO bucardo_delta(tablename, rowid) VALUES ($oid{source}{'bucardo_test1'},'12345')");
	$dbh1->do("INSERT INTO bucardo_delta(tablename, rowid) VALUES ($oid{source}{'bucardo_test2'},'67890')");
	$sth = $dbh1->prepare("INSERT INTO bucardo_track(txntime, tablename, targetdb) VALUES ((SELECT now()),?,?)");
	$sth->execute('999','invalid1');
	$sth->execute('888','invalid1');
	$sth->execute($oid{source}{'bucardo_test1'},'invalid1');
	$sth->execute($oid{source}{'bucardo_test2'},'invalid2');
	my $now = $dbh1->selectall_arrayref("SELECT now()")->[0][0];
	$dbh1->commit();

	my $deltaval = [
					[$oid{source}{'bucardo_test1'},'12345',$now],
					[$oid{source}{'bucardo_test2'},'67890',$now],
					];

	$SQL = "SELECT bucardo_purge_delta('1 second'::interval)";
	$got = $dbh1->selectall_arrayref("SELECT bucardo_purge_delta('0 second'::interval)")->[0][0];
	$t=q{ Calling bucardo_purge_delta returns the expected text string};
	is($got, "Rows deleted from bucardo_delta: 2 Rows deleted from bucardo_track: 2", $t);

	$t=q{ Calling bucardo_purge_delta purges the expected rows};
	$got = $dbh1->selectall_arrayref($viewdelta);
	is_deeply($got, $deltaval, $t);
	
	return;

} ## end of sub test_purge


sub basic_pushdelta_testing {

	my ($table,$sdbh,$rdbh) = @_;

	$location = 'pushdelta';

	$type = $tabletype{$table};
	my $oid = $table{$sdbh}{$table};

	compare_tables($table,$sdbh,$rdbh) or BAIL_OUT "Compare tables failed?!\n";

	$val = $val{$type}{1};

	$masterdbh->do("LISTEN bucardo_syncdone_pushdeltatest");
	$masterdbh->commit();

	$SQL = "INSERT INTO $table(id,data1,inty) VALUES ('$val','one',1)";
	$sdbh->do($SQL);
	$sdbh->commit;

	$t=qq{ Second table $table still empty before commit };
	$SQL = "SELECT id,data1 FROM $table";
	$result = [];
	bc_deeply($result, $rdbh, $SQL, $t);

	$t=q{ After insert, trigger and rule both populate droptest table };
	my $DROPSQL = "SELECT type,inty FROM droptest WHERE name = ".$sdbh->quote($table)." ORDER BY 1,2";
	$result = [['rule',1],['trigger',1]];
	bc_deeply($result, $sdbh, $DROPSQL, $t);
   
	$t=q{ Table droptest is empty on remote database };
	$result = [];
	bc_deeply($result, $rdbh, $DROPSQL, $t);

	wait_for_notice($masterdbh, 'bucardo_syncdone_pushdeltatest');

	## Insert to 1 should be echoed to two, after a slight delay:
	$t=qq{ Second table $table got the pushdelta row};
	$SQL = "SELECT id,data1 FROM $table";
	$result = [[qq{$val},'one']];
	bc_deeply($result, $rdbh, $SQL, $t);

	$t=q{ Triggers and rules did not fire on remote table };
	$result = [];
	bc_deeply($result, $rdbh, $DROPSQL, $t);

	## Add a row to two, should not get removed or replicated
	my $rval = $val{$type}{9};
	$SQL = "INSERT INTO $table(id,data1,inty) VALUES ('$rval','nine',9)";
	$rdbh->do($SQL);
	$rdbh->commit;

	## Another source change, but with a different trigger drop method
	$SQL = "UPDATE sync SET disable_triggers = 'SQL'";
	$masterdbh->do($SQL);
	$masterdbh->do("NOTIFY bucardo_reload_sync_pushdeltattest");
	$masterdbh->commit();

	$val = $val{$type}{2};
	$SQL = "INSERT INTO $table(id,data1,inty) VALUES ('$val','two',2)";
	$sdbh->do($SQL);
	$sdbh->commit;

	$t=q{ After insert, trigger and rule both populate droptest table4 };
	$result = [['rule',1],['rule',2],['trigger',1],['trigger',2]];
	bc_deeply($result, $sdbh, $DROPSQL, $t);
   
	$t=q{ Table droptest has correct entries on remote database };
	$result = [['rule',9],['trigger',9]];
	bc_deeply($result, $rdbh, $DROPSQL, $t);

	wait_for_notice($masterdbh, 'bucardo_syncdone_pushdeltatest');

	## Insert to 1 should be echoed to two, after a slight delay:
	$t=qq{ Second table $table got the pushdelta row};
	$SQL = "SELECT data1,inty FROM $table ORDER BY inty";
	$result = [['one',1],['two',2],['nine',9]];
	bc_deeply($result, $rdbh, $SQL, $t);

	$t=q{ Triggers and rules did not fire on remote table };
	$result = [['rule',9],['trigger',9]];
	bc_deeply($result, $rdbh, $DROPSQL, $t);

	$t=q{ Source table did not get updated for pushdelta sync };
	$SQL = "SELECT count(*) FROM $table WHERE inty = 9";
	$count = $sdbh->selectall_arrayref($SQL)->[0][0];
	is($count, 0, $t);

	## Now with many rows
	$SQL = "INSERT INTO $table(id,data1,inty) VALUES (?,?,?)";
	$sth = $sdbh->prepare($SQL);
	for (3..6) {
		$val = $val{$type}{$_};
		$sth->execute($val,'bob',$_);
	}
	$sdbh->commit;

	## Sanity check
	$t=qq{ Rows are not in target table before the kick for $table};
	$sth = $rdbh->prepare("SELECT 1 FROM $table WHERE inty BETWEEN 3 and 6");
	$count = $sth->execute();
	$sth->finish();
	is($count, '0E0', $t);

	wait_for_notice($masterdbh, 'bucardo_syncdone_pushdeltatest');

	$t=qq{ Second table $table got the pushdelta rows};
	$SQL = "SELECT inty FROM $table ORDER BY 1";
	$result = [['1'],['2'],['3'],['4'],['5'],['6'],['9']];
	bc_deeply($result, $rdbh, $SQL, $t);
	$sdbh->commit();
	$rdbh->commit();
	return;

} ## end of basic_pushdelta_testing


sub makedelta_testing {

	my ($table,$sdbh,$rdbh) = @_;

	$location = 'makedelta';

	$type = $tabletype{$table};
	my $oid = $table{$sdbh}{$table};
	my $roid = $table{$rdbh}{$table};

	my ($src_delta,$src_track,$tgt_delta,$tgt_track);

	for my $dbh ($sdbh, $rdbh) {
		$dbh->rollback;
		$dbh->do("DELETE FROM $table");
		$dbh->do("DELETE FROM bucardo.bucardo_delta");
		$dbh->do("DELETE FROM bucardo.bucardo_track");
		$dbh->commit;
	}

	compare_tables($table,$sdbh,$rdbh) or BAIL_OUT "Compare tables failed?!\n";

	$val = $val{$type}{1};

	my $sourcerows = "SELECT * FROM bucardo.bucardo_delta WHERE tablename = $oid ".
		"ORDER BY txntime DESC, rowid DESC";
	my $remoterows = "SELECT tablename,rowid FROM bucardo.bucardo_delta WHERE tablename = $roid ".
		"ORDER BY txntime DESC, rowid DESC";
	my $remotetrackrows = "SELECT tablename,targetdb FROM bucardo.bucardo_track WHERE tablename = $roid ".
		"ORDER BY txntime DESC, tablename DESC";
	my $sourcetrackrows = "SELECT tablename,targetdb FROM bucardo.bucardo_track WHERE tablename = $oid ".
		"ORDER BY txntime DESC, tablename DESC";

	$t=qq{ Insert to source $table populated source bucardo_delta correctly };
	$SQL = "INSERT INTO $table(id,data1,inty) VALUES ('$val','one',1)";
	$sdbh->do($SQL);
	$now = now_time($sdbh);
	$info = $sdbh->selectall_arrayref($sourcerows);
	$result = [[$oid,$val,$now]];
	$src_delta = [[$oid,$val,$now]];
	is_deeply($info, $result, $t);
	$sdbh->commit();

	## Wait until the row gets synced to the target database
	wait_until_true($rdbh => "SELECT 1 FROM $table");

	$t=qq{ Insert to source $table with makedelta created a target bucardo_delta row };
	$info = $rdbh->selectall_arrayref($remoterows);
	$tgt_delta = [[$roid,$val]];
	is_deeply($info, $tgt_delta, $t);

	$t=qq{ Insert to source $table with makedelta created a target bucardo_track row };
	$info = $rdbh->selectall_arrayref($remotetrackrows);
	$tgt_track = [[$roid,'bctest1']];
	is_deeply($info, $tgt_track, $t);

	$t=qq{ Insert to source $table with makedelta created a source bucardo_track row };
	$info = $sdbh->selectall_arrayref($sourcetrackrows);
	$src_track = [[$oid,'bctest2']];
	is_deeply($info, $src_track, $t);

	$t=q{ All rows in bucardo_delta and bucardo_track have the same txntime };
	$SQL = "SELECT count(*) FROM bucardo.bucardo_delta d, bucardo.bucardo_track t WHERE d.txntime <> t.txntime";
	$info = $rdbh->selectall_arrayref($SQL)->[0][0];
	is($info, 0, $t);

	$t=qq{ Update to source $table populated source bucardo_delta correctly };
	$SQL = "UPDATE $table SET inty = 2";
	$sdbh->do($SQL);
	$now = now_time($sdbh);
	$info = $sdbh->selectall_arrayref($sourcerows);
	unshift @$src_delta, [$oid,$val,$now];
	is_deeply($info, $src_delta, $t);
	$sdbh->commit();

	wait_until_true($rdbh => "SELECT 1 FROM $table WHERE inty = 2");

	$t=qq{ Update to source $table with makedelta created a new bucardo_delta row };
	$info = $rdbh->selectall_arrayref($remoterows);
	unshift @$tgt_delta, [$roid, $val];
	is_deeply($info, $tgt_delta, $t);

	$t=qq{ Update to source $table with makedelta created a new bucardo_track row };
	$info = $rdbh->selectall_arrayref($remotetrackrows);
	unshift @$tgt_track, [$roid,'bctest1'];
	is_deeply($info, $tgt_track, $t);

	$t=q{ All rows in bucardo_delta and bucardo_track have the same txntime tablename };
	$SQL = "SELECT count(*) FROM bucardo.bucardo_delta d, bucardo.bucardo_track t ".
		"WHERE d.txntime = t.txntime AND d.tablename = $roid AND t.tablename = $roid";
	$info = $rdbh->selectall_arrayref($SQL)->[0][0];
	is($info, 2, $t);

	## Do an update of the primary key: which should give two rows in bucardo_delta on both ends
	$t=qq{ Update to pk of source $table populated source bucardo_delta correctly };
	my $newval = $val{$type}{3};
	$SQL = "UPDATE $table SET id = '$newval' WHERE id = '$val'";
	$sdbh->do($SQL);
	$now = now_time($sdbh);
	$info = $sdbh->selectall_arrayref($sourcerows);
	unshift @$src_delta, [$oid,$newval,$now],[$oid,$val,$now];
	is_deeply($info, $src_delta, $t);
	$sdbh->commit();

	wait_until_true($rdbh => "SELECT 1 FROM $table WHERE id = '$newval'");

	$t=qq{ Update to pk of source $table with makedelta created two target bucardo_delta rows ($val) };
	$info = $rdbh->selectall_arrayref($remoterows);
	unshift @$tgt_delta, [$roid, $newval], [$roid,$val];
	is_deeply($info, $tgt_delta, $t);

	$t=qq{ Update to pk of source $table with makedelta created a target bucardo_track row };
	$info = $rdbh->selectall_arrayref($remotetrackrows);
	unshift @$tgt_track, [$roid,'bctest1'];
	is_deeply($info, $tgt_track, $t);

	$t=q{ All rows in bucardo_delta and bucardo_track have the same txntime tablename };
	my $bothrows = "SELECT count(*) FROM bucardo.bucardo_delta d, bucardo.bucardo_track t ".
		"WHERE d.txntime = t.txntime AND d.tablename = $roid AND t.tablename = $roid";
	$info = $rdbh->selectall_arrayref($bothrows)->[0][0];
	is($info, 4, $t);

	## Delete should also add a ghost row
	$t=qq{ Delete to source $table populated source bucardo_delta correctly };
	$SQL = "DELETE FROM $table WHERE id = '$newval'";
	$count = $sdbh->do($SQL);
	$now = now_time($sdbh);
	$sdbh->commit();
	$info = $sdbh->selectall_arrayref($sourcerows);
	unshift @$src_delta, [$oid,$newval,$now];
	is_deeply($info, $src_delta, $t);

	wait_until_false($rdbh => "SELECT 1 FROM $table WHERE id = '$newval'");

	$t=qq{ Delete to source $table with makedelta created a target bucardo_delta row };
	$info = $rdbh->selectall_arrayref($remoterows);
	unshift @$tgt_delta, [$roid,$newval];
	is_deeply($info, $tgt_delta, $t);

	$t=qq{ Delete to source $table with makedelta created a target bucardo_track row };
	$info = $rdbh->selectall_arrayref($remotetrackrows);
	unshift @$tgt_track, [$roid,'bctest1'];
	is_deeply($info, $tgt_track, $t);

	$t=q{ All rows in bucardo_delta and bucardo_track have the same txntime tablename };
	$info = $rdbh->selectall_arrayref($bothrows)->[0][0];
	is($info, 5, $t);

	## Now the same thing, but the other way
	$sdbh->do("DELETE FROM bucardo.bucardo_track");
	$sdbh->do("DELETE FROM bucardo.bucardo_delta");
	$sdbh->commit();
	$rdbh->do("DELETE FROM bucardo.bucardo_delta");
	$rdbh->do("DELETE FROM bucardo.bucardo_track");
	$rdbh->commit();
	$src_delta = []; $src_track = []; $tgt_delta = []; $tgt_track = [];

	$sourcerows = "SELECT tablename,rowid FROM bucardo.bucardo_delta WHERE tablename = $oid ".
		"ORDER BY txntime DESC, rowid DESC";

	## Insert:
	$val = $val{$type}{4};
	$SQL = "INSERT INTO $table(id,data1,inty) VALUES ('$val','one',4)";
	$rdbh->do($SQL);
	$rdbh->commit();

	wait_until_true($sdbh => "SELECT 1 FROM $table WHERE inty = 4");

	## The source delta and track tables should now have entries
	$t=qq{ Insert to target $table with makedelta created a source bucardo_delta row };
	$info = $sdbh->selectall_arrayref($sourcerows);
	unshift @$src_delta, [$oid,$val];
	is_deeply($info, $src_delta, $t);

	$t=qq{ Insert to target $table with makedelta created a source bucardo_track row };
	$info = $sdbh->selectall_arrayref($sourcetrackrows);
	unshift @$src_track, [$oid,'bctest2'];
	is_deeply($info, $src_track, $t);

	$t=qq{ Update to target $table populated target bucardo_delta correctly };
	$SQL = "UPDATE $table SET inty = 5 WHERE inty = 4";
	$rdbh->do($SQL);
	$now = now_time($rdbh);
	$info = $rdbh->selectall_arrayref($remoterows);
	unshift @$tgt_delta, [$roid,$val], [$roid,$val];
	is_deeply($info, $tgt_delta, $t);
	$rdbh->commit();

	wait_until_true($sdbh => "SELECT 1 FROM $table WHERE inty = 5");

	$t=qq{ Update to target $table with makedelta created a source bucardo_delta row };
	$info = $sdbh->selectall_arrayref($sourcerows);
	unshift @$src_delta, [$oid, $val];
	is_deeply($info, $src_delta, $t);

	$t=qq{ Update to target $table with makedelta created a source bucardo_track row };
	$info = $sdbh->selectall_arrayref($sourcetrackrows);
	unshift @$src_track, [$oid,'bctest2'];
	is_deeply($info, $src_track, $t);

	## Delete should also add a ghost row
	$t=qq{ Delete to target $table populated target bucardo_delta correctly };
	$SQL = "DELETE FROM $table WHERE inty = 5";
	$rdbh->do($SQL);
	$now = now_time($rdbh);
	$info = $rdbh->selectall_arrayref($remoterows);
	unshift @$tgt_delta, [$roid,$val];
	is_deeply($info, $tgt_delta, $t);
	$rdbh->commit();

	wait_until_false($sdbh => "SELECT 1 FROM $table WHERE inty = 5");

	$t=qq{ Delete to target $table with makedelta created a source bucardo_delta row };
	$info = $sdbh->selectall_arrayref($sourcerows);
	unshift @$src_delta, [$oid,$val];
	is_deeply($info, $src_delta, $t);

	$t=qq{ Delete to target $table with makedelta created a source bucardo_track row };
	$info = $sdbh->selectall_arrayref($sourcetrackrows);
	unshift @$src_track, [$oid,'bctest2'];
	is_deeply($info, $src_track, $t);

	$sdbh->commit();
	$rdbh->commit();
	return;

} ## end of makedelta_testing


sub basic_copy_testing {

	my ($table,$sdbh,$rdbh) = @_;

	$location = 'fullcopy';

	$type = $tabletype{$table};
	my $oid = $table{$sdbh}{$table};

	compare_tables($table,$sdbh,$rdbh) or BAIL_OUT "Compare tables failed?!\n";

	$val = $val{$type}{1};

	$SQL = "INSERT INTO $table(id,data1,inty) VALUES ('$val','one',1)";
	$sdbh->do($SQL);
	$sdbh->commit;

	$t=q{ After insert, trigger and rule both populate droptest table };
	my $DROPSQL = "SELECT type,inty FROM droptest WHERE name = ".$sdbh->quote($table)." ORDER BY 1,2";
	$result = [['rule',1],['trigger',1]];
	bc_deeply($result, $sdbh, $DROPSQL, $t);

	$t=q{ Table droptest is empty on remote database };
	$result = [];
	bc_deeply($result, $rdbh, $DROPSQL, $t);

	$t=qq{ Second table $table still empty before kick };
	$SQL = "SELECT id,data1 FROM $table";
	$result = [];
	bc_deeply($result, $rdbh, $SQL, $t);
   
	bucardo_ctl("Kick copytest 0");

	$t=qq{ Second table $table got the fullcopy row};
	$SQL = "SELECT id,data1 FROM $table";
	$result = [[qq{$val},'one']];
	bc_deeply($result, $rdbh, $SQL, $t);

	$t=q{ Triggers and rules did NOT fire on remote table };
	$result = [];
	bc_deeply($result, $rdbh, $DROPSQL, $t);

	## Same thing, but with a different trigger drop method
	$SQL = "UPDATE sync SET disable_triggers = 'SQL', disable_rules = 'pg_class'";
	$masterdbh->do($SQL);
	$masterdbh->do("NOTIFY bucardo_reload_sync_copytest");
	$masterdbh->commit();

	$val = $val{$type}{2};
	$SQL = "INSERT INTO $table(id,data1,inty) VALUES ('$val','two',2)";
	$sdbh->do($SQL);
	$sdbh->commit;

	$t=q{ After insert, trigger and rule both populate droptest table };
	$result = [['rule',1],['rule',2],['trigger',1],['trigger',2]];
	bc_deeply($result, $sdbh, $DROPSQL, $t);
   
	$t=q{ Table droptest is empty on remote database };
	$result = [];
	bc_deeply($result, $rdbh, $DROPSQL, $t);

	bucardo_ctl("kick copytest 0");

	$t=qq{ Second table $table got the fullcopy row};
	$SQL = "SELECT id,data1 FROM $table WHERE inty=2";
	$result = [[qq{$val},'two']];
	bc_deeply($result, $rdbh, $SQL, $t);

	$t=q{ Triggers and rules did NOT fire on remote table };
	$result = [];
	bc_deeply($result, $rdbh, $DROPSQL, $t);

	$rdbh->commit; $sdbh->commit; $masterdbh->commit;
	## Now with many rows
	$SQL = "INSERT INTO $table(id,data1,inty) VALUES (?,?,?)";
	$sth = $sdbh->prepare($SQL);
	for (3..6) {
		$val = $val{$type}{$_};
		$sth->execute($val,'bob',$_);
	}
	$sdbh->commit;

	## Sanity check
	$t=qq{ Rows are not in target table before the kick for $table};
	$sth = $rdbh->prepare("SELECT 1 FROM $table WHERE inty >= 3");
	$count = $sth->execute();
	$sth->finish();
	is($count, '0E0', $t);

	bucardo_ctl("kick copytest 0");

	$t=q{ Second table $table got the fullcopy rows};
	$SQL = "SELECT inty FROM $table ORDER BY 1";
	$result = [['1'],['2'],['3'],['4'],['5'],['6']];
	bc_deeply($result, $rdbh, $SQL, $t);
	$sdbh->commit();
	$rdbh->commit();
	pass(" End of basic_copy_testing for $table");
	return;

} ## end of basic_copy_testing


sub analyze_after_copy {

	my ($table,$sdbh,$rdbh) = @_;

	$location = 'fullcopy analyze';

	$type = $tabletype{$table};
	my $oid = $table{$sdbh}{$table};

	compare_tables($table,$sdbh,$rdbh) or BAIL_OUT "Compare tables failed?!\n";

	my $insertval = 7;
	$val = $val{$type}{$insertval};

	## Make sure by default we do an analyze
	$SQL = "INSERT INTO $table(id,inty) VALUES ('$val', $insertval)";
	$sdbh->do($SQL);

	bucardo_ctl("kick copytest 0");

	$t=q{ By default, analyze_after_copy is run};
	$SQL = qq{
         SELECT reltuples
         FROM pg_class c, pg_namespace n
         WHERE c.relnamespace = n.oid AND n.nspname=? AND c.relname=?
	};
	$sth{reltuples} = $sth = $rdbh->prepare_cached($SQL);
	$sth->execute($TEST_SCHEMA,$table);
	$count = $sth->fetchall_arrayref()->[0][0];
	is($count, $insertval, $t);

	$t=q{ After truncate, reltuples is 0};
	$rdbh->do("TRUNCATE TABLE $table");
	$sth->execute($TEST_SCHEMA,$table);
	$count = $sth->fetchall_arrayref()->[0][0];
	is($count, 0, $t);
	$sdbh->do("TRUNCATE TABLE $table");
	$sdbh->commit();
	$rdbh->commit();

	## Turn off at the goat level, reload the sync, should not analyze
	our $sync_reloaded_notice = 'bucardo_reloaded_sync_copytest';
	$masterdbh->do("LISTEN $sync_reloaded_notice");
	$SQL = "UPDATE goat SET analyze_after_copy = false WHERE tablename = '$table'";
	$masterdbh->do($SQL);
	$masterdbh->commit();
	$masterdbh->do("NOTIFY bucardo_reload_sync_copytest");
	$masterdbh->commit();
	wait_for_notice($masterdbh, $sync_reloaded_notice);

	$SQL = "INSERT INTO $table(id,inty) VALUES ('$val', $insertval)";
	$sdbh->do($SQL);

	bucardo_ctl("kick copytest 0");

	$t=q{ With goat-level analyze_after_copy false, analyze is not run};
	$sth = $sth{reltuples};
	$sth->execute($TEST_SCHEMA,$table);
	$count = $sth->fetchall_arrayref()->[0][0];
	is($count, 0, $t);

	## Turn off at sync level, on at goat, reload the sync, should not analyze
	$rdbh->do("TRUNCATE TABLE $table");
	$sdbh->do("TRUNCATE TABLE $table");
	$sdbh->commit();
	$rdbh->commit();
	$SQL = "UPDATE goat SET analyze_after_copy = true WHERE tablename = '$table'";
	$masterdbh->do($SQL);
	$SQL = "UPDATE sync SET analyze_after_copy = false WHERE name = 'copytest'";
	$masterdbh->do($SQL);
	$masterdbh->commit();
	$masterdbh->do("NOTIFY bucardo_reload_sync_copytest");
	$masterdbh->commit();
	wait_for_notice($masterdbh, $sync_reloaded_notice);

	$SQL = "INSERT INTO $table(id,inty) VALUES ('$val', $insertval)";
	$sdbh->do($SQL);

	bucardo_ctl("kick copytest 0");

	$t=q{ With sync-level analyze_after_copy false, analyze is not run};
	$sth = $sth{reltuples};
	$sth->execute($TEST_SCHEMA,$table);
	$count = $sth->fetchall_arrayref()->[0][0];
	is($count, 0, $t);

	## Turn them both back on, should now run
	$rdbh->do("TRUNCATE TABLE $table");
	$sdbh->do("TRUNCATE TABLE $table");
	$sdbh->commit();
	$rdbh->commit();
	$SQL = "UPDATE sync SET analyze_after_copy = true WHERE name = 'copytest'";
	$masterdbh->do($SQL);
	$masterdbh->commit();
	$masterdbh->do("NOTIFY bucardo_reload_sync_copytest");
	$masterdbh->commit();
	wait_for_notice($masterdbh, $sync_reloaded_notice);

	$SQL = "INSERT INTO $table(id,inty) VALUES ('$val', $insertval)";
	$sdbh->do($SQL);

	bucardo_ctl("kick copytest 0");

	$t=q{ With analyze_after_copy both true, analyze is run};
	$sth = $sth{reltuples};
	$sth->execute($TEST_SCHEMA,$table);
	$count = $sth->fetchall_arrayref()->[0][0];
	is($count, 1, $t);

	return;

} ## end of analyze_after_copy


sub basic_swap_testing {

	my ($table,$sdbh,$rdbh) = @_;

	$location = 'swap';

	$type = $tabletype{$table};
	my $oid = $table{$sdbh}{$table};

	clean_swap_table($table,[$sdbh,$rdbh]);

	compare_tables($table,$sdbh,$rdbh) or BAIL_OUT "Compare tables failed?!\n";

	$val = $val{$type}{1};

	$SQL = "INSERT INTO $table(id,data1,inty) VALUES ('$val','one',1)";
	$sdbh->do($SQL);

	$t=qq{ Second table $table still empty before commit};
	$SQL = "SELECT id,data1 FROM $table";
	$result = [];
	bc_deeply($result, $rdbh, $SQL, $t);

	$t=qq{ Sync on $table does not create a bucardo_track entry before commit};
	$SQL = "SELECT * FROM bucardo.bucardo_track WHERE tablename = $oid";
	bc_deeply([], $sdbh, $SQL, $t);

	$t=q{ After insert, trigger and rule both populate droptest table };
	my $DROPSQL = "SELECT type,inty FROM droptest WHERE name = ".$sdbh->quote($table)." ORDER BY 1,2";
	$result = [['rule',1],['trigger',1]];
	bc_deeply($result, $sdbh, $DROPSQL, $t);
   
	$t=q{ Table droptest is empty on remote database };
	$result = [];
	bc_deeply($result, $rdbh, $DROPSQL, $t);

	## Insert to 1 should be echoed to two, after a slight delay:
	$t=qq{ Second table $table got the sync insert row};
	$now = now_time($sdbh);
	$sdbh->commit();
	wait_until_true($rdbh => "SELECT 1 FROM $table");
	$SQL = "SELECT id,data1 FROM $table";
	$result = [[qq{$val},'one']];
	bc_deeply($result, $rdbh, $SQL, $t);

	$t=qq{ Sync on $table creates a valid bucardo_track entry};
	$SQL = "SELECT * FROM bucardo.bucardo_track WHERE tablename = $oid";
	$result2 = [[$now,$oid,$db{$rdbh}]];
	bc_deeply($result2, $sdbh, $SQL, $t);

	$t=q{ Table droptest is empty on remote database after sync };
	$result = [];
	bc_deeply($result, $rdbh, $DROPSQL, $t);

	## An update should echo
	$t=qq{ Second table $table caught the sync update};
	$SQL = "UPDATE $table SET data1 = 'upper' WHERE id = '$val'";
	$sdbh->do($SQL);
	$now = now_time($sdbh);
	$sdbh->commit();
	wait_until_true($rdbh => "SELECT 1 FROM $table WHERE data1 = 'upper'");
	$SQL = "SELECT id,data1 FROM $table";
	$result = [[qq{$val},'upper']];
	bc_deeply($result, $rdbh, $SQL, $t);

	$t=qq{ Second sync on $table creates a valid bucardo_track entry};
	$SQL = "SELECT * FROM bucardo.bucardo_track WHERE tablename = $oid ORDER BY txntime";
	push @$result2, [$now,$oid,$db{$rdbh}];
	## XX Sometimes make test fails here - race condition?
	bc_deeply($result2, $sdbh, $SQL, $t);

	$t=qq{ Second table $table caught the delete};
	$SQL = "DELETE FROM $table WHERE id = '$val'";
	$sdbh->do($SQL);
	$sdbh->commit();
	wait_until_false($rdbh => "SELECT 1 FROM $table WHERE id = '$val'");
	$SQL = "SELECT id,data1 FROM $table";
	$result = [];
	bc_deeply($result, $rdbh, $SQL, $t);

	## False update, just because
	$SQL = "UPDATE $table SET data1 = 'foobar' WHERE id = '$val'";
	$sdbh->do($SQL);
	$rdbh->do($SQL);

	## Insert, reverse direction
	$t=qq{ First table $table synced the insert};
	$val = $val{$type}{3};
	$SQL = "INSERT INTO $table(id,data1,inty) VALUES ('$val','revins',3)";
	$rdbh->do($SQL);
	$rdbh->commit();
	wait_until_true($sdbh => "SELECT 1 FROM $table WHERE inty = 3");
	$SQL = "SELECT id,data1 FROM $table";
	$result = [[qq{$val},'revins']];
	bc_deeply($result, $sdbh, $SQL, $t);

	## Insert, forward direction, and update, reverse
	$val2 = $val;
	$val = $val{$type}{4};
	$SQL = "INSERT INTO $table(id,data1,inty) VALUES ('$val','insert',4)";
	$sdbh->do($SQL);

	$SQL = "UPDATE $table SET data1 = 'gator' WHERE id = '$val2'";
	$rdbh->do($SQL);

	$t=qq{ Sync on $table inserted to second};
	$sdbh->commit();
	$rdbh->commit();
	wait_until_true($sdbh => "SELECT 1 FROM $table WHERE data1 = 'gator'");
	$SQL = "SELECT id,data1 FROM $table WHERE id = '$val'";
	$result = [[qq{$val},'insert']];
	bc_deeply($result, $rdbh, $SQL, $t);

	$t=qq{ Sync on $table updated first};
	$SQL = "SELECT id,data1 FROM $table WHERE id = '$val2'";
	$result = [[qq{$val2},'gator']];
	bc_deeply($result, $sdbh, $SQL, $t);

	## Add to both sides, delete from both sides, update both sides
	## They currently both have:
	# 3 | gator
	# 4 | insert
	## Add to second: 12, 14, 16

	$rdbh->do("INSERT INTO $table(id,data1,inty) VALUES ('$val{$type}{12}','insert',12)");
	$rdbh->do("INSERT INTO $table(id,data1,inty) VALUES ('$val{$type}{14}','insert',14)");
	$rdbh->do("INSERT INTO $table(id,data1,inty) VALUES ('$val{$type}{16}','insert',16)");
	## Add to first: 13, 15, 17
	$sdbh->do("INSERT INTO $table(id,data1,inty) VALUES ('$val{$type}{13}','insert',13)");
	$sdbh->do("INSERT INTO $table(id,data1,inty) VALUES ('$val{$type}{15}','insert',15)");
	$sdbh->do("INSERT INTO $table(id,data1,inty) VALUES ('$val{$type}{17}','insert',17)");

	## Delete one from each
	$rdbh->do("DELETE FROM $table WHERE id = '$val{$type}{14}'");
	$sdbh->do("DELETE FROM $table WHERE id = '$val{$type}{13}'");

	## Update one old and one new
	$sdbh->do("UPDATE $table SET data1 = 'updated' WHERE id = '$val{$type}{3}'");
	$sdbh->do("UPDATE $table SET data1 = 'updated' WHERE id = '$val{$type}{17}'");
	$rdbh->do("UPDATE $table SET data1 = 'updated' WHERE id = '$val{$type}{4}'");
	$rdbh->do("UPDATE $table SET data1 = 'updated' WHERE id = '$val{$type}{12}'");

	$sdbh->commit();
	$rdbh->commit();
	wait_until_true($rdbh => "SELECT 1 FROM $table WHERE inty = 15");

	$SQL = "SELECT id,data1 FROM $table ORDER BY inty";
	$result = [
			   [qq{$val{$type}{3}},'updated'],
			   [qq{$val{$type}{4}},'updated'],
			   [qq{$val{$type}{12}},'updated'],
			   [qq{$val{$type}{15}},'insert'],
			   [qq{$val{$type}{16}},'insert'],
			   [qq{$val{$type}{17}},'updated'],
			   ];
	bc_deeply($result, $sdbh,  $SQL, " Complex sync of $table looks good on first database");
	bc_deeply($result, $rdbh, $SQL, " Complex sync of $table looks good on second database");

	$sdbh->do("DELETE FROM droptest");
	$rdbh->do("DELETE FROM droptest");

	$sdbh->commit();
	$rdbh->commit();

	return;

} ## end of basic_swap_testing


sub bucardo_delta_populate {

	## Tests the population of bucardo_delta
	my ($table,$dbh) = @_;

	$location = 'delta populate';

	my $oid = $table{$dbh}{$table};
	$type = $tabletype{$table};
	## Just in case, empty out the bucardo_delta table
	$dbh->rollback;
	$dbh->do("DELETE FROM $table");
	$dbh->do("DELETE FROM bucardo.bucardo_delta WHERE tablename = '$oid'");
	$dbh->commit;

	## Does an insert create an entry in the bucardo_delta table?
	$val = $val{$type}{1};

	my $sourcerows = "SELECT * FROM bucardo.bucardo_delta WHERE tablename = $oid ".
		"ORDER BY txntime DESC, rowid DESC";

	$t=q{ Insert to $table populated bucardo_delta correctly};
	$SQL = "INSERT INTO $table(id,data1,inty) VALUES ('$val','one',1)";
	$dbh->do($SQL);
	$now = now_time($dbh);
	$info = $dbh->selectall_arrayref($sourcerows);
	$result = [[$oid,$val,$now]];
	is_deeply($info, $result, $t);

	## Does an update do the same?
	$t=q{ Update to $table populated bucardo_delta correctly};
	$SQL = "UPDATE $table SET data1='changed' WHERE id = '$val'";
	$dbh->do($SQL);
	$now = now_time($dbh);
	$info = $dbh->selectall_arrayref($sourcerows);
	unshift @$result, [$oid,$val,$now];
	is_deeply($info, $result, $t);

	$t=q{ Update to $table populated bucardo_delta correctly};
	$val2 = $val;
	$val = $val{$type}{18};
	$SQL = "UPDATE $table SET id='$val' WHERE id = '$val2'";
	$dbh->do($SQL);
	$info = $dbh->selectall_arrayref($sourcerows);
	unshift @$result, [$oid,$val,$now], [$oid,$val2,$now];
	is_deeply($info, $result, $t);

	## Does a delete add a new row as well?
	$t=qq{ Delete to $table populated bucardo_delta correctly };
	$SQL = "DELETE FROM $table WHERE id = '$val'";
	$dbh->do($SQL);
	$info = $dbh->selectall_arrayref($sourcerows);
	unshift @$result, [$oid,$val,$now];
	is_deeply($info, $result, $t);

	## Two inserts at once
	$t=q{ Double insert to $table populated bucardo_delta correctly};
	$val = $val{$type}{22};
	$val2 = $val{$type}{23};
	$SQL = qq{
		INSERT INTO $table(id,data1,inty)
		SELECT '$val'::$type, 'twentytwo',22
		UNION ALL
		SELECT '$val2'::$type, 'twentythree',23
	};
	$dbh->do($SQL);
	$info = $dbh->selectall_arrayref($sourcerows);
	unshift @$result, [$oid,$val2,$now], [$oid,$val,$now];
	is_deeply($info, $result, $t);
	$dbh->rollback;
	return;

} ## end of bucardo_delta_populate


sub test_customcode {

	our ($table,$sdbh,$tdbh) = @_;

	$location = 'customcode';
	
	$type = $tabletype{$table};
	my $oid = $table{$sdbh}{$table};
	my $toid = $table{$tdbh}{$table};
	die unless $table =~ /(\d+)/;
	my $goatnumber = ($1*2)-1; ## no critic

	clean_swap_table($table,[$sdbh,$tdbh]);
	## Make sure Bucardo has a controller out for this code
	$SQL = "SELECT 1 FROM bucardo.audit_pid WHERE sync = 'customcode' AND type='CTL' AND killdate IS NULL";
	wait_until_true($masterdbh => $SQL);

	## We want to know when the mcp and syncs are reloaded
	our $mcp_reloaded_notice = 'bucardo_reloaded_mcp';
	$masterdbh->do("LISTEN $mcp_reloaded_notice");
	our $sync_reloaded_notice = 'bucardo_reloaded_sync_customcode';
	$masterdbh->do("LISTEN $sync_reloaded_notice");
	$masterdbh->do("DELETE FROM customcode");
	$masterdbh->commit();

	## Test "bad" code
	my $badcode = q{use strict; return 1; };

	my $code = $bc->customcode
		({
		  src_code => $badcode,
		  name     => 'custom code test',
		  sync     => 'customcode',
		  whenrun  => 'before_txn',
		  });

	$t=q{ The customcode method returned a number };
	my $codeid = $code->{id};
	like($codeid, qr{^\d+$}, $t);

	$t=q{ Bucardo was reloaded };
	$masterdbh->do("NOTIFY bucardo_mcp_reload");
	$masterdbh->commit();
	wait_for_notice($masterdbh, $mcp_reloaded_notice);
	pass($t);

	$t=q{ Lack of "dummy" in customcode prevents a sync from activating };
	$SQL = "SELECT 1 FROM bucardo.audit_pid WHERE sync = 'customcode' AND type='CTL' AND killdate IS NULL";
	wait_until_false($masterdbh => $SQL);
	pass($t);

	$badcode = q{use strict; my ($arg) = @_; return if $arg->{dummy}; throwerror; return 1; }; ## no critic

	$SQL = "UPDATE customcode SET src_code =?";
	$sth = $masterdbh->prepare($SQL);
	$sth->execute($badcode);

	$t=q{ Bucardo was reloaded };
	$masterdbh->do("NOTIFY bucardo_mcp_reload");
	$masterdbh->commit();
	wait_for_notice($masterdbh, $mcp_reloaded_notice);
	pass($t);

	$t=q{ Custom code that does not compile prevents a sync from activating };
	$SQL = "SELECT 1 FROM bucardo.audit_pid WHERE sync = 'customcode' AND type='CTL' AND killdate IS NULL";
	$sth = $masterdbh->prepare($SQL);
	wait_until_false($masterdbh => $SQL);
	pass($t);

	## Now load code that does work

	## no critic
	my $codetemplate = q{
use strict;

my ($arg) = @_;

return if exists $arg->{dummy};

my $file = "BC_TEST_FILE";
system("touch $file");

$arg->{message} = "Created file $file";

return;
};
	## use critic

	our (%testcode,%testfile);
	my $BC_TEST_FILE = "/tmp/bucardo_test_file";
	for my $num (1..12) {
		$testcode{$num} = $codetemplate;
		$testfile{$num} = "$BC_TEST_FILE.$num";
		$testcode{$num} =~ s/BC_TEST_FILE/$BC_TEST_FILE.$num/;
		## Remove any previous test file
		unlink $testfile{$num};
	}

	## XXX Make this into a $bc method
	$t=q{ Update of customcode worked};
	$SQL = "UPDATE customcode SET src_code =? WHERE id = ?";
	$sth = $masterdbh->prepare($SQL);
	$count = $sth->execute($testcode{1},$codeid);
	is($count, 1, $t);

	$masterdbh->do("NOTIFY bucardo_mcp_reload");
	$masterdbh->commit();

	$t=q{ Bucardo replied to mcp_reload notice };
	wait_for_notice($masterdbh, $mcp_reloaded_notice);
	pass($t);

	## The sync should be there, and active
	$SQL = "SELECT 1 FROM pg_listener WHERE relname = 'bucardo_syncdone_customcode_bctest2'";
	wait_until_true($masterdbh => $SQL);
	pass(" Sync with good customcode is now active after a reload");

	## Check most of the custom code types
	## TESTCOUNT + 4

	$val = $val{$type}{1};
	$sdbh->do("INSERT INTO $table(id,data1,inty) VALUES ('$val','one',1)");
	$sdbh->commit();

	our $sync_done_notice = "bucardo_syncdone_customcode";
	$masterdbh->do("LISTEN $sync_done_notice");
	$masterdbh->commit();

	sub quick_cc_test {
		my ($name,$number) = @_;

		$bc->customcode
			({
			  src_code => $testcode{$number},
			  name     => "${name}_test",
			  sync     => 'customcode',
			  whenrun  => $name,
			  });

		$masterdbh->do("NOTIFY bucardo_reload_sync_customcode");
		$masterdbh->commit();
		wait_for_notice($masterdbh, $sync_reloaded_notice, 10);
		$masterdbh->commit();

		$sdbh->do("UPDATE $table SET inty = $number");
		$sdbh->commit();
		wait_until_true($tdbh => "SELECT 1 FROM $table WHERE inty = $number");

		my $timeout = 20;
		my $found = 0;
		{
			if (-e $testfile{$number}) {
				$found = 1;
				last;
			}
			last if $timeout-- < 1;
			sleep 0.5;
			redo;
		}

		$t=qq{ Test file "$testfile{$number}" was created by '$name' custom code };
		is(-e _, 1, $t);
		unlink $testfile{$number};
		return;

	} ## end of quick_cc_test

	quick_cc_test('before_txn',            2);
	quick_cc_test('before_check_rows',     3);
	quick_cc_test('before_trigger_drop',   4);
	quick_cc_test('before_trigger_enable', 5);
	quick_cc_test('after_trigger_enable',  6);
	quick_cc_test('after_txn',             7);

	quick_cc_test('before_sync',           11);
	quick_cc_test('after_sync',            12);

	for (1..12) {
		unlink $testfile{$_};
	}

	## Test that conflict code fires, with a simple "target wins" resolution
	my $conflict_code = $testcode{8};

	$conflict_code =~ s/return;/\$arg->{rowinfo}{action} = 2;\nreturn;/;

	## Generate a conflict!

	## Turn off this sync
	my $syncoff = "bucardo_deactivated_sync_customcode";
	$masterdbh->do("LISTEN $syncoff");
	$masterdbh->do("NOTIFY bucardo_deactivate_sync_customcode");
	$masterdbh->commit();

	wait_for_notice($masterdbh, $syncoff);

	## Load in the conflict code
	$masterdbh->do("DELETE FROM customcode");
	$masterdbh->commit();
	$code = $bc->customcode
		({
		  src_code => $conflict_code,
		  name     => 'custom code test',
		  goat     => $goatnumber,
		  whenrun  => 'conflict',
		  });

	$t=q{ The customcode method returned a number };
	$codeid = $code->{id};
	like($codeid, qr{^\d+$}, $t);

	## Create the conflict
	$val = $val{$type}{3};
	$sdbh->do("INSERT INTO $table(id,data1,inty) VALUES ('$val','source',3)");
	$sdbh->commit();
	$tdbh->do("INSERT INTO $table(id,data1,inty) VALUES ('$val','target',33)");
	$tdbh->commit();

	## Start up this sync, then kick it
	my $syncon = "bucardo_activated_sync_customcode";
	$masterdbh->do("LISTEN $syncon");
	$masterdbh->do("NOTIFY bucardo_activate_sync_customcode");
	$masterdbh->commit();

	wait_for_notice($masterdbh, $syncon);
	pass(" Activated sync for conflict testing (table $table)");

	bucardo_ctl("kick customcode 0");

	$t=qq{ Test file "$testfile{8}" was created by 'conflict' custom code };
	is(-e $testfile{8}, 1, $t);
	unlink $testfile{8};

	## Try out 'exception'

	## Turn off this sync
	$masterdbh->do("NOTIFY bucardo_deactivate_sync_customcode");
	$masterdbh->commit();
	wait_for_notice($masterdbh, $syncoff);

	my $exception_code = $testcode{9};

	## no critic
	my $newcode = q{

$SIG{__DIE__} = sub {
	$arg->{warning} = shift;
	die "Out of here!\n";
};

my $rowinfo = $arg->{rowinfo};

my $sdbh = $arg->{sourcedbh};
my $tdbh = $arg->{targetdbh};

my $sourcerow = $rowinfo->{sourcerow};
my $targetrow = $rowinfo->{targetrow};

my $error = $rowinfo->{dbi_error};

if ($error =~ /unique constraint "bucardo_test._email_key"/) {
	## Who threw the error?
	my ($okdbh,$errdbh,$email,$email2);
	if ($rowinfo->{source_error}) { ## target to source failed
		$email = $targetrow->{email};
		$email2 = $sourcerow->{email};
		$errdbh = $sdbh;
		$okdbh = $tdbh;
	}
	else { ## source to target failed
		$email = $sourcerow->{email};
		$email2 = $targetrow->{email};
		$errdbh = $tdbh;
		$okdbh = $sdbh;
	}
	## Our solution? Remove the offending row
	my ($S,$T) = ($rowinfo->{schema},$rowinfo->{table});
	my ($pkeyname,$pkey) = ($rowinfo->{pkeyname}, $rowinfo->{pkey});
	my $SQL = "DELETE FROM $S.$T WHERE email = ?";
	my $sth = $errdbh->prepare($SQL);
	my $count = $sth->execute($email);
	$arg->{runagain} = 1;
	return;
}

$arg->{message} = "Cannot handle unknown error: $error";

};
	## use critic

	$exception_code =~ s/return;/$newcode\nreturn;/;

	## Make the table conflictable - currently has id:inty of 1:11 and 3:33
	$SQL = "UPDATE $table SET inty=55, email = 'nobody\@example.com' WHERE inty=12";
	$sdbh->do($SQL);
	$sdbh->commit();
	$SQL = "UPDATE $table SET inty=44, email = 'nobody\@example.com' WHERE inty=33";
	$tdbh->do($SQL);
	$tdbh->commit();
	## Tables are now: source 1:55 3:33 target 1:7 3:44

	$masterdbh->do("DELETE FROM customcode");
	$masterdbh->commit();
	$code = $bc->customcode
		({
		  src_code => $exception_code,
		  name     => 'custom code test',
		  goat     => $goatnumber,
		  whenrun  => 'exception',
		  });

	$t=q{ The customcode method returned a number };
	$codeid = $code->{id};
	like($codeid, qr{^\d+$}, $t);

	## Start up this sync, then kick it
	$masterdbh->do("NOTIFY bucardo_activate_sync_customcode");
	$masterdbh->commit();
	wait_for_notice($masterdbh, $syncon);
	pass(" Activated sync for exception testing (table $table)");

	bucardo_ctl("kick customcode 0");

	$SQL = "SELECT 1 FROM $table WHERE inty=55";
	wait_until_true($sdbh => $SQL);
	wait_until_true($tdbh => $SQL);

	$t=qq{ Test file "$testfile{9}" was created by 'exception' custom code };
	is(-e $testfile{9}, 1, $t);
	unlink $testfile{9};
	return;

} ## end of test_customcode


sub random_swap_testing {

	## Run lots of random transactions, then compare the differences
	## NOTE: These will not always succeed! But they are good for catching problems

	my ($table,$sdbh,$rdbh) = @_;

	$location = 'random swap';

	## Clean out everything
	clean_swap_table($table,[$sdbh,$rdbh]);

	compare_tables($table,$sdbh,$rdbh) or BAIL_OUT "Compare tables failed?!\n";

	$type = $tabletype{$table};

	$SQL = "INSERT INTO $table(id,data1,inty) VALUES (?,?,?)";
	my $s_insert = $sdbh->prepare($SQL);
	my $r_insert = $rdbh->prepare($SQL);

	for (1..100) {
		my $dbh = rand(2) > 1 ? $sdbh : $rdbh;
		my $action = int rand(100);
		my $commit = int rand (100);
		if ($action < 3) { ## Changed from 3
			## Update
			my $num = int 1+rand(3);
			$SQL = qq{
				UPDATE $table
				SET data1 = 'random_update'
				WHERE inty IN 
					(SELECT inty
					 FROM $table
					 ORDER BY random()
					 LIMIT $num
				)};
			$dbh->do($SQL);
		}
		elsif ($action < 90) {
			## Insert
			$SQL = "SELECT max(inty) FROM $table";
			my $max = $dbh->selectall_arrayref($SQL)->[0][0];
			my $num = int 1+rand(10);
			for (1..$num) {
				$max++;
				my $val = $max;
				next if $val == 3 or $val==4;
				if ($type eq 'TEXT') {
					$val = "bc$val";
				}
				elsif ($type eq 'DATE') {
					$SQL = "SELECT '2001-11-01'::date + '$max days'::interval";
					$val = $dbh->selectall_arrayref($SQL)->[0][0];
				}
				elsif ($type eq 'TIMESTAMP') {
					$SQL = "SELECT '2001-11-01 12:34:56'::timestamp + '$max days'::interval";
					$val = $dbh->selectall_arrayref($SQL)->[0][0];
				}
				eval {
					$sth = ($dbh eq $sdbh) ? $s_insert : $r_insert;
					$sth->execute($val,'newrandom',$max);
				};
				if ($@) {
					$dbh->rollback();
				}
			}
		}
		else {
			## Delete
			my $num = int 1+rand(5);
			$SQL = "DELETE FROM $table WHERE inty IN (SELECT inty FROM $table ORDER BY random() LIMIT $num)";
			$dbh->do($SQL);
		}
		if ($commit < 5) {
			$dbh->commit();
		}
		elsif ($commit < 10) {
			$dbh->rollback();
		}

	} ## end random iterations

	## Final inserts so we know when all syncing has completed
	$val = $val{$type}{3};
	$SQL = "INSERT INTO $table(id,data1,inty) VALUES ('$val','stop',9999)";
	$sdbh->do($SQL);
	$sdbh->commit();
	$val = $val{$type}{4};
	$SQL = "INSERT INTO $table(id,data1,inty) VALUES ('$val','stop',8888)";
	$rdbh->do($SQL);
	$rdbh->commit();

	wait_until_true($rdbh => "SELECT 1 FROM $table WHERE inty = 9999");
	wait_until_true($sdbh => "SELECT 1 FROM $table WHERE inty = 8888");

	compare_tables($table,$sdbh,$rdbh);

	return;

} ## end of random_swap_testing


__DATA__
## The above __DATA__ line must be kept for the test counting
