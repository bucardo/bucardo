#!perl -- -*-cperl-*-

package BucardoTesting;

## Helper module for the Bucardo tests
## Contains shared code for setup and breakdown

use strict;
use warnings;
use DBI;
use Time::HiRes qw/sleep gettimeofday tv_interval/;
use Cwd;
use Data::Dumper;

use vars qw/$SQL $sth $count $COM %dbh/;

my $DEBUG = 0; ## XXX

use base 'Exporter';
our @EXPORT = qw/%tabletype %val compare_tables bc_deeply wait_for_notice $location/;

our $location = 'setup';
my $testmsg  = ' ?';
my $testline = '?';
my $showline = 1;
my $showtime = 0;
## Sometimes, we want to stop as soon as we see an error
my $bail_on_error = $ENV{BUCARDO_TESTBAIL} || 0;
my $total_errors = 0;
## Used by the tt sub
my %timing;

my $user = qx{whoami};
chomp $user;

my $FRESHLOG = 1;
if ($FRESHLOG) {
	unlink 'tmp/bucardo.log';
}

## Test test databases are labelled as A, B, C, etc.
my @dbs = qw/A B C D/;

our %tabletype =
	(
	 'bucardo_test1' => 'SMALLINT',
	 'bucardo_test2' => 'INT',
	 'bucardo_test3' => 'BIGINT',
	 'bucardo_test4' => 'TEXT',
	 'bucardo_test5' => 'DATE',
	 'bucardo_test6' => 'TIMESTAMP',
	 'bucardo_test7' => 'NUMERIC',
	 'bucardo_test8' => 'BYTEA',
	 );

our @tables2empty = (qw/droptest bucardo_test_multicol/);

my %debug = (
			 recreatedb     => 0,
			 recreateschema => 1,
			 recreateuser   => 0,
		 );

my $DEBUGDIR = ".";
-e $DEBUGDIR or mkdir $DEBUGDIR;

## To avoid stepping on other instance's toes
my $PIDDIR = "/tmp/bucardo_testing_$ENV{USER}";
mkdir $PIDDIR if ! -e $PIDDIR;

my %clusterinfo = (
				   A => {port => 58921},
				   B => {port => 58922},
				   C => {port => 58923},
				   D => {port => 58924},
);

## Location of files
my $initdb = $ENV{PGBINDIR} ? "$ENV{PGBINDIR}/initdb" : 'initdb';
my $pg_ctl = $ENV{PGBINDIR} ? "$ENV{PGBINDIR}/pg_ctl" : 'pg_ctl';

# Set a semi-unique name to make killing old tests easier
my $xname = "bctest_$ENV{USER}";

## Maximum time to wait for bucardo_ctl to return
my $ALARM_BUCARDO_CTL = 3;
## Maximum time to wait for a kid to appear via pg_listener
my $ALARM_WAIT4KID = 3;
## How long to wait for most syncs to take effect?
my $TIMEOUT_SYNCWAIT = 3;
## How long to sleep between checks for sync being done?
my $TIMEOUT_SLEEP = 0.1;
## How long to wait for a notice to be issued?
my $TIMEOUT_NOTICE = 2;

## Default test schema name.
my $TEST_SCHEMA = 'bucardo_schema';

## File to store connectin information.
my $TEST_INFO_FILE = 'bucardo.test.data';

## Bail if the bucardo_ctl file does not exist / does not compile
for my $file (qw/bucardo_ctl Bucardo.pm/) {
	if (! -e $file) {
		die "Cannot run without file $file\n";
	}
	eval {
		$ENV{BUCARDO_CTL_TEST} = 1;
		require $file;
		$ENV{BUCARDO_CTL_TEST} = 0;
	};
	if ($@) {
		die "Cannot run unless $file compiles cleanly\n";
	}
}

## Prepare some test values for easy use
our %val;
for (1..30) {
	$val{SMALLINT}{$_} = $_;
	$val{INT}{$_} = 1234567+$_;
	$val{BIGINT}{$_} = 7777777777 + $_;
	$val{TEXT}{$_} = "bc$_";
	$val{DATE}{$_} = sprintf "2001-10-%02d", $_;
	$val{TIMESTAMP}{$_} = $val{DATE}{$_} . " 12:34:56";
	$val{NUMERIC}{$_} = 0.7 + $_;
	$val{BYTEA}{$_} = "$_\0Z";
}


sub new {

	## Create a new BucardoTesting object.
	## Most defaults should be fine.

	my $class = shift;
	my $arg = shift || {};
	my $self = {};
	$self->{file} = (caller)[1];

	## Short name for this test. Should always be set.
	$self->{name} = $arg->{name} || '?';

	## Bail on first error? Default is ENV, then false.
	$self->{bail} = exists $arg->{bail} ? $arg->{bail} : $ENV{BUCARDO_TESTBAIL} || 0;

	## Whether to show what line an error came from. Defaults true.
	$self->{showline} = exists $arg->{showline} ? $arg->{showline} : 1;

	## Whether to show a running time. Defaults false.
	$self->{showtime} = $arg->{showtime} || 0;

	## Name of the test schema. Should rarely need to be set
	$self->{schema} = $arg->{schema} || $TEST_SCHEMA;

	## Where to find the connection data. Rarely changed.
	$self->{info_file} = $arg->{info_file} || $TEST_INFO_FILE;

	bless $self, $class;

	#$self->read_test_info();

	## Let's find out where bucardo_ctl is. Prefer the blib ones, which are shebang adjusted
	if (-e 'blib/script/bucardo_ctl') {
		$self->{bucardo_ctl} = 'blib/script/bucardo_ctl';
	}
	elsif (-e '../blib/script/bucardo_ctl') {
		$self->{bucardo_ctl} = '../blib/script/bucardo_ctl';
	}
	elsif (-e './bucardo_ctl') {
		$self->{bucardo_ctl} = './bucardo_ctl';
	}
	elsif (-e '../bucardo_ctl') {
		$self->{bucardo_ctl} = '../bucardo_ctl';
	}
	else {
		die qq{Could not find bucardo_ctl\n};
	}

	return $self;
}


sub read_test_info {

	## Read connection information from a file
	## Populates the hashref $self->{bcinfo}

	my $self = shift;

	my $file = $self->{info_file};

	## Check for a 't' dir first, then current dir
	my $fh;
	if (-e "t/$file") {
		open $fh, '<', "t/$file" or die qq{Could not open "t/$file": $!\n};
	}
	elsif (-e $file) {
		open $fh, '<', "$file" or die qq{Could not open "$file": $!\n};
	}
	else {
		die qq{Could not find file "$file": $!\n};
	}

	my %bc;
	while (<$fh>) {
		next unless /^\s*(\w\S+?):?\s+(.*?)\s*$/;
		$bc{$1} = $2; ## no critic
		$DEBUG >= 3 and warn "Read $1: $2\n";
	}

	## Quick sanity check
	for my $req (qw(DBNAME DBUSER TESTDB TESTBC)) {
		for my $suffix ('bucardo', @dbs) {
			my $name = "${req}_$suffix";
			exists $bc{$name} or die qq{Required test arg "$name" not found in config file};
		}
	}
	close $fh;

	$self->{bcinfo} = \%bc;

	return;

} ## end of read_test_info

sub blank_database {

	## Create, start, and empty out a database ("server");

	my $self = shift;
	my $name = shift || 'A';

	## Does it exist? If not, create with initdb
	$self->create_cluster($name);

	## Make sure it is started up
    $self->start_cluster($name);

	## Empty it out (drop and recreate the test database)
    my $dbh = $self->fresh_database($name);

	## Populate a test database
	$self->add_test_schema($dbh,'foo');

	return $dbh;

} ## end of blank_database


sub create_cluster {

	## Create a cluster if it does not already exist

	my $self = shift;
	my $name = shift || 'A';
	my $arg = shift || ''; ## A string to append to initdb call

	my $clusterinfo = $clusterinfo{$name}
		or die qq{I do not know how to create a cluster named "$name"};

	my $dirname = "bucardo_test_database_$name";

	return if -d $dirname;

	$DEBUG and warn qq{Running initdb for cluster "$name"\n};

	qx{$initdb -D $dirname $arg 2>&1};

	## Make some minor adjustments
	my $file = "$dirname/postgresql.conf";
	open my $fh, '>>', $file or die qq{Could not open "$file": $!\n};
	printf $fh "\n\nport = %d\nmax_connections = 20\nrandom_page_cost = 2.5\nlog_statement = 'all'\nclient_min_messages = WARNING\n\n",
		$clusterinfo->{port};
	print $fh "logging_collector = off\n";
	close $fh or die qq{Could not close "$file": $!\n};

	return;


} ## end of create_cluster


sub start_cluster {

	## Startup a cluster if not already running

	my $self = shift;
	my $name = shift || 'A';
	my $arg = shift || '';

	my $dirname = "bucardo_test_database_$name";

	## Just in case
	-d $dirname or $self->create_cluster($name);

	my $pidfile = "$dirname/postmaster.pid";
	if (-e $pidfile) {
		open my $fh, '<', $pidfile or die qq{Could not open "$pidfile": $!\n};
		<$fh> =~ /(\d+)/ or die qq{No PID found in file "$pidfile"\n};
		my $pid = $1;
		close $fh or die qq{Could not close "$pidfile": $!\n};
		## Make sure it's still around
		$count = kill 0 => $pid;
		return if $count == 1;
		$DEBUG and warn qq{Server seems to have died, removing file "$pidfile"\n};
		unlink $pidfile or die qq{Could not remove file "$pidfile"\n};
	}

	$DEBUG and warn qq{Starting cluster "$name"\n};

	my $option = '';
	if ($^O !~ /Win32/) {
		my $sockdir = "$dirname/socket";
		-e $sockdir or mkdir $sockdir;
		$option = q{-o '-k socket'};
	}
	$COM = qq{$pg_ctl $option -l $dirname/pg.log -D $dirname start};
	qx{$COM};

	{
		last if -e $pidfile;
		sleep 0.1;
		redo;
	}

	## Wait for "ready to accept connections"
	my $logfile = "$dirname/pg.log";
	open my $fh, '<', $logfile or die qq{Could not open "$logfile": $!\n};
	seek $fh, -100, 2;
	LOOP: {
		  while (<$fh>) {
			  last LOOP if /system is ready/;
		  }
		  sleep 0.1;
		  seek $fh, 0, 1;
		  redo;
	  }
	close $fh or die qq{Could not close "$logfile": $!\n};

	return;

} ## end of start_cluster


sub fresh_database {

	## Drop and create the bucardo_test database
	## First arg is cluster name
	## Second arg is hashref, can be 'dropdb'

	my $self = shift;
	my $name = shift || 'A';
	my $arg = shift || {};

	my $dirname = "bucardo_test_database_$name";

	## Just in case
	-d $dirname or $self->create_cluster($name);
	-e "$dirname/postmaster.pid" or $self->start_cluster($name);

	my $dbh = $self->connect_database($name, 'postgres');

	my $dbname = 'bucardo_test';
	my $brandnew = 0;
	{
		if (database_exists($dbh => $dbname) and $arg->{dropdb}) {
			local $dbh->{AutoCommit} = 1;
			$DEBUG and warn "Dropping database $dbname\n";
			$dbh->do("DROP DATABASE $dbname");
		}
		if (!database_exists($dbh => $dbname)) {
			local $dbh->{AutoCommit} = 1;
			$DEBUG and warn "Creating database $dbname\n";
			$dbh->do("CREATE DATABASE $dbname");
			$brandnew = 1;
			$dbh->disconnect();
		}
	}

	$dbh = $self->connect_database($name, $dbname);

	return $dbh if $brandnew;

	$self->empty_test_database($dbh);

	return $dbh;

} ## end of fresh_database


sub empty_test_database {

	## Wipe all data tables from a test database
	## Takes a database handle as only arg

	my $self = shift;
	my $dbh = shift;

	for my $table (sort keys %tabletype) {
		$dbh->do("TRUNCATE TABLE $table");
	}

	for my $table (@tables2empty) {
		$dbh->do("TRUNCATE TABLE $table");
	}

	$dbh->commit;

	return;

} ## end of empty_test_database

sub shutdown_cluster {

	## Shutdown a cluster if running
	## Takes the cluster name

	my $self = shift;
	my $name = shift;

	my $dirname = "bucardo_test_database_$name";

	return if ! -d $dirname;

	my $pidfile = "$dirname/postmaster.pid";
	return if ! -e $pidfile;

	open my $fh, '<', $pidfile or die qq{Could not open "$pidfile": $!\n};
	<$fh> =~ /(\d+)/ or die qq{No PID found in file "$pidfile"\n};
	my $pid = $1;
	close $fh or die qq{Could not close "$pidfile": $!\n};
	## Make sure it's still around
	$count = kill 0 => $pid;
	if ($count != 1) {
		$DEBUG and warn "Removing $pidfile\n";
		unlink $pidfile;
	}
	$count = kill 15 => $pid;
	print "New count: $count\n";
	{
		$count = kill 0 => $pid;
		last if $count != 1;
		sleep 0.2;
		redo;
	}

	return;

} ## end of shutdown_cluster


sub remove_cluster {

	## Remove a cluster, shutting it down first
	## Takes the cluster name

	my $self = shift;
	my $name = shift;

	my $dirname = "bucardo_test_database_$name";

	return if ! -d $dirname;

	## Just in case
	$self->shutdown_cluster($name);

	system("rm -fr $dirname");

	return;

} ## end of remove_cluster

sub connect_database {

	## Given a cluster name, return a connection to it
	## Second arg is the database name, defaults to 'bucardo_test'

	my $self = shift;
	my $name = shift || 'A';
	my $dbname = shift || 'bucardo_test';

	my $clusterinfo = $clusterinfo{$name}
		or die qq{I do not know about a cluster named "$name"};

	my $dbport = $clusterinfo->{port};
	my $dbhost = getcwd;
	$dbhost .= "/bucardo_test_database_$name/socket";

	my $dsn = "dbi:Pg:dbname=$dbname;port=$dbport;host=$dbhost";

	if (exists $dbh{$dsn}) {
		my $dbh = $dbh{$dsn};
		$dbh->ping and return $dbh;
		delete $dbh{$dsn};
	}

	my $dbh = DBI->connect($dsn, '', '', {AutoCommit=>0, RaiseError=>1, PrintError=>0});

	$dbh->ping();

	return $dbh;

} ## end of connect_database


sub add_test_schema {

	## Add an empty test schema to a database
	## Takes a database handle

	my $self = shift;
	my $dbh = shift;

	## Assume it is empty and just load it in

	## Empty out or create the droptest table
	if (table_exists($dbh => 'droptest')) {
		$dbh->do('TRUNCATE TABLE droptest');
	}
	else {
		$dbh->do(q{
            CREATE TABLE droptest (
              name TEXT NOT NULL,
              type TEXT NOT NULL,
              inty INTEGER NOT NULL
            )
        });
	}

	## Create the language if needed
	if (!language_exists($dbh => 'plpgsql')) {
		$dbh->do('CREATE LANGUAGE plpgsql');
	}
	$dbh->commit();

	## Create supporting functions as needed
	if (!function_exists($dbh => 'trigger_test')) {
		$dbh->do(q{
                CREATE FUNCTION trigger_test()
                RETURNS trigger
                LANGUAGE plpgsql
                AS $_$ BEGIN
                INSERT INTO droptest(name,type,inty)
                    VALUES (TG_RELNAME, 'trigger', NEW.inty);
                RETURN NULL;
                END;
                $_$
            });
	}
	if (!function_exists($dbh => 'trigger_test_zero')) {
		$dbh->do(q{
                CREATE FUNCTION trigger_test_zero()
                RETURNS trigger
                LANGUAGE plpgsql
                AS $_$ BEGIN
                INSERT INTO droptest(name,type,inty)
                    VALUES (TG_RELNAME, 'trigger', 0);
                RETURN NULL;
                END;
                $_$;
            });
	}

	## Create one table for each table type
	for my $table (sort keys %tabletype) {

		local $dbh->{Warn} = 0;

		## Does the table already exist? If so, drop it.
		if (table_exists($dbh => $table)) {
			$dbh->do("DROP TABLE $table");
		}

		my $pkeyname = $table =~ /test5/ ? q{"id space"} : 'id';
		my $pkindex = $table =~ /test2/ ? '' : 'PRIMARY KEY';
		$SQL = qq{
			CREATE TABLE $table (
				$pkeyname    $tabletype{$table} NOT NULL $pkindex};
		$SQL .= $table =~ /0/ ? "\n)" : qq{,
    	        data1 TEXT                   NULL,
        	    inty  SMALLINT               NULL,
            	bite1 BYTEA                  NULL,
            	bite2 BYTEA                  NULL,
            	email TEXT                   NULL UNIQUE
        	)
			};

		$dbh->do($SQL);

		if ($table =~ /test2/) {
			$dbh->do("ALTER TABLE $table ADD CONSTRAINT multipk PRIMARY KEY ($pkeyname,data1)");
		}

		## Create a trigger to test trigger supression during syncs
		$SQL = qq{
			CREATE TRIGGER bctrig_$table
			AFTER INSERT OR UPDATE ON $table
			FOR EACH ROW EXECUTE PROCEDURE trigger_test()
			};
		$table =~ /0/ and ($SQL =~ s/trigger_test/trigger_test_zero/);
		$dbh->do($SQL);

		## Create a rule to test rule supression during syncs
		$SQL = qq{
			CREATE OR REPLACE RULE bcrule_$table
			AS ON INSERT TO $table
			DO ALSO INSERT INTO droptest(name,type,inty) VALUES ('$table','rule',NEW.inty)
			};
		$table =~ /0/ and $SQL =~ s/NEW.inty/0/;
		$dbh->do($SQL);

	}
	if ( !table_exists($dbh => 'bucardo_test_multicol') ) {
		$dbh->do(q{CREATE TABLE bucardo_test_multicol (
        id   INTEGER,
        id2  INTEGER,
        id3  INTEGER,
        data TEXT,
        PRIMARY KEY (id, id2, id3))});
	}
	$dbh->commit();

	return;

} ## end of add_test_schema

sub setup_bucardo {

	## Import the bucardo schema into a database named 'bucardo_control_test'
	## Takes a cluster name and an optional database handle
	## Returns a handle to the control database

	my $self = shift;
	my $name = shift || 'A';
	my $dbh = shift || $self->connect_database($name);

	my $dbname = 'bucardo_control_test';

	if (!database_exists($dbh => $dbname)) {
		local $dbh->{AutoCommit} = 1;
		$dbh->do("CREATE DATABASE $dbname");
		$dbh->do("CREATE SCHEMA bucardo");
		$dbh->do("CREATE SCHEMA freezer");
		$dbh->do("ALTER DATABASE $dbname SET search_path = bucardo, freezer, public");
		$DEBUG and warn "Creating database $dbname\n";
	}

	## Are we connected to this database? If not, connect to it
	$SQL = "SELECT current_database()";
	my $localdb = $dbh->selectall_arrayref($SQL)->[0][0];
	if ($localdb ne $dbname) {
		$dbh = $self->connect_database($name, $dbname);
	}

	## Create the languages if needed
	if (!language_exists($dbh => 'plpgsql')) {
		$dbh->do('CREATE LANGUAGE plpgsql');
	}
	if (!language_exists($dbh => 'plperlu')) {
		$dbh->do('CREATE LANGUAGE plperlu');
	}
	$dbh->commit();

	## Drop the existing schemas
	if (schema_exists($dbh => 'bucardo')) {
		local $dbh->{Warn};
		$dbh->do('DROP SCHEMA bucardo CASCADE');
		$dbh->do('DROP SCHEMA freezer CASCADE');
	}
	$dbh->commit();

	add_bucardo_schema_to_database($dbh);

	return $dbh;

} ## end of setup_bucardo

sub thing_exists {
	my ($dbh,$name,$table,$column) = @_;
	my $SQL = "SELECT 1 FROM $table WHERE $column = ?";
	my $sth = $dbh->prepare($SQL);
	$count = $sth->execute($name);
	$sth->finish();
	$dbh->commit();
	return $count < 1 ? 0 : $count;
}

sub schema_exists   { return thing_exists(@_, 'pg_namespace', 'nspname'); }
sub language_exists { return thing_exists(@_, 'pg_language',  'lanname'); }
sub database_exists { return thing_exists(@_, 'pg_database',  'datname'); }
sub user_exists     { return thing_exists(@_, 'pg_user',      'usename'); }
sub table_exists    { return thing_exists(@_, 'pg_class',     'relname'); }
sub function_exists { return thing_exists(@_, 'pg_proc',      'proname'); }

## no critic
{
	no warnings; ## Yes, we know they are being redefined!
	sub is_deeply {
		t($_[2],$_[3] || (caller)[2]);
		return if Test::More::is_deeply($_[0],$_[1],$testmsg);
		if ($bail_on_error > $total_errors++) {
			my $line = (caller)[2];
			my $time = time;
			Test::More::diag("GOT: ".Dumper $_[0]);
			Test::More::diag("EXPECTED: ".Dumper $_[1]);
			Test::More::BAIL_OUT "Stopping on a failed 'is_deeply' test from line $line. Time: $time";
		}
	} ## end of is_deeply
	sub like($$;$) {
		t($_[2],(caller)[2]);
		return if Test::More::like($_[0],$_[1],$testmsg);
		if ($bail_on_error > $total_errors++) {
			my $line = (caller)[2];
			my $time = time;
			Test::More::diag("GOT: ".Dumper $_[0]);
			Test::More::diag("EXPECTED: ".Dumper $_[1]);
			Test::More::BAIL_OUT "Stopping on a failed 'like' test from line $line. Time: $time";
		}
	} ## end of like
	sub pass(;$) {
		t($_[0],$_[1]||(caller)[2]);
		Test::More::pass($testmsg);
	} ## end of pass
	sub is($$;$) {
		t($_[2],(caller)[2]);
		return if Test::More::is($_[0],$_[1],$testmsg);
		if ($bail_on_error > $total_errors++) {
			my $line = (caller)[2];
			my $time = time;
			Test::More::BAIL_OUT "Stopping on a failed 'is' test from line $line. Time: $time";
		}
	} ## end of is
	sub isa_ok($$;$) {
		t("Object isa $_[1]",(caller)[2]);
		my ($name, $type, $msg) = ($_[0],$_[1]);
		if (ref $name and ref $name eq $type) {
			Test::More::pass($testmsg);
			return;
		}
		$bail_on_error > $total_errors++ and Test::More::BAIL_OUT "Stopping on a failed test";
	} ## end of isa_ok
	sub ok($;$) {
		t($_[1]||$testmsg);
		return if Test::More::ok($_[0],$testmsg);
		if ($bail_on_error > $total_errors++) {
			my $line = (caller)[2];
			my $time = time;
			Test::More::BAIL_OUT "Stopping on a failed 'ok' test from line $line. Time: $time";
		}
	} ## end of ok
}
## use critic


sub tt {
	## Simple timing routine. Call twice with the same arg, before and after
	my $name = shift or die qq{Need a name!\n};
	if (exists $timing{$name}) {
		my $newtime = tv_interval($timing{$name});
		$DEBUG and warn "Timing for $name: $newtime\n";
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

sub add_bucardo_schema_to_database {

	## Parses the bucardo.schema file and creates the database
	## Assumes the schema 'bucardo' does not exist yet
	## First argument is a database handle

	my $dbh = shift;

	if (schema_exists($dbh => 'bucardo')) {
		return;
	}

	my $schema_file = 'bucardo.schema';
	-e $schema_file or die qq{Cannot find the file "$schema_file"!};
	open my $fh, '<', $schema_file or die qq{Could not open "$schema_file": $!\n};
	my $sql='';
	my (%copy,%copydata);
	my ($start,$copy,$insidecopy) = (0,0,0);
	while (<$fh>) {
		if (!$start) {
			next unless /ON_ERROR_STOP on/;
			$start = 1;
			next;
		}
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

	## Make some adjustments
	$sth = $dbh->prepare('UPDATE bucardo.bucardo_config SET value = $2 WHERE setting = $1');
	$count = $sth->execute('piddir' => $PIDDIR);
	$count = $sth->execute('reason_file' => "$PIDDIR/reason");
	$dbh->commit();

} ## end of add_bucardo_schema_to_database

sub add_db_args {

	## Return a DSN-like string for a particular named cluster
	my ($self,$name) = @_;

	my $clusterinfo = $clusterinfo{$name}
		or die qq{I do not know how to create a cluster named "$name"};

	my $port = $clusterinfo->{port};

	my $host = getcwd;
	$host .= "/bucardo_test_database_$name/socket";

	my $arg = "name=$name user=$user port=$port host=$host";

	return $arg;

} ## end of add_db_args


sub ctl {

	## Run a simple non-forking command against bucardo_ctl, get the answer back as a string
	## Emulates a command-line invocation

	my ($self,$args) = @_;

	my $info;
	my $ctl = $self->{bucardo_ctl};

	## Build the connection options
	my $bc = $self->{bcinfo};
	my $connopts = '';
	for my $arg (qw/host port pass/) {
		my $val = 'DB' . (uc $arg) . '_bucardo';
		next unless exists $bc->{$val} and length $bc->{$val};
		$connopts .= " --db$arg=$bc->{$val}";
	}
	$connopts .= " --dbname=bucardo_control_test --debugfile=1";
	$connopts .= " --dbuser=$user";
	## Just hard-code these, no sense in multiple Bucardo base dbs yet:
	$connopts .= " --dbport=58921";
	my $dbhost = getcwd;
	$dbhost .= "/bucardo_test_database_A/socket";
	$connopts .= " --dbhost=$dbhost";

	$DEBUG >=3 and warn "Connection options: $connopts Args: $args\n";
	eval {
		$info = qx{$ctl $connopts $args 2>&1};
	};
	if ($@) {
		return "Error running bucardo_ctl: $@\n";
	}
	$DEBUG >= 3 and warn "bucardo_ctl said: $info\n";

	return $info;

} ## end of ctl

sub add_test_databases {

	## Add one or more databases to the bucardo.db table
	## Arg is a string containing white-space separated db names

	my $self = shift;
	my $string = shift;

	for my $db (split /\s+/ => $string) {
		my $ctlargs = $self->add_db_args($db);
		my $i = $self->ctl("add database bucardo_test $ctlargs");
		die $i if $i =~ /ERROR/;
	}

	return;

} ## end of add_test_databases


sub add_test_tables_to_herd {

	## Add all of the test tables to a herd
	## Create the herd if it does not exist
	## First arg is database name, second arg is the herdname

	my $self = shift;
	my $db = shift;
	my $herd = shift;

	my $result = $self->ctl("add herd $herd");
	if ($result !~ /Herd added/) {
		die "Failed to add herd $herd: $result\n";
	}

	my $addstring = join ' ' => sort keys %tabletype;
    $addstring .= ' bucardo_test_multicol';
	my $com = "add table $addstring db=$db herd=$herd";
	$result = $self->ctl($com);
	if ($result !~ /Tables? added:/) {
		die "Failed to add tables: $result (command was: $com)\n";
	}

	return;

} ## end of add_test_tables_to_herd



sub restart_bucardo {

	## Start Bucardo, but stop first if it is already running
	## Pass in a database handle to the bucardo_control_test db

	my ($self,$dbh,$notice,$passmsg) = @_;

	## Which notice is good enough?
	$notice ||= 'bucardo_started';
	$passmsg ||= 'Bucardo was started';

	$self->stop_bucardo();

	pass('Starting up Bucardo');
	$dbh->do('LISTEN bucardo_boot');
	$dbh->do('LISTEN bucardo_started');
	$dbh->do('LISTEN bucardo_nosyncs');
	$dbh->commit();

	$self->ctl('start testing');

	my $bail = 10;
	my $n;
  WAITFORIT: {
		if ($bail--<0) {
			die "Bucardo did not start, but we waited!\n";
		}
		while ($n = $dbh->func('pg_notifies')) {
			last WAITFORIT if $n->[0] eq $notice;
		}
		$dbh->commit();
		sleep 0.2;
		redo;
	}
	pass($passmsg);

	return 1;

} ## end of restart_bucardo


sub stop_bucardo {

	my ($self,$dbh) = @_;

	$self->ctl('stop testing');

	sleep 0.2;

	return 1;

} ## end of stop_bucardo


sub bc_deeply {

	my ($exp,$dbh,$sql,$msg,$oline) = @_;
	my $line = (caller)[2];

	local $Data::Dumper::Terse = 1;
	local $Data::Dumper::Indent = 0;

	die "Very invalid statement from line $line: $sql\n" if $sql !~ /^\s*select/i;

	my $got;
	eval {
		$got = $dbh->selectall_arrayref($sql);
	};
	if ($@) {
		die "bc_deeply failed from line $line. SQL=$sql\n";
	}

	$dbh->commit();
	return is_deeply($got,$exp,$msg,$oline||(caller)[2]);

} ## end of bc_deeply


sub wait_for_notice {

	my $dbh = shift;
	my $text = shift;
	my $timeout = shift || $TIMEOUT_NOTICE;
	my $sleep = shift || $TIMEOUT_SLEEP;
	my $n;
	eval {
		local $SIG{ALRM} = sub { die "Lookout!\n"; };
		alarm $timeout;
	  N: {
			while ($n = $dbh->func('pg_notifies')) {
				last N if $n->[0] eq $text;
			}
			sleep $sleep;
			redo;
		}
		alarm 0;
	};
	if ($@) {
		if ($@ =~ /Lookout/o) {
			my $line = (caller)[2];
			Test::More::BAIL_OUT (qq{Gave up waiting for notice "$text": timed out at $timeout from line $line});
			return;
		}
	}
	return;

} ## end of wait_for_notice

sub get_pgctl_options {
    my $dirname = shift;
    my $option;
	if ($^O !~ /Win32/) {
		my $sockdir = "$dirname/socket";
		-e $sockdir or mkdir $sockdir;
		$option = q{-o '-k socket'};
	}
    return $option;
}

sub remove_single_dir {
    my $dirname = shift;
    print "Removing test database in $dirname\n";
    # Try stopping PostgreSQL
    my $options = get_pgctl_options($dirname);
    qx{$pg_ctl $options -l $dirname/pg.log -D $dirname stop -m immediate};
    sleep 2;
    qx{rm -rf $dirname};
    return;
}

sub drop_database {
    my ($self, $dir) = @_;
    if ($dir eq 'all') {
        ok(opendir(my $dh, '.'), 'Open current directory to clean up');
        my @test_db_dirs = grep { -d $_ && /^bucardo_test_database/ } readdir $dh;
        close($dh);

        for my $dirname (@test_db_dirs) {
            remove_single_dir($dirname);
        }
    }
    else {
        remove_single_dir($dir);
    }
    return;
}

sub scrub_bucardo_tables {

	## Empty out all stuff from the bucardo schema

	my $self = shift;
	my $dbh = shift;

	$dbh->do("DELETE FROM bucardo.sync");
	$dbh->do("DELETE FROM bucardo.herd");
	$dbh->do("DELETE FROM bucardo.herdmap");
	$dbh->do("DELETE FROM bucardo.goat");
	$dbh->do("DELETE FROM bucardo.db_connlog");
	$dbh->do("DELETE FROM bucardo.dbgroup");
	$dbh->do("DELETE FROM bucardo.db");
	$dbh->do("DELETE FROM bucardo.q");
	$dbh->commit;

	return;

} ## end of scrub_bucardo_tables



1;
