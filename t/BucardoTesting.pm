#!/usr/bin/env perl
# -*-mode:cperl; indent-tabs-mode: nil-*-

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

my $DEBUG = 1;

use base 'Exporter';
our @EXPORT = qw/%tabletype %sequences %val compare_tables bc_deeply clear_notices wait_for_notice $location/;

my $dbname = 'bucardo_test';


our $location = 'setup';
my $testmsg  = ' ?';
my $testline = '?';
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
     'bucardo_test9' => 'int_unsigned',
     );

our @tables2empty = (qw/droptest bucardo_test_multicol/);

our %sequences =
    (
    'bucardo_test_seq1' => '',
    'bucardo_test_seq2' => '',
    'bucardo_test_seq3' => '',
    );

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

## Location of files
my $initdb = $ENV{PGBINDIR} ? "$ENV{PGBINDIR}/initdb" : 'initdb';
my $pg_ctl = $ENV{PGBINDIR} ? "$ENV{PGBINDIR}/pg_ctl" : 'pg_ctl';

## Get the default initdb location
my $pgversion = qx{$initdb -V};
my ($pg_ver, $pg_major_version, $pg_minor_version, $pg_point_version);
if ($pgversion =~ /initdb \(PostgreSQL\) (\d+\..*)/) {
    $pg_ver = $1;
    ($pg_major_version, $pg_minor_version, $pg_point_version) = split /\./, $pg_ver;
}

## Each database can also have a custom version
## We do this by setting PGBINDIR[A-Z]
## This allows us to test (for example) a 8.1 master and an 8.4 slave
my %pgver;
my %clusterinfo;
my $lport = 58920;
for my $name ('A'..'Z') {
    $lport++;
    $clusterinfo{$name}{port} = $lport;

    my $lbindir = $ENV{PGBINDIR} || '';
    my $linitdb = $initdb;
    my $lpgctl  = $pg_ctl;
    my $localver = $pg_ver;
    my ($lmaj,$lmin,$lrev) = ($pg_major_version, $pg_minor_version, $pg_point_version);
    if (exists $ENV{"PGBINDIR$name"}) {
        $lbindir = $ENV{"PGBINDIR$name"};
        -d $lbindir or die qq{Invalid ENV "PGBINDIR$name"\n};
        $linitdb = "$lbindir/initdb";
        $lpgctl = "$lbindir/pg_ctl";

        $COM = "$linitdb -V";
        my $answer = qx{$COM};
        die "Cannot find version from: $COM" if $answer !~ /initdb \(PostgreSQL\) (\d+\..*)/;
        $localver = $1;
        ($lmaj,$lmin,$lrev) = split /\./, $localver;
    }
    $pgver{$name} = {
        bindir  => $lbindir,
        initdb  => $linitdb,
        pgctl   => $lpgctl,
        version => $localver,
        ver     => "$lmaj.$lmin",
        vmaj    => $lmaj,
        vmin    => $lmin,
        vrev    => $lrev,
        dirname => "bucardo_test_database_${name}_$lmaj.$lmin",
        port    => $lport,
    };
}

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
    $val{int_unsigned}{$_} = 5000 + $_;
}



sub new {

    ## Create a new BucardoTesting object.
    ## Arguments:
    ## 1. Hashref of options (optional)
    ## Returns: reference to a new BucardoTesting object

    my $class = shift;
    my $arg   = shift || {};
    my $self  = {};
    bless $self, $class;

    ## Make a note of which file invoked us for later debugging
    $self->{file} = (caller)[1];

    ## Bail on first error? Default is ENV, then false.
    $self->{bail} = exists $arg->{bail} ? $arg->{bail} : $ENV{BUCARDO_TESTBAIL} || 0;

    ## Name of the test schema
    $self->{schema} = 'bucardo_schema';

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

} ## end of new


sub debug {

    ## Simply internal debugging routine, prints a message if $DEBUG is set
    ## Arguments:
    ## 1. Message to print
    ## 2. Optional level, defaults to 0
    ## Returns: nothing

    $DEBUG or return;

    my $msg = shift || 'No message?!';
    my $level = shift || 0;

    return if $DEBUG < $level;

    chomp $msg;
    warn "DEBUG: $msg\n";

    return;

} ## end of debug


sub empty_cluster {

    ## Empty out a cluster's database
    ## Creates the cluster and 'bucardo_test' database as needed
    ## For existing databases, removes all known schemas
    ## Always recreates the public schema
    ## Arguments:
    ## 1. Name of the cluster
    ## Returns: database handle to the 'bucardo_test' database

    my $self = shift;
    my $clustername = shift or die;

    ## Create the cluster if needed
    $self->create_cluster($clustername);

    ## Start it up if needed
    $self->start_cluster($clustername);

    ## Get a handle to the postgres database
    my $dbh = $self->connect_database($clustername, 'postgres');

    if (database_exists($dbh, $dbname)) {
        $dbh = $self->connect_database($clustername, $dbname);
        ## Remove any of our known schemas
        my @slist;
        for my $sname (qw/ public bucardo freezer /) {
            push @slist => $sname if $self->drop_schema($dbh, $sname);
        }
        debug(qq{Schemas dropped from $dbname on $clustername: } . join ',' => @slist);

        ## Recreate the public schema
        $dbh->do("CREATE SCHEMA public");
        $dbh->commit();
    }
    else {
        local $dbh->{AutoCommit} = 1;
        debug(qq{Creating database $dbname});
        $dbh->do("CREATE DATABASE $dbname");
        $dbh = $self->connect_database($clustername, $dbname);
    }

    return $dbh;

} ## end of empty_cluster


sub create_cluster {

    ## Create a cluster if it does not already exist
    ## Runs initdb, then modifies postgresql.conf
    ## Arguments:
    ## 1. Name of the cluster
    ## Returns: nothing

    my $self = shift;
    my $clustername = shift or die;

    my $info = $pgver{$clustername}
        or die qq{No such cluster as "$clustername"\n};

    my $dirname = $info->{dirname};

    return if -d $dirname;

    my $localinitdb = $info->{initdb};

    debug(qq{Running $localinitdb for cluster "$clustername"});

    qx{$localinitdb -D $dirname 2>&1};

    ## Make some minor adjustments
    my $file = "$dirname/postgresql.conf";
    open my $fh, '>>', $file or die qq{Could not open "$file": $!\n};
    printf {$fh} "

port                       = %d
max_connections            = 20
random_page_cost           = 2.5
log_statement              = 'all'
log_min_duration_statement = 0
client_min_messages        = WARNING
log_line_prefix            = '%s %s[%s] '
listen_addresses           = ''

",
    $info->{port}, '%m', '%d', '%p';

    ## Make some per-version adjustments
    if ($info->{ver} >= 8.3) {
        print {$fh} "logging_collector = off\n";
    }
    else {
        print {$fh} "redirect_stderr   = off\n";
    }
    close $fh or die qq{Could not close "$file": $!\n};

    return;


} ## end of create_cluster


sub start_cluster {

    ## Startup a cluster if not already running
    ## Arguments:
    ## 1. Name of the cluster
    ## Returns: nothing

    my $self = shift;
    my $clustername = shift || 'A';

    ## Create the cluster if needed
    $self->create_cluster($clustername);

    my $info = $pgver{$clustername};

    my $dirname = $info->{dirname};

    ## Check the PID file. If it exists and is active, simply return
    my $pidfile = "$dirname/postmaster.pid";
    if (-e $pidfile) {
        open my $fh, '<', $pidfile or die qq{Could not open "$pidfile": $!\n};
        <$fh> =~ /(\d+)/ or die qq{No PID found in file "$pidfile"\n};
        my $pid = $1;
        close $fh or die qq{Could not close "$pidfile": $!\n};
        ## An active process should respond to a "ping kill"
        $count = kill 0 => $pid;
        #warn "GOT A count of $count for $pid!\n";
        #my $count2 = kill 1 => $pid;
        #warn "GOT A count of $count2 for $pid kill 1!\n";
        return if 1 == $count;
        ## If no response, remove the pidfile ourselves and go on
        debug(qq{Server seems to have died, removing file "$pidfile"});
        unlink $pidfile or die qq{Could not remove file "$pidfile"\n};
    }

    my $port = $info->{port};
    debug(qq{Starting cluster "$clustername" on port $port});

    ## If not Windows, we'll use Unix sockets with a custom socket dir
    my $option = '';
    if ($^O !~ /Win32/) {
        my $sockdir = "$dirname/socket";
        -e $sockdir or mkdir $sockdir;
        $option = q{-o '-k socket'};
        ## Older versions do not assume socket is right off of data dir
        if ($info->{ver} <= 8.0) {
            $option = qq{-o '-k $dirname/socket'};
        }
    }

    ## Attempt to start it up with a pg_ctl call
    my $localpgctl = $info->{pgctl};

    $COM = qq{$localpgctl $option -l $dirname/pg.log -D $dirname start};
    debug(qq{Running: $COM});
    qx{$COM};

    ## Wait for the pidfile to appear
    my $maxwaitseconds = 20;
    my $loops = 0;
    {
        last if -e $pidfile;
        sleep 0.1;
        if ($loops++ > ($maxwaitseconds * 10)) {
            die "Failed to startup cluster $clustername, command was $COM\n";
        }
        redo;
    }

    ## Keep attempting to get a database connection until we get one or timeout
    $maxwaitseconds = 10;

    my $dbhost = getcwd;
    $dbhost .= "/$dirname/socket";

    ## Using the "invalidname" is a nice way to work around locale issues
    my $dsn = "dbi:Pg:dbname=invalidname;port=$port;host=$dbhost";
    my $dbh;

    debug(qq{Connecting as $dsn});

    $loops = 0;
  LOOP: {
        eval {
            $dbh = DBI->connect($dsn, '', '', { AutoCommit=>0, RaiseError=>1, PrintError=>0 });
        };
        last if $@ =~ /"invalidname"/;
        sleep 0.1;
        if ($loops++ > ($maxwaitseconds * 10)) {
            die "Database did not come up: dsn was $dsn\n";
        }
        redo;
    }

    return;

} ## end of start_cluster


sub connect_database {

    ## Return a connection to a database within a cluster
    ## Arguments:
    ## 1. Name of the cluster
    ## 2. Name of the database (optional, defaults to 'bucardo_test')
    ## Returns: database handle

    my $self = shift;
    my $clustername = shift or die;
    my $ldbname = shift || $dbname;

    ## Create and start the cluster as needed
    $self->start_cluster($clustername);

    ## Build the DSN to connect with
    my $info = $pgver{$clustername};
    my $dbport = $info->{port};
    my $dbhost = getcwd . "/$info->{dirname}/socket";
    my $dsn = "dbi:Pg:dbname=$ldbname;port=$dbport;host=$dbhost";

    ## If we already have a cached version and it responds, return it
    if (exists $dbh{$dsn}) {
        my $dbh = $dbh{$dsn};
        $dbh->ping and return $dbh;
        ## No ping? Remove from the cache
        $dbh->disconnect();
        delete $dbh{$dsn};
    }

    my $dbh;
    eval {
        $dbh = DBI->connect($dsn, '', '', { AutoCommit=>0, RaiseError=>1, PrintError=>0 });
    };
    if ($@) {
        if ($ldbname eq 'postgres' and $@ =~ /"postgres"/) {

            ## Probably an older version that uses template1
            (my $localdsn = $dsn) =~ s/dbname=postgres/dbname=template1/;

            ## Give up right away if we are already trying template1
            die $@ if $localdsn eq $dsn;

            debug(qq{Connection failed, trying to connect to template1 to create a postgres database});

            ## Connect as template1 and create a postgres database
            $dbh = DBI->connect($localdsn, '', '', { AutoCommit=>1, RaiseError=>1, PrintError=>0 });
            $dbh->do('CREATE DATABASE postgres');
            $dbh->disconnect();

            ## Reconnect to our new database
            $dbh = DBI->connect($dsn, '', '', { AutoCommit=>0, RaiseError=>1, PrintError=>0 });
        }
        else {
            die "$@\n";
        }
    }

    ## Just in case, set the search path
    $dbh->do('SET search_path = public');
    $dbh->commit();

    return $dbh;

} ## end of connect_database

sub drop_schema {

    ## Drop a schema if it exists
    ## Two arguments:
    ## 1. database handle
    ## 2. name of the schema
    ## Returns 1 if dropped, 0 if not

    my ($self,$dbh,$sname) = @_;

    return 0 if ! schema_exists($dbh, $sname);

    local $dbh->{AutoCommit} = 1;
    local $dbh->{Warn} = 0;
    $dbh->do("DROP SCHEMA $sname CASCADE");

    return 1;

} ## end of drop_schema


sub repopulate_cluster {

    ## Make sure a cluster is empty, then add in the sample data
    ## Arguments:
    ## 1. Name of the cluster
    ## Returns: database handle to the 'bucardo_test' database

    my $self = shift;
    my $clustername = shift or die;

    my $dbh = $self->empty_cluster($clustername);

    $self->add_test_schema($dbh);

    return $dbh;

} ## end of repopulate_cluster


sub add_test_schema {

    ## Add an empty test schema to a database
    ## Arguments:
    ## 1. database handle (usually to 'bucardo_test')
    ## Returns: nothing

    my $self = shift;
    my $dbh = shift or die;

    my ($tcount,$scount,$fcount) = (0,0,0);

    ## Empty out or create the droptest table
    if (table_exists($dbh => 'droptest')) {
        $dbh->do('TRUNCATE TABLE droptest');
    }
    else {
        $tcount++;
        $dbh->do(q{
            CREATE TABLE droptest (
              name TEXT NOT NULL,
              type TEXT NOT NULL
            )
        });
    }

    ## Create the language if needed
    if (!language_exists($dbh => 'plpgsql')) {
        debug(q{Creating language plpgsql'});
        $dbh->do('CREATE LANGUAGE plpgsql');
    }
    $dbh->commit() if ! $dbh->{AutoCommit};

    ## Create supporting functions as needed
    if (!function_exists($dbh => 'trigger_test')) {
        $fcount++;
        $dbh->do(q{
                CREATE FUNCTION trigger_test()
                RETURNS trigger
                LANGUAGE plpgsql
                AS $_$ BEGIN
                INSERT INTO droptest(name,type)
                    VALUES (TG_RELNAME, 'trigger');
                RETURN NULL;
                END;
                $_$
            });
    }
    if (!function_exists($dbh => 'trigger_test_zero')) {
        $fcount++;
        $dbh->do(q{
                CREATE FUNCTION trigger_test_zero()
                RETURNS trigger
                LANGUAGE plpgsql
                AS $_$ BEGIN
                INSERT INTO droptest(name,type)
                    VALUES (TG_RELNAME, 'trigger');
                RETURN NULL;
                END;
                $_$;
            });
    }

    ## Create our helper domain for pseudo-types
    if (domain_exists($dbh => 'int_unsigned')) {
        $dbh->do('DROP DOMAIN int_unsigned CASCADE');
    }
    $dbh->do('CREATE DOMAIN int_unsigned INTEGER CHECK (value >= 0)');

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
        $tcount++;

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
            DO ALSO INSERT INTO droptest(name,type) VALUES ('$table','rule')
            };
        $table =~ /0/ and $SQL =~ s/NEW.inty/0/;
        $dbh->do($SQL);
    }

    if ( !table_exists($dbh => 'bucardo_test_multicol') ) {
        $tcount++;
        $dbh->do(q{CREATE TABLE bucardo_test_multicol (
        id   INTEGER,
        id2  INTEGER,
        id3  INTEGER,
        data TEXT,
        PRIMARY KEY (id, id2, id3))});
    }

    ## Create one sequence for each table type
    for my $seq (sort keys %sequences) {

        local $dbh->{Warn} = 0;

        ## Does the sequence already exist? If so, drop it.
        if (table_exists($dbh => $seq)) {
            $dbh->do("DROP SEQUENCE $seq");
        }

        $SQL = qq{CREATE SEQUENCE $seq};
        $dbh->do($SQL);
        $scount++;
    }

    debug("Test objects created. Tables: $tcount  Sequences: $scount  Functions: $fcount");

    $dbh->commit() if ! $dbh->{AutoCommit};

    return;

} ## end of add_test_schema


sub add_test_databases {

    ## Add one or more databases to the bucardo.db table
    ## Arguments:
    ## 1. White-space separated db names
    ## Returns: nothing

    my $self = shift;
    my $string = shift or die;

    for my $db (split /\s+/ => $string) {
        my $ctlargs = $self->add_db_args($db);
        my $i = $self->ctl("add database bucardo_test $ctlargs");
        die $i if $i =~ /ERROR/;
    }

    return;

} ## end of add_test_databases


sub add_db_args {

    ## Arguments:
    ## 1. Name of a cluster
    ## Returns: DSN-like string to connect to that cluster
    ## May return string or array dependig on how it was called

    my $self = shift;
    my $clustername = shift or die;

    ## Build the DSN to connect with
    my $info = $pgver{$clustername};
    my $dbport = $info->{port};
    my $dbhost = getcwd . "/$info->{dirname}/socket";
    my $dsn = "dbi:Pg:dbname=$dbname;port=$dbport;host=$dbhost";

    my $arg = 

    return wantarray
        ? ($user,$dbport,$dbhost)
        : "name=$dbname user=$user port=$dbport host=$dbhost";

} ## end of add_db_args


sub stop_bucardo {

    ## Stops Bucardo via a bucardo_ctl request
    ## Arguments: none
    ## Returns: 1

    my $self = shift;

    $self->ctl('stop testing');

    sleep 0.2;

    return 1;

} ## end of stop_bucardo


sub ctl {

    ## Run a simple non-forking command against bucardo_ctl
    ## Emulates a command-line invocation
    ## Arguments:
    ## 1. String to pass to bucardo_ctl
    ## Returns: answer as a string

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
    $connopts .= " --dbname=bucardo --debugfile=1";
    $connopts .= " --dbuser=$user";
    ## Just hard-code these, no sense in multiple Bucardo base dbs yet:
    $connopts .= " --dbport=58921";
    my $dbhost = getcwd;
    my $dirname = $pgver{A}{dirname};
    $dbhost .= "/$dirname/socket";
    $connopts .= " --dbhost=$dbhost";

    ## Whitespace cleanup
    $args =~ s/^\s+//s;

    ## Allow the caller to look better
    $args =~ s/^bucardo_ctl//;

    debug("Connection options: $connopts Args: $args", 3);
    eval {
        $info = qx{$ctl $connopts $args 2>&1};
    };
    if ($@) {
        return "Error running bucardo_ctl: $@\n";
    }
    debug("bucardo_ctl said: $info", 3);

    return $info;

} ## end of ctl


sub restart_bucardo {

    ## Start Bucardo, but stop first if it is already running
    ## Arguments:
    ## 1. database handle to the bucardo_control_test db
    ## 2. The notice we wait for, defaults to: bucardo_started
    ## 3. The message to give to the "pass" function, defaults to: Bucardo was started
    ## Returns: nothing

    my ($self,$dbh,$notice,$passmsg) = @_;

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

sub setup_bucardo {

    ## Installs bucardo via "bucardo_ctl install" into a database
    ## The database will be emptied out first if it already exists
    ## If it does not exist, it will be created
    ## If the cluster does not exist, it will be created
    ## Arguments:
    ## 1. Name of the cluster
    ## Returns: database handle to the bucardo database

    my $self = shift;
    my $clustername = shift or die;

    $self->create_cluster($clustername);
    my $dbh = $self->connect_database($clustername, 'postgres');
    if (database_exists($dbh,'bucardo')) {
        ## Kick off all other people
        $SQL = q{SELECT procpid FROM pg_stat_activity WHERE datname = 'bucardo' and procpid <> pg_backend_pid()};
        for my $row (@{$dbh->selectall_arrayref($SQL)}) {
            my $pid = $row->[0];
            $SQL = 'SELECT pg_terminate_backend(?)';
            $sth = $dbh->prepare($SQL);
            $sth->execute($pid);
        }
        $dbh->commit();
        debug(qq{Dropping database bucardo from cluster $clustername});
        local $dbh->{AutoCommit} = 1;
        $dbh->do('DROP DATABASE bucardo');
    }

    ## Make sure we have a postgres role
    if (! user_exists($dbh, 'postgres')) {
        $dbh->do('CREATE USER postgres SUPERUSER');
        $dbh->commit();
    }

    ## Now run the install. Timeout after a few seconds
    debug(qq{Running bucardo_ctl install on cluster $clustername});
    my $info;
    eval {
        local $SIG{ALRM} = sub { die "Alarum!\n"; };
        alarm 5;
        $info = $self->ctl('install --batch');
        alarm 0;
    };
    if ($@ =~ /Alarum/ or $info =~ /Alarum/) {
        warn "bucardo_ctl install never finished!\n";
        exit;
    }
    $@ and die $@;

    if ($info !~ /Installation is now complete/) {
        die "Installation failed: $info\n";
    }

    ## Reconnect to the new database
    $dbh = $self->connect_database($clustername, 'bucardo');

    ## Make some adjustments
    $sth = $dbh->prepare('UPDATE bucardo.bucardo_config SET value = $2 WHERE setting = $1');
    $count = $sth->execute('piddir' => $PIDDIR);
    $count = $sth->execute('reason_file' => "$PIDDIR/reason");
    $count = $sth->execute('audit_pid' => 1);
    $dbh->commit();

    ## Adjust a second way
    $self->ctl('set log_level=debug');

    debug(qq{Install complete});

    return $dbh;

} ## end of setup_bucardo

## Utility functions for object existences:
sub thing_exists {
    my ($dbh,$name,$table,$column) = @_;
    my $SQL = "SELECT 1 FROM $table WHERE $column = ?";
    my $sth = $dbh->prepare($SQL);
    $count = $sth->execute($name);
    $sth->finish();
    $dbh->commit() if ! $dbh->{AutoCommit};
    return $count < 1 ? 0 : $count;
}
sub schema_exists   { return thing_exists(@_, 'pg_namespace', 'nspname'); }
sub language_exists { return thing_exists(@_, 'pg_language',  'lanname'); }
sub database_exists { return thing_exists(@_, 'pg_database',  'datname'); }
sub user_exists     { return thing_exists(@_, 'pg_user',      'usename'); }
sub table_exists    { return thing_exists(@_, 'pg_class',     'relname'); }
sub function_exists { return thing_exists(@_, 'pg_proc',      'proname'); }
sub domain_exists   { return thing_exists(@_, 'pg_type',      'typname'); }

## Hack to override some Test::More methods
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
## end of Test::More hacks


sub wait_for_notice {

    ## Wait until a named NOTIFY is issued
    ## Arguments:
    ## 1. The listen string
    ## 2. Seconds until we give up
    ## 3. Seconds we sleep between checks
    ## 4. Boolean: bail out if not found (defaults to true)

    my $dbh = shift;
    my $text = shift;
    my $timeout = shift || $TIMEOUT_NOTICE;
    my $sleep = shift || $TIMEOUT_SLEEP;
    my $bail = shift;
    $bail = 1 if !defined($bail);
    my $n;
    eval {
        local $SIG{ALRM} = sub { die "Lookout!\n"; };
        alarm $timeout;
      N: {
            while ($n = $dbh->func('pg_notifies')) {
                if ($n->[0] eq $text) {
                    last N;
                }
                else {
                    debug("notice was $n->[0]", 1);
                }
            }
            sleep $sleep;
            redo;
        }
        alarm 0;
    };
    if ($@) {
        if ($@ =~ /Lookout/o) {
            my $line = (caller)[2];
            my $notice = qq{Gave up waiting for notice "$text": timed out at $timeout from line $line};
            if ($bail) {
                Test::More::BAIL_OUT ($notice);
            }
            else {
                die $notice;
            }
            return;
        }
    }
    return;

} ## end of wait_for_notice


## Older methods:

sub fresh_database {

    ## Drop and create the bucardo_test database
    ## First arg is cluster name
    ## Second arg is hashref, can be 'dropdb'

    my $self = shift;
    my $name = shift || 'A';
    my $arg = shift || {};

    my $dirname = $pgver{$name}{dirname};

    ## Just in case
    -d $dirname or $self->create_cluster($name);
    -e "$dirname/postmaster.pid" or $self->start_cluster($name);

    my $dbh = $self->connect_database($name, 'postgres');

    my $brandnew = 0;
    {
        if (database_exists($dbh => $dbname) and $arg->{dropdb}) {
            local $dbh->{AutoCommit} = 1;
            debug("Dropping database $dbname");
            $dbh->do("DROP DATABASE $dbname");
        }
        if (!database_exists($dbh => $dbname)) {
            local $dbh->{AutoCommit} = 1;
            debug("Creating database $dbname");
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



sub create_database {

    ## Create a new database
    ## First argument is the cluster name
    ## Second argument is the name of the database
    ## If the database already exists, nothing will be done
    ## Returns a database handle to the database

    my $self = shift;
    my $clustername = shift or die;
    my $dbname = shift or die;

    my $dirname = $pgver{$clustername}{dirname};

    ## Create the cluster if needed
    -d $dirname or $self->create_cluster($clustername);

    ## Start the cluster up if needed
    -e "$dirname/postmaster.pid" or $self->start_cluster($clustername);

    ## Connect to the database

    my $dbh = $self->connect_database($clustername, 'postgres');

    if (! database_exists($dbh => $dbname)) {
        local $dbh->{AutoCommit} = 1;
        debug("Creating database $dbname");
        $dbh->do("CREATE DATABASE $dbname");
        $dbh->disconnect();
    }

    $dbh = $self->connect_database($clustername, $dbname);

    return $dbh;

} ## end of create_database


sub empty_test_database {

    ## Wipe all data tables from a test database
    ## Takes a database handle as only arg

    my $self = shift;
    my $dbh = shift;

    if ($dbh->{pg_server_version} >= 80300) {
        $dbh->do(q{SET session_replication_role = 'replica'});
    }

    for my $table (sort keys %tabletype) {
        $dbh->do("TRUNCATE TABLE $table");
    }

    for my $table (@tables2empty) {
        $dbh->do("TRUNCATE TABLE $table");
    }

    if ($dbh->{pg_server_version} >= 80300) {
        $dbh->do(q{SET session_replication_role = 'origin'});
    }
    $dbh->commit;

    return;

} ## end of empty_test_database

sub shutdown_cluster {

    ## Shutdown a cluster if running
    ## Takes the cluster name

    my $self = shift;
    my $name = shift;

    my $dirname = $pgver{$name}{dirname};

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
        debug("Removing $pidfile");
        unlink $pidfile;
    }
    $count = kill 15 => $pid;
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

    my $dirname = $pgver{$name}{dirname};

    return if ! -d $dirname;

    ## Just in case
    $self->shutdown_cluster($name);

    system("rm -fr $dirname");

    return;

} ## end of remove_cluster








sub tt {
    ## Simple timing routine. Call twice with the same arg, before and after
    my $name = shift or die qq{Need a name!\n};
    if (exists $timing{$name}) {
        my $newtime = tv_interval($timing{$name});
        debug("Timing for $name: $newtime");
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
    $testmsg .= " [line: $testline]";
    my $time = time;
    $testmsg .= " [time: $time]";

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
        $dbh->do('CREATE schema bucardo');
        $dbh->do('CREATE schema freezer');

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
    $count = $sth->execute('audit_pid' => 1);
    $dbh->commit();

} ## end of add_bucardo_schema_to_database




sub add_test_tables_to_herd {

    ## Add all of the test tables (and sequences) to a herd
    ## Create the herd if it does not exist
    ## First arg is database name, second arg is the herdname

    my $self = shift;
    my $db = shift;
    my $herd = shift;

    my $result = $self->ctl("add herd $herd");
    if ($result !~ /Added herd/) {
        die "Failed to add herd $herd: $result\n";
    }

    my $addstring = join ' ' => sort keys %tabletype;
    $addstring .= ' bucardo_test_multicol';
    my $com = "add table $addstring db=$db herd=$herd";
    $result = $self->ctl($com);
    if ($result !~ /Added table/) {
        die "Failed to add tables: $result (command was: $com)\n";
    }

    $addstring = join ' ' => sort keys %sequences;
    $com = "add sequence $addstring db=$db herd=$herd";
    $result = $self->ctl($com);
    if ($result !~ /Added sequence/) {
        die "Failed to add sequences: $result (command was: $com)\n";
    }

    return;

} ## end of add_test_tables_to_herd







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

sub clear_notices {
    my $dbh = shift;
    my $timeout = shift || $TIMEOUT_NOTICE;
    sleep $timeout;
    0 while (my $n = $dbh->func('pg_notifies'));
}


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




sub setup_monkey_data {

    my ($self,$dbh) = @_;

    ## Make sure the database is setup with the data for the "monkey" tests

    ## If already there, reset everything

    debug('Inside setup_monkey_data');

    ## Database A contains bucardo itself and is the first "master"
    $self->start_cluster('A');

    my $dbhA = $self->connect_database('A', 'postgres');

    $dbhA->{AutoCommit} = 1;

    if ( !database_exists($dbh => 'bucardo')) {
        
    }




    ## First, we need the clusters to be there.
    $self->start_cluster('B');
    $self->start_cluster('C');




    ## Next, we install Bucardo into database A
    if (schema_exists($dbhA => 'bucardo')) {
        $dbhA->do('DROP SCHEMA bucardo CASCADE');
        $dbhA->commit();
    }
    eval {
        $dbhA->do('CREATE USER postgres SUPERUSER');
    };
    $dbhA->commit();

    my $info;
    eval {
        local $SIG{ALRM} = sub { die "Alarum!\n"; };
        alarm 3;
        $info = $self->ctl('install --batch');
        alarm 0;
    };
    if ($@ and $@ =~ /Alarum/ or $info =~ /Alarum/) {
        warn "bucardo_ctl install never finished!\n";
        exit;
    }
    $@ and die $@;

    if ($info !~ /Installation is now complete/) {
        die "Installation failed\n";
    }
die Dumper $info;
exit;
    debug('Bucardo has been installed on database A');

    my $dbname = 'monkey';

    ## If needed, create the monkey database
    if (! database_exists($dbhA => $dbname)) {
        debug("Creating database $dbname");
        $dbhA->do("CREATE DATABASE $dbname");
    }

    ## Drop the public schema and recreate with all tables
    if (schema_exists($dbhA => 'public')) {
        debug('Dropping the public schema');
        $dbhA->do('DROP SCHEMA public CASCADE');
    }
    $dbhA->do('CREATE SCHEMA public');

    $self->add_test_schema($dbhA);

    if (! database_exists($dbhA => "${dbname}_template")) {
        $dbhA->do("CREATE DATABASE ${dbname}_template");
    }



    $dbhA->disconnect();

    ## XXX: Make a template db for all of this

    return;

} ## End of setup_monkey_data


1;
