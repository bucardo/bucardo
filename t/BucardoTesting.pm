#!/usr/bin/env perl
# -*-mode:cperl; indent-tabs-mode: nil-*-

package BucardoTesting;

## Helper module for the Bucardo tests
## Contains shared code for setup and breakdown

use strict;
use warnings;
use utf8;

use Encode qw/ decode /;
use Encode::Locale;
use DBI;
use DBD::Pg;
use Time::HiRes qw/sleep gettimeofday tv_interval/;
use Cwd;
use Data::Dumper;
use Symbol;
require Test::More;

use vars qw/$SQL $sth $count $COM %dbh/;

my $DEBUG = $ENV{BUCARDO_DEBUG} || 0;

$ENV{BUCARDO_CONFIRM} = 0 if exists $ENV{BUCARDO_CONFIRM};

use base 'Exporter';
our @EXPORT = qw/%tabletype %tabletypemysql %tabletypemariadb %tabletypeoracle %tabletypesqlite %tabletypefirebird
                 %sequences %val
                 compare_tables bc_deeply clear_notices wait_for_notice
                 $location $oldherd_msg $newherd_msg $addtable_msg $deltable_msg $nomatch_msg/;

## Special global vars for munging the data
my (%gsth, %gdbh);

my $dbname = 'bucardo_test';

## We need to use the local Bucardo.pm, not a system installed one!
$ENV{PERL5LIB} = '.';

## Shortcuts for ease of changes and smaller text:
our $addtable_msg = 'Added the following tables or sequences';
our $deltable_msg = 'Removed the following tables';
our $nomatch_msg = 'Did not find matches for the following terms';
our $oldherd_msg = 'The following tables or sequences are now part of the relgroup';
our $newherd_msg = 'The following tables or sequences are now part of the relgroup';

our $location = 'setup';
my $testmsg  = ' ?';
my $testline = '?';
## Sometimes, we want to stop as soon as we see an error
my $bail_on_error = $ENV{BUCARDO_TESTBAIL} || 0;
my $total_errors = 0;
## Used by the tt sub
my %timing;

## If true, turns off the epoch "time" output at the end of each testing output line
my $notime = 1;

my $user = qx{whoami};
chomp $user;

my $FRESHLOG = 1;
if ($FRESHLOG) {
    unlink 'tmp/bucardo.log';
}

my $piddir = 'pid';
if (! -e $piddir) {
    mkdir $piddir;
}

if ($ENV{BUCARDO_LOG_ERROR_CONTEXT}) {
    no strict 'refs';
    no warnings qw/prototype redefine/;
    my ($package) = caller();

    # wrap these routines
    for my $subname ( qw(ok is like) ) {

        my $glob = qualify_to_ref($subname,$package);

        if (my $sub = *$glob{CODE}) {
            *$glob = sub {
                # get result; this is not a general wrapper, since most of
                # the testing ignores return values here, we aren't worried
                # about wantarray, etc; we need the return value to decide
                # if we're going to output a bunch of additional debugging
                # information.
                my $res = $sub->( @_ );
                if (!$res) {
                    _log_context("@_");
                }
                $res;
            }
        }
    }
}

## Test databases are labelled as A, B, C, etc.
my @dbs = qw/A B C D E/;

### TODO: Add point type (which has no natural ordering operator!)

our %tabletype =
    (
     'bucardo_test1'  => 'SMALLINT',
     'bucardo_test2'  => 'INT',
     'Bucardo_test3'  => 'BIGINT',
     'bucardo_test4'  => 'TEXT',
     'bucardo_test5'  => 'DATE',
     'bucardo_test6'  => 'TIMESTAMP',
     'bucardo_test7'  => 'NUMERIC',
     'bucardo_test8'  => 'BYTEA',
     'bucardo_test9'  => 'int_unsigned',
     'bucardo_test10' => 'TIMESTAMPTZ',
     'bucardo space test' => 'INT',
     );

our %tabletypemysql =
    (
     'bucardo_test1'  => 'SMALLINT',
     'bucardo_test2'  => 'INT',
     'Bucardo_test3'  => 'BIGINT',
     'bucardo_test4'  => 'VARCHAR(700)',
     'bucardo_test5'  => 'DATE',
     'bucardo_test6'  => 'DATETIME',
     'bucardo_test7'  => 'NUMERIC(5,1)',
     'bucardo_test8'  => 'VARBINARY(1000)',
     'bucardo_test9'  => 'INTEGER UNSIGNED',
     'bucardo_test10' => 'DATETIME',
     'bucardo space test' => 'INT',
     );

our %tabletypemariadb =
    (
     'bucardo_test1'  => 'SMALLINT',
     'bucardo_test2'  => 'INT',
     'Bucardo_test3'  => 'BIGINT',
     'bucardo_test4'  => 'VARCHAR(700)',
     'bucardo_test5'  => 'DATE',
     'bucardo_test6'  => 'DATETIME',
     'bucardo_test7'  => 'NUMERIC(5,1)',
     'bucardo_test8'  => 'VARBINARY(1000)',
     'bucardo_test9'  => 'INTEGER UNSIGNED',
     'bucardo_test10' => 'DATETIME',
     'bucardo space test' => 'INT',
     );

our %tabletypefirebird =
    (
     'bucardo_test1'  => 'SMALLINT',
     'bucardo_test2'  => 'INT',
     'Bucardo_test3'  => 'BIGINT',
     'bucardo_test4'  => 'VARCHAR(700)',
     'bucardo_test5'  => 'DATE',
     'bucardo_test6'  => 'DATETIME',
     'bucardo_test7'  => 'NUMERIC(5,1)',
     'bucardo_test8'  => 'VARBINARY(1000)',
     'bucardo_test9'  => 'INTEGER UNSIGNED',
     'bucardo_test10' => 'TIMESTAMP',
     'bucardo space test' => 'INT',
     );

our %tabletypeoracle =
    (
     'bucardo_test1'  => 'SMALLINT',
     'bucardo_test2'  => 'INT',
     'Bucardo_test3'  => 'BIGINT',
     'bucardo_test4'  => 'NVARCHAR2(1000)',
     'bucardo_test5'  => 'DATE',
     'bucardo_test6'  => 'TIMESTAMP',
     'bucardo_test7'  => 'NUMERIC(5,1)',
     'bucardo_test8'  => 'BLOB',
     'bucardo_test9'  => 'INTEGER',
     'bucardo_test10' => 'TIMESTAMP WITH TIME ZONE',
     'bucardo space test' => 'INT',
     );

our %tabletypesqlite =
    (
     'bucardo_test1'  => 'SMALLINT',
     'bucardo_test2'  => 'INT',
     'Bucardo_test3'  => 'BIGINT',
     'bucardo_test4'  => 'VARCHAR(1000)',
     'bucardo_test5'  => 'DATE',
     'bucardo_test6'  => 'DATETIME',
     'bucardo_test7'  => 'NUMERIC(5,1)',
     'bucardo_test8'  => 'VARBINARY(1000)',
     'bucardo_test9'  => 'INTEGER UNSIGNED',
     'bucardo_test10' => 'DATETIME',
     'bucardo space test' => 'INT',
     );


our @tables2empty = (qw/droptest_bucardo/);

our %sequences =
    (
    'bucardo_test_seq1' => '',
    'bucardo_test_seq2' => '',
    'Bucardo_test_seq3' => '',
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

## Let pg_config guide us to a likely initdb/pg_ctl location
my $output = qx{pg_config --bindir};
chomp $output;
my $bindir = $output =~ m{^/} ? $1 : '';

## Location of files
my $initdb = $ENV{PGBINDIR} ? "$ENV{PGBINDIR}/initdb" : $bindir ? "$bindir/initdb" : 'initdb';
my $pg_ctl = $ENV{PGBINDIR} ? "$ENV{PGBINDIR}/pg_ctl" : $bindir ? "$bindir/pg_ctl" : 'pg_ctl';

## Get the default initdb location
my $pgversion = qx{$initdb -V};
my ($pg_ver, $pg_major_version, $pg_minor_version, $pg_point_version);
if (defined $pgversion and $pgversion =~ /initdb \(PostgreSQL\) (\d+\..*)/) {
    $pg_ver = $1;
    ($pg_major_version, $pg_minor_version, $pg_point_version) = split /\./, $pg_ver;
    $pg_minor_version =~ s/(\d+).+/$1/;
}
else {
    die qq{Could not determine initdb version information from running "$initdb -V"\n};
}

sub pg_major_version { join '.', $pg_major_version, $pg_minor_version }

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
        $lmin =~ s/(\d+).+/$1/;
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

## Maximum time to wait for bucardo to return
my $ALARM_BUCARDO = 25;
## Maximum time to wait for a kid to appear via pg_listener
my $ALARM_WAIT4KID = 3;
## How long to wait for most syncs to take effect?
my $TIMEOUT_SYNCWAIT = 3;
## How long to sleep between checks for sync being done?
my $TIMEOUT_SLEEP = 0.1;
## How long to wait for a notice to be issued?
my $TIMEOUT_NOTICE = 4;

## Bail if the bucardo file does not exist / does not compile
for my $file (qw/bucardo Bucardo.pm/) {
    if (! -e $file) {
        die "Cannot run without file $file\n";
    }
    eval {
        $ENV{BUCARDO_TEST} = 1;
        require $file;
        $ENV{BUCARDO_TEST} = 0;
    };
    if ($@) {
        die "Cannot run unless $file compiles cleanly: $@\n";
    }
}

## Prepare some test values for easy use
## The secondary names are for other databases, e.g. MySQL
our %val;
my $xvalmax = 30;
for (1..$xvalmax) {
    $val{SMALLINT}{$_} = $_;
    $val{INT}{$_} = 1234567+$_;
    $val{BIGINT}{$_} = 7777777777 + $_;
    $val{TEXT}{$_} = $val{'VARCHAR(1000)'}{$_} = $val{'VARCHAR(700)'}{$_} = "\\Pbc'$_";
    $val{DATE}{$_} = sprintf '2001-10-%02d', $_;
    $val{TIMESTAMP}{$_} = $val{DATE}{$_} . ' 12:34:56';
    $val{NUMERIC}{$_} = $val{'NUMERIC(5,1)'}{$_} = 0.7 + $_;
    $val{BYTEA}{$_} = "$_\0Z";
    $val{int_unsigned}{$_} = $val{'INTEGER UNSIGNED'}{$_} = 5000 + $_;
    $val{TIMESTAMPTZ}{$_} = $val{DATETIME}{$_} = $val{DATE}{$_} . ' 11:22:33+00';
    $val{DATETIME}{$_} =~ s/\+00//;
    $val{TIMESTAMPTZNOZERO} = $val{DATE}{$_} . ' 11:22:33';
}


sub diag {
    Test::More::diag(@_);
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

    if ($arg->{notime}) {
        $notime = 1;
    }

    ## Make a note of which file invoked us for later debugging
    $self->{file} = (caller)[1];

    ## Bail on first error? Default is ENV, then false.
    $bail_on_error = exists $arg->{bail} ? $arg->{bail} : $ENV{BUCARDO_TESTBAIL} || 0;

    ## Name of the test schema
    $self->{schema} = 'bucardo_schema';

    ## Let's find out where bucardo is. Prefer the blib ones, which are shebang adjusted
    if (-e 'blib/script/bucardo') {
        $self->{bucardo} = 'blib/script/bucardo';
    }
    elsif (-e '../blib/script/bucardo') {
        $self->{bucardo} = '../blib/script/bucardo';
    }
    elsif (-e './bucardo') {
        $self->{bucardo} = './bucardo';
    }
    elsif (-e '../bucardo') {
        $self->{bucardo} = '../bucardo';
    }
    else {
        die qq{Could not find bucardo\n};
    }

    ## Handle both old and new way of setting location
    if ($location eq 'setup' and $arg->{location}) {
        $location = $self->{location} = $arg->{location};
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

    ## Empty out a cluster's databases
    ## Creates the cluster and 'bucardo_test' database as needed
    ## For existing databases, removes all known schemas
    ## Always recreates the public schema
    ## Arguments: one
    ## 1. Name of the cluster
    ## Returns: arrayref of database handles to the 'bucardo_test*' databases

    my $self = shift;
    my $clustername = shift or die;

    ## Create the cluster if needed
    $self->create_cluster($clustername);

    ## Start it up if needed
    $self->start_cluster($clustername);

    my $alldbh;

    ## Get a handle to the postgres database
    my $masterdbh = $self->connect_database($clustername, 'postgres');

    my $dbh;
    if (database_exists($masterdbh, $dbname)) {
        $dbh = $self->connect_database($clustername, $dbname);
        ## Remove any of our known schemas
        my @slist;
        for my $sname (qw/ public bucardo freezer tschema /) {
            push @slist => $sname if $self->drop_schema($dbh, $sname);
        }
        debug(qq{Schemas dropped from $dbname on $clustername: } . join ',' => @slist);

        ## Recreate the public schema
        $dbh->do("CREATE SCHEMA public");
        $dbh->commit();
    }
    else {
        local $masterdbh->{AutoCommit} = 1;
        debug(qq{Creating database $dbname});
        $masterdbh->do("CREATE DATABASE $dbname");
        $dbh = $self->connect_database($clustername, $dbname);
    }

    $masterdbh->disconnect();

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

    my $line = (caller)[2];
    my $info = $pgver{$clustername}
        or die qq{No such cluster as "$clustername" (called from line $line)\n};

    my $dirname = $info->{dirname};

    if (-d $dirname) {
        ## Sometimes these test clusters get left in a broken state.
        my $file = "$dirname/postgresql.conf";
        if (! -e $file) {
            ## Just move it out of the way, rather than deleting it
            rename $dirname, "$dirname.old";
        }
        return;
    }

    my $localinitdb = $info->{initdb};

    debug(qq{Running $localinitdb for cluster "$clustername"});
    my $com = qq{$localinitdb -D $dirname 2>&1};
    debug($com);
    my $res = qx{$com};
    die $res if $? != 0;
    if ($DEBUG) {
        warn Dumper $res;
    }

    ## Make some minor adjustments
    my $connections = $clustername eq 'A' ? 150 : 75;
    my $file = "$dirname/postgresql.conf";
    open my $fh, '>>', $file or die qq{Could not open "$file": $!\n};
    printf {$fh} "

port                       = %d
max_connections            = $connections
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
            Test::More::BAIL_OUT ( 'Failed to connect to database' );
            die "Failed to startup cluster $clustername, command was $COM\n";
        }
        redo;
    }

    ## Keep attempting to get a database connection until we get one or timeout
    $maxwaitseconds = 20;

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

    ## This may be one of the "extra" databases. In which case the true cluster must be revealed:
    $clustername =~ s/\d+$//;

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

    $dbh->do(q{SET TIME ZONE 'UTC'});

    if ($DEBUG) {
        my $file = 'bucardo.debug.dsns.txt';
        if (open my $fh, '>>', $file) {
            print {$fh} "\n$dsn\n";
            my ($host,$port,$db);
            $dsn =~ /port=(\d+)/ and $port=$1;
            $dsn =~ /dbname=(.+?);/ and $db=$1;
            $dsn =~ /host=(.+)/ and $host=$1;
            printf {$fh} "psql%s%s%s\n", " -h $host", " -p $port", " $db";
            close $fh or die qq{Could not close file "$file": $!\n};
        }
    }

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
    ## Arguments: two
    ## 1. Name of the cluster
    ## 2. Optional - number of additional databases to create
    ## Returns: database handle to the 'bucardo_test' database

    my $self = shift;
    my $clustername = shift or die;
    my $extradbs = shift || 0;

    Test::More::note("Recreating cluster $clustername");

    my $dbh = $self->empty_cluster($clustername);
    $self->add_test_schema($dbh, $clustername);

    ## Now recreate all the extra databases via templating
    for my $number (1..$extradbs) {
        my $dbname2 = "$dbname$number";
        local $dbh->{AutoCommit} = 1;
        if (database_exists($dbh, $dbname2)) {
            ## First, kill other sessions!
            my $odbh = $self->connect_database($clustername, $dbname2);
            eval {
                $SQL = 'SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = ? AND pid <> pg_backend_pid()';
                $sth = $odbh->prepare($SQL);
                $odbh->execute($dbname2);
                $odbh->commit();
            };
            $odbh->disconnect();
            $dbh->do("DROP DATABASE $dbname2");
        }
        $dbh->do("CREATE DATABASE $dbname2 TEMPLATE $dbname");
    }

    ## Store our names away
    $gdbh{$clustername} = $dbh;

    return $dbh;

} ## end of repopulate_cluster


sub add_test_schema {

    ## Add an empty test schema to a database
    ## Arguments: two
    ## 1. database handle (usually to 'bucardo_test')
    ## 2. Cluster name
    ## Returns: nothing

    my $self = shift;
    my $dbh = shift or die;
    my $clustername = shift or die;

    my ($tcount,$scount,$fcount) = (0,0,0);

    ## Empty out or create the droptest table
    if (table_exists($dbh => 'droptest_bucardo')) {
        $dbh->do('TRUNCATE TABLE droptest_bucardo');
    }
    else {
        $tcount++;
        $dbh->do(q{
            CREATE TABLE droptest_bucardo (
              name TEXT NOT NULL,
              type TEXT NOT NULL
            )
        });
    }

    ## Create the language if needed
    if (!language_exists($dbh => 'plpgsql')) {
        debug(q{Creating language plpgsql});
        $dbh->do('CREATE LANGUAGE plpgsql');
    }

    ## Create supporting functions as needed
    if (!function_exists($dbh => 'trigger_test')) {
        $fcount++;
        $dbh->do(q{
                CREATE FUNCTION trigger_test()
                RETURNS trigger
                LANGUAGE plpgsql
                AS $_$ BEGIN
                INSERT INTO droptest_bucardo(name,type)
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
                INSERT INTO droptest_bucardo(name,type)
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
            $dbh->do(qq{DROP TABLE "$table"});
        }

        my $pkeyname = $table =~ /test5/ ? q{"id space"} : 'id';
        my $pkindex = $table =~ /test2/ ? '' : 'PRIMARY KEY';
        $SQL = qq{
            CREATE TABLE "$table" (
                $pkeyname    $tabletype{$table} NOT NULL $pkindex};
        $SQL .= $table =~ /X/ ? "\n)" : qq{,
                data1 TEXT                   NULL,
                inty  SMALLINT               NULL,
                booly BOOLEAN                NULL,
                bite1 BYTEA                  NULL,
                bite2 BYTEA                  NULL,
                email TEXT                   NULL UNIQUE
            )
            };

        $dbh->do($SQL);
        $tcount++;

        if ($table =~ /test2/) {
            $dbh->do(qq{ALTER TABLE "$table" ADD CONSTRAINT multipk PRIMARY KEY ($pkeyname,data1)});
        }

        ## Create a trigger to test trigger supression during syncs
        $SQL = qq{
            CREATE TRIGGER "bctrig_$table"
            AFTER INSERT OR UPDATE ON "$table"
            FOR EACH ROW EXECUTE PROCEDURE trigger_test()
            };
        $table =~ /0/ and ($SQL =~ s/trigger_test/trigger_test_zero/);
        $dbh->do($SQL);

        ## Create a rule to test rule supression during syncs
        $SQL = qq{
            CREATE OR REPLACE RULE "bcrule_$table"
            AS ON INSERT TO "$table"
            DO ALSO INSERT INTO droptest_bucardo(name,type) VALUES ('$table','rule')
            };
        $table =~ /0/ and $SQL =~ s/NEW.inty/0/;
        $dbh->do($SQL);
    }

    ## Create the foreign key tables
    #$dbh->do('CREATE TABLE bucardo_fkey1 (fkid INTEGER NOT NULL PRIMARY KEY, data2 TEXT)');
    $SQL = q{
ALTER TABLE bucardo_fkey1
  ADD CONSTRAINT "bucardo_fkey1"
  FOREIGN KEY (fkid)
  REFERENCES bucardo_test1 (id)
  ON DELETE CASCADE ON UPDATE CASCADE
};
    #$dbh->do($SQL);

    ## Create one sequence for each table type
    for my $seq (sort keys %sequences) {

        local $dbh->{Warn} = 0;

        ## Does the sequence already exist? If so, drop it.
        if (table_exists($dbh => $seq)) {
            $dbh->do(qq{DROP SEQUENCE "$seq"});
        }

        $SQL = qq{CREATE SEQUENCE "$seq"};
        $dbh->do($SQL);
        $scount++;
    }

    debug("Test objects created for $clustername. Tables: $tcount  Sequences: $scount  Functions: $fcount");
#    diag("Test objects created for $clustername. Tables: $tcount  Sequences: $scount  Functions: $fcount");

    $dbh->commit() if ! $dbh->{AutoCommit};

    return;

} ## end of add_test_schema

sub mock_serialization_failure {
    my ($self, $dbh, $table) = @_;
    return if $dbh->{pg_server_version} < 80401;
    $table ||= 'bucardo_test1';

    # Mock a serialization failure on every other INSERT. Runs only when
    # `session_replica_role` is "replica", which it true for Bucardo targets.
    $dbh->do(qq{
        DROP SEQUENCE IF EXISTS serial_seq;
        CREATE SEQUENCE serial_seq;

        CREATE OR REPLACE FUNCTION mock_serial_fail(
        ) RETURNS trigger LANGUAGE plpgsql AS \$_\$
        BEGIN
            IF nextval('serial_seq') % 2 = 0 THEN RETURN NEW; END IF;
            RAISE EXCEPTION 'Serialization error'
                  USING ERRCODE = 'serialization_failure';
        END;
        \$_\$;

        CREATE TRIGGER mock_serial_fail AFTER INSERT ON "$table"
            FOR EACH ROW EXECUTE PROCEDURE mock_serial_fail();
        ALTER TABLE "$table" ENABLE REPLICA TRIGGER mock_serial_fail;
    });
    $dbh->commit;

    return 1;
} ## end of mock_serialization_failure

sub unmock_serialization_failure {
    my ($self, $dbh, $table) = @_;
    return if $dbh->{pg_server_version} < 80401;
    $table ||= 'bucardo_test1';

    $dbh->do(qq{
        DROP TRIGGER IF EXISTS mock_serial_fail ON "$table";
        DROP FUNCTION IF EXISTS mock_serial_fail();
        DROP SEQUENCE IF EXISTS serial_seq;
    });

    return 1;
} ## end of unmock_serialization_failure

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
    ## Allows for "same" databases o the form X# e.g. A1, B1
    ## May return string or array depending on how it was called

    my $self = shift;
    my $clustername = shift or die;

    $clustername =~ s/\d+$//;

    ## Build the DSN to connect with
    my $info = $pgver{$clustername};
    my $dbport = $info->{port};
    my $dbhost = getcwd . "/$info->{dirname}/socket";
    my $dsn = "dbi:Pg:dbname=$dbname;port=$dbport;host=$dbhost";

    return wantarray
        ? ($user,$dbport,$dbhost)
        : "name=$dbname user=$user port=$dbport host=$dbhost";

} ## end of add_db_args


sub stop_bucardo {

    ## Stops Bucardo via a bucardo request
    ## Arguments: none
    ## Returns: 1

    my $self = shift;

    $self->ctl('stop testing');

    sleep 0.2;

    return 1;

} ## end of stop_bucardo


sub ctl {

    ## Run a simple non-forking command against bucardo
    ## Emulates a command-line invocation
    ## Arguments:
    ## 1. String to pass to bucardo
    ## 2. Database name to connect to. Used only when we're not confident the bucardo database exists already.
    ## Returns: answer as a string

    my ($self,$args, $db) = @_;
    $db ||= 'bucardo';

    my $info;
    my $ctl = $self->{bucardo};

    ## Build the connection options
    my $bc = $self->{bcinfo};
    my $connopts = '';
    for my $arg (qw/host port pass/) {
        my $val = 'DB' . (uc $arg) . '_bucardo';
        next unless exists $bc->{$val} and length $bc->{$val};
        $connopts .= " --db$arg=$bc->{$val}";
    }
    $connopts .= " --dbname=$db --log-dest .";
    $connopts .= " --dbuser=$user";
    ## Just hard-code these, no sense in multiple Bucardo base dbs yet:
    $connopts .= " --dbport=58921";
    my $dbhost = getcwd;
    my $dirname = $pgver{A}{dirname};
    $dbhost .= "/$dirname/socket";
    $connopts .= " --dbhost=$dbhost";
    $connopts .= " --no-bucardorc";

    ## Whitespace cleanup
    $args =~ s/^\s+//s;

    ## Allow the caller to look better
    $args =~ s/^bucardo\s+//;

    ## Set a timeout
    alarm 0;
    eval {
        local $SIG{ALRM} = sub { die "Alarum!\n"; };
        alarm $ALARM_BUCARDO;
        debug("Script: $ctl Connection options: $connopts Args: $args", 3);
        $info = decode( locale => qx{$ctl $connopts $args 2>&1} );
        debug("Exit value: $?", 3);
        die $info if $? != 0;
        alarm 0;
    };

    if ($@ =~ /Alarum/ or $info =~ /Alarum/) {
        return __PACKAGE__ . ' timeout hit, giving up';
    }
    if ($@) {
        return "Error running bucardo: " . decode( locale => $@ ) . "\n";
    }

    debug("bucardo said: $info", 3);

    return $info;

} ## end of ctl


sub restart_bucardo {

    ## Start Bucardo, but stop first if it is already running
    ## Arguments: one, two, or three
    ## 1. database handle to the bucardo_control_test db
    ## 2. The notice we wait for, defaults to: bucardo_started
    ## 3. The message to give to the "pass" function, defaults to: Bucardo was started
    ## Returns: nothing

    my ($self,$dbh,$notice,$passmsg) = @_;

    my $line = (caller)[2];

    $notice ||= 'bucardo_started';
    $passmsg ||= "Bucardo was started (caller line $line)";

    $self->stop_bucardo();

    ## Because the stop signal arrives before the PID is removed, sleep a bit
    sleep 2;

    pass("Starting up Bucardo (caller line $line)");
    $dbh->do('LISTEN bucardo');
    $dbh->do('LISTEN bucardo_boot');
    $dbh->do("LISTEN $notice");
    $dbh->do('LISTEN bucardo_nosyncs');
    $dbh->commit();

    my $output = $self->ctl('start --exit-on-nosync --quickstart testing');

    my $bail = 50;
    my $n;
  WAITFORIT: {
        if ($bail--<0) {
            $output =~ s/^/#     /gmx;
            my $time = localtime;
            die "Bucardo did not start, but we waited!\nTime: $time\nStart output:\n\n$output\n";
        }
        while ($n = $dbh->func('pg_notifies')) {
            my ($name, $pid, $payload) = @$n;
            if ($dbh->{pg_server_version} >= 9999990000) {
                next if $name ne 'bucardo';
                $name = $payload;
            }
            last WAITFORIT if $name eq $notice;
        }
        $dbh->commit();
        sleep 0.2;
        redo;
    }
    pass($passmsg);

    ## There is a race condition here for testing
    ## Bucardo starts up, and gives the notice above.
    ## However, after it does so, CTLs and KIDs start up and look for new rows
    ## If the caller of this function makes changes right away and then kicks,
    ## Bucardo may see them on the "startup kick" and thus the caller will
    ## get a "syncdone" message that was not initiated by *their* kick.
    ## One way around this is to make sure your caller immediately does a 
    ## kick 0, which will flush out the startup kick. If it arrives after the 
    ## startup kick, then it simply returns as a sync with no activity

    return 1;

} ## end of restart_bucardo

sub setup_bucardo {

    ## Installs bucardo via "bucardo install" into a database
    ## The database will be emptied out first if it already exists
    ## If it does not exist, it will be created
    ## If the cluster does not exist, it will be created
    ## Arguments:
    ## 1. Name of the cluster
    ## Returns: database handle to the bucardo database

    my $self = shift;
    my $clustername = shift or die;

    Test::More::note('Installing Bucardo');

    $self->create_cluster($clustername);
    my $dbh = $self->connect_database($clustername, 'postgres');
    if (database_exists($dbh,'bucardo')) {
        my $retries = 5;
        my $pidcol = $dbh->{pg_server_version} >= 90200 ? 'pid' : 'procpid';
        do {
            ## Kick off all other people
            $SQL = qq{SELECT $pidcol FROM pg_stat_activity WHERE datname = 'bucardo' and $pidcol <> pg_backend_pid()};
            for my $row (@{$dbh->selectall_arrayref($SQL)}) {
                my $pid = $row->[0];
                $SQL = 'SELECT pg_terminate_backend(?)';
                $sth = $dbh->prepare($SQL);
                $sth->execute($pid);
            }
            $dbh->commit();
        } while ($dbh->selectrow_array(qq{SELECT count(*) FROM pg_stat_activity WHERE datname = 'bucardo' and $pidcol <> pg_backend_pid()}))[0] && $retries--;
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
    debug(qq{Running bucardo install on cluster $clustername});
    my $info = $self->ctl('install --batch', 'postgres');

    if ($info !~ /Installation is now complete/) {
        die "Installation failed: $info\n";
    }

    ## Reconnect to the new database
    $dbh = $self->connect_database($clustername, 'bucardo');

    ## Make some adjustments
    $sth = $dbh->prepare('UPDATE bucardo.bucardo_config SET setting = $2 WHERE name = $1');
    $count = $sth->execute('piddir' => $PIDDIR);
    $count = $sth->execute('reason_file' => "$PIDDIR/reason");
    $count = $sth->execute('sendmail_file' => 'debug.sendmail.txt');
    $count = $sth->execute('audit_pid' => 1);
    $dbh->commit();

    ## Adjust a second way
    $self->ctl('set log_level=debug log_microsecond=1 log_showline=1');

    debug(qq{Install complete});

    return $dbh;

} ## end of setup_bucardo

# utility sub called on test error to output pg and bucardo logs to a single
# output file with context; mainly useful for CI debugging/output
sub _log_context {
    return unless $ENV{BUCARDO_LOG_ERROR_CONTEXT};

    warn "Logging context for @_; dir=$ENV{PWD}\n";
    system("echo '====================' >> log.context");
    system("date >> log.context");
    system(sprintf "echo '%s' >> log.context", quotemeta($_[0])) if $_[0];
    system("tail -n 100 log.bucardo bucardo_test_database_*/pg.log 2>/dev/null >> log.context");
}

## Utility functions for object existences:
sub thing_exists {
    my ($dbh,$name,$table,$column) = @_;
    my $SQL = "SELECT 1 FROM $table WHERE $column = ?";
    ## Only want tables from the public schema for now
    if ($table eq 'pg_class') {
        $SQL .= qq{ AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')};
    }
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


sub wait_for_notice {

    ## Wait until a named NOTIFY is issued
    ## Arguments:
    ## 1. The listen string or array of strings
    ## 2. Seconds until we give up
    ## 3. Seconds we sleep between checks
    ## 4. Boolean: bail out if not found (defaults to true)
    ## Returns true if the NOTIFY was recieved.

    my $self = shift;
    my $dbh = shift;
    my $text = shift;
    my $timeout = shift || $TIMEOUT_NOTICE;
    my $sleep = shift || $TIMEOUT_SLEEP;
    my $bail = shift;
    $bail = 0 if !defined($bail);
    my $n;
    my %wait_for;
    for my $str (ref $text ? @{ $text } : $text) {
        $wait_for{$str}++;
    }

    eval {
        local $SIG{ALRM} = sub { die "Lookout!\n"; };
        alarm $timeout;
      N: {
            while ($n = $dbh->func('pg_notifies')) {
                my ($name, $pid, $payload) = @$n;
                $name = $payload if length $payload;
                if (exists $wait_for{$name}) {
                    if (--$wait_for{$name} == 0) {
                        delete $wait_for{$name};
                        last N unless %wait_for;
                    }
                }
                else {
                    debug("notice was $name", 1);
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
            my $now = scalar localtime;
            my $texts = join '", "', keys %wait_for;
            my $pl = keys %wait_for > 1 ? 's' : '';
            my $notice = qq{Gave up waiting for notice$pl "$texts": timed out at $timeout from line $line. Time=$now};
            if ($bail) {
                Test::More::BAIL_OUT ($notice);
            }
            else {
                die $notice;
            }
            return;
        }
    }
    return 1;

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
        $dbh->do(qq{TRUNCATE TABLE "$table"});
    }

    for my $table (@tables2empty) {
        $dbh->do(qq{TRUNCATE TABLE "$table"});
    }

    if ($dbh->{pg_server_version} >= 80300) {
        $dbh->do(q{SET session_replication_role = 'origin'});
    }
    $dbh->commit;

    return;

} ## end of empty_test_database

END {
#    __PACKAGE__->shutdown_cluster($_) for keys %pgver;
}

sub shutdown_cluster {

    ## Shutdown a cluster if running
    ## Takes the cluster name

    my $self = shift;
    my $name = shift;

    my $dirname = $pgver{$name}{dirname};

    return if ! -d $dirname;

    my $pidfile = "$dirname/postmaster.pid";
    return if ! -e $pidfile;

    Test::More::note("Stopping cluster $name");
    my @cmd = ($pg_ctl, '-D', $dirname, '-s', '-m', 'fast', 'stop');
    system(@cmd) == 0 or die "@cmd failed: $?\n";

    ## Hang around until the PID file is gone
    my $loops = 0;
    {
        sleep 0.2;
        last if ! -e $pidfile;
        redo;
    }

    delete $gdbh{$name};

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

    $testmsg = shift || '';
    $testline = shift || (caller)[2];
    $testmsg =~ s/^\s+//;
    if ($location) {
        $testmsg = "($location) $testmsg";
    }
    $testmsg .= " [line: $testline]";
    my $time = time;
    $testmsg .= " [time: $time]" unless $notime;

    return;

} ## end of t


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
        die "bc_deeply failed from line $line. SQL=$sql\n$@\n";
    }

    local $Test::Builder::Level = $Test::Builder::Level + 1;
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


sub add_row_to_database {

    ## Add a row to each table in one of the databases
    ## Arguments: three
    ## 1. Database name to use
    ## 2. Value to use (lookup, not the direct value)
    ## 3. Do we commit or not? Boolean, defaults to true
    ## Returns: undef

    my ($self, $dbname, $xval, $commit) = @_;


    if ($xval > $xvalmax) {
        die "Too high of an ID: max is $xvalmax\n";
    }

    $commit = 1 if ! defined $commit;

    my $dbh = $gdbh{$dbname} or die "No such database: $dbname";

    ## Loop through each table we know about
    for my $table (sort keys %tabletype) {

        ## Look up the actual value to use
        my $type = $tabletype{$table};
        my $value = $val{$type}{$xval};

        ## Prepare it if we have not already
        if (! exists $gsth{$dbh}{insert}{$xval}{$table}) {

            ## Handle odd pkeys
            my $pkey = $table =~ /test5/ ? q{"id space"} : 'id';

            ## Put some standard values in, plus a single placeholder
            my $SQL = qq{INSERT INTO "$table"($pkey,data1,inty,booly) VALUES (?,'foo',$xval,'true')};
            $gsth{$dbh}{insert}{$xval}{$table} = $dbh->prepare($SQL);

            ## If this is a bytea, we need to tell DBD::Pg about it
            if ('BYTEA' eq $type) {
                $gsth{$dbh}{insert}{$xval}{$table}->bind_param(1, undef, {pg_type => PG_BYTEA});
            }

        }

        ## Execute!
        $gsth{$dbh}{insert}{$xval}{$table}->execute($value);

    }

    $dbh->commit() if $commit;

    return undef;

} ## end of add_row_to_database


sub update_row_in_database {

    ## Change a row in each table in a database
    ## We always change the "inty" field
    ## Arguments: four
    ## 1. Database name to use
    ## 2. Primary key to update
    ## 3. New value
    ## 4. Do we commit or not? Boolean, defaults to true
    ## Returns: undef

    my ($self, $dbname, $pkeyvalue, $newvalue, $commit) = @_;

    $commit = 1 if ! defined $commit;

    my $dbh = $gdbh{$dbname} or die "No such database: $dbname";

    ## Loop through each table we know about
    for my $table (sort keys %tabletype) {

        ## Look up the actual value to use
        my $type = $tabletype{$table};
        my $value = $val{$type}{$pkeyvalue};

        ## Prepare it if we have not already
        if (! exists $gsth{$dbh}{update}{inty}{$table}) {

            ## Handle odd pkeys
            my $pkey = $table =~ /test5/ ? q{"id space"} : 'id';

            my $SQL = qq{UPDATE "$table" SET inty=? WHERE $pkey = ?};
            $gsth{$dbh}{update}{inty}{$table} = $dbh->prepare($SQL);

            if ('BYTEA' eq $type) {
                $gsth{$dbh}{update}{inty}{$table}->bind_param(2, undef, {pg_type => PG_BYTEA});
            }

        }

        ## Execute!
        $gsth{$dbh}{update}{inty}{$table}->execute($newvalue,$value);

    }

    $dbh->commit() if $commit;

    return undef;

} ## end of update_row_in_database


sub remove_row_from_database {

    ## Delete a row from each table in one of the databases
    ## Arguments: three
    ## 1. Database name to use
    ## 2. Value to use (lookup, not the direct value). Can be an arrayref.
    ## 3. Do we commit or not? Boolean, defaults to true
    ## Returns: undef

    my ($self, $dbname, $val, $commit) = @_;

    $commit = 1 if ! defined $commit;

    my $dbh = $gdbh{$dbname} or die "No such database: $dbname";

    ## Loop through each table we know about
    for my $table (sort keys %tabletype) {

        ## Prepare it if we have not already
        if (! exists $gsth{$dbh}{delete}{$table}) {

            ## Delete, based on the inty
            my $SQL = qq{DELETE FROM "$table" WHERE inty = ?};
            $gsth{$dbh}{delete}{$table} = $dbh->prepare($SQL);

        }

        ## Execute it.
        if (ref $val) {
            for (@$val) {
                $gsth{$dbh}{delete}{$table}->execute($_);
            }
        }
        else {
            $gsth{$dbh}{delete}{$table}->execute($val);
        }

    }

    $dbh->commit() if $commit;

    return undef;

} ## end of remove_row_from_database


sub truncate_all_tables {

    ## Truncate all the tables
    ## Arguments: two
    ## 1. Database to use
    ## 3. Do we commit or not? Boolean, defaults to true
    ## Returns: undef

    my ($self, $dbname, $commit) = @_;

    $commit = 1 if ! defined $commit;

    my $dbh = $gdbh{$dbname} or die "No such database: $dbname";

    ## Loop through each table we know about
    for my $table (sort keys %tabletype) {
        $dbh->do(qq{TRUNCATE Table "$table"});
    }

    $dbh->commit() if $commit;

    return undef;

} ## end of truncate_all_tables


sub delete_all_tables {

    ## Delete all the tables.
    ## Mostly for old versions that do not support truncate triggers.
    ## Arguments: two
    ## 1. Database to use
    ## 3. Do we commit or not? Boolean, defaults to true
    ## Returns: undef

    my ($self, $dbname, $commit) = @_;

    $commit = 1 if ! defined $commit;

    my $dbh = $gdbh{$dbname} or die "No such database: $dbname";

    ## Loop through each table we know about
    for my $table (sort keys %tabletype) {
        $dbh->do(qq{DELETE FROM "$table"});
    }

    $dbh->commit() if $commit;

    return undef;

} ## end of delete_all_tables


sub check_for_row {

    ## Check that a given row is on the database as expected: checks the inty column only
    ## Arguments: two or three or four
    ## 1. The result we are expecting, as an arrayref
    ## 2. A list of database names (should be inside gdbh)
    ## 3. Optional text to append to output message
    ## 4. Optional tables to limit checking to
    ## Returns: undef

    my ($self, $res, $dblist, $text, $filter) = @_;

    ## Get largest tablename
    my $maxtable = 1;
    for my $table (keys %tabletype) {
        ## Allow skipping tables
        if (defined $filter) {
            my $f = $filter;
            if ($f =~ s/^\!//) {
                if ($table =~ /$f$/) {
                    delete $tabletype{$table};
                    next;
                }
            }
            else {
                if ($table !~ /$f$/) {
                    delete $tabletype{$table};
                    next;
                }
            }
        }
        $maxtable = length $table if length $table > $maxtable;
    }

    for my $dbname (@$dblist) {

        if (! $gdbh{$dbname}) {
            $gdbh{$dbname} = $self->connect_database($dbname,$BucardoTesting::dbname);
        }

        my $dbh = $gdbh{$dbname};

        my $maxdbtable = $maxtable + 1 + length $dbname;

        for my $table (sort keys %tabletype) {

            ## Handle odd pkeys
            my $pkey = $table =~ /test5/ ? q{"id space"} : 'id';

            my $type = $tabletype{$table};
            my $t = sprintf qq{%-*s copy ok (%s)},
                $maxdbtable,
                "$dbname.$table",
                    $type;

            ## Change the message if no rows
            if (ref $res eq 'ARRAY' and ! defined $res->[0]) {
                $t = sprintf qq{No rows as expected in %-*s for pkey type %s},
                    $maxdbtable,
                    "$dbname.$table",
                    $type;
            }

            if (defined $text and length $text) {
                $t .= " $text";
            }

            my $SQL = qq{SELECT inty FROM "$table" ORDER BY inty};
            $table =~ /X/ and $SQL =~ s/inty/$pkey/;

            local $Test::Builder::Level = $Test::Builder::Level + 1;
            my $result = bc_deeply($res, $dbh, $SQL, $t, (caller)[2]);
            $dbh->commit();
            if (!$result) {
                my $line = (caller)[2];
                Test::More::BAIL_OUT("Stopping on a failed 'check_for_row' test from line $line");
            }
        }
    }

    return;

} ## end of check_for_row


sub check_sequences_same {

    ## Check that sequences are the same across all databases
    ## Arguments: one
    ## 1. A list of database names (should be inside gdbh)
    ## Returns: undef

    my ($self, $dblist) = @_;

    for my $seq (sort keys %sequences) {

        $SQL = qq{SELECT * FROM "$seq"};

        ## The first we come across will be the standard for the others
        my (%firstone, $firstdb);

        ## Store failure messages
        my @msg;

        for my $dbname (@$dblist) {

            my $dbh = $gdbh{$dbname} or die "Invalid database name: $dbname";

            my $sth = $dbh->prepare($SQL);
            $sth->execute();
            my $info = $sth->fetchall_arrayref({})->[0];

            if (! defined $firstone{$seq}) {
                $firstone{$seq} = $info;
                $firstdb = $dbname;
                next;
            }

            ## Compare certain items
            for my $item (qw/ last_value start_value increment_by min_value max_value is_cycled is_called/) {
                my ($uno,$dos) = ($firstone{$seq}->{$item}, $info->{$item});
                next if ! defined $uno or ! defined $dos;
                if ($uno ne $dos) {
                    push @msg, "$item is different on $firstdb vs $dbname: $uno vs $dos";
                }
            }

        } ## end each sequence

        if (@msg) {
            Test::More::fail("Sequence $seq NOT the same");
            for (@msg) {
                diag($_);
            }
        }
        else {
            Test::More::pass("Sequence $seq is the same across all databases");
        }

    } ## end each database


    return;


} ## end of check_sequences_same




## Hack to override some Test::More methods
## no critic

sub is_deeply {

    t($_[2],$_[3] || (caller)[2]);
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    my $rv = Test::More::is_deeply($_[0],$_[1],$testmsg);
    return $rv if $rv;
    if ($bail_on_error and ++$total_errors => $bail_on_error) {
        my $line = (caller)[2];
        my $time = time;
        diag("GOT: ".Dumper $_[0]);
        diag("EXPECTED: ".Dumper $_[1]);
        Test::More::BAIL_OUT("Stopping on a failed 'is_deeply' test from line $line. Time: $time");
    }
} ## end of is_deeply
sub like($$;$) {
    t($_[2],(caller)[2]);
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    my $rv = Test::More::like($_[0],$_[1],$testmsg);
    return $rv if $rv;
    if ($bail_on_error and ++$total_errors => $bail_on_error) {
        my $line = (caller)[2];
        my $time = time;
#        Test::More::diag("GOT: ".Dumper $_[0]);
#        Test::More::diag("EXPECTED: ".Dumper $_[1]);
        Test::More::BAIL_OUT("Stopping on a failed 'like' test from line $line. Time: $time");
    }
} ## end of like
sub pass(;$) {
    t($_[0],$_[1]||(caller)[2]);
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    Test::More::pass($testmsg);
} ## end of pass
sub is($$;$) {
    t($_[2],(caller)[2]);
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    my $rv = Test::More::is($_[0],$_[1],$testmsg);
    return $rv if $rv;
    ## Where exactly did this fail?
    my $char = 0;
    my $onelen = length $_[0];
    my $twolen = length $_[1];
    my $line = 1;
    my $lchar = 1;
    for ($char = 0; $char < $onelen and $char < $twolen; $char++) {
        my $one = ord(substr($_[0],$char,1));
        my $two = ord(substr($_[1],$char,1));
        if ($one != $two) {
            diag("First difference at character $char ($one vs $two) (line $line, char $lchar)");
            last;
        }
        if (10 == $one) {
            $line++;
            $lchar = 1;
        }
        else {
            $lchar++;
        }
    }
    if ($bail_on_error and ++$total_errors => $bail_on_error) {
        my $line = (caller)[2];
        my $time = time;
        Test::More::BAIL_OUT("Stopping on a failed 'is' test from line $line. Time: $time");
    }
} ## end of is
sub isa_ok($$;$) {
    t("Object isa $_[1]",(caller)[2]);
    my ($name, $type, $msg) = ($_[0],$_[1]);
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    if (ref $name and ref $name eq $type) {
        Test::More::pass($testmsg);
        return;
    }
    if ($bail_on_error and ++$total_errors => $bail_on_error) {
        Test::More::BAIL_OUT("Stopping on a failed test");
    }
} ## end of isa_ok
sub ok($;$) {
    t($_[1]||$testmsg);
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    my $rv = Test::More::ok($_[0],$testmsg);
    return $rv if $rv;
    if ($bail_on_error and ++$total_errors => $bail_on_error) {
        my $line = (caller)[2];
        my $time = time;
        Test::More::BAIL_OUT("Stopping on a failed 'ok' test from line $line. Time: $time");
    }
} ## end of ok

## use critic


1;
