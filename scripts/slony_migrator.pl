#!/usr/bin/env perl
# -*-mode:cperl; indent-tabs-mode: nil-*-

## Slony migrator
##
## Greg Sabino Mullane <greg@turnstep.com>, Joshua Tolley <josh@endpoint.com>
## End Point Corporation http://www.endpoint.com/
## BSD licensed, see complete license at bottom of this script
## The latest version can be found in the Bucardo distribution at:
## http://www.bucardo.org/
##
## See the HISTORY section for other contributors

package slony_migrator;

use 5.006001;
use strict;
use warnings;
use Getopt::Long qw/GetOptions/;
Getopt::Long::Configure(qw/no_ignore_case/);
use File::Basename qw/basename/;
use File::Temp qw/tempfile tempdir/;
File::Temp->safe_level( File::Temp::MEDIUM );
use Cwd;
use Data::Dumper qw/Dumper/;
$Data::Dumper::Varname = 'SLONY';
$Data::Dumper::Indent = 2;
$Data::Dumper::Useqq = 1;

our $VERSION = '0.0.3';

use vars qw/ %opt $PSQL $res $COM $SQL $db /;

## If psql is not in your path, it is recommended that hardcode it here,
## as an alternative to the --PSQL option
$PSQL = '';

our $SLONIK = 'slonik';

## If this is true, $opt{PSQL} is disabled for security reasons
our $NO_PSQL_OPTION = 1;

## If true, we show how long each query took by default. Requires Time::HiRes to be installed.
$opt{showtime} = 0;

## Which user to connect as if --dbuser is not given
$opt{defaultuser} = 'postgres';

## Default time display format, used for last_vacuum and last_analyze
our $SHOWTIME = 'HH24:MI FMMonth DD, YYYY';

## Nothing below this line should need to be changed for normal usage.
## If you do find yourself needing to change something,
## please email the author as it probably indicates something
## that could be made into a command-line option or moved above.

our $ME = basename($0);
our $ME2 = 'slony_migrator.pl';
our $USAGE = qq{\nUsage: $ME <options>\n Try "$ME --help" for a complete list of options\n\n};

## Global error string, mostly used for MRTG error handling
our $ERROR = '';

## For options that take a time e.g. --critical="10 minutes" Fractions are allowed.
our $timere = qr{^\s*(\d+(?:\.\d+)?)\s*(\w*)\s*$}i;

$opt{test} = 0;
$opt{timeout} = 10;

die $USAGE unless
    GetOptions(
               \%opt,
               'version|V',
               'verbose|v+',
               'help|h',

               'host|H=s@',
               'port=s@',
               'dbname|db=s@',
               'dbuser|u=s@',
               'dbpass=s@',
               'timeout=i',

               'PSQL=s',

               'slonyschema=s',
               'slonyset=i',

               'slonik',
               'bucardo',
               'check',
               )
    and keys %opt
    and ! @ARGV;

our $VERBOSE = $opt{verbose} || 0;

$VERBOSE >= 3 and warn Dumper \%opt;

if ($opt{version}) {
    print qq{$ME2 version $VERSION\n};
    exit 0;
}

if ($opt{help}) {
    print qq{Usage: $ME2 <options>
Slony Migrator
This is version $VERSION.

Main functional options:
  --bucardo          print commands to migrate this Slony cluster to  Bucardo replication
  --slonik           print slonik scripts to recreate this Slony cluster

Common connection options:
 -H,  --host=NAME    hostname(s) to connect to; defaults to none (Unix socket)
 -p,  --port=NUM     port(s) to connect to; defaults to 5432.
 -db, --dbname=NAME  database name(s) to connect to; defaults to 'postgres' or 'template1'
 -u   --dbuser=NAME  database user(s) to connect as; defaults to 'postgres'
      --dbpass=PASS  database password(s); use a .pgpass file instead when possible

Other options:
  --PSQL=FILE        location of the psql executable; avoid using if possible
  -v, --verbose      verbosity level; can be used more than once to increase the level
  -h, --help         display this help information
  -t X, --timeout=X  how long in seconds before we timeout. Defaults to 10 seconds.
  --check            sanity checks the schema (experimental)

For a complete list of options and full documentation, please view the POD for this file.
Two ways to do this is to run:
pod2text $ME | less
pod2man $ME | man -l -
Or simply visit: https://bucardo.org/


};
    exit 0;
}

## Die if Time::HiRes is needed but not found
if ($opt{showtime}) {
    eval {
        require Time::HiRes;
        import Time::HiRes qw/gettimeofday tv_interval sleep/;
    };
    if ($@) {
        die qq{Cannot find Time::HiRes, needed if 'showtime' is true\n};
    }
}

## Everything from here on out needs psql, so find and verify a working version:
if ($NO_PSQL_OPTION) {
    delete $opt{PSQL};
}

if (! defined $PSQL or ! length $PSQL) {
    if (exists $opt{PSQL}) {
        $PSQL = $opt{PSQL};
        $PSQL =~ m{^/[\w\d\/]*psql$} or die qq{Invalid psql argument: must be full path to a file named psql\n};
        -e $PSQL or die qq{Cannot find given psql executable: $PSQL\n};
    }
    else {
        chomp($PSQL = qx{which psql});
        $PSQL or die qq{Could not find a suitable psql executable\n};
    }
}
-x $PSQL or die qq{The file "$PSQL" does not appear to be executable\n};
$res = qx{$PSQL --version};
$res =~ /^psql \(PostgreSQL\) (\d+\.\d+)/ or die qq{Could not determine psql version\n};
our $psql_version = $1;

$VERBOSE >= 1 and warn qq{psql=$PSQL version=$psql_version\n};

$opt{defaultdb} = $psql_version >= 7.4 ? 'postgres' : 'template1';

## Which schema is slony in?
my $schema = $opt{slonyschema} || find_slony_schema();

## Now determine the version of Slony we are dealing with
## Not needed, but a great sanity check
my ($postgres_version, $slony_version, $slony_node) = find_slony_version($schema);

## Next, we want to slurp a bunch of information from Slony tables
## Because no matter what we're doing, we're going to need some of it
## Things to grab:
## sl_set: Basic set information
## sl_node: Basic info on each node
## sl_nodelock: Which nodes are busy
## sl_path: How to reach each node
## sl_listen: What's listening where
## sl_subscribe: Who's subscribed to each set
my $slonyinfo = get_slony_info($schema);
sanitycheck() if defined $opt{check};
if (defined $opt{slonik}) {
    print_slonik($slonyinfo);
}
elsif (defined $opt{bucardo}) {
    make_bucardo_init($slonyinfo);
}
else {
    printinfo();
}

exit 0;

sub sanitycheck {
    print "Beginning sanity check...\n";
    print " * Checking for triggers...\n";
    for my $trigname (($schema.'_logtrigger', $schema.'_denyaccess')) {
        my $SQL = qq{SELECT tab_relname
                    FROM (
                        SELECT tab_relname, tgname FROM $schema.sl_table
                        LEFT JOIN (
                            SELECT tgrelid, tgname FROM pg_trigger
                            WHERE tgname ~ '$trigname'
                        ) f ON ( tab_reloid = tgrelid)) g
                        WHERE tgname IS NULL};
        my $res = run_command($SQL);
        for my $db (@{$res->{db}}) {
            my $s = $db->{slurp};
            for my $row (split /\n/ => $s) {
                print "Table $row is missing the $trigname trigger in database at " . $db->{pname} . "\n";
            }
        }
    }

    my @tables = qw/ sl_path sl_subscribe sl_set sl_node sl_table sl_listen /;
    print ' * Making sure ' . (join ' ', @tables) . " match between databases...\n";
    for my $table (@tables) {
        reduce(
            sub {
                print "Difference in $table instances between databases at \"" .
                    $_[0]{pname} . '" and "' . $_[1]{pname} . "\"\n"
                    if ( join ("\n", sort( split "\n", $_[0]{slurp})) ne join ("\n", sort( split "\n", $_[1]{slurp})));
            },
            @{$slonyinfo->{$table}{db}});
    }
    return;
}

sub reduce {
    my $code = shift;
    my $val = shift;
    for (@_) { $val = $code->($val, $_); }
    return $val;
}

sub printinfo {
    print "Slony version: $slony_version\n";
    print "psql version: $psql_version\n";
    print "Postgres version: $postgres_version\n";
    print "Slony schema: $schema\n";
    print "Local node: $slony_node\n";

    for my $slony_set (sort { $a <=> $b } keys %{$slonyinfo->{set}}) {

        ## Overall set information
        my $s = $slonyinfo->{set}{$slony_set};
        my $comm = $s->{comment} || '';
        print "SET $slony_set: $comm\n";
        if ($s->{locked}) {
            print " This set is locked by txn $s->{locked}\n";
        }

        ## The master
        my $showconn = 1;
        my $origin = $s->{origin};
        my $master = $slonyinfo->{node}{$origin};
        printf qq{* Master node: $origin  Active: %s%s  Comment: "%s"\n%s\n},
            $master->{active} ? 'Yes' : 'No',
            $master->{active} ? "  PID: $master->{pid}" : '',
            $master->{comment},
            $showconn ? "  ($slonyinfo->{path}{$origin}{conninfo})" : '';;

        ## All slaves subscribed to this set
        for my $sub (keys %{$slonyinfo->{sub}}) {
            next if $sub != $slony_set;
            for my $slave (sort { $a <=> $b } keys %{$slonyinfo->{sub}{$sub}}) {
                $s = $slonyinfo->{sub}{$sub}{$slave};
                my $p = $slonyinfo->{path}{$slave};
                my $active = find_slave_status($p->{conninfo}, $slave, $slony_set, $s->{provider});
                printf qq{  ** Slave node: %2d  Active: %3s  Forward: %3s  Provider: %2d  Comment: "%s"\n    %s\n},
                    $slave,
                    $active eq 't' ? 'Yes' : 'No',
                    $s->{forward} ? 'Yes' : 'No',
                    $s->{provider},
                    $slonyinfo->{node}{$slave}{comment},
                    $showconn ? " ($slonyinfo->{path}{$slave}{conninfo})" : '';
            }
        }

    }
    return;
} ## End of printinfo


sub pretty_size {

    ## Transform number of bytes to a SI display similar to Postgres' format

    my $bytes = shift;
    my $rounded = shift || 0;

    return "$bytes bytes" if $bytes < 10240;

    my @unit = qw/kB MB GB TB PB EB YB ZB/;

    for my $p (1..@unit) {
        if ($bytes <= 1024**$p) {
            $bytes /= (1024**($p-1));
            return $rounded ?
                sprintf ('%d %s', $bytes, $unit[$p-2]) :
                    sprintf ('%.2f %s', $bytes, $unit[$p-2]);
        }
    }

    return $bytes;

} ## end of pretty_size


sub run_command {

    ## Run a command string against each of our databases using psql
    ## Optional args in a hashref:
    ## "failok" - don't report if we failed
    ## "target" - use this targetlist instead of generating one
    ## "timeout" - change the timeout from the default of $opt{timeout}
    ## "regex" - the query must match this or we throw an error
    ## "emptyok" - it's okay to not match any rows at all
    ## "version" - alternate versions for different versions
    ## "dbnumber" - connect with an alternate set of params, e.g. port2 dbname2

    my $string = shift || '';
    my $arg = shift || {};
    my $info = { command => $string, db => [], hosts => 0 };

    $VERBOSE >= 3 and warn qq{Starting run_command with "$string"\n};

    my (%host,$passfile,$passfh,$tempdir,$tempfile,$tempfh,$errorfile,$errfh);
    my $offset = -1;

    ## Build a list of all databases to connect to.
    ## Number is determined by host, port, and db arguments
    ## Multi-args are grouped together: host, port, dbuser, dbpass
    ## Grouped are kept together for first pass
    ## The final arg in a group is passed on
    ##
    ## Examples:
    ## --host=a,b --port=5433 --db=c
    ## Connects twice to port 5433, using database c, to hosts a and b
    ## a-5433-c b-5433-c
    ##
    ## --host=a,b --port=5433 --db=c,d
    ## Connects four times: a-5433-c a-5433-d b-5433-c b-5433-d
    ##
    ## --host=a,b --host=foo --port=1234 --port=5433 --db=e,f
    ## Connects six times: a-1234-e a-1234-f b-1234-e b-1234-f foo-5433-e foo-5433-f
    ##
    ## --host=a,b --host=x --port=5432,5433 --dbuser=alice --dbuser=bob -db=baz
    ## Connects three times: a-5432-alice-baz b-5433-alice-baz x-5433-bob-baz

    ## The final list of targets:
    my @target;

    ## Default connection options
    my $conn =
        {
         host   => ['<none>'],
         port   => [5432],
         dbname => [$opt{defaultdb}],
         dbuser => [$opt{defaultuser}],
         dbpass => [''],
         inputfile => [''],
         };

    my $gbin = 0;
  GROUP: {
        ## This level controls a "group" of targets

        ## If we were passed in a target, use that and move on
        if (exists $arg->{target}) {
            push @target, $arg->{target};
            last GROUP;
        }

        my %group;
        my $foundgroup = 0;
        for my $v (keys %$conn) {
            my $vname = $v;
            ## Something new?
            if ($arg->{dbnumber}) {
                $v .= "$arg->{dbnumber}";
            }
            if (defined $opt{$v}->[$gbin]) {
                my $new = $opt{$v}->[$gbin];
                $new =~ s/\s+//g;
                ## Set this as the new default
                $conn->{$vname} = [split /,/ => $new];
                $foundgroup = 1;
            }
            $group{$vname} = $conn->{$vname};
        }

        if (!$foundgroup) { ## Nothing new, so we bail
            last GROUP;
        }
        $gbin++;

        ## Now break the newly created group into individual targets
        my $tbin = 0;
      TARGET: {
            my $foundtarget = 0;
            ## We know th
            my %temptarget;
#            map { $temptarget{$_} = '' } qw/port host dbname dbuser/;
            for my $g (keys %group) {
                if (defined $group{$g}->[$tbin]) {
                    $conn->{$g} = [$group{$g}->[$tbin]];
                    $foundtarget = 1;
                }
                $temptarget{$g} = $conn->{$g}[0] || '';
            }

            ## Leave if nothing new
            last TARGET if ! $foundtarget;

            ## Add to our master list
            push @target, \%temptarget;

            $tbin++;
            redo;
        } ## end TARGET

        redo;
    } ## end GROUP

    if (! @target) {
        die qq{No target databases found\n};
    }

    ## Create a temp file to store our results
    $tempdir = tempdir(CLEANUP => 1);
    ($tempfh,$tempfile) = tempfile('slony_bucardo_migrator.XXXXXXX', SUFFIX => '.tmp', DIR => $tempdir);

    ## Create another one to catch any errors
    ($errfh,$errorfile) = tempfile('slony_bucardo_migrator.XXXXXXX', SUFFIX => '.tmp', DIR => $tempdir);

    for $db (@target) {

        ## Just to keep things clean:
        truncate $tempfh, 0;
        truncate $errfh, 0;

        ## Store this target in the global target list
        push @{$info->{db}}, $db;

        $db->{pname} = "port=$db->{port} host=$db->{host} db=$db->{dbname} user=$db->{dbuser}";
        my @args = ('-q', '-U', "$db->{dbuser}", '-d', $db->{dbname}, '-t');
        if ($db->{host} ne '<none>') {
            push @args => '-h', $db->{host};
            $host{$db->{host}}++; ## For the overall count
        }
        push @args => '-p', $db->{port};

        if (defined $db->{dbpass} and length $db->{dbpass}) {
            ## Make a custom PGPASSFILE. Far better to simply use your own .pgpass of course
            ($passfh,$passfile) = tempfile('nagios.XXXXXXXX', SUFFIX => '.tmp', DIR => $tempdir);
            $VERBOSE >= 3 and warn "Created temporary pgpass file $passfile\n";
            $ENV{PGPASSFILE} = $passfile;
            printf $passfh "%s:%s:%s:%s:%s\n",
                $db->{host} eq '<none>' ? '*' : $db->{host},
                $db->{port},   $db->{dbname},
                $db->{dbuser}, $db->{dbpass};
            close $passfh or die qq{Could not close $passfile: $!\n};
        }


        push @args, '-o', $tempfile;

        ## If we've got different SQL, use this first run to simply grab the version
        ## Then we'll use that info to pick the real query
        if ($arg->{version}) {
            $arg->{oldstring} = $string;
            $string = 'SELECT version()';
        }

        if (defined $db->{inputfile} and length $db->{inputfile}) {
            push @args, '-f', $db->{inputfile};
        } else {
            push @args, '-c', $string;
        }

        $VERBOSE >= 3 and warn Dumper \@args;

        local $SIG{ALRM} = sub { die 'Timed out' };
        my $timeout = $arg->{timeout} || $opt{timeout};
        alarm 0;

        my $start = $opt{showtime} ? [gettimeofday()] : 0;
        eval {
            alarm $timeout;
#            print "$PSQL " . (join ' ', @args);
            $res = system $PSQL => @args;
        };
        my $err = $@;
        alarm 0;
        if ($err) {
            if ($err =~ /Timed out/) {
                die qq{Command: "$string" timed out! Consider boosting --timeout higher than $timeout\n};
            }
            else {
                die q{Unknown error inside of the "run_command" function};
            }
        }

        $db->{totaltime} = sprintf '%.2f', $opt{showtime} ? tv_interval($start) : 0;

        if ($res) {
            $res >>= 8;
            $db->{fail} = $res;
            $VERBOSE >= 3 and !$arg->{failok} and warn qq{System call failed with a $res\n};
            seek $errfh, 0, 0;
            {
                local $/;
                $db->{error} = <$errfh> || '';
                $db->{error} =~ s/\s*$//;
                $db->{error} =~ s/^psql: //;
                $ERROR = $db->{error};
            }
            if (!$db->{ok} and !$arg->{failok}) {
                die "Query failed: $string\n";
            }
        }
        else {
            seek $tempfh, 0, 0;
            {
                local $/;
                $db->{slurp} = <$tempfh>;
            }
            $db->{ok} = 1;

            ## Allow an empty query (no matching rows) if requested
            if ($arg->{emptyok} and $db->{slurp} =~ /^\s*$/o) {
            }
            ## If we were provided with a regex, check and bail if it fails
            elsif ($arg->{regex}) {
                if ($db->{slurp} !~ $arg->{regex}) {
                    die "Regex failed for query: $string\n";
                }
            }

        }

        ## If we are running different queries based on the version,
        ## find the version we are using, replace the string as needed,
        ## then re-run the command to this connection.
        if ($arg->{version}) {
            if ($db->{error}) {
                die $db->{error};
            }
            if ($db->{slurp} !~ /PostgreSQL (\d+\.\d+)/) {
                die qq{Could not determine version of Postgres!\n};
            }
            $db->{version} = $1;
            $string = $arg->{version}{$db->{version}} || $arg->{oldstring};
            delete $arg->{version};
            redo;
        }
    } ## end each database

#    close $errfh or die qq{Could not close $errorfile: $!\n};
#    close $tempfh or die qq{Could not close $tempfile: $!\n};

    $info->{hosts} = keys %host;

    $VERBOSE >= 3 and warn Dumper $info;

    return $info;


} ## end of run_command


sub size_in_bytes { ## no critic (RequireArgUnpacking)

    ## Given a number and a unit, return the number of bytes.

    my ($val,$unit) = ($_[0],lc substr($_[1]||'s',0,1));
    return $val * ($unit eq 's' ? 1 : $unit eq 'k' ? 1024 : $unit eq 'm' ? 1024**2 :
                   $unit eq 'g' ? 1024**3 : $unit eq 't' ? 1024**4 :
                   $unit eq 'p' ? 1024**5 : $unit eq 'e' ? 1024**6 :
                   $unit eq 'z' ? 1024**7 : 1024**8);

} ## end of size_in_bytes


sub size_in_seconds {

    my ($string,$type) = @_;

    return '' if ! length $string;
    if ($string !~ $timere) {
        my $l = substr($type,0,1);
        die qq{Value for '$type' must be a valid time. Examples: -$l 1s  -$l "10 minutes"\n};
    }
    my ($val,$unit) = ($1,lc substr($2||'s',0,1));
    my $tempval = sprintf '%.9f', $val * ($unit eq 's' ? 1 : $unit eq 'm' ? 60 : $unit eq 'h' ? 3600 : 86600);
    $tempval =~ s/0+$//;
    $tempval = int $tempval if $tempval =~ /\.$/;
    return $tempval;

} ## end of size_in_seconds


sub get_slony_info {

    ## Extract some information from the Slony sl_ tables
    ## Returns a hashref

    my $schema = shift;
    my (%info, $info, $s);

    ## sl_node
    $SQL = qq{SELECT no_id, no_active, no_comment FROM $schema.sl_node};
    #$SQL = qq{SELECT no_id, no_active, no_spool, no_comment FROM $schema.sl_node};
    $info = run_command($SQL);
    $s = $info->{db}[0]{slurp};
    for my $row (split /\n/ => $s) {
        my @i = split /\s*\|\s*/ => $row;
        my $id = int $i[0];
        $info{node}{$id}{active} = $i[1] eq 't' ? 1 : 0;
#        $info{node}{$id}{spool} = $i[2] eq 't' ? 1 : 0;
        #$info{node}{$id}{comment} = $i[3];
        $info{node}{$id}{comment} = $i[2];
    }
    $info{sl_node} = $info;

    ## sl_nodelock
    $SQL = qq{SELECT nl_nodeid, nl_conncnt, nl_backendpid FROM $schema.sl_nodelock};
    $info = run_command($SQL);
    $s = $info->{db}[0]{slurp};
    for my $row (split /\n/ => $s) {
        my @i = split /\s*\|\s*/ => $row;
        my $id = int $i[0];
        $info{node}{$id}{connectnumber} = $i[1];
        $info{node}{$id}{pid} = int $i[2];
    }
    $info{sl_nodelock} = $info;

    ## sl_set
    $SQL = qq{SELECT set_id, set_origin, set_locked, set_comment FROM $schema.sl_set};
    $info = run_command($SQL);
    $s = $info->{db}[0]{slurp};
    for my $row (split /\n/ => $s) {
        my @i = split /\s*\|\s*/ => $row;
        my $id = int $i[0];
        $info{set}{$id}{origin} = $i[1];
        $info{set}{$id}{locked} = $i[2];
        $info{set}{$id}{comment} = $i[3];
    }
    $info{sl_set} = $info;

    ## sl_subscribe
    $SQL = qq{SELECT sub_set, sub_provider, sub_receiver, sub_forward, sub_active FROM $schema.sl_subscribe};
    $info = run_command($SQL);
    $s = $info->{db}[0]{slurp};
    for my $row (split /\n/ => $s) {
        my @i = split /\s*\|\s*/ => $row;
        my $id = int $i[0];
        $info{sub}{$id}{$i[2]}{provider} = $i[1];
        $info{sub}{$id}{$i[2]}{forward}  = $i[3] ? 1 : 0;
        $info{sub}{$id}{$i[2]}{active}   = $i[4] ? 1 : 0;
    }
    $info{sl_subscribe} = $info;

    ## sl_path
    $SQL = qq{SELECT pa_server, pa_client, pa_connretry, pa_conninfo FROM $schema.sl_path};
    $info = run_command($SQL);
    $s = $info->{db}[0]{slurp};
    for my $row (split /\n/ => $s) {
        my @i = split /\s*\|\s*/ => $row;
        my $id = int $i[0];
        $info{path}{$id}{client} = $i[1];
        $info{path}{$id}{delay} = $i[2];
        $info{path}{$id}{conninfo} = $i[3];
    }
    $info{sl_path} = $info;


    ## sl_listen
    $SQL = qq{SELECT li_origin, li_provider, li_receiver FROM $schema.sl_listen};
    $info = run_command($SQL);
    $s = $info->{db}[0]{slurp};
    for my $row (split /\n/ => $s) {
        my @i = split /\s*\|\s*/ => $row;
        my $id = int $i[0];
        $info{listen}{$id}{provider} = $i[1];
        $info{listen}{$id}{receiver} = $i[2];
    }
    $info{sl_listen} = $info;


    ## sl_table
    $SQL = qq{SELECT tab_id, tab_nspname || '.' || tab_relname, tab_set, tab_idxname, tab_comment, set_origin FROM $schema.sl_table JOIN $schema.sl_set ON (set_id = tab_set) ORDER BY tab_set, tab_id};
    $info = run_command($SQL);
    $s = $info->{db}[0]{slurp};
    for my $row (split /\n/ => $s) {
        my @i = split /\s*\|\s*/ => $row;
        my $id                     = int $i[0];
        $info{table}{$id}{FQN}     = $i[1];
        $info{table}{$id}{set}     = int $i[2];
        $info{table}{$id}{key}     = $i[3];
        $info{table}{$id}{comment} = $i[4];
        $info{table}{$id}{origin}  = int $i[5];
    }
    $info{sl_table} = $info;

    ## sl_sequence
    $SQL = qq{SELECT seq_id, seq_nspname || '.' || seq_relname, seq_set, seq_comment, set_origin FROM $schema.sl_sequence JOIN $schema.sl_set ON (set_id = seq_set) ORDER BY seq_set, seq_id};
    $info = run_command($SQL);
    $s = $info->{db}[0]{slurp};
    for my $row (split /\n/ => $s) {
        my @i = split /\s*\|\s*/ => $row;
        my $id                        = int $i[0];
        $info{sequence}{$id}{FQN}     = $i[1];
        $info{sequence}{$id}{set}     = int $i[2];
        $info{sequence}{$id}{comment} = $i[3];
        $info{sequence}{$id}{origin}  = int $i[4];
    }
    $info{sl_sequence} = $info;

    return \%info;

} ## end of get_slony_info


sub find_slony_schema {

    ## Attempt to figure out the name of the Slony schema
    ## Returns the name of the schema, quoted if needed
    ## Dies if none found, or more than one found

    $SQL = q{SELECT quote_ident(nspname) FROM pg_namespace WHERE oid IN}.
        q{(SELECT pronamespace FROM pg_proc WHERE proname = 'slonyversion')};

    my $info = run_command($SQL);

    my $schema = '';
    if (defined $info->{db}[0] and exists $info->{db}[0]{slurp}) {
        (my @names) = map { s/\s//g; $_ } grep { /\S/ } split /\s*\|\s*/ => $info->{db}[0]{slurp};
        if (@names) {
            my $num = @names;
            if ($num > 1) {
                ## Or should we simply show them all?
                my $list = join ',' => map { qq{"$_"} } @names;
                die "Please specify a slony scheme. We found: $list\n";
            }
            $schema = $names[0];
        }
    }
    if (! length $schema) {
        die "Could not find a slony schema, please specify one using the --slonyschema option\n";
    }

    return $schema;

} ## end of find_slony_schema


sub find_slony_version {

    ## Returns the version of Slony via the slonyversion() function

    my $schema = shift; ## make global?

    my $safeschema = $schema;
    $safeschema =~ s/'/''/g;

    $SQL = qq{SELECT version(), $schema.slonyversion(), $schema.getlocalnodeid('$safeschema')};

    my $info = run_command($SQL, { regex => qr{([\d\.]+)} });

    my ($pg_version, $sl_version, $sl_node) = (0,0,0);
    if (defined $info->{db}[0] and exists $info->{db}[0]{slurp}) {
        if ($info->{db}[0]{slurp} =~ /PostgreSQL (\S+).*\| ([\d\.]+)\s*\|\s*(\d+)/) {
            ($pg_version, $sl_version, $sl_node) = ($1,$2,$3);
        }
    }

    ## Usually due to an incorrect schema
    $sl_version or die "Could not determine the version of Slony\n";
    $sl_node or die "Could not determine the local Slony node\n";
    $pg_version or die "Could not determine the version of Postgres\n";

    return $pg_version, $sl_version, $sl_node;

} ## end of find_slony_version


sub find_slave_status {

    my ($conninfo, $slave, $slony_set, $provider) = @_;
    my ($info, %info);

    # Create a new target for $PSQL query because
    # sl_subscribe.sub_active is only meaningful on the slave

    # parse out connection information from $conninfo
    my %target = ();
    # Figure out a way to fail gracefully if the port selection doesn't work
    $target{port}   = $conninfo =~ /port=(\d+)/   ? $1 : ($opt{port}[0] || 5432);
    $target{host}   = $conninfo =~ /host=(\S+)/   ? $1 : die 'No host found?';
    $target{dbname} = $conninfo =~ /dbname=(\S+)/ ? $1 : die 'No dbname found?';
    $target{dbuser} = $conninfo =~ /user=(\S+)/   ? $1 : die 'No dbuser found?';

    eval {
        my $SQL = qq{SELECT sub_active FROM $schema.sl_subscribe WHERE sub_receiver = $slave }.
            qq{AND sub_provider = $provider AND sub_set = $slony_set};
        $info = run_command($SQL, { target => \%target });
    };
    if ($@) {
        print "Failed\n";
    }
    my $status = '';
    if (defined $info->{db}[0] and exists $info->{db}[0]{slurp}) {
        my (@statuses) = map { s/\s//g; $_ } grep { /\S/ } split /\s*\|\s*/ => $info->{db}[0]{slurp};
        if (@statuses) {
            my $num = @statuses;
            if ($num > 1) {
                die "Oops, found more than one subscription on set $slony_set to provider $provider from node $slave\n";
            }
            $status = $statuses[0];
        }
    }
    if (!length $status) {
        die qq{Could not figure out status of slave $slave};
    }

    return $status;

} ## end of find_slave_status

sub get_slony_set {

    if (defined $opt{slonyset}) {
        return $opt{slonyset};
    }

    my $slony_set;
    my @sets = keys %{$slonyinfo->{set}};
    if (@sets) {
        my $num = @sets;
        if ($num > 1) {
            my $list = join ', ' => @sets;
            die "Please specify a set with the --slonyset option. We found $list\n";
        }
        $slony_set = $sets[0];
    }

    return $slony_set;

} ## end of get_slony_set


#
# Slonyinfo helpers
#

sub get_conninfo {

    my ($node) = @_;
    unless (defined $slonyinfo->{path}{$node} and exists $slonyinfo->{path}{$node}{conninfo}) {
        die "ERROR: Unable to find node $node. Are you sure that node exists?\n";
    }

    return ($slonyinfo->{path}{$node}{conninfo});
}

sub get_master {

    my $slony_set = get_slony_set();
    my $s = $slonyinfo->{set}{$slony_set}; ## or die
    my $master = $s->{origin};

    return $master;
}

# returns a string suitable for passing to slonik
sub create_store_paths {

    my ($new_node, $new_conninfo) = @_;
    my $paths;
    # for each node in the slony network, create a store path to a new_node node
    # store path ( server = ? , client = ? , conninfo = $conninfo ' );
    foreach my $old_node (sort keys %{$slonyinfo->{node}}) {
        my $old_conninfo = get_conninfo($old_node);
        $paths .= qq{store path ( server=$old_node, client=$new_node, conninfo='$old_conninfo' );\n};
        $paths .= qq{store path ( server=$new_node, client=$old_node, conninfo='$new_conninfo' );\n};
    }

    return $paths;
}

# generates all admin paths for all nodes
# returns a string suitable for passing to slonik
sub create_admin_paths {

    # can indicate a node to skip
    my ($skip_node) = @_;
    my $connections;
    # for each node in the slony network, create a store path to a new_node node
    # store path ( server = ? , client = ? , conninfo = $conninfo ' );
    foreach my $node (keys %{$slonyinfo->{node}}) {
        next if (defined $skip_node and $node == $skip_node);
        my $conninfo = get_conninfo($node);
        $connections .= qq{node $node admin conninfo='$conninfo';\n}
    }

    return $connections;
}

#
# Utility functions
#

sub prompt_user {
    my ($prompt_string, $default) = @_;
    if ($default) {
        print $prompt_string, '[', $default, ']: ';
    } else {
        print $prompt_string, ': ';
    }

    $| = 1;
    $_ = <STDIN>;

    chomp;
    if ("$default") {
        return $_ ? $_ : $default # return $_ if it has a value
    } else {
        return $_;
    }
}

sub make_bucardo_init {
    my $info = shift;
    my (@dbs, @herds, @syncs, @tables, @sequences);
    my $cluster_name = $schema;
    $cluster_name =~ s/^_//;

    PATHS:
    for my $p (keys %{$info->{path}}) {
        my ($name, $conninfo) = ($cluster_name.'_'.$p, $info->{path}{$p}{conninfo});
        if ($conninfo eq '<event pending>') {
            warn "Couldn't get connection info for database $name.";
            next PATHS;
        }
        my @connopts = split /\s+/, $conninfo;
        my ($dbname, $conn) = ('', '');
        for my $opt (@connopts) {
            my ($key, $value) = split /=/, $opt;
            my $match;
            if ($key eq 'dbname') { $dbname = $value; }
            else {
                for my $a (qw/host port user pass/) {
                    if ($key eq $a) {
                        $match = 1;
                        $conn .= " $a=$value";
                    }
                }
                $conn .= " $key=$value" unless defined $match;
            }
        }
        $dbs[$p] = {
            name => $name,
            conninfo => $conninfo,
        };
        print "./bucardo add db $name dbname=$dbname $conn\n";
    }

    for my $set (@{ get_ordered_subscribes($info->{sub}, $info->{set}, $info->{node}) }) {
        traverse_set($set, sub {
            my $node = shift;
            my $set_num = $set->{set_num};
            my $db = $cluster_name . '_' . $node->{num};
            my $herd = $cluster_name . '_node' . $node->{num} . '_set' . $set_num;
            if (exists $node->{children} and $#{$node->{children}} > -1) {
                map {
                    my $name = $info->{table}{$_}{FQN};
                    if ($info->{table}{$_}{set} == $set_num) {
                        print "./bucardo add table $name db=$db autokick=true conflict_strategy=source herd=$herd\n";
                    }
                } keys %{$info->{table}};
                map {
                    my $name = $info->{sequence}{$_}{FQN};
                    if ($info->{sequence}{$_}{set} == $set_num) {
                        print "./bucardo add sequence $name db=$db autokick=true conflict_strategy=source herd=$herd\n";
                    }
                } keys %{$info->{sequence}};
                for my $child (@{$node->{children}}) {
                    my $targetdbname = $cluster_name . '_' . $child;
                    my $syncname = $cluster_name . '_set' . $set_num . '_node' . $node->{num} . '_to_node' . $child;
                    my $childnode = $set->{$child};
                    print "./bucardo add sync $syncname source=$herd targetdb=$targetdbname type=pushdelta";
                    print " target_makedelta=on"
                        if (exists $childnode->{children} and $#{$childnode->{children}} > -1);
                    print "\n";
                }
            }
        }, { include_origin => 1 });
    }
    return;
}

sub print_slonik {
    my $info = shift;
    my $cluster = $schema;

    $cluster =~ s/^_//;
    print "CLUSTER NAME = $cluster;\n";
    my $master_id;
    for my $p (keys %{$info->{path}}) {
        not $master_id and $master_id = $p;
        print "NODE $p ADMIN CONNINFO = '" . $info->{path}{$p}{conninfo} ."';\n";
    }

    # Set up nodes
    print "INIT CLUSTER (ID = $master_id, COMMENT = '" . $info->{node}{$master_id}{comment} . "');\n";
    for my $p (keys %{$info->{node}}) {
        next if $p eq $master_id;
        # TODO Make sure EVENT NODE is right, here
        print "STORE NODE (ID = $p, EVENT NODE = $master_id, COMMENT = '" . $info->{node}{$p}{comment} ."');\n";
    }

    # Set up paths
    for my $p (sort keys %{$info->{path}}) {
        print "STORE PATH (SERVER = $p, CLIENT = " . $info->{path}{$p}{client} .
                           ', CONNINFO = \''  . $info->{path}{$p}{conninfo} .
                           '\', CONNRETRY = ' . $info->{path}{$p}{delay} . ");\n";
    }

    print "ECHO 'Please start up replication nodes here';\n";

    for my $p (sort keys %{$info->{set}}) {
        print "TRY {
    CREATE SET (ID = $p, ORIGIN = " . $info->{set}{$p}{origin} .
    ', COMMENT = \'' . $info->{set}{$p}{comment} . "');
} ON ERROR {
    EXIT -1;
}\n";
    }

    for my $p (keys %{$info->{table}}) {
        print "SET ADD TABLE (ID = $p, ORIGIN = " . $info->{table}{$p}{origin}
            . ', SET ID = ' . $info->{table}{$p}{set}
            . ', FULLY QUALIFIED NAME = \'' . $info->{table}{$p}{FQN}
            . '\', KEY = \'' . $info->{table}{$p}{key}
            . '\', COMMENT = \'' . $info->{table}{$p}{comment} . "');\n";
    }

    for my $p (keys %{$info->{sequence}}) {
        print "SET ADD SEQUENCE (ID = $p, ORIGIN = " . $info->{sequence}{$p}{origin}
            . ', SET ID = ' . $info->{sequence}{$p}{set}
            . ', FULLY QUALIFIED NAME = \'' . $info->{sequence}{$p}{FQN}
            . '\', COMMENT = \'' . $info->{sequence}{$p}{comment} . "');\n";
    }

    my $p = 0;
    for my $set (@{ get_ordered_subscribes($info->{sub}, $info->{set}, $info->{node}) }) {
        traverse_set($set, sub {
            my $node = shift;
            print "SUBSCRIBE SET (ID = $set->{set_num}, PROVIDER = $node->{parent}, RECEIVER = $node->{num}, "
                    . "FORWARD = " . ($node->{forward} ? 'YES' : 'NO') . ");\n";
        }, {} );
    }
    return;
}

sub process_child {
    my ($set, $node, $callback) = @_;
    $callback->($node);
    map { process_child($set, $set->{$_}, $callback) } @{$node->{children}};
    return;
}

sub traverse_set {
    my ($set, $callback, $args) = @_;
    $callback->($set->{origin}) if (exists ($args->{include_origin}) and $args->{include_origin});
    map { process_child($set, $set->{$_}, $callback) if (exists $set->{$_}) } @{$set->{origin}{children}};
    return;
}

sub get_ordered_subscribes {
    my ($subs, $sets, $nodes) = @_;
    # Bucardo needs to know each set; slonik just needs to know a valid subscribe order
    my @results;
    #map { push @subs, $subs->{$_}; } keys %{ $subs };

    for my $set_num (keys %$subs) {
        my $origin = { num => $sets->{$set_num}{origin}, };
        my $set = { set_num => $set_num, origin => $origin, $origin->{num} => $origin };
        for my $sub (keys %{$subs->{$set_num}}) {
            my $node;
            my ($prov, $recv) = ($subs->{$set_num}{$sub}{provider}, $sub);
            if (! exists ($set->{$recv})) {
                $node = { num => $recv, forward => $subs->{$set_num}{$sub}{forward}, };
                $set->{$recv} = $node;
            }
            else {
                $node = $set->{$recv};
            }
            $node->{parent} = $prov;
            if (! exists ($set->{$prov})) {
                my $newnode = { num => $prov, forward => $subs->{$set_num}{$sub}{forward}, };
                $set->{$prov} = $newnode;
            }
            push @{$set->{$prov}->{children}}, $recv;
        }
        push @results, $set;
    }
    return \@results;
}

=pod

=head1 NAME

B<slony_migrator.pl> - Slony-to-Bucardo migration tool

=head1 SYNOPSIS

Provides information about a running Slony cluster, including a summary
description (default), Slonik scripts (the --slonik option), and
Slony-to-Bucardo migration scripts (the --bucardo option).

=head1 DESCRIPTION

Connects to a running Slony cluster and provides one of the following: A
summary of the sets and nodes involved in the cluster, a slonik script to
rebuild the cluster from scratch, or bucardo commands to build the same
cluster based on Bucardo. This last will allow migration from Slony to Bucardo.

=head1 OPTIONS FOR PRINCIPLE FUNCTIONS

=over 4

=item B<--bucardo>

Returns a list of bucardo commands which will allow migration of a Slony
cluster off of Slony and on to Bucardo. After installing Bucardo with
I<bucardo install>, these scripts will tell Bucardo about all the tables
and sequences in the Slony sets, each node in the Slony cluster, and configure
Bucardo to replicate those objects in the same way Slony does. This includes
the use of cascaded replication.

=item B<--slonik>

Returns a Slonik script which will recreate the Slony cluster from scratch.

=back

=head1 DATABASE CONNECTION OPTIONS

=over 4

=item B<-H NAME> or B<--host=NAME>

Connect to the host indicated by NAME.

=item B<-p PORT> or B<--port=PORT>

Connects using the specified PORT number.

=item B<-db NAME> or B<--dbname=NAME>

Specifies which database to connect to. If no dbname option is provided,
defaults to 'postgres' if psql is version 8 or greater, and 'template1'
otherwise.

=item B<-u USERNAME> or B<--dbuser=USERNAME>

The name of the database user to connect as. If this is not provided, the
default is 'postgres'.

=item B<--dbpass=PASSWORD>

Provides the password to connect to the database with. Use of this option is highly discouraged.
Instead, one should use a .pgpass file.

=back

=head1 OTHER OPTIONS

Other options include:

=over 4

=item B<-t VAL> or B<--timeout=VAL>

Sets the timeout in seconds after which the script will abort whatever it is doing
and return an UNKNOWN status. The timeout is per Postgres cluster, not for the entire
script. The default value is 10; the units are always in seconds.

=item B<-h> or B<--help>

Displays a help screen with a summary of all actions and options.

=item B<-V> or B<--version>

Shows the current version.

=item B<-v> or B<--verbose>

Set the verbosity level. Can call more than once to boost the level. Setting it to three
or higher (in other words, issuing C<-v -v -v>) turns on debugging information for this
program which is sent to stderr.

=item B<--PSQL=PATH>

Tells the script where to find the psql program. Useful if you have more than
one version of the psql executable on your system, or if there is no psql program
in your path. Note that this option is in all uppercase. By default, this option
is I<not allowed>. To enable it, you must change the C<$NO_PSQL_OPTION> near the
top of the script to 0. Avoid using this option if you can, and instead hard-code
your psql location into the C<$PSQL> variable, also near the top of the script.

=back

=head1 DEPENDENCIES

Access to a working version of psql, and Perl v5.6.1 or later. Also the
Time::HiRes Perl module if C<$opt{showtime}> is set to true, which is the
default.

=head1 DEVELOPMENT

Development happens using the git system. You can clone the latest version by doing:

 git clone https://bucardo.org/bucardo.git/

=head1 HISTORY

=over 4

=item B<Version 0.0.3>, first release

=back

=head1 BUGS AND LIMITATIONS

Slony paths aren't all captured, so --slonik output might need some tweaking to
work correctly 

Please report any problems to josh@endpoint.com.

=head1 AUTHORS

 Greg Sabino Mullane <greg@turnstep.com>
 Selena Decklemann <selena@endpoint.com>
 Joshua Tolley <josh@endpoint.com>

=head1 LICENSE AND COPYRIGHT

Copyright (c) 2007-2009 Greg Sabino Mullane <greg@turnstep.com>.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

  1. Redistributions of source code must retain the above copyright notice,
     this list of conditions and the following disclaimer.
  2. Redistributions in binary form must reproduce the above copyright notice,
     this list of conditions and the following disclaimer in the documentation
     and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE AUTHOR "AS IS" AND ANY EXPRESS OR IMPLIED
WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
OF SUCH DAMAGE.

=cut
