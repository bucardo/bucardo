#!/usr/bin/env perl
# -*-mode:cperl; indent-tabs-mode: nil-*-

## The main Bucardo program
##
## This script should only be called via the 'bucardo_ctl' program
##
## Copyright 2006-2010 Greg Sabino Mullane <greg@endpoint.com>
##
## Please visit http://bucardo.org for more information

package Bucardo;
use 5.008003;
use strict;
use warnings;

our $VERSION = '4.5.0';

use sigtrap qw( die normal-signals ); ## Call die() on HUP, INT, PIPE, or TERM
use Config;                           ## Used to map signal names
use Time::HiRes qw( sleep );          ## For better resolution than the built-in sleep
use DBI 1.51;                         ## How Perl talks to databases
use DBD::Pg 2.0;                      ## The Postgres driver for DBI
use POSIX qw( strftime );             ## For grabbing the local timezone
use Net::SMTP;                        ## Used to send out email alerts
use Sys::Hostname qw( hostname );     ## Used for debugging/mail sending
use IO::Handle qw( autoflush );       ## Used to prevent stdout/stderr buffering
use Sys::Syslog qw( openlog syslog ); ## In case we are logging via syslog()
use DBIx::Safe '1.2.4';               ## Filter out what DB calls customcode may use
use Data::Dumper qw( Dumper );        ## Used to dump information in email alerts

## Formatting of Dumper() calls:
$Data::Dumper::Varname = 'BUCARDO';
$Data::Dumper::Indent = 1;

## Common variables we don't want to declare over and over:
use vars qw($SQL %SQL $sth %sth $count $info);

## Map system signal numbers to standard names
my $x = 0;
my %signumber;
for (split(' ', $Config{sig_name})) {
    $signumber{$_} = $x++;
}

## Prevent buffering of output:
*STDOUT->autoflush(1);
*STDERR->autoflush(1);

## Configuration of DBIx::Safe
## Specify exactly what database handles are allowed to do within custom code
## Here, 'strict' means 'inside the main transaction that Bucardo uses to make changes'
my %dbix = (
    source => {
        strict => {
            allow_command   => 'SELECT INSERT UPDATE DELETE',
            allow_attribute => '',
            allow_regex     => '', ## Must be qr{} if not empty
            deny_regex      => ''
        },
        notstrict => {
            allow_command   => 'SELECT INSERT UPDATE DELETE COMMIT ROLLBACK SET pg_savepoint pg_release pg_rollback_to NOTIFY',
            allow_attribute => 'RaiseError PrintError',
            allow_regex     => [qr{CREATE TEMP TABLE},qr{CREATE(?: UNIQUE)? INDEX}],
            deny_regex      => ''
        },
    },
    target => {
        strict => {
            allow_command   => 'SELECT INSERT UPDATE DELETE',
            allow_attribute => '',
            allow_regex     => '', ## Must be qr{} if not empty
            deny_regex      => ''
        },
        notstrict => {
            allow_command   => 'SELECT INSERT UPDATE DELETE COMMIT ROLLBACK SET pg_savepoint pg_release pg_rollback_to NOTIFY',
            allow_attribute => 'RaiseError PrintError',
            allow_regex     => [qr{CREATE TEMP TABLE}],
            deny_regex      => ''
        },
    }
);

## Grab our full and shortened host name:
my $hostname = hostname;
my $shorthost = $hostname;
$shorthost =~ s/^(.+?)\..*/$1/;

## Items pulled from bucardo_config and shared everywhere:
our %config;
our %config_about;

## Everything else is subroutines

sub new {

    ## Create a new Bucardo object and return it
    ## Takes a hashref of options as the only argument

    my $class = shift;
    my $params = shift || {};

    ## The hash for this object, with default values:
    my $self = {
        created      => scalar localtime,
        ppid         => $$,
        verbose      => 1,
        debugsyslog  => 1,
        debugdir     => './tmp',
        debugfile    => 0,
        warning_file => '',
        debugfilesep => 0,
        debugname    => '',
        cleandebugs  => 0,
        dryrun       => 0,
        sendmail     => 1,
        extraname    => '',
        logprefix    => 'BC!',
        version      => $VERSION,
    };

    ## Add any passed in parameters to our hash:
    for (keys %$params) {
        $self->{$_} = $params->{$_};
    }

    ## Transform our hash into a genuine 'Bucardo' object:
    bless $self, $class;

    ## Remove any previous debugging files if requested
    if ($self->{cleandebugs}) {
        ## If the dir does not exists, silently proceed
        if (opendir my $dh, $self->{debugdir}) {
            for my $file (grep { /^log\.bucardo\./ } readdir $dh) {
                my $f = "$self->{debugdir}/$file";
                unlink "$self->{debugdir}/$file" or warn qq{Could not remove "$f": $!\n};
            }
            closedir $dh or warn qq{Could not closedir "$self->{debugdir}": $!\n};
        }
    }

    ## Zombie stopper
    $SIG{CHLD} = 'IGNORE';

    ## Basically, dryrun does a rollback instead of a commit at the final sync step
    ## This is not 100% safe, if (for example) you have custom code that reaches
    ## outside the database to do things.
    if (exists $ENV{BUCARDO_DRYRUN}) {
        $self->{dryrun} = 1;
    }
    if ($self->{dryrun}) {
        $self->glog("'** DRYRUN - Syncs will not be committed! **\n");
    }

    ## This gets appended to the process description ($0)
    if ($self->{extraname}) {
        $self->{extraname} = " ($self->{extraname})";
    }

    ## Connect to the main Bucardo database
    $self->{masterdbh} = $self->connect_database();

    ## Load in the configuration information
    $self->reload_config_database();

    ## If using syslog, open with the current facility
    if ($self->{debugsyslog}) {
        openlog 'Bucardo', 'pid nowait', $config{syslog_facility};
    }

    ## Figure out if we are writing emails to a file
    $self->{sendmail_file} = $ENV{BUCARDO_EMAIL_DEBUG_FILE} || $config{email_debug_file} || '';

    ## Where to store our PID:
    $self->{pidfile} = "$config{piddir}/bucardo.mcp.pid";

    ## The file to ask all processes to stop:
    $self->{stopfile} = "$config{piddir}/$config{stopfile}";

    ## Send all log lines starting with "Warning" to a separate file
    $self->{warning_file} ||= $config{warning_file};

    ## Make sure we are running where we are supposed to be
    ## This prevents things in bucardo.db from getting run on QA
    ## Or at least makes sure people have to work a little harder
    ## to shoot themselves in the foot.
    if (length $config{host_safety_check}) {
        my $safe = $config{host_safety_check};
        my $osafe = $safe;
        my $ok = 0;
        ## Regular expression
        if ($safe =~ s/^~//) {
            $ok = 1 if $hostname =~ qr{$safe};
        }
        ## Set of choices
        elsif ($safe =~ s/^=//) {
            for my $string (split /,/ => $safe) {
                if ($hostname eq $string) {
                    $ok=1;
                    last;
                }
            }
        }
        ## Simple string
        elsif ($safe ne $hostname) {
            $ok = 1;
        }

        if (! $ok) {
            warn qq{Cannot start: configured to only run on "$osafe". This is "$hostname"\n};
            warn qq{  This is usually done to prevent a configured Bucardo from running\n};
            warn qq{  on the wrong host. Please verify the 'db' settings by doing:\n};
            warn qq{bucardo_ctl list dbs\n};
            warn qq{  Once you are sure the bucardo.db table has the correct values,\n};
            warn qq{  you can adjust the 'host_safety_check' value\n};
            exit 2;
        }
    }

    return $self;

} ## end of new


sub connect_database {

    ## Connect to the given database
    ## First and only argument is the database id
    ## If blank or zero, we return the main database
    ## Returns the string 'inactive' if set as such in the db table
    ## Returns the database handle and the backend PID

    my $self = shift;

    my $id = shift || 0;

    my ($dsn,$dbh,$user,$pass,$ssp);

    ## If id is 0, connect to the main database
    if (!$id) {
        $dsn = "dbi:Pg:dbname=$self->{dbname}";
        defined $self->{dbport} and length $self->{dbport} and $dsn .= ";port=$self->{dbport}";
        defined $self->{dbhost} and length $self->{dbhost} and $dsn .= ";host=$self->{dbhost}";
        defined $self->{dbconn} and length $self->{dbconn} and $dsn .= ";$self->{dbconn}";
        $user = $self->{dbuser};
        $pass = $self->{dbpass};
        $ssp = 1;
    }
    else {
        my $db = $self->get_dbs;
        exists $db->{$id} or die qq{Invalid database id!: $id\n};

        my $d = $db->{$id};
        if ($d->{status} ne 'active') {
            return 0, 'inactive';
        }

        $dsn = "dbi:Pg:dbname=$d->{dbname}";
        defined $d->{dbport} and length $d->{dbport} and $dsn .= ";port=$d->{dbport}";
        defined $d->{dbhost} and length $d->{dbhost} and $dsn .= ";host=$d->{dbhost}";
        length $d->{dbconn} and $dsn .= ";$d->{dbconn}";
        $user = $d->{dbuser};
        $pass = $d->{dbpass} || '';
        $ssp = $d->{server_side_prepares};
    }

    $dbh = DBI->connect
        (
         $dsn,
         $user,
         $pass,
         {AutoCommit=>0, RaiseError=>1, PrintError=>0}
    );

    ## If we are using something like pgbouncer, we need to tell Bucardo not to
    ## use server-side prepared statements, as they will not span commits/rollbacks.
    if (! $ssp) {
        $dbh->{pg_server_prepare} = 0;
        $self->glog('Turning off server-side prepares for this database connection');
    }

    ## Grab the backend PID for this Postgres process
    ## Also a nice check that everything is working properly
    $SQL = 'SELECT pg_backend_pid()';
    my $backend = $dbh->selectall_arrayref($SQL)->[0][0];
    $dbh->rollback();

    if (!$id) {
        ## Prepend bucardo to the search path
        $dbh->do(q{SELECT pg_catalog.set_config('search_path', 'bucardo,' || current_setting('search_path'), false)});
        $dbh->commit();
    }

    return $backend, $dbh;

} ## end of connect_database


sub reload_config_database {

    ## Reload the %config and %config_about hashes from the bucardo_config table
    ## Calls commit on the masterdbh

    my $self = shift;

    undef %config;
    undef %config_about;

    $SQL = 'SELECT setting,value,about,type,name FROM bucardo_config';
    $sth = $self->{masterdbh}->prepare($SQL);
    $sth->execute();
    for my $row (@{$sth->fetchall_arrayref({})}) {
        if (defined $row->{type}) {
            $config{$row->{type}}{$row->{name}}{$row->{setting}} = $row->{value};
            $config_about{$row->{type}}{$row->{name}}{$row->{setting}} = $row->{about};
        }
        else {
            $config{$row->{setting}} = $row->{value};
            $config_about{$row->{setting}} = $row->{about};
        }
    }
    $self->{masterdbh}->commit();

    return;

} ## end of reload_config_database


sub glog { ## no critic (RequireArgUnpacking)

    ## Reformat and log internal messages to the correct place
    ## First argument is the message
    ## Second argument is the log level - defaults to 0 (normal)

    ## Quick shortcut if verbose is 'off' (which is not recommended!)
    return if ! $_[0]->{verbose};

    my $self = shift;
    my $msg = shift;
    chomp $msg;
    my $loglevel = shift || 0;

    ## Return if we have not met the minimum log level
    return if $loglevel > $config{log_level};

    ## We should always have a prefix, either BC!, MCP, CTL, or KID
    my $prefix = $self->{logprefix} || '???';
    $msg = "$prefix $msg";

    ## We may also show other optional things: PID, timestamp, line we came from
    my $header = sprintf '%s%s',
        $config{log_showpid}  ? "($$) " : '',
        1 == $config{log_showtime}       ? ('['.time.'] ')
            : 2 == $config{log_showtime} ? ('['.scalar gmtime(time).'] ')
            : 3 == $config{log_showtime} ? ('['.scalar localtime(time).'] ')
            : '',
        $config{log_showline} ? (sprintf '#%04d ', (caller)[2]) : '';

    ## If using syslog, send the message at the 'info' priority
    $self->{debugsyslog} and syslog 'info', $msg;

    ## Warning messages may also get written to a separate file
    if ($self->{warning_file} and $msg =~ /^Warning|ERROR|FATAL/o) {
        my $file = $self->{warning_file};
        open $self->{warningfilehandle}, '>>', $file or die qq{Could not append to "$file": $!\n};
        print {$self->{warningfilehandle}} "$header $msg\n";
        close $self->{warningfilehandle} or warn qq{Could not close "$file": $!\n};
    }

    ## Possibly send the message to a debug file
    if ($self->{debugfile}) {
        if (!exists $self->{debugfilename}) {
            $self->{debugfilename} = "$self->{debugdir}/log.bucardo";
            if ($self->{debugname}) {
                $self->{debugfilename} .= ".$self->{debugname}";
            }
        }
        ## If we are writing each process to a separate file, append the PID to the file name
        my $file = $self->{debugfilename};
        if ($self->{debugfilesep}) {
            $file = $self->{debugfilename} . ".$prefix.$$";
        }

        ## If this file has not been opened yet, do so
        if (!exists $self->{debugfilehandle}{$$}{$file}) {
            open $self->{debugfilehandle}{$$}{$file}, '>>', $file or die qq{Could not append to "$file": $!\n};
            select((select($self->{debugfilehandle}{$$}{$file}),$|=1)[0]);
        }

        ## Write the message.
        printf {$self->{debugfilehandle}{$$}{$file}} "%s %s\n",
            $header,
            $msg;
    }

    return;

} ## end of glog


sub clog {

    ## Log a message to the conflict log file at config{log_conflict_file}

    my ($self,$msg,@extra) = @_;
    chomp $msg;

    ## Extra args indicates we are using printf style $msg string
    if (@extra) {
        $msg = sprintf $msg, @extra;
    }

    my $cfile = $config{log_conflict_file};
    my $clog;
    if (! open $clog, '>>', $cfile) {
        warn qq{Could not append to file "$cfile": $!};
        return;
    }

    print {$clog} "$msg\n";
    close $clog or warn qq{Could not close "$cfile": $!\n};

    return;

} ## end of clog


sub get_dbs {

    ## Return a hashref of everything in the db table

    ## Used by start_controller(), connect_database()

    my $self = shift;

    $SQL = 'SELECT * FROM bucardo.db';
    $sth = $self->{masterdbh}->prepare($SQL);
    $sth->execute();
    my $info = $sth->fetchall_hashref('name');
    $self->{masterdbh}->commit();

    return $info;

} ## end of get_dbs


sub get_dbgroups {

    ## Return a hashref of dbgroups

    ## Called by validate_sync

    my $self = shift;

    $SQL = q{
        SELECT    d.name, m.db, m.priority
        FROM      bucardo.dbgroup d
        LEFT JOIN dbmap m ON (m.dbgroup=d.name)
        ORDER BY  m.priority ASC, random()
    };
    my $maindbh = $self->{masterdbh};
    $sth = $maindbh->prepare($SQL);
    $sth->execute();
    my $groups;
    for my $x (@{$sth->fetchall_arrayref({})}) {
        if (!exists $groups->{$x->{name}}) {
            $groups->{$x->{name}}{members} = [];
        }
        defined $x->{db} and push @{$groups->{$x->{name}}{members}}, $x->{db};
    }
    $maindbh->commit();

    return $groups;

} ## end of get_dbgroups


sub get_goats {

    ## Return a hashref of everything in the goat table

    ## Used by find_goats()

    my $self = shift;

    $SQL = 'SELECT * FROM bucardo.goat';
    $sth = $self->{masterdbh}->prepare($SQL);
    $sth->execute();
    my $info = $sth->fetchall_hashref('id');
    $self->{masterdbh}->commit();
    return $info;

} ## end of get_goats


sub find_goats {

    ## Given a herd, return an arrayref of goats

    ## Used in validate_sync

    my ($self,$herd) = @_;
    my $goats = $self->get_goats();
    my $maindbh = $self->{masterdbh};
    $SQL = q{
        SELECT   goat
        FROM     bucardo.herdmap
        WHERE    herd = ?
        ORDER BY priority DESC, goat ASC
    };
    $sth = $maindbh->prepare($SQL);
    $sth->execute($herd);
    my $newgoats = [];
    for (@{$sth->fetchall_arrayref()}) {
        push @$newgoats, $goats->{$_->[0]};
    }
    $maindbh->commit();
    return $newgoats;

} ## end of find_goats


sub get_syncs {

    ## Return a hashref of everything in the sync table

    ## Used by reload_mcp()

    my $self = shift;

    $SQL = q{
        SELECT *,
            COALESCE(EXTRACT(epoch FROM checktime),0) AS checksecs,
            COALESCE(EXTRACT(epoch FROM lifetime),0) AS lifetimesecs
        FROM     bucardo.sync
        ORDER BY priority DESC, name DESC
    };
    $sth = $self->{masterdbh}->prepare($SQL);
    $sth->execute();
    my $info = $sth->fetchall_hashref('name');
    $self->{masterdbh}->commit();

    return $info;

} ## end of get_syncs


sub get_reason {

    ## Returns the current string (if any) in the reason file
    ## If given an arg, the reason file is unlinked

    my $delete = shift || 0;

    my $reason = '';
    if (open my $fh, '<', $config{reason_file}) {
        if (<$fh> =~ /\|\s*(.+)/) {
            $reason = $1;
        }
        close $fh or warn qq{Could not close "$config{reason_file}": $!\n};
        $delete and unlink $config{reason_file};
    }

    return $reason;

} ## end of get_reason


sub start_mcp {

    ## Start the Bucardo daemon. Called by bucardo_ctl after setsid()

    my ($self,$arg) = @_;

    ## Store the original invocation line, then modify it
    my $old0 = $0;
    $0 = "Bucardo Master Control Program v$VERSION.$self->{extraname}";

    ## Prefix all lines in the log file with this TLA
    $self->{logprefix} = 'MCP';

    ## If the pid file already exists, cowardly refuse to run
    if (-e $self->{pidfile}) {
        my $extra = '';
        my $fh;
        if (open ($fh, '<', $self->{pidfile})) {
            if (<$fh> =~ /(\d+)/) {
                $extra = " (PID=$1)";
            }
            close $fh or warn qq{Could not close "$self->{pidfile}": $!\n};
        }
        my $msg = qq{File "$self->{pidfile}" already exists$extra: cannot run until it is removed};
        $self->glog($msg);
        warn $msg;
        exit 1;
    }

    ## We'll also refuse if the global stop file exists
    if (-e $self->{stopfile}) {
        my $msg = qq{Cannot run while this file exists: "$self->{stopfile}"};
        $self->glog($msg);
        warn $msg;
        ## Show the first few lines
        if (open my $fh, '<', $self->{stopfile}) {
            while (<$fh>) {
                $msg = "Line $.: $_";
                $self->glog($msg);
                warn $msg;
                last if $. > 10;
            }
            close $fh or warn qq{Could not close "$self->{stopfile}": $!\n};
        }
        exit 1;
    }

    ## Create a new (temporary) pid file
    open my $pid, '>', $self->{pidfile} or die qq{Cannot write to $self->{pidfile}: $!\n};
    my $now = scalar localtime;
    print {$pid} "$$\n$old0\n$now\n";
    close $pid or warn qq{Could not close "$self->{pidfile}": $!\n};

    ## Create a pretty version of the current $self, with the password elided
    my $oldpass = $self->{dbpass};
    $self->{dbpass} = '<not shown>';
    my $dump = Dumper $self;
    $self->{dbpass} = $oldpass;

    ## Prepare to send an email letting people know we have started up
    ## no critic (ProhibitHardTabs)
    my $body = qq{
        Master Control Program $$ was started on $hostname
        Args: $old0
        Version: $VERSION
    };
    ## use critic
    my $subject = qq{Bucardo $VERSION started on $shorthost};

    ## If someone left a message in the reason file, append it, and delete the file
    my $reason = get_reason('delete');
    if ($reason) {
        $body .= "Reason: $reason\n";
        $subject .= " ($reason)";
    }
    $body =~ s/^\s+//gsm;

    $self->send_mail({ body => "$body\n\n$dump", subject => $subject });

    ## Drop the existing database connection, fork, and get a new one
    eval {
        $self->{masterdbh}->disconnect();
    };
    $@ and $self->glog("Warning! Disconnect failed $@");

    my $seeya = fork;
    if (! defined $seeya) {
        die q{Could not fork mcp!};
    }
    if ($seeya) {
        exit 0;
    }

    my $mcp_backend;
    ($mcp_backend, $self->{masterdbh}) = $self->connect_database();

    ## Let any listeners know we have gotten this far
    $self->{masterdbh}->do('NOTIFY bucardo_boot') or die 'NOTIFY bucardo_boot failed!';
    $self->{masterdbh}->commit();

    ## Now that we've forked, overwrite the PID file with our new value
    open $pid, '>', $self->{pidfile} or die qq{Cannot write to $self->{pidfile}: $!\n};
    $now = scalar localtime;
    print {$pid} "$$\n$old0\n$now\n";
    close $pid or warn qq{Could not close "$self->{pidfile}": $!\n};

    ## Start outputting some interesting things to the log
    $self->glog("Starting Bucardo version $VERSION");
    my $systemtime = time;
    $SQL = q{SELECT extract(epoch FROM now()), now(), current_setting('timezone')};
    my $dbtime = $self->{masterdbh}->selectall_arrayref($SQL)->[0];
    $self->glog("Local system epoch: $systemtime  Database epoch: $dbtime->[0]");
    $systemtime = scalar localtime ($systemtime);
    $self->glog("Local system time: $systemtime  Database time: $dbtime->[1]");
    $systemtime = strftime('%Z (%z)', localtime());
    $self->glog("Local system timezone: $systemtime  Database timezone: $dbtime->[2]");
    $self->glog("PID: $$");
    $self->glog("Backend PID: $mcp_backend");
    $self->glog("bucardo_ctl: $old0");
    $self->glog('Bucardo.pm: ' . $INC{'Bucardo.pm'});
    $self->glog("Perl: $^X $^V");
    $self->glog("Log level: $config{log_level}");

    ## Again with the password trick
    $self->{dbpass} = '<not shown>';
    my $objdump = "Bucardo object:\n";
    my $maxlen = 5;
    for (keys %$self) {
        $maxlen = length($_) if length($_) > $maxlen;
    }
    for (sort keys %$self) {
        $objdump .= sprintf " %-*s => %s\n", $maxlen, $_, (defined $self->{$_}) ? qq{'$self->{$_}'} : 'undef';
    }
    $self->glog($objdump);
    $self->{dbpass} = $oldpass;

    ## Dump all configuration variables to the log
    $objdump = "Bucardo config:\n";
    $maxlen = 5;
    for (keys %config) {
        $maxlen = length($_) if length($_) > $maxlen;
    }
    for (sort keys %config) {
        $objdump .= sprintf " %-*s => %s\n", $maxlen, $_, (defined $config{$_}) ? qq{'$config{$_}'} : 'undef';
    }
    $self->glog($objdump);


    ## Clean up old files in the piddir directory
    my $piddir = $config{piddir};
    opendir my $dh, $piddir or die qq{Could not opendir "$piddir": $!\n};
    my @pidfiles = readdir $dh;
    closedir $dh or warn qq{Could not closedir "$piddir" $!\n};
    for my $pidfile (sort @pidfiles) {
        next unless $pidfile =~ /^bucardo.*\.pid$/o;
        next if $pidfile eq 'bucardo.mcp.pid'; ## That's us!
        if (unlink "$piddir/$pidfile") {
            $self->glog("Removed old pid file $piddir/$pidfile");
        }
        else {
            $self->glog("Failed to remove pid file $piddir/$pidfile");
        }
    }

    ## Which syncs to activate? Default is all of them
    if (exists $arg->{sync}) {
        if (! ref $arg->{sync}) {
            $self->{dosyncs}{$arg->{sync}} = 1;
        }
        elsif (ref $arg->{sync} eq 'ARRAY') {
            %{$self->{dosyncs}} = map { $_ => 1} @{$arg->{sync}};
        }
        elsif (ref $arg->{sync} eq 'HASH') { ## Can set to 0 as well
            %{$self->{dosyncs}} = map { $_ => 1 } grep { $arg->{sync}{$_} } keys %{$arg->{sync}};
        }
    }
    if (keys %{$self->{dosyncs}}) {
        $self->glog('Only doing these syncs: ' . join ' ' => sort keys %{$self->{dosyncs}});
        $0 .= ' Requested syncs: ' . join ' ' => sort keys %{$self->{dosyncs}};
    }

    ## Get all syncs, and check if each can be activated

    ## From this point forward, we want to die gracefully
    $SIG{__DIE__} = sub {
        my $msg = shift;
        my $line = (caller)[2];
        $self->glog("Warning: Killed (line $line): $msg");

        my $diebody = "MCP $$ was killed: $msg";
        my $diesubject = "Bucardo MCP $$ was killed";

        my $respawn = (
                       $msg =~  /DBI connect/
                       or $msg =~ /Ping failed/
                       or $msg =~ /Restart Bucardo/
                       ) ? 1 : 0;

        ## Sometimes we don't want to respawn at all (e.g. during some tests)
        if (! $config{mcp_dbproblem_sleep}) {
            $self->glog('Database problem, but will not attempt a respawn due to mcp_dbproblem_sleep=0');
            $respawn = 0;
        }

        ## Most times we do want to respawn
        if ($respawn) {
            $self->glog("Database problem, will respawn after a short sleep: $config{mcp_dbproblem_sleep}");
            $diebody .= " (will attempt respawn in $config{mcp_dbproblem_sleep} seconds)";
            $diesubject .= ' (respawning)';
        }

        ## Callers can prevent an email being sent by setting this before they die
        if (! $self->{clean_exit}) {
            $self->send_mail({ body => $diebody, subject => $diesubject });
        }

        ## Kill children, remove pidfile, update tables, etc.
        $self->cleanup_mcp("Killed: $msg");

        if ($respawn) {
            sleep($config{mcp_dbproblem_sleep});

            ## We assume this is bucardo_ctl, and that we are in same directory as when called
            my $RUNME = $old0;
            ## Check to see if $RUNME is executable as is, before we assume we're in the same directory
            if (! -x $RUNME) {
                $RUNME = "./$RUNME" if index ($RUNME,'.') != 0;
            }
            $RUNME .= q{ start "Attempting automatic respawn after MCP death"};
            $self->glog("Respawn attempt: $RUNME");
            exec $RUNME;
        }

        ## We are not respawning, so we exit
        exit 1;

    }; ## end SIG{__DIE__}

    ## Resets listeners, kills children, loads and activate syncs
    my $active_syncs = $self->reload_mcp();
    $self->glog("Active syncs: $active_syncs");
    if (!$active_syncs) {
        ## Should we allow an option to hang around anyway?
        $self->glog('No active syncs were found, so we are exiting');
        $self->{masterdbh}->do('NOTIFY bucardo_nosyncs');
        $self->{masterdbh}->commit();
        exit 1;
    }

    ## We want to reload everything if someone HUPs us
    $SIG{HUP} = sub {
        $self->reload_mcp();
    };

    ## Let others know we're here
    my $mcpdbh = $self->{masterdbh};
    $mcpdbh->do('NOTIFY bucardo_started')  or warn 'NOTIFY failed';
    $mcpdbh->commit();

    $self->{cdate} = scalar localtime;

    ## Enter ourself into the audit_pid file (if config{audit_pid} is set)
    if ($config{audit_pid}) {
        my $synclist;
        for (sort keys %{$self->{sync}}) {
            $synclist .= "$_:$self->{sync}{$_}{mcp_active} | ";
        }
        if (! defined $synclist) {
            die qq{The sync table appears to be empty!\n};
        }
        $synclist =~ s/\| $//;

        $SQL = q{SELECT nextval('audit_pid_id_seq')};
        $self->{mcpauditid} = $mcpdbh->selectall_arrayref($SQL)->[0][0];
        $SQL = q{INSERT INTO bucardo.audit_pid (type,id,familyid,sync,ppid,pid,birthdate) }.
            qq{VALUES ('MCP',?,?,?,$self->{ppid},$$,?)};
        $sth = $mcpdbh->prepare($SQL);
        $sth->execute($self->{mcpauditid},$self->{mcpauditid},$synclist,$self->{cdate});
    }

    ## Kick all syncs that may have sent a notice while we were down.
    for my $syncname (keys %{$self->{sync}}) {
        my $s = $self->{sync}{$syncname};
        ## Skip inactive syncs
        next unless $s->{mcp_active};
        ## Skip fullcopy syncs
        next if $s->{synctype} eq 'fullcopy';
        ## Skip if ping is false
        next if ! $s->{ping};
        $s->{mcp_kicked} = 1;
    }

    ## Start the main loop
    $self->mcp_main();

    ##
    ## Everything from this point forward in start_mcp is subroutines
    ##

    sub mcp_main {

        my $self = shift;

        $self->glog('Entering main loop');

        my $maindbh = $self->{masterdbh};
        my $sync = $self->{sync};

        ## Used to gather up and handle any notices received via the listen/notify system
        my ($n,@notice);

        ## Used to keep track of the last time we pinged the database
        my $lastpingcheck = 0;

      MCP: {

            ## Bail if the stopfile exists
            if (-e $self->{stopfile}) {
                $self->glog(qq{Found stopfile "$self->{stopfile}": exiting});
                my $msg = 'Found stopfile';

                ## Grab the reason if it exists so we can propogate it onward
                my $mcpreason = get_reason(0);
                if ($mcpreason) {
                    $msg .= ": $mcpreason";
                }
                $self->cleanup_mcp("$msg\n");
                $self->glog('Exiting');
                exit 1;
            }

            ## Every once in a while, make sure our db connections are still there
            if (time() - $lastpingcheck >= $config{mcp_pingtime}) {
                ## This message must have "Ping failed" to match the $respawn above
                $maindbh->ping or die qq{Ping failed for main database!\n};
                ## Check each remote database in undefined order
                for my $db (keys %{$self->{pingdbh}}) {
                    $self->{pingdbh}{$db}->ping
                        or die qq{Ping failed for remote database $db\n};
                }
                $lastpingcheck = time();
            }

            ## Gather up and handle any received notices
            undef @notice;

            ## Grab all notices from the main database
            while ($n = $maindbh->func('pg_notifies')) {
                push @notice, [$n->[0],$n->[1],'main'];
            }

            ## Grab any notices on each remote database
            for my $pdb (keys %{$self->{pingdbh}}) {
                my $pingdbh = $self->{pingdbh}{$pdb};
                while ($n = $pingdbh->func('pg_notifies')) {
                    push @notice, [$n->[0],$n->[1],"database $pdb"];
                }
            }

            ## Handle each notice one by one
            for (@notice) {
                my ($name,$pid,$db) = @$_;
                $self->glog(qq{Got notice "$name" from $pid on $db}, 7);

                ## Request to stop everything
                if ('bucardo_mcp_fullstop' eq $name) {
                    $self->glog("Received full stop notice from PID $pid, leaving");
                    $self->cleanup_mcp("Received stop NOTICE from PID $pid");
                    exit 0;
                }

                ## Request that a named sync get kicked
                elsif ($name =~ /^bucardo_kick_sync_(.+)/o) {
                    my $syncname = $1;
                    my $msg = '';
                    if (! exists $self->{sync}{$syncname}) {
                        $msg = qq{ERROR: Unknown sync to be kicked: "$syncname"\n};
                    }
                    elsif (! $self->{sync}{$syncname}{mcp_active}) {
                        $msg = qq{Cannot kick inactive sync "$syncname"};
                    }
                    else {
                        $sync->{$syncname}{mcp_kicked} = 1;
                    }

                    if ($msg) {
                        $self->glog($msg);
                        ## As we don't want people to wait around for a syncdone...
                        $maindbh->do(qq{NOTIFY "bucardo_syncerror_$syncname"}) or warn 'NOTIFY failed';
                        $maindbh->commit();
                    }
                }

                ## Request to reload the configuration file
                elsif ('bucardo_reload_config' eq $name) {
                    $self->glog('Reloading configuration table');
                    $self->reload_config_database();

                    ## We need to reload ourself as well
                    $self->reload_mcp();

                    ## Let anyone listening know we are done
                    $maindbh->do('NOTIFY bucardo_reload_config_finished') or warn 'NOTIFY failed';
                    $maindbh->commit();
                    $self->glog('Sent notice bucardo_reload_config_finished');
                }

                ## Request to reload the MCP
                elsif ('bucardo_mcp_reload' eq $name) {
                    $self->glog('Reloading MCP');
                    $self->reload_mcp();

                    ## Let anyone listening know we are done
                    $maindbh->do('NOTIFY bucardo_reloaded_mcp') or warn 'NOTIFY failed';
                    $maindbh->commit();
                    $self->glog('Sent notice bucardo_reloaded_mcp');
                }

                ## Request for a ping via listen/notify
                elsif ('bucardo_mcp_ping' eq $name) {
                    $self->glog("Got a ping from PID $pid, issuing pong", 'INFO');
                    $maindbh->do('NOTIFY bucardo_mcp_pong') or warn 'NOTIFY failed';
                    $maindbh->commit();
                }

                ## Request that we parse and empty the log message table
                elsif ('bucardo_log_message' eq $name) {
                    $self->glog('Checking for log messages', 'INFO');
                    $SQL = 'SELECT msg,cdate FROM bucardo_log_message ORDER BY cdate';
                    $sth = $maindbh->prepare_cached($SQL);
                    $count = $sth->execute();
                    if ($count ne '0E0') {
                        for my $row (@{$sth->fetchall_arrayref()}) {
                            $self->glog("MESSAGE ($row->[1]): $row->[0]");
                        }
                        $maindbh->do('TRUNCATE TABLE bucardo_log_message');
                        $maindbh->commit();
                    }
                }

                ## Request that a named sync get reloaded
                elsif ($name =~ /^bucardo_reload_sync_(.+)/o) {
                    my $syncname = $1;
                    if (! exists $sync->{$syncname}) {
                        $self->glog(qq{Invalid sync reload: "$syncname"});
                    }
                    elsif (!$sync->{$syncname}{mcp_active}) {
                        $self->glog(qq{Cannot reload: sync "$syncname" is not active});
                    }
                    else {
                        $self->glog("Deactivating sync $syncname");
                        $self->deactivate_sync($sync->{$syncname});

                        ## Reread from the database
                        $SQL = q{SELECT *, }
                            . q{COALESCE(EXTRACT(epoch FROM checktime),0) AS checksecs, }
                            . q{COALESCE(EXTRACT(epoch FROM lifetime),0) AS lifetimesecs }
                            . q{FROM bucardo.sync WHERE name = ?};
                        $sth = $maindbh->prepare($SQL);
                        $count = $sth->execute($syncname);
                        if ($count eq '0E0') {
                            $sth->finish();
                            $self->glog(qq{Warning! Cannot reload sync "$syncname": no longer in the database!\n});
                            $maindbh->commit();
                            next; ## Handle the next notice
                        }

                        ## TODO: Actually do a full disconnect and redo all the items in here

                        my $info = $sth->fetchall_arrayref({})->[0];
                        $maindbh->commit();

                        ## Only certain things can be changed "on the fly"
                        ## no critic (ProhibitHardTabs)
                        for my $val (qw/checksecs stayalive limitdbs do_listen txnmode deletemethod status ping
                                        analyze_after_copy targetgroup targetdb usecustomselect onetimecopy
                                        lifetimesecs maxkicks rebuild_index/) {
                            $sync->{$syncname}{$val} = $self->{sync}{$syncname}{$val} = $info->{$val};
                        }
                        ## use critic

                        ## TODO: Fix those double assignments

                        ## Empty all of our custom code arrays
                        for my $key (grep { /^code_/ } sort keys %{$self->{sync}{$syncname}}) {
                            $sync->{$syncname}{$key} = $self->{sync}{$syncname}{$key} = [];
                        }

                        sleep 2; ## TODO: Actually wait somehow, perhaps fork

                        $self->glog("Reactivating sync $syncname");
                        $sync->{$syncname}{mcp_active} = 0;
                        if (! $self->activate_sync($sync->{$syncname})) {
                            $self->glog(qq{Warning! Reactivation of sync "$syncname" failed});
                        }
                        else {
                            ## Let anyone listening know the sync is now ready
                            $maindbh->do(qq{NOTIFY "bucardo_reloaded_sync_$syncname"}) or warn 'NOTIFY failed';
                            $self->glog("Sent notice bucardo_reloaded_sync_$syncname");
                        }
                        $maindbh->commit();
                    }
                }

                ## Request that a named sync get activated
                elsif ($name =~ /^bucardo_activate_sync_(.+)/o) {
                    my $syncname = $1;
                    if (! exists $sync->{$syncname}) {
                        $self->glog(qq{Invalid sync activation: "$syncname"});
                    }
                    elsif ($sync->{$syncname}{mcp_active}) {
                        $self->glog(qq{Sync "$syncname" is already activated});
                        $maindbh->do(qq{NOTIFY "bucardo_activated_sync_$syncname"}) or warn 'NOTIFY failed';
                        $maindbh->commit();
                    }
                    else {
                        if ($self->activate_sync($sync->{$syncname})) {
                            $sync->{$syncname}{mcp_active} = 1;
                        }
                    }
                }

                ## Request that a named sync get deactivated
                elsif ($name =~ /^bucardo_deactivate_sync_(.+)/o) {
                    my $syncname = $1;
                    if (! exists $sync->{$syncname}) {
                        $self->glog(qq{Invalid sync "$syncname"});
                    }
                    elsif (! $sync->{$syncname}{mcp_active}) {
                        $self->glog(qq{Sync "$syncname" is already deactivated});
                        $maindbh->do(qq{NOTIFY "bucardo_deactivated_sync_$syncname"}) or warn 'NOTIFY failed';
                        $maindbh->commit();
                    }
                    else {
                        if ($self->deactivate_sync($sync->{$syncname})) {
                            $sync->{$syncname}{mcp_active} = 0;
                        }
                    }
                }

            } ## end each notice

            $maindbh->commit();

            ## Just in case:
            $sync = $self->{sync};

            ## Startup controllers for eligible syncs
          SYNC: for my $syncname (keys %$sync) {

                ## Skip if this sync has not been activated
                next unless $sync->{$syncname}{mcp_active};

                my $s = $sync->{$syncname};

                ## If this is not a stayalive, AND is not being kicked, skip it
                next if ! $s->{stayalive} and ! $s->{mcp_kicked};

                ## If this is a previous stayalive, see if it is active, kick if needed
                if ($s->{stayalive} and $s->{controller}) {
                    $count = kill 0 => $s->{controller};
                    if (! $count) {
                        $self->glog("Could not find controller $s->{controller}, will create a new one. Kicked is $s->{mcp_kicked}");
                        $s->{controller} = 0;
                    }
                    else { ## Presume it is alive and listening to us, restart and kick as needed
                        if ($s->{mcp_kicked}) {
                            ## See if controller needs to be killed, because of time limit or job count limit
                            my $restart_reason = '';
                            if ($s->{maxkicks} > 0 and $s->{ctl_kick_counts} >= $s->{maxkicks}) {
                                $restart_reason = "Total kicks ($s->{ctl_kick_counts}) >= limit ($s->{maxkicks})";
                            }
                            elsif ($s->{lifetimesecs} > 0) {
                                my $thistime = time();
                                my $timediff = $thistime - $s->{start_time};
                                if ($thistime - $s->{start_time} > $s->{lifetimesecs}) {
                                    $restart_reason = "Time is $timediff, limit is $s->{lifetimesecs} ($s->{lifetime})";
                                }
                            }
                            if ($restart_reason) {
                                ## Kill and restart controller
                                $self->glog("Restarting controller for sync $syncname. $restart_reason");
                                kill $signumber{USR1} => $s->{controller};
                                $self->fork_controller($s, $syncname);
                                ## Extra little sleep to ensure the new controller gets the upcoming kick
                                sleep 0.5;
                            }

                            ## Perform the kick
                            my $notify = "bucardo_ctl_kick_$syncname";
                            $maindbh->do(qq{NOTIFY "$notify"}) or die "NOTIFY $notify failed";
                            $maindbh->commit();
                            $self->glog(qq{Sent a kick request to controller $s->{controller} for sync "$syncname"},'INFO');
                            $s->{mcp_kicked} = 0;
                            $s->{ctl_kick_counts}++;
                        }
                        next SYNC;
                    }
                }

                ## At this point, we are either:
                ## 1. Not a stayalive
                ## 2. A stayalive that has not been run yet
                ## 3. A stayalive that has been run but is not responding

                ## Make sure there is nothing out there already running
                my $syncname = $s->{name};
                my $pidfile = "$config{piddir}/bucardo.ctl.sync.$syncname.pid";
                if ($s->{mcp_changed}) {
                    $self->glog(qq{Checking for existing controllers for sync "$syncname"});
                }

                if (-e $pidfile and ! $s->{mcp_problemchild}) {
                    $self->glog("File exists staylive=$s->{stayalive} controller=$s->{controller}");
                    my $pid;
                    if (!open $pid, '<', $pidfile) {
                        $self->glog(qq{ERROR: Could not open file "$pidfile": $!});
                        $s->{mcp_problemchild} = 1;
                        next SYNC;
                    }
                    my $oldpid = <$pid>;
                    chomp $oldpid;
                    close $pid or warn qq{Could not close "$pidfile": $!\n};
                    ## We don't need to know about this every time
                    if ($s->{mcp_changed}) {
                        $self->glog(qq{Found previous controller $oldpid from "$pidfile"});
                    }
                    if ($oldpid !~ /^\d+$/) {
                        $self->glog(qq{ERROR: Invalid pid found inside of file "$pidfile" ($oldpid)});
                        $s->{mcp_changed} = 0;
                        $s->{mcp_problemchild} = 2;
                        next SYNC;
                    }
                    ## Is it still alive?
                    $count = kill 0 => $oldpid;
                    if ($count==1) {
                        if ($s->{mcp_changed}) {
                            $self->glog(qq{Skipping sync "$syncname", seems to be already handled by $oldpid});
                            ## Make sure this kid is still running
                            $count = kill 0 => $oldpid;
                            if (!$count) {
                                $self->glog(qq{Warning! PID $oldpid was not found. Removing PID file});
                                unlink $pidfile or $self->glog("Warning! Failed to unlink $pidfile");
                                $s->{mcp_problemchild} = 3;
                                next SYNC;
                            }
                            $s->{mcp_changed} = 0;
                        }
                        if (! $s->{stayalive}) {
                            $self->glog(qq{Non stayalive sync "$syncname" still active - sending it a notify});
                        }
                        my $notify = "bucardo_ctl_kick_$syncname";
                        $maindbh->do(qq{NOTIFY "$notify"}) or die "NOTIFY $notify failed";
                        $maindbh->commit();
                        $s->{mcp_kicked} = 0;
                        next SYNC;
                    }
                    $self->glog("No active pid $oldpid found. Killing just in case, and removing file");
                    $self->kill_bucardo_pid($oldpid => 'normal');
                    unlink $pidfile or $self->glog("Warning! Failed to unlink $pidfile");
                    $s->{mcp_changed} = 1;
                } ## end if pidfile found for this sync

                ## We may have found an error in the pid file detection the first time through
                $s->{mcp_problemchild} = 0;

                ## Fork off the controller, then clean up the $s hash
                $self->{masterdbh}->commit();
                $self->fork_controller($s, $syncname);
                $s->{mcp_kicked} = 0;
                $s->{mcp_changed} = 1;

            } ## end each sync

            sleep $config{mcp_loop_sleep};
            redo MCP;

        } ## end of MCP loop

        return;

    } ## end of mcp_main

    sub fork_controller {

        my ($self, $s, $syncname) = @_;
        my $controller = fork;
        if (!defined $controller) {
            die qq{ERROR: Fork for controller failed!\n};
        }

        if (!$controller) {
            sleep 0.05;
            $self->{masterdbh}->{InactiveDestroy} = 1;
            $self->{masterdbh} = 0;
            for my $db (values %{$self->{pingdbh}}) {
                $db->{InactiveDestroy} = 1;
            }

            ## No need to keep information about other syncs around
            $self->{sync} = $s;

            $self->start_controller($s);
            exit 0;
        }

        $self->glog(qq{Created controller $controller for sync "$syncname". Kick is $s->{mcp_kicked}});
        $s->{controller} = $controller;

        ## Reset counters for ctl restart via maxkicks and lifetime settings
        $s->{ctl_kick_counts} = 0;
        $s->{start_time} = time();

        return;
    }


    sub reload_mcp {

        ## Reset listeners, kill children, load and activate syncs
        ## Returns how many syncs we activated

        my $self = shift;

        $self->{sync} = $self->get_syncs();

        ## This unlistens any old syncs
        $self->reset_mcp_listeners();

        ## Sleep for a small amount of time to give controllers time to exit gracefully
        sleep 0.5;

        ## Kill any existing children
        opendir my $dh, $config{piddir} or die qq{Could not opendir "$config{piddir}": $!\n};
        my $name;
        while (defined ($name = readdir($dh))) {
            next unless $name =~ /bucardo\.ctl\.sync\.(.+)\.pid/;
            my $syncname = $1; ## no critic (ProhibitCaptureWithoutTest)
            $self->glog(qq{Attempting to kill controller process for "$syncname"});
            next unless open my $fh, '<', "$config{piddir}/$name";
            if (<$fh> !~ /(\d+)/) {
                $self->glog(qq{Warning! File "$config{piddir}/$name" did not contain a PID!\n});
                next;
            }
            my $pid = $1; ## no critic (ProhibitCaptureWithoutTest)
            $self->glog(qq{Asking process $pid to terminate for reload_mcp});
            kill $signumber{USR1} => $pid;
            close $fh or warn qq{Could not close "$config{piddir}/$name": $!\n};
        }
        closedir $dh or warn qq{Warning! Could not closedir "$config{piddir}": $!\n};

        $self->glog('LOADING TABLE sync. Rows=' . (scalar (keys %{$self->{sync}})));

        ## At this point, we are authoritative, so we can safely clean out the q table
        $SQL = q{
          UPDATE bucardo.q
          SET aborted=now(), whydie=?
          WHERE started is NOT NULL
          AND ended IS NULL
          AND aborted IS NULL
        };
        my $maindbh = $self->{masterdbh};
        $sth = $maindbh->prepare($SQL);
        my $cleanmsg = 'MCP removing stale q entry';
        $count = $sth->execute($cleanmsg);
        $maindbh->commit();
        if ($count >= 1) {
            $self->glog("Entries cleaned from the q table: $count");
        }

        ## Load each sync in alphabetical order
        my @activesyncs;
        for (sort keys %{$self->{sync}}) {
            my $s = $self->{sync}{$_};
            my $syncname = $s->{name};

            ## Note that the mcp has changed this sync
            $s->{mcp_changed} = 1;

            ## Reset some boolean flags for this sync
            $s->{mcp_active} = $s->{mcp_kicked} = $s->{controller} = 0;

            ## If this sync is active, don't bother going any further
            if ($s->{status} ne 'active') {
                $self->glog(qq{Skipping sync "$syncname": status is "$s->{status}"});
                next;
            }

            ## If we are doing specific syncs, check the name
            if (keys %{$self->{dosyncs}}) {
                if (! exists $self->{dosyncs}{$syncname}) {
                    $self->glog(qq{Skipping sync "$syncname": not explicitly named});
                    next;
                }
                $self->glog(qq{Activating sync "$syncname": explicitly named});
            }
            else {
                $self->glog(qq{Activating sync "$syncname"});
            }

            ## Activate this sync!
            $s->{mcp_active} = 1;
            if (! $self->activate_sync($s)) {
                $s->{mcp_active} = 0;
            }

            # If it was successfully activated, push it on the queue
            push @activesyncs, $syncname if $s->{mcp_active};

        } ## end each sync

        ## Change our process name, and list all active syncs
        $0 = "Bucardo Master Control Program v$VERSION.$self->{extraname} Active syncs: ";
        $0 .= join ',' => @activesyncs;

        my $count = @activesyncs;

        return $count;

    } ## end of reload_mcp


    sub reset_mcp_listeners {

        ## Unlisten everything, the relisten to specific entries
        ## Called by reload_mcp()

        my $self = shift;

        my $maindbh = $self->{masterdbh};

        $maindbh->do('UNLISTEN *') or warn 'UNLISTEN failed';

        ## Listen for MCP specific items
        for my $l
            (
             'mcp_fullstop',
             'mcp_reload',
             'reload_config',
             'log_message',
             'mcp_ping',
         ) {
            $self->glog(qq{Listening for "bucardo_$l"});
            $maindbh->do("LISTEN bucardo_$l") or die "LISTEN bucardo_$l failed";
        }

        ## Listen for sync specific items
        for my $syncname (keys %{$self->{sync}}) {
            for my $l
                (
                 'activate_sync',
                 'deactivate_sync',
                 'reload_sync',
                 'kick_sync',
             ) {

                ## If the sync is inactive, no sense in listening for anything but activate/reload requests
                if ($self->{sync}{$syncname}{status} ne 'active') {
                    next if $l eq 'deactivate_sync' or $l eq 'kick_sync';
                }
                else {
                    ## If sync is active, no need to listen for an activate request
                    next if $l eq 'activate_sync';
                }

                my $listen = "bucardo_${l}_$syncname";
                $maindbh->do(qq{LISTEN "$listen"}) or die "LISTEN $listen failed";
                $self->glog(qq{Listening for "$listen"});
            }
        }

        $maindbh->commit();

        return;

    } ## end of reset_mcp_listeners


    sub activate_sync {

        ## We've got a new sync to be activated (but not started)
        ## Returns boolean success/failure

        my ($self,$s) = @_;

        my $maindbh = $self->{masterdbh};
        my $syncname = $s->{name};

        ## Connect to each database used by this sync and validate tables
        if (! $self->validate_sync($s)) {
            $self->glog("Validation of sync $s->{name} FAILED");
            $s->{mcp_active} = 0;
            return 0;
        }

        ## If the kids stay alive, the controller must too
        if ($s->{kidsalive} and !$s->{stayalive}) {
            $s->{stayalive} = 1;
            $self->glog('Warning! Setting stayalive to true because kidsalive is true');
        }

        $self->{sync}{$syncname}{mcp_active} = 1;

        ## Let any listeners know we are done
        $maindbh->do(qq{NOTIFY "bucardo_activated_sync_$syncname"}) or warn 'NOTIFY failed';
        ## We don't need to listen for activation requests anymore
        $maindbh->do(qq{UNLISTEN "bucardo_activate_sync_$syncname"});
        ## But we do need to listen for deactivate and kick requests
        $maindbh->do(qq{LISTEN "bucardo_deactivate_sync_$syncname"});
        $maindbh->do(qq{LISTEN "bucardo_kick_sync_$syncname"});
        $maindbh->commit();

        ## Redo our process name to include an updated list of active syncs
        my @activesyncs;
        for my $syncname (sort keys %{$self->{sync}}) {
            next if ! $self->{sync}{$syncname}{mcp_active};
            push @activesyncs, $syncname;
        }

        $0 = "Bucardo Master Control Program v$VERSION.$self->{extraname} Active syncs: ";
        $0 .= join ',' => @activesyncs;

        return 1;

    } ## end of activate_sync


    sub validate_sync {

        ## Check each database a sync needs to use, and (optionally) validate all tables and columns
        ## Returns boolean success/failure

        my ($self,$s) = @_;

        my $syncname = $s->{name};

        $self->glog(qq{Running validate_sync on "$s->{name}"});

        ## Get a list of all dbgroups in case targetgroups is set
        my $dbgroups = $self->get_dbgroups;

        ## Grab the authoritative list of goats from the source herd
        $s->{goatlist} = $self->find_goats($s->{source});

        ## Get the sourcedb from the first goat (should all be the same)
        $s->{sourcedb} = $s->{goatlist}[0]{db};

        ## Connect to the source database and prepare to check tables and columns
        if (! $self->{pingdbh}{$s->{sourcedb}}) {
            my $backend;
            ($backend, $self->{pingdbh}{$s->{sourcedb}}) = $self->connect_database($s->{sourcedb});
            if (defined $backend) {
                $self->glog("Source database backend PID is $backend");
            }
        }
        my $srcdbh = $self->{pingdbh}{$s->{sourcedb}};
         if ($srcdbh eq 'inactive') {
            $self->glog('Source database is inactive, cannot proceed. Consider making the sync inactive instead');
            die 'Source database is not active';
         }

        ## Prepare some SQL statements for immediate and future use
        my %SQL;

        ## Given a schema and table name, return the oid and safely quoted names
        $SQL{checktable} = q{
            SELECT c.oid, quote_ident(n.nspname), quote_ident(c.relname), quote_literal(n.nspname), quote_literal(c.relname)
            FROM   pg_class c, pg_namespace n
            WHERE  c.relnamespace = n.oid
            AND    nspname = ?
            AND    relname = ?
        };
        $sth{checktable} = $srcdbh->prepare($SQL{checktable});

        ## Given a string, return a quoted version (ident, so user -> "user", but foo -> foo)
        $SQL = 'SELECT quote_ident(?)';
        $sth{quoteident} = $srcdbh->prepare($SQL);

        ## Given a table oid, return detailed column information
        $SQL{checkcols} = q{
            SELECT   attname, quote_ident(attname) AS qattname, atttypid, format_type(atttypid, atttypmod) AS ftype,
                     attnotnull, atthasdef, attnum,
                     (SELECT pg_get_expr(adbin, adrelid) FROM pg_attrdef WHERE adrelid=attrelid
                      AND adnum=attnum AND atthasdef) AS def
            FROM     pg_attribute
            WHERE    attrelid = ? AND attnum > 0 AND NOT attisdropped
            ORDER BY attnum
        };
        $sth{checkcols} = $srcdbh->prepare($SQL{checkcols});

        ## TODO: Check constraints as well

        ## Connect to each target database used and start checking things out
        my %targetdbh;
        my $pdbh = $self->{pingdbh};
        if (defined $s->{targetdb}) {
            my $tdb = $s->{targetdb};
            $self->glog(qq{Connecting to target database "$tdb"});
            if (! $pdbh->{$tdb}) {
                my $backend;
                ($backend, $pdbh->{$tdb}) = $self->connect_database($tdb);
                if (defined $backend) {
                    $self->glog("Target database backend PID is $backend");
                }
            }
            ## If the database is marked as inactive, we'll remove it from this syncs list
            if ($pdbh->{$tdb} eq 'inactive') {
                $self->glog(qq{Deleting inactive target database "$tdb"});
                delete $pdbh->{$tdb};
            }
            else {
                $s->{targetdbs}{$tdb}++;
                $targetdbh{$tdb}++;
            }
        }
        elsif (defined $s->{targetgroup}) {
            for my $tdb (@{$dbgroups->{$s->{targetgroup}}{members}}) {
                $self->glog(qq{Connecting to target database "$tdb"});
                if (! $pdbh->{$tdb}) {
                    my $backend;
                    ($backend, $pdbh->{$tdb}) = $self->connect_database($tdb);
                    if (defined $backend) {
                        $self->glog("Target database backend PID is $backend");
                    }
                }
                if ($pdbh->{$tdb} eq 'inactive') {
                    $self->glog(qq{Deleting inactive target database "$tdb"});
                    delete $pdbh->{$tdb};
                }
                else {
                    $targetdbh{$tdb}++;
                    $s->{targetdbs}{$tdb}++;
                }
            }
        }
        else {
            my $msg = qq{ERROR: Could not figure out a target for sync "$syncname"};
            $self->glog($msg);
            warn $msg;
            return 0;
        }

        ## Reset custom code related counters for this sync
        $s->{need_rows} = $s->{need_safe_dbh} = $s->{need_safe_dbh_strict} = 0;

        ## Empty out any existing lists of code types
        for my $key (grep { /^code_/ } sort keys %$s) {
            $s->{$key} = [];
        }

        ## Validate all (active) custom code for this sync
        my $goatlistcodes = join ',' => map { $_->{id} } @{$s->{goatlist}};

        $SQL = qq{
            SELECT c.src_code, c.id, c.whenrun, c.getdbh, c.name, c.getrows, COALESCE(c.about,'?') AS about,
                   c.trigrules, m.active, m.priority, COALESCE(m.goat,0) AS goat
            FROM customcode c, customcode_map m
            WHERE c.id=m.code AND m.active IS TRUE
            AND (m.sync = ? OR m.goat IN ($goatlistcodes))
            ORDER BY priority ASC
        };
        $sth = $self->{masterdbh}->prepare($SQL);
        $sth->execute($syncname);

        for my $c (@{$sth->fetchall_arrayref({})}) {
            $self->glog(qq{  Validating custom code $c->{id} ($c->{whenrun}) (goat=$c->{goat}): $c->{name}});
            my $dummy = q{->{dummy}};
            if ($c->{src_code} !~ /$dummy/) {
                $self->glog(qq{Warning! Code $c->{id} ("$c->{name}") does not contain the string $dummy\n});
                return 0;
            }
            else {
                $self->glog(q{    OK: code contains a dummy string});
            }

            ## Carefully compile the code and see what falls out
            $c->{coderef} = sub { local $SIG{__DIE__} = sub {}; eval $c->{src_code}; }; ## no critic (ProhibitStringyEval)
            &{$c->{coderef}}({ dummy => 1 });
            if ($@) {
                $self->glog(qq{Warning! Custom code $c->{id} for sync "$syncname" did not compile: $@});
                return 0;
            }

            ## If this code is run at the goat level, push it to each goat's list of code
            if ($c->{goat}) {
                my ($goat) = grep { $_->{id}==$c->{goat} } @{$s->{goatlist}};
                push @{$goat->{"code_$c->{whenrun}"}}, $c;
                if ($c->{whenrun} eq 'exception') {
                    $goat->{has_exception_code}++;
                }
            }
            else {
                push @{$s->{"code_$c->{whenrun}"}}, $c;
            }

            ## Some custom code needs row information - the default is 0
            if ($c->{getrows}) {
                $s->{need_rows} = 1;
            }

            ## Some custom code needs database handles - if so, gets one of two types
            if ($c->{getdbh}) {
                if ($c->{whenrun} eq 'before_txn'
                    or $c->{whenrun} eq 'after_txn'
                    or $c->{whenrun} eq 'before_sync'
                    or $c->{whenrun} eq 'after_sync') {
                    $s->{need_safe_dbh} = 1;
                }
                else {
                    $s->{need_safe_dbh_strict} = 1;
                }
            }

        } ## end each custom code

        ## Consolidate some things that are set at both sync and goat levels

        ## The makedelta settings indicates which sides (source/target) get manual delta rows
        ## This is required if other syncs going to other targets need to see the changed data
        ## Note that fullcopy is always 0, and pushdelta can only change target_makedelta
        ## The db is on or off, and the sync then inherits, forces it on, or forces it off
        ## Each goat then does the same: inherits, forces on, forces off

        ## Get information on all databases, unless we are a fullcopy sync
        my $dbinfo;
        if ($s->{synctype} ne 'fullcopy') {
            $dbinfo = $self->get_dbs();
        }

        ## Sometimes we want to enable triggers and rules on bucardo_delta
        $s->{does_source_makedelta_triggers} = $dbinfo->{$s->{sourcedb}}{makedelta_triggers};

        ## The source database can only be changed on a swap sync
        $s->{does_source_makedelta} = 0;
        if ($s->{synctype} eq 'swap') {
            ## This gets enabled if the database has it on, or we override it on at the sync level
            $s->{does_source_makedelta} = 1
                if $dbinfo->{$s->{sourcedb}}{makedelta} eq 'on'
                    or $s->{source_makedelta} eq 'on';
        }

        ## The target database can only be changed by pushdelta and swap syncs
        $s->{does_target_makedelta} = 0;
        if ($s->{synctype} ne 'fullcopy') {
            ## We assume that all target databases are equal
            my $oldval = '';
            for my $name (sort keys %targetdbh) {
                ## Unlike source, this is a hash to allow for differences
                $s->{does_target_makedelta_triggers}{$name} = $dbinfo->{$name}{makedelta_triggers};
                my $md = $dbinfo->{$name}{makedelta};
                $s->{does_target_makedelta} = 1 if $md eq 'on';
                if ($oldval ne $md and $oldval) {
                    $self->glog(qq{Warning! Not all target databases have the same makedelta});
                }
                $oldval = $md;
            }
            ## TODO: See if its worth it to allow some databases but not others to be makedelta
            ## Allow the sync to override the database default
            $s->{does_target_makedelta} = 1 if $s->{target_makedelta} eq 'on';
        }

        ## We want to catch the case where the sync is on but all goats are off
        my $source_makedelta_goats_on = 0;
        my $target_makedelta_goats_on = 0;

        ## Go through each goat in this sync, adjusting items and possibly bubbling up info to sync
        for my $g (@{$s->{goatlist}}) {
            ## None of this applies to non-tables
            next if $g->{reltype} ne 'table';

            ## If we didn't find exception custom code above, set it to 0 for this goat
            $g->{has_exception_code} ||= 0;

            ## If goat.rebuild_index is null, use the sync's value
            if (!defined $g->{rebuild_index}) {
                $g->{rebuild_index} = $s->{rebuild_index};
            }

            ## Fullcopy never does makedelta
            next if $s->{synctype} eq 'fullcopy';

            ## If a swap sync, allow the goat to override the source
            $g->{does_source_makedelta} = $s->{does_source_makedelta};
            if ($s->{synctype} eq 'swap' and $g->{source_makedelta} ne 'inherits') {
                $g->{does_source_makedelta} = $g->{source_makedelta} eq 'on' ? 1: 0;
            }
            $source_makedelta_goats_on++ if $g->{does_source_makedelta};

            ## If not fullcopy, allow the goat to override the target
            $g->{does_target_makedelta} = $s->{does_target_makedelta};
            if ($s->{synctype} ne 'fullcopy' and $g->{target_makedelta} ne 'inherits') {
                $g->{does_target_makedelta} = $g->{target_makedelta} eq 'on' ? 1 : 0;
            }
            $target_makedelta_goats_on++ if $g->{does_target_makedelta};

        } ## end each goat

        ## If the sync is on but all goats were forced off, switch the sync off
        if ($s->{does_source_makedelta} and !$source_makedelta_goats_on) {
            $s->{does_source_makedelta} = 0;
        }
        if ($s->{does_target_makedelta} and !$target_makedelta_goats_on) {
            $s->{does_target_makedelta} = 0;
        }

        ## If at least one goat is on but the sync is off, turn the sync on
        if (!$s->{does_source_makedelta} and $source_makedelta_goats_on) {
            $s->{does_source_makedelta} = 1;
        }
        if (!$s->{does_target_makedelta} and $target_makedelta_goats_on) {
            $s->{does_target_makedelta} = 1;
        }

        ## There are things that a fullcopy sync does not do
        if ($s->{synctype} eq 'fullcopy') {
            $s->{track_rates} = 0;
        }

        ## Go through each table and make sure it exists and matches everywhere
        for my $g (@{$s->{goatlist}}) {
            $self->glog(qq{  Inspecting source $g->{reltype} $g->{schemaname}.$g->{tablename} on database "$s->{sourcedb}"});

            ## Check the source table, save escaped versions of the names
            $sth = $sth{checktable};
            $count = $sth->execute($g->{schemaname},$g->{tablename});
            if ($count != 1) {
                my $msg = qq{Could not find $g->{reltype} $g->{schemaname}.$g->{tablename}\n};
                $self->glog($msg);
                warn $msg;
                return 0;
            }

            ## Store oid and quoted names for this goat
            ($g->{oid},$g->{safeschema},$g->{safetable},$g->{safeschemaliteral},$g->{safetableliteral})
                = @{$sth->fetchall_arrayref()->[0]};

            ## If swap, verify the standard_conflict
            if ($s->{synctype} eq 'swap' and $g->{standard_conflict}) {
                my $sc = $g->{standard_conflict};
                if ($g->{reltype} eq 'table') {
                    die qq{Unknown standard_conflict for $syncname $g->{schemaname}.$g->{tablename}: $sc\n}
                        unless
                        'source' eq $sc or
                        'target' eq $sc or
                        'skip'   eq $sc or
                        'random' eq $sc or
                        'latest' eq $sc or
                        'abort'  eq $sc;
                }
                elsif ($g->{reltype} eq 'sequence') {
                    die qq{Unknown standard_conflict for $syncname $g->{schemaname}.$g->{tablename}: $sc\n}
                        unless
                        'source'  eq $sc or
                        'target'  eq $sc or
                        'skip'    eq $sc or
                        'lowest'  eq $sc or
                        'highest' eq $sc;
                }
                else {
                    die q{Invalid reltype!};
                }
                $self->glog(qq{    Standard conflict method "$sc" chosen});
            } ## end standard conflict

            ## Swap syncs must have some way of resolving conflicts
            if ($s->{synctype} eq 'swap' and !$g->{standard_conflict} and !exists $g->{code_conflict}) {
                $self->glog(qq{Warning! Tables used in swaps must specify a conflict handler. $g->{schemaname}.$g->{tablename} appears to have neither standard or custom handler.});
                return 0;
            }

            my $colinfo;
            if ($g->{reltype} eq 'table') {

                ## Save information about each column in the primary key
                if (!defined $g->{pkey} or !defined $g->{qpkey}) {
                    die "Table $g->{safetable} has no pkey or qpkey - do you need to run validate_goat on it?\n";
                }

                ## Much of this is used later on, for speed of performing the sync
                $g->{pkeyjoined}     = $g->{pkey};
                $g->{qpkeyjoined}    = $g->{qpkey};
                $g->{pkeytypejoined} = $g->{pkeytypejoined};
                $g->{pkey}           = [split /\|/o => $g->{pkey}];
                $g->{qpkey}          = [split /\|/o => $g->{qpkey}];
                $g->{pkeytype}       = [split /\|/o => $g->{pkeytype}];
                $g->{pkcols}         = @{$g->{pkey}};
                $g->{hasbinarypk}    = 0;
                for (@{$g->{pkey}}) {
                    push @{$g->{binarypkey}} => 0;
                }

                ## Turn off the search path, to help the checks below match up
                $srcdbh->do('SET LOCAL search_path = pg_catalog');

                ## Check the source columns, and save them
                $sth = $sth{checkcols};
                $sth->execute($g->{oid});
                $colinfo = $sth->fetchall_hashref('attname');
                ## Allow for 'dead' columns in the attnum ordering
                $x=1;
                for (sort { $colinfo->{$a}{attnum} <=> $colinfo->{$b}{attnum} } keys %$colinfo) {
                    $colinfo->{$_}{realattnum} = $x++;
                }
                $g->{columnhash} = $colinfo;

                ## Build lists of columns
                $x = 1;
                $g->{cols} = [];
                $g->{safecols} = [];
              COL: for my $colname (sort { $colinfo->{$a}{attnum} <=> $colinfo->{$b}{attnum} } keys %$colinfo) {
                    ## Skip if this column is part of the primary key
                    for my $pk (@{$g->{pkey}}) {
                        next COL if $pk eq $colname;
                    }
                    push @{$g->{cols}}, $colname;
                    push @{$g->{safecols}}, $colinfo->{$colname}{qattname};
                    $colinfo->{$colname}{order} = $x++;
                }

                ## Stringified versions of the above lists, for ease later on
                $g->{columnlist} = join ',' => @{$g->{cols}};
                $g->{safecolumnlist} = join ',' => @{$g->{safecols}};

                ## Note which columns are bytea
              BCOL: for my $colname (keys %$colinfo) {
                    my $c = $colinfo->{$colname};
                    next if $c->{atttypid} != 17; ## Yes, it's hardcoded, no sweat
                    $x = 0;
                    for my $pk (@{$g->{pkey}}) {
                        if ($colname eq $pk) {
                            $g->{binarypkey}[$x] = 1;
                            $g->{hasbinarypk} = 1;
                            next BCOL;
                        }
                        $x++;
                    }
                    ## This is used to bind_param these as binary during inserts and updates
                    push @{$g->{binarycols}}, $colinfo->{$colname}{order};
                }

                $srcdbh->do('RESET search_path');

            } ## end if reltype is table

            ## If a sequence, grab all info as a hash
            ## Saves us from worrying about future changes or version specific columns
            if ($g->{reltype} eq 'sequence') {
                $SQL = "SELECT * FROM $g->{safeschema}.$g->{safetable}";
                $sth = $srcdbh->prepare($SQL);
                $sth->execute();
                $g->{sequenceinfo} = $sth->fetchall_arrayref({})->[0];
            }

            ## Customselect may be null, so force to a false value
            $g->{customselect} ||= '';
            my $do_customselect = ($g->{customselect} and $s->{usecustomselect}) ? 1 : 0;
            if ($do_customselect) {
                if ($s->{synctype} ne 'fullcopy') {
                    my $msg = qq{ERROR: Custom select can only be used for fullcopy\n};
                    $self->glog($msg);
                    warn $msg;
                    return 0;
                }
                $self->glog(qq{Transforming custom select query "$g->{customselect}"});
                $sth = $srcdbh->prepare("SELECT * FROM ($g->{customselect}) AS foo LIMIT 0");
                $sth->execute();
                $g->{customselectNAME} = $sth->{NAME};
                $sth->finish();
            }

            ## Verify sequences or tables+columns on remote databases
            ## TODO: Fork to speed this up? (more than one target at a time)
            my $maindbh = $self->{masterdbh};
            for my $db (sort keys %targetdbh) {

                ## Respond to ping here and now for very impatient watchdog programs
                my $notice;
                $maindbh->commit();
                while ($notice = $maindbh->func('pg_notifies')) {
                    my ($name, $pid) = @$notice;
                    if ($name eq 'bucardo_mcp_fullstop') {
                        $self->glog("Received full stop notice from PID $pid, leaving");
                        $self->cleanup_mcp("Received stop NOTICE from PID $pid");
                        exit 0;
                    }
                    if ($name eq 'bucardo_mcp_ping') {
                        $self->glog("Got a ping from PID $pid, issuing pong");
                        $maindbh->do('NOTIFY bucardo_mcp_pong') or warn 'NOTIFY failed';
                        $maindbh->commit();
                    }
                }

                ## Get a handle for the remote database
                my $dbh = $pdbh->{$db};

                ## If a sequence, verify the information and move on
                if ($g->{reltype} eq 'sequence') {
                    $SQL = "SELECT * FROM $g->{safeschema}.$g->{safetable}";
                    $sth = $dbh->prepare($SQL);
                    $sth->execute();
                    $info = $sth->fetchall_arrayref({})->[0];
                    for my $key (sort keys %$info) {
                        next if $key eq 'log_cnt';
                        if (! exists $g->{sequenceinfo}{$key}) {
                            $self->glog(qq{Warning! Sequence on target has item $key, but source does not!});
                            next;
                        }
                        my $sseq = $g->{sequenceinfo}{$key};
                        if ($info->{$key} ne $sseq) {
                            $self->glog("Warning! Sequence mismatch. Source $key=$sseq, target is $info->{$key}");
                            next;
                        }
                    }

                    ## Grab oid of the sequence on the remote database
                    $sth = $dbh->prepare($SQL{checktable});
                    $count = $sth->execute($g->{schemaname},$g->{tablename});
                    if ($count != 1) {
                        my $msg = qq{Could not find remote sequence $g->{schemaname}.$g->{tablename} on $db\n};
                        $self->glog($msg);
                        warn $msg;
                        return 0;
                    }
                    $g->{targetoid}{$db} = $sth->fetchall_arrayref()->[0][0];

                    next;
                }

                ## Grab oid and quoted information about the table on the remote database
                $sth = $dbh->prepare($SQL{checktable});
                $count = $sth->execute($g->{schemaname},$g->{tablename});
                if ($count != 1) {
                    my $msg = qq{Could not find remote table $g->{schemaname}.$g->{tablename} on $db\n};
                    $self->glog($msg);
                    warn $msg;
                    return 0;
                }
                my $oid = $sth->fetchall_arrayref()->[0][0];
                ## Store away our oid, as we may need it later to access bucardo_delta
                $g->{targetoid}{$db} = $oid;

                ## Turn off the search path, to help the checks below match up
                $dbh->do('SET LOCAL search_path = pg_catalog');

                ## Grab column information about this table
                $sth = $dbh->prepare($SQL{checkcols});
                $sth->execute($oid);
                my $targetcolinfo = $sth->fetchall_hashref('attname');

                ## Allow for 'dead' columns in the attnum ordering
                $x=1;
                for (sort { $colinfo->{$a}{attnum} <=> $colinfo->{$b}{attnum} } keys %$targetcolinfo) {
                    $targetcolinfo->{$_}{realattnum} = $x++;
                }

                $dbh->do('RESET search_path');

                my $t = "$g->{schemaname}.$g->{tablename}";

                ## We'll state no problems until we are proved wrong
                my $column_problems = 0;

                ## For customselect, the transformed output must match the slave
                ## Note: extra columns on the target are okay
                if ($do_customselect) {
                    my $msg;
                    my $newcols = [];
                    my $info2;
                    for my $col (@{$g->{customselectNAME}}) {
                        my $ok = 0;
                        if (!exists $targetcolinfo->{$col}) {
                            $msg = qq{ERROR: Custom SELECT returned column "$col" that does not exist on target "$db"\n};
                            $self->glog($msg);
                            warn $msg;
                            return 0;
                        }
                        ## Get a quoted version of this column
                        $sth{quoteident}->execute($col);
                        push @$info2, $sth{quoteident}->fetchall_arrayref()->[0][0];
                    }
                    ## Replace the actual set of columns with our subset
                    my $collist = join ' | ' => @{$g->{cols}};
                    $self->glog("Old columns: $collist");
                    $collist = join ' | ' => @{$g->{customselectNAME}};
                    $self->glog("New columns: $collist");
                    $g->{cols} = $g->{customselectNAME};
                    $g->{safecols} = $info2;

                    ## Replace the column lists
                    $g->{columnlist} = join ',' => @{$g->{customselectNAME}};
                    $g->{safecolumnlist} = join ',' => @$info2;

                } ## end custom select

                ## Check each column in alphabetic order
                for my $colname (sort keys %$colinfo) {

                    ## We've already checked customselect above
                    next if $do_customselect;

                    ## Simple var mapping to make the following code sane
                    my $fcol = $targetcolinfo->{$colname};
                    my $scol = $colinfo->{$colname};

                    $self->glog(qq{    Checking column on target database "$db": "$colname" ($scol->{ftype})});

                    ## Always fatal: column on source but not target
                    if (! exists $targetcolinfo->{$colname}) {
                        $column_problems = 2;
                        my $msg = qq{Source database for sync "$s->{name}" has column "$colname" of table "$t", but target database "$db" does not};
                        $self->glog("FATAL: $msg");
                        warn $msg;
                        next;
                    }

                    ## Almost always fatal: types do not match up
                    if ($scol->{ftype} ne $fcol->{ftype}) {
                        ## Carve out some known exceptions (but still warn about them)
                        ## Allowed: varchar == text
                        if (($scol->{ftype} eq 'character varying' and $fcol->{ftype} eq 'text') or
                            ($fcol->{ftype} eq 'character varying' and $scol->{ftype} eq 'text')) {
                            my $msg = qq{Source database for sync "$s->{name}" has column "$colname" of table "$t" as type "$scol->{ftype}", but target database "$db" has a type of "$fcol->{ftype}". You should really fix that.};
                            $self->glog("Warning: $msg");
                        }
                        else {
                            $column_problems = 2;
                            my $msg = qq{Source database for sync "$s->{name}" has column "$colname" of table "$t" as type "$scol->{ftype}", but target database "$db" has a type of "$fcol->{ftype}"};
                            $self->glog("FATAL: $msg");
                            warn $msg;
                            next;
                        }
                    }

                    ## Fatal in strict mode: NOT NULL mismatch
                    if ($scol->{attnotnull} != $fcol->{attnotnull}) {
                        $column_problems ||= 1; ## Don't want to override a setting of "2"
                        my $msg = sprintf q{Source database for sync "%s" has column "%s" of table "%s" set as %s, but target database "%s" has column set as %s},
                            $s->{name},
                            $colname,
                            $t,
                            $scol->{attnotnull} ? 'NOT NULL' : 'NULL',
                            $db,
                            $scol->{attnotnull} ? 'NULL'     : 'NOT NULL';
                        $self->glog("Warning: $msg");
                        warn $msg;
                    }

                    ## Fatal in strict mode: DEFAULT existence mismatch
                    if ($scol->{atthasdef} != $fcol->{atthasdef}) {
                        $column_problems ||= 1; ## Don't want to override a setting of "2"
                        my $msg = sprintf q{Source database for sync "%s" has column "%s" of table "%s" %s, but target database "%s" %s},
                            $s->{name},
                            $colname,
                            $t,
                            $scol->{atthasdef} ? 'with a DEFAULT value' : 'has no DEFAULT value',
                            $db,
                            $scol->{atthasdef} ? 'has none'             : 'does';
                        $self->glog("Warning: $msg");
                        warn $msg;
                    }

                    ## Fatal in strict mode: DEFAULT exists but does not match
                    if ($scol->{atthasdef} and $fcol->{atthasdef} and $scol->{def} ne $fcol->{def}) {
                        ## Make an exception for Postgres versions returning DEFAULT parenthesized or not
                        ## e.g. as "-5" in 8.2 or as "(-5)" in 8.3
                        my $scol_def = $scol->{def};
                        my $fcol_def = $fcol->{def};
                        for ($scol_def, $fcol_def) {
                            s/\A\(//;
                            s/\)\z//;
                        }
                        my $msg;
                        if ($scol_def eq $fcol_def) {
                            $msg = q{Postgres version mismatch leads to this difference, which is being tolerated: };
                        }
                        else {
                            $column_problems ||= 1; ## Don't want to override a setting of "2"
                            $msg = '';
                        }
                        $msg .= qq{Source database for sync "$s->{name}" has column "$colname" of table "$t" with a DEFAULT of "$scol->{def}", but target database "$db" has a DEFAULT of "$fcol->{def}"};
                        $self->glog("Warning: $msg");
                        warn $msg;
                    }

                    ## Fatal in strict mode: order of columns does not match up
                    if ($scol->{realattnum} != $fcol->{realattnum}) {
                        $column_problems ||= 1; ## Don't want to override a setting of "2"
                        my $msg = qq{Source database for sync "$s->{name}" has column "$colname" of table "$t" at position $scol->{realattnum} ($scol->{attnum}), but target database "$db" has it in position $fcol->{realattnum} ($fcol->{attnum})};
                        $self->glog("Warning: $msg");
                        warn $msg;
                    }

                } ## end each column to be checked

                ## Fatal in strict mode: extra columns on the target side
                for my $colname (sort keys %$targetcolinfo) {
                    next if $do_customselect;
                    next if exists $colinfo->{$colname};
                    $column_problems ||= 1; ## Don't want to override a setting of "2"
                    my $msg = qq{Target database has column "$colname" on table "$t", but source database "$s->{name}" does not};
                    $self->glog("Warning: $msg");
                    warn $msg;
                }

                ## Real serious problems always bail out
                return 0 if $column_problems >= 2;

                ## If other problems, only bail if strict checking is on both sync and goat
                ## This allows us to make a sync strict, but carve out exceptions for goats
                return 0 if $column_problems and $s->{strict_checking} and $g->{strict_checking};

            } ## end each target database

            ## If not a table, we can skip the rest
            next if $g->{reltype} ne 'table';

        } ## end each goat

        ## Listen to the source if pinging
        $srcdbh->commit();
        if ($s->{ping} or $s->{do_listen}) {
            my $l = "bucardo_kick_sync_$syncname";
            $self->glog(qq{Listening on source server "$s->{sourcedb}" for "$l"});
            $srcdbh->do(qq{LISTEN "$l"}) or die "LISTEN $l failed";
            $srcdbh->commit();
        }

        ## Same for the targets, but only if synctype is also "swap"
        for my $db (sort keys %targetdbh) {
            my $dbh = $pdbh->{$db};

            $dbh->commit();
            next if (! $s->{ping} and ! $s->{do_listen}) or $s->{synctype} ne 'swap';
            my $l = "bucardo_kick_sync_$syncname";
            $self->glog(qq{Listening on remote server $db for "$l"});
            $dbh->do(qq{LISTEN "$l"}) or die "LISTEN $l failed";
            $dbh->commit();
        }

        ## Success!
        return 1;

    } ## end of validate_sync


    sub deactivate_sync {

        ## We need to turn off a running sync
        ## Returns boolean success/failure

        my ($self,$s) = @_;

        my $maindbh = $self->{masterdbh};
        my $syncname = $s->{name};

        ## Kill the controller
        my $ctl = $s->{controller};
        if (!$ctl) {
            $self->glog('Warning! Controller not found');
        }
        else {
            $count = kill $signumber{USR1} => $ctl;
            $self->glog("Sent kill USR1 to CTL process $ctl. Result: $count");
        }
        $s->{controller} = 0;

        $self->{sync}{$syncname}{mcp_active} = 0;

        ## Let any listeners know we are done
        $maindbh->do(qq{NOTIFY "bucardo_deactivated_sync_$syncname"}) or warn 'NOTIFY failed';
        ## We don't need to listen for deactivation or kick requests
        $maindbh->do(qq{UNLISTEN "bucardo_deactivate_sync_$syncname"});
        $maindbh->do(qq{UNLISTEN "bucardo_kick_sync_$syncname"});
        ## But we do need to listen for an activation request
        $maindbh->do(qq{LISTEN "bucardo_activate_sync_$syncname"});
        $maindbh->commit();

        ## If we are listening for kicks on the source, stop doing so
        $self->{pingdbh}{$s->{sourcedb}} ||= $self->connect_database($s->{sourcedb});
        my $srcdbh = $self->{pingdbh}{$s->{sourcedb}};
        $srcdbh->commit();
        if ($s->{ping} or $s->{do_listen}) {
            my $l = "bucardo_kick_sync_$syncname";
            $self->glog(qq{Unlistening on source server "$s->{sourcedb}" for "$l"});
            $srcdbh->do(qq{UNLISTEN "$l"}) or warn "UNLISTEN $l failed";
            $srcdbh->commit();
            ## Same for the targets, but only if synctype is also "swap"
            if ($s->{synctype} eq 'swap') {
                my $pdbh = $self->{pingdbh};
                for my $db (sort keys %$pdbh) {
                    my $dbh = $pdbh->{$db};
                    my $lname = "bucardo_kick_sync_$syncname";
                    $self->glog(qq{Unlistening on remote server $db for "$lname"});
                    $dbh->do(qq{UNLISTEN "$lname"}) or warn "UNLISTEN $lname failed";
                    $dbh->commit();
                }
            }
        }

        ## Redo our process name to include an updated list of active syncs
        my @activesyncs;
        for my $syncname (keys %{$self->{sync}}) {
            push @activesyncs, $syncname;
        }

        $0 = "Bucardo Master Control Program v$VERSION.$self->{extraname} Active syncs: ";
        $0 .= join ',' => @activesyncs;

        return 1;

    } ## end of deactivate_sync


    sub cleanup_mcp {

        ## MCP is shutting down
        ## Disconnect from the database
        ## Attempt to kill any controller children
        ## Send a final NOTIFY
        ## Remove our PID file

        my ($self,$exitreason) = @_;

        if (!ref $self) {
            print {*STDERR} "Oops! cleanup_mcp was not called correctly. This is a Bad Thing\n";
            return;
        }

        ## Rollback and disconnect from the master database
        if ($self->{masterdbh}) {
            $self->{masterdbh}->rollback();
            $self->{masterdbh}->disconnect();
        }

        ## Reconnect to the master database for some final cleanups
        my ($finalbackend,$finaldbh) = $self->connect_database();
        $self->glog("Final database backend PID is $finalbackend");

        ## Kill all children controllers belonging to us
        if ($config{audit_pid}) {
            $SQL = q{
                SELECT pid
                FROM   bucardo.audit_pid
                WHERE  parentid = ?
                AND    type = 'CTL'
                AND    killdate IS NULL
            };
            $sth = $finaldbh->prepare($SQL);
            $count = $sth->execute($self->{mcpauditid});

            for (@{$sth->fetchall_arrayref()}) {
                my $kid = $_->[0];
                $self->glog("Found active controller $kid");
                if (kill 0 => $kid) {
                    $count = kill $signumber{USR1} => $kid;
                    $self->glog("Kill results: $count");
                }
                else {
                    $self->glog("Controller $$ not found!");
                }
            }

            ## Update the audit_pid table
            $SQL = q{
                UPDATE bucardo.audit_pid
                SET    killdate = timeofday()::timestamptz, death = ?
                WHERE  type='MCP'
                AND    id = ?
                AND    killdate IS NULL
            };
            $sth = $finaldbh->prepare($SQL);
            $exitreason =~ s/\s+$//;
            $sth->execute($exitreason,$self->{mcpauditid});
            $finaldbh->commit();

        }

        ## Sleep a bit to let the processes clean up their own pid files
        sleep 0.3;

        ## We know we are authoritative for all pid files in the piddir
        ## Use those to kill any open processes that we think are still bucardo related
        my $piddir2 = $config{piddir};
        opendir my $dh, $piddir2 or die qq{Could not opendir "$piddir2" $!\n};
        my @pidfiles2 = readdir $dh;
        closedir $dh or warn qq{Could not closedir "$piddir2": $!\n};
        for my $pidfile (sort @pidfiles2) {
            next unless $pidfile =~ /^bucardo.*\.pid$/o;
            next if $pidfile eq 'bucardo.mcp.pid'; ## That's us!
            my $pfile = "$piddir2/$pidfile";
            if (open my $fh, '<', $pfile) {
                my $pid = <$fh>;
                close $fh or warn qq{Could not close "$pfile": $!\n};
                if ($pid !~ /^\d+$/) {
                    $self->glog("No PID found in file, so removing $pfile");
                    unlink $pfile;
                }
                else {
                    $self->kill_bucardo_pid($pid => 'strong');
                }
            }
            else {
                $self->glog("Could not open file, so removing $pfile\n");
                unlink $pfile;
            }
        }

        my $end_systemtime = scalar localtime;
        my $end_dbtime = $finaldbh->selectall_arrayref('SELECT now()')->[0][0];
        $self->glog(qq{End of cleanup_mcp. Sys time: $end_systemtime. Database time: $end_dbtime});
        $finaldbh->do('NOTIFY bucardo_stopped')  or warn 'NOTIFY failed';
        $finaldbh->commit();
        $finaldbh->disconnect();

        ## Remove our PID file
        if (unlink $self->{pidfile}) {
            $self->glog(qq{Removed pid file "$self->{pidfile}"});
        }
        else {
            $self->glog("Warning! Failed to remove pid file $self->{pidfile}");
        }


        return;

    } ## end of cleanup_mcp

    die 'We should never reach this point!';

} ## end of start_mcp


sub kill_bucardo_pid {

    my ($self,$pid,$nice) = @_;

    $self->glog("Attempting to kill PID $pid");

    ## We want to confirm this is still a Bucardo process
    ## The most portable way at the moment is a plain ps -p
    ## Windows users are on their own

    $pid =~ /^\d+$/ or die;

    my $com = "ps -p $pid";

    my $info = qx{$com};

    if ($info !~ /\b$pid\b/) {
        ## PID has probably gone away, so silently return
        return -1;
    }

    if ($info !~ /bucardo_ctl/o) {
        chomp $info;
        $info =~ s/\n/\\n/g;
        $self->glog("Refusing to kill pid $pid, as it has no bucardo_ctl string (had: $info)");
        return -1;
    }

    $self->glog("Sending signal $signumber{TERM} to pid $pid");
    $count = kill $signumber{TERM} => $pid;

    if ($count >= 1) {
        $self->glog("Successfully signalled pid $pid");
        return 1;
    }

    if ($nice ne 'strict') {
        $self->glog("Failed to signal pid $pid");
        return -2;
    }

    $self->glog("Sending signal $signumber{KILL} to pid $pid");
    $count = kill $signumber{KILL} => $pid;

    if ($count >= 1) {
        $self->glog("Successfully signalled pid $pid");
        return 1;
    }

    $self->glog("Failed to signal pid $pid");
    return -3;

} ## end of kill_bucardo_pid

sub start_controller {

    ## For a particular sync, does all the listening and issuing of jobs

    our ($self,$sync) = @_;

    $self->{logprefix} = 'CTL';

    ## For custom code:
    our $input = {};

    ## Custom code may require a copy of the rows
    our $rows_for_custom_code;

    ## no critic (ProhibitHardTabs)
    my ($syncname, $synctype, $kicked,  $source, $limitdbs) = @$sync{qw(
           name     synctype mcp_kicked  source   limitdbs)};
    my ($sourcedb, $stayalive, $kidsalive, $checksecs, $track_rates) = @$sync{qw(
         sourcedb   stayalive   kidsalive   checksecs   track_rates)};
    ## use critic

    ## Set our process name
    $0 = qq{Bucardo Controller.$self->{extraname} Sync "$syncname" ($synctype) for source "$source"};

    ## Reset some variables of interest
    $self->{syncname}    = $syncname;
    $self->{kidpid}      = {};
    $self->{ccdate}      = scalar localtime;
    $sync->{targetdb}    ||= 0;
    $sync->{targetgroup} ||= 0;

    ## Upgrade any specific sync configs to real configs
    if (exists $config{sync}{$syncname}) {
        while (my ($setting, $value) = each %{$config{sync}{$syncname}}) {
            $config{$setting} = $value;
            $self->glog("Set sync-level config setting $setting: $value");
        }
    }

    ## Store our PID into a file
    my $SYNCPIDFILE = "$config{piddir}/bucardo.ctl.sync.$syncname.pid";
    open my $pid, '>', $SYNCPIDFILE or die qq{Cannot write to $SYNCPIDFILE: $!\n};
    print {$pid} "$$\n";
    close $pid or warn qq{Could not close "$SYNCPIDFILE": $!\n};
    $self->{SYNCPIDFILE} = $SYNCPIDFILE;

    my $msg = qq{Controller starting for sync "$syncname". Source herd is "$source"};
    $self->glog($msg);
    $self->glog("PID: $$");

    ## Log some startup information, and squirrel some away for later emailing
    my $showtarget = sprintf '%s: %s',
        $sync->{targetdb} ? 'database' : 'database group',
        $sync->{targetdb} ||= $sync->{targetgroup};
    my $mailmsg = "$msg\n";
    $msg = qq{  $showtarget synctype: $synctype stayalive: $stayalive checksecs: $checksecs };
    $self->glog($msg);
    $mailmsg .= "$msg\n";

    my $otc = $sync->{onetimecopy} || 0;
    $msg = qq{  limitdbs: $limitdbs kicked: $kicked kidsalive: $kidsalive onetimecopy: $otc};
    $self->glog($msg);
    $mailmsg .= "$msg\n";

    my $lts = $sync->{lifetimesecs};
    my $lti = $sync->{lifetime} || '<NULL>';
    my $mks = $sync->{maxkicks};
    $msg = qq{  lifetimesecs: $lts ($lti) maxkicks: $mks};
    $self->glog($msg);
    $mailmsg .= "$msg\n";

    ## Allow the MCP to signal us in a friendly manner
    $SIG{USR1} = sub {
        die "MCP request\n";
    };

    ## From this point forward, we want to die gracefully
    $SIG{__DIE__} = sub {

        my ($diemsg) = @_;
        my $line = (caller)[2];

        ## Callers can prevent an email being sent by setting this before they die
        if (! $self->{clean_exit} and ($self->{sendmail} or $self->{sendmail_file})) {
            my $warn = $diemsg =~ /MCP request/ ? '' : 'Warning! ';
            $self->glog(qq{${warn}Controller for "$syncname" was killed at line $line: $diemsg});
            for (values %{$self->{dbs}}) {
                $_->{dbpass} = '???' if defined $_->{dbpass};
            }

            my $oldpass = $self->{dbpass};
            $self->{dbpass} = '???';
            ## TODO: Strip out large src_code sections
            my $dump = Dumper $self;
            $self->{dbpass} = $oldpass; ## For our final cleanup connection
            ## no critic (ProhibitHardTabs)
            my $body = qq{
                Controller $$ has been killed at line $line
                Host: $hostname
                Sync name: $syncname
                Stats page: $config{stats_script_url}?host=$sourcedb&sync=$syncname
                Source herd: $source
                Target $showtarget
                Error: $diemsg
                Parent process: $self->{ppid}
                Version: $VERSION
            };
            ## use critic
            $body =~ s/^\s+//gsm;

            ## Give some hints in the subject lines for known types of errors
            my $moresub = '';
            if ($diemsg =~ /Found stopfile/) {
                $moresub = ' (stopfile)';
            }
            elsif ($diemsg =~ /could not serialize access/) {
                $moresub = ' (serialization)';
            }
            elsif ($diemsg =~ /deadlock/) {
                $moresub = ' (deadlock)';
            }
            elsif ($diemsg =~ /could not connect/) {
                $moresub = ' (no connection)';
            }

            my $subject = qq{Bucardo "$syncname" controller killed on $shorthost$moresub};
            if ($subject !~ /stopfile/) {
                $self->send_mail({ body => "$body\n", subject => $subject });
            }
        }

        $self->cleanup_controller($diemsg);

        exit 0;
    };

    ## Connect to the master database
    my $ctl_backend;
    ($ctl_backend, $self->{masterdbh}) = $self->connect_database();
    our $maindbh = $self->{masterdbh};
    $self->glog("Bucardo database backend PID is $ctl_backend");

    ## Listen for kick requests from the MCP
    my $kicklisten = "bucardo_ctl_kick_$syncname";
    $self->glog(qq{Listening for "$kicklisten"});
    $maindbh->do(qq{LISTEN "$kicklisten"}) or die "LISTEN $kicklisten failed";

    ## Listen for a ping request
    $maindbh->do('LISTEN bucardo_ctl_'.$$.'_ping');
    $maindbh->commit();

    ## Add ourself to the audit table
    if ($config{audit_pid}) {
        $SQL = q{INSERT INTO bucardo.audit_pid (type,parentid,familyid,sync,source,ppid,pid,birthdate)}.
            qq{ VALUES ('CTL',?,?,?,?,$self->{ppid},$$,?)};
        $sth = $maindbh->prepare($SQL);
        $sth->execute($self->{mcpauditid},$self->{mcpauditid},$syncname,$source,$self->{ccdate});
        $SQL = q{SELECT currval('audit_pid_id_seq')};
        $self->{ctlauditid} = $maindbh->selectall_arrayref($SQL)->[0][0];
        $maindbh->commit();
    }

    ## Prepare to see how busy this sync is
    $self->{SQL}{qfree} = $SQL = q{
        SELECT targetdb
        FROM   bucardo.q
        WHERE  sync=?
        AND    ended IS NULL
        AND    aborted IS NULL
    };
    $sth{qfree} = $maindbh->prepare($SQL);

    ## Prepare to see how busy everyone is
    $self->{SQL}{qfreeall} = $SQL = q{
        SELECT sourcedb, targetdb
        FROM   bucardo.q
        WHERE  ended IS NULL
        AND    aborted IS NULL
    };
    $sth{qfreeall} = $maindbh->prepare($SQL);

    ## Output goat information to the logs
    for my $m (@{$sync->{goatlist}}) {
        $msg = sprintf q{  Herd member %s: %s.%s%s%s%s},
            $m->{oid},
            $m->{schemaname},
            $m->{tablename},
            $m->{ghost}          ? ' [GHOST]'     : '',
            $m->{has_delta}      ? ' [DELTA]'     : '',
            ($m->{does_source_makedelta} or $m->{does_target_makedelta}) ?
                sprintf (q{ [MAKEDELTA: source=%s target=%s]},
                    $m->{does_source_makedelta}, $m->{does_target_makedelta}
            ) :'';
        $self->glog($msg);
        if (defined $m->{customselect}) {
            $self->glog("   customselect: $m->{customselect}");
        }
        if ($m->{reltype} eq 'table') {
            $self->glog('    Target oids: ' . join ' ' => map { "$_:$m->{targetoid}{$_}" } sort keys %{$m->{targetoid}});
        }
    }

    ## Load database information to get concurrency information
    my $dbinfo = $self->get_dbs();
    my $targetdb = $sync->{targetdbs};
    for (keys %$targetdb) {
        $sync->{targetdbs}{$_} = $dbinfo->{$_};
        if ($dbinfo->{$_}{status} ne 'active') {
            $self->glog("Database $_ is not active, so removing from list (status=$dbinfo->{$_}{status}");
            delete $targetdb->{$_};
        }
    }

    ## Check for concurrency limits on all of our databases
    my %dbinuse;
    my $limitperdb = 0;
    for my $db (keys %$targetdb) {
        $limitperdb += $dbinfo->{$db}{targetlimit};
        $dbinuse{target}{$db} = 0;

        ## Listen for a kid announcing that they are done for each target database
        my $listen = "bucardo_syncdone_${syncname}_$db";
        $maindbh->do(qq{LISTEN "$listen"}) or die "LISTEN $listen failed";
    }

    ## Make sure we are checking the source database as well
    $limitperdb += $dbinfo->{$sourcedb}{sourcelimit};
    $dbinuse{source}{$sourcedb} = 0;

    ## This is how we tell kids to go:
    $SQL = q{INSERT INTO bucardo.q (sync, ppid, sourcedb, targetdb, synctype)}.
        q{ VALUES (?,?,?,?,?) };
    $sth{qinsert} = $maindbh->prepare($SQL);

    ## Checks if there are any matching entries already in the q
    ## We are only responsible for making sure there is one nullable
    $SQL = q{
        SELECT 1
        FROM   bucardo.q
        WHERE  sync=?
        AND    sourcedb=?
        AND    targetdb=?
        AND    started IS NULL
    };
    $sth{qcheck} = $maindbh->prepare($SQL);

    ## Fetches information about a running q entry
    $SQL = q{
        SELECT targetdb, pid, whydie
        FROM   bucardo.q
        WHERE  sync=?
        AND    started IS NOT NULL
        AND    ended IS NULL
        AND    aborted IS NOT NULL
    };
    $sth{qcheckaborted} = $maindbh->prepare($SQL);

    ## Ends an aborted entry in the q for a sync/target combo (by PID)
    $SQL = q{
        UPDATE bucardo.q
        SET    ended = timeofday()::timestamptz
        WHERE  sync=?
        AND    targetdb = ?
        AND    pid = ?
        AND    started IS NOT NULL
        AND    ended IS NULL
        AND    aborted IS NOT NULL
    };
    $sth{qfixaborted} = $maindbh->prepare($SQL);

    ## Ends all aborted entried in the queue for a sync/target combo (not by PID)
    $SQL = q{
        UPDATE bucardo.q
        SET    ended = timeofday()::timestamptz
        WHERE  sync=?
        AND    targetdb = ?
        AND    started IS NOT NULL
        AND    ended IS NULL
        AND    aborted IS NOT NULL
    };
    $sth{qclearaborted} = $maindbh->prepare($SQL);

    ## Aborts a specific entry in the queue, given sync, pid, ppid, and target
    $SQL = q{
        UPDATE bucardo.q
        SET    aborted=timeofday()::timestamptz, whydie=?
        WHERE  sync = ?
        AND    pid = ?
        AND    ppid = ?
        AND    targetdb = ?
        AND    started IS NOT NULL
        AND    ended IS NULL
        AND    aborted IS NULL
    };
    $sth{qupdateabortedpid} = $maindbh->prepare($SQL);

    ## Rather than simply grab the local time, we grab this from the database
    ## and attempt to figure out the last time this sync was started up,
    ## then use that time as our 'lastheardfrom'
    my $lastheardfrom = time();
    my $safesyncname = $maindbh->quote($syncname);
    if ($checksecs) {
        $SQL = "SELECT date(now() - checktime) FROM bucardo.sync WHERE name = $safesyncname";
        my $cdate = $maindbh->selectall_arrayref($SQL)->[0][0];
        ## World of hurt here if constraint_exclusion is not set!

        ## TODO: Rethink this whole section, we don't want to rely on freezer

        $maindbh->do(q{SET constraint_exclusion = 'true'});
        $SQL = qq{
            SELECT ceil(extract(epoch from COALESCE(max(e),now()))) AS seconds
            FROM (
                SELECT max(cdate) AS e FROM freezer.master_q
                WHERE sync = $safesyncname AND cdate >= '$cdate'
                UNION ALL
                SELECT max(cdate) AS e FROM bucardo.q
                WHERE sync = $safesyncname AND cdate >= '$cdate') AS foo
        };
        my $lhf = $maindbh->selectall_arrayref($SQL)->[0][0];
        if ($lhf != $lastheardfrom) {
            $self->glog("Changed lastheardfrom $lastheardfrom to $lhf");
            $lastheardfrom = $lhf;
        }
        $maindbh->commit();
    }

    my $notify;
    my $queueclear = 1;
    my (@q, %q, $activecount);
    my $kidchecktime = 0;
    my $kid_check_abort = 0;

    ## Clean out any lingering q entries caused by something unusual
    ## Kick them off again after marking the row as aborted
    $SQL = qq{
        SELECT cdate, targetdb, ppid, COALESCE(pid,0) AS pid,
               CASE WHEN started IS NULL THEN 0 ELSE 1 END AS was_started,
               CASE WHEN ended   IS NULL THEN 0 ELSE 1 END AS was_ended,
               CASE WHEN aborted IS NULL THEN 0 ELSE 1 END AS was_aborted
        FROM   bucardo.q
        WHERE  sync = $safesyncname
        AND    (started IS NULL OR ended IS NULL)
    };
    $sth{cleanq} = $maindbh->prepare($SQL);
    $count = $sth{cleanq}->execute();
    if ($count eq '0E0') {
        $sth{cleanq}->finish();
    }
    else {
        for my $q (@{$sth{cleanq}->fetchall_arrayref({})}) {
            $self->glog("Cleaning out old q entry. sync=$safesyncname pid=$q->{pid} ppid=$q->{ppid} targetdb=$q->{targetdb} started:$q->{was_started} ended:$q->{was_ended} aborted:$q->{was_aborted} cdate=$q->{cdate}");
            ## Make sure we kick this off again
            if (exists $targetdb->{$q->{targetdb}}) {
                $targetdb->{$q->{targetdb}}{kicked} = 1;
                $kicked = 2;
            }
            else {
                $q->{targetdb} ||= 'NONE';
                $self->glog("Warning! Invalid targetdb found for $safesyncname: $q->{targetdb} pid=$q->{pid} cdate=$q->{cdate}");
                $self->glog("Warning! SQL was $SQL. Count was $count");
            }
        }

        ## Mark all unstarted entries as aborted
        $SQL = qq{
            UPDATE bucardo.q
            SET started=timeofday()::timestamptz, ended=timeofday()::timestamptz, aborted=timeofday()::timestamptz, whydie='Controller cleaning out unstarted q entry'
            WHERE sync = $safesyncname
            AND started IS NULL
        };
        $maindbh->do($SQL);

        ## Clear out any aborted kids (the kids don't end so we can populate targetdb->{kicked} above)
        ## The whydie has already been set by the kid
        $SQL = qq{
            UPDATE bucardo.q
            SET ended=timeofday()::timestamptz
            WHERE sync = $safesyncname
            AND ended IS NULL AND aborted IS NOT NULL
        };
        $maindbh->do($SQL);

        ## Clear out any lingering entries which have not ended
        $SQL = qq{
            UPDATE bucardo.q
            SET ended=timeofday()::timestamp, aborted=timeofday()::timestamp, whydie='Controller cleaning out unended q entry'
            WHERE sync = $safesyncname
            AND ended IS NULL
        };
        $maindbh->do($SQL);

        $maindbh->commit();

    } ## end found lingering q entries

    ## If running an after_sync customcode, we need a timestamp
    if (exists $sync->{code_after_sync}) {
        $SQL = 'SELECT now()';
        $sync->{starttime} = $maindbh->selectall_arrayref($SQL)->[0][0];
        $maindbh->rollback();
    }

    ## If these are perpetual children, kick them off right away
    ## Also handle "onetimecopy" here as well
    if ($kidsalive or $otc) {
        for my $dbname (sort keys %$targetdb) {
            my $kid = $targetdb->{$dbname};
            if ($kid->{pid}) { ## Can this happen?
                my $pid = $kid->{pid};
                $count = kill 0 => $pid;
                if ($count) {
                    $self->glog(qq{A kid is already handling database "$dbname": not starting});
                    next;
                }
            }
            $kid->{dbname} = $dbname;
            $self->{kidcheckq} = 1;
            if ($otc and $sync->{synctype} eq 'pushdelta') {
                $sth{qinsert}->execute($syncname,$self->{ppid},$sourcedb,$dbname,'fullcopy');
                $maindbh->commit();
                $sync->{synctype} = 'fullcopy';
                $sync->{kidsalive} = 0;
                $sync->{track_rates} = 0;
                $sync->{onetimecopy_savepid} = 1;
            }
            $self->create_newkid($sync,$kid);
        }
    }

    my $lastpingcheck = 0;

    ## A kid will control a specific sync for a specific targetdb
    ## We tell all targetdbs for this sync by setting $kicked to 1
    ## For individual ones only, we set $targetdb->{$dbname}{kicked} to true
    ## and $kicked to 2

  CONTROLLER: {

        ## Bail if the stopfile exists
        if (-e $self->{stopfile}) {
            $self->glog(qq{Found stopfile "$self->{stopfile}": exiting});
            my $stopmsg = 'Found stopfile';

            ## Grab the reason if it exists so we can propogate it onward
            my $ctlreason = get_reason(0);
            if ($ctlreason) {
                $stopmsg .= ": $ctlreason";
            }
            die "$stopmsg\n";
        }

        ## Every once in a while, make sure we can still talk to the database
        if (time() - $lastpingcheck >= $config{ctl_pingtime}) {
            ## If this fails, simply have the MCP restart it
            $maindbh->ping or die qq{Ping failed for main database!\n};
            $lastpingcheck = time();
        }

        ## See if we got any notices - unless we've already been kicked
        if (!$kicked) {

            my ($n,@notice);
            while ($n = $maindbh->func('pg_notifies')) {
                push @notice, [$n->[0],$n->[1]];
            }
            $maindbh->commit();
            for (@notice) {
                my ($name, $pid) = @$_;
                my $nmsg = sprintf q{Got notice "%s" from %s%s},
                    $name,
                    $pid,
                    exists $self->{kidpid}{$pid-1} ? (' (kid on database '.$self->{kidpid}{$pid-1}{dbname} .')') : '';
                $self->glog($nmsg, 7);
                ## Kick request from the MCP?
                if ($name eq $kicklisten) {
                    $kicked = 1;
                    ## TODO: Reset the abort count for all targets?
                }

                ## Request for a ping via listen/notify
                elsif ($name eq 'bucardo_ctl_'.$$.'_ping') {
                    $self->glog('Got a ping, issuing pong');
                    $maindbh->do('NOTIFY bucardo_ctl_'.$$.'_pong') or warn 'NOTIFY failed';
                    $maindbh->commit();
                }

                ## A kid has just finished doing a sync
                elsif ($name =~ /^bucardo_syncdone_${syncname}_(.+)$/o) {
                    my $dbname = $1;
                    ## If they are all finished, possibly exit
                    $targetdb->{$dbname}{finished} = 1;
                    ## Reset the abort count for this database
                    $self->{aborted}{$dbname} = 0;
                    ## If everyone is finished, tell the MCP (overlaps?)
                    if (! grep { ! $_->{finished} } values %$targetdb) {
                        my $notifymsg = "bucardo_syncdone_$syncname";
                        $maindbh->do(qq{NOTIFY "$notifymsg"}) or die "NOTIFY $notifymsg failed";
                        $self->glog(qq{Sent notice "bucardo_syncdone_$syncname"}, 6);
                        $maindbh->commit();

                        ## Reset the one-time-copy flag, so we only do it one time!
                        if ($otc) {
                            $otc = 0;
                            $SQL = 'UPDATE sync SET onetimecopy = 0 WHERE name = ?';
                            $sth = $maindbh->prepare($SQL);
                            $sth->execute($syncname);
                            $maindbh->commit();
                            $sync->{onetimecopy_savepid} = 0;
                            ## Reset to the original values, in case we changed them
                            $sync->{synctype} = $synctype;
                            $sync->{kidsalive} = $kidsalive;
                            $sync->{track_rates} = $track_rates;
                        }

                        ## Run all after_sync custom codes
                        for my $code (@{$sync->{code_after_sync}}) {
                            ## Do we need row information?
                            if ($code->{getrows} and ! exists $rows_for_custom_code->{source}) {
                                ## Connect to the source database
                                my $srcdbh = $self->connect_database($sourcedb);
                                ## Create a list of all targets
                                my $targetlist = join ',' => map { s/\'/''/g; qq{'$_'} } keys %$targetdb;
                                my $numtargets = keys %$targetdb;
                                for my $g (@{$sync->{goatlist}}) {

                                    next unless $g->{has_delta};

                                    next if $g->{reltype} ne 'table';

                                    ## TODO: Do a deltacount for fullcopy?

                                    ### TODO: Refactor this
                                    my ($S,$T) = ($g->{safeschema},$g->{safetable});

                                    my $drow = q{d.rowid AS "BUCARDO_ID"};
                                    $x=1;
                                    for my $qpk (@{$g->{qpkey}}) {
                                        $SQL .= sprintf ' %s %s = ?',
                                            $x>1 ? 'AND' : 'WHERE',
                                                $g->{binarypkey}[$x-1] ? qq{ENCODE($qpk,'base64')} : $qpk;
                                        $x > 1 and $drow .= qq{,d.rowid$x AS "BUCARDO_ID$x"};
                                        $x++;
                                    }

                                    my $aliaslist = join ',' => @{$g->{safecols}};
                                    if (length $aliaslist) {
                                        $aliaslist = ",$aliaslist";
                                    }

                                    $SQL{trix} = qq{
                                      SELECT    DISTINCT $drow,
                                                BUCARDO_PK, $aliaslist
                                      FROM      bucardo.bucardo_delta d
                                      LEFT JOIN $S.$T t ON BUCARDO_JOIN
                                      WHERE     d.tablename = \$1::oid
                                      AND       d.txntime IN (
                                        SELECT txntime FROM bucardo.bucardo_track
                                        WHERE tablename = \$1::oid 
                                        AND txntime >= '$sync->{starttime}'
                                        AND targetdb IN (TARGETLIST)
                                        GROUP BY 1
                                        HAVING COUNT(targetdb) = $numtargets
                                      )
                                    };
                                    $SQL{trix} =~ s/^ {38}//g;
                                    ($SQL = $SQL{trix}) =~ s/\$1/$g->{oid}/go;
                                    $SQL =~ s/TARGETLIST/$targetlist/;

                                    my $clause = '';
                                    my $cols = '';
                                    $x = 0;
                                    for my $qpk (@{$g->{qpkey}}) {
                                        $clause .= sprintf q{%s::%s = d.rowid%s::%s AND },
                                        $g->{binarypkey}[$x] ? qq{ENCODE(t.$qpk,'base64')} : "t.$qpk",
# 8.2 can't cast ENCODE()'s TEXT return value to BYTEA; leaving at TEXT appears to work
                                        $g->{binarypkey}[$x] ? 'text' : $g->{pkeytype}[$x],
                                        $x ? $x+1 : '',
                                        $g->{binarypkey}[$x] ? 'text' : $g->{pkeytype}[$x];
                                        $cols ||= $g->{binarypkey}[0] ? qq{ENCODE(t.$qpk,'base64'),} : "t.$qpk";
                                        $x++;
                                    }
                                    $clause =~ s/ AND $//;
                                    $SQL =~ s/BUCARDO_JOIN/($clause)/;
                                    $SQL =~ s/BUCARDO_PK/$cols/;
                                    $sth = $srcdbh->prepare($SQL);
                                    $sth->execute();

                                    ## TODO: Fix for multi-col
                                    $rows_for_custom_code->{source}{$S}{$T} = $sth->fetchall_hashref('BUCARDO_ID');

                                    if ($synctype eq 'swap') {
                                        ## TODO: Separate getrows into swap and targets in case we don't need both?
                                        (my $safesourcedb = $sourcedb) =~ s/\'/''/go;
                                        ($SQL = $SQL{delta}) =~ s/\$1/$g->{targetoid}{$sourcedb}/g;
                                        $SQL =~ s/TARGETLIST/'$safesourcedb'/;
                                        (my $targetname) = keys %$targetdb;
                                        my $tgtdbh = $self->connect_database($targetname);
                                        $sth = $tgtdbh->prepare($SQL);
                                        $sth->execute();
                                        $rows_for_custom_code->{source}{$S}{$T} = $sth->fetchall_hashref('BUCARDO_ID');
                                    }
                                    $srcdbh->rollback;
                                } ## end each goat

                                $srcdbh->disconnect();

                            } ## end populate rowinfo

                            my $result = run_ctl_custom_code($code, 'nostrict');
                            $self->glog("End of after_sync $code->{id}");

                        } ## end each custom code

                        ## If we are not a stayalive, this is a good time to leave
                        if (! $stayalive and ! $kidsalive) {
                            $self->cleanup_controller('Children are done');
                            exit 0;
                        }

                        ## If we ran an after_sync and grabbed rows, reset some things
                        if (exists $rows_for_custom_code->{source}) {
                            $rows_for_custom_code = {};
                            $SQL = 'SELECT timeofday()::timestamp';
                            $sync->{starttime} = $maindbh->selectall_arrayref($SQL)->[0][0];
                        }

                        ## Reset the finished marker on all kids
                        for my $d (keys %$targetdb) {
                            $targetdb->{$d}{finished} = 0;
                        }

                    } ## end all kids finished

                } ## end kid finished notice

            } ## end each notice

            ## Has it been long enough to force a sync?
            if ($checksecs and time() - $lastheardfrom >= $checksecs) {
                $self->glog(qq{Timed out - force a sync for "$syncname"}, 6);
                $lastheardfrom = time();
                $kicked = 1;
            }

            ## Clean up any aborted children and create new jobs for them as needed
            if (time() - $kid_check_abort >= $config{ctl_checkabortedkids_time}) {
                $sth{qcheckaborted}->execute($syncname);
                for (@{$sth{qcheckaborted}->fetchall_arrayref()}) {
                    my ($atarget,$apid,$whydie) = @$_;
                    $sth{qfixaborted}->execute($syncname,$atarget,$apid);
                    my $seenit = ++$self->{aborted}{$atarget};
                    if ($seenit >= $config{kid_abort_limit}) {
                        if ($seenit == $config{kid_abort_limit}) {
                            $self->glog("Too many kids have been killed for $atarget ($seenit).".
                                        'Will not create this until a kick.');
                        }
                        next;
                    }
                    $self->glog(qq{Cleaning up aborted sync from q table for "$atarget". PID was $apid});
                    ## Recreate this entry, unless it is already there
                    $count = $sth{qcheck}->execute($syncname,$sourcedb,$atarget);
                    $sth{qcheck}->finish();
                    if ($count >= 1) {
                        $self->glog('Already an empty slot, so not re-adding');
                    }
                    else {
                        $self->glog(qq{Re-adding sync to q table for database "$atarget"});
                        $count = $sth{qinsert}->execute($syncname,$self->{ppid},$sourcedb,$atarget,$synctype);
                        $maindbh->commit();
                        sleep $config{kid_abort_sleep};
                        $self->glog('Creating kid to handle resurrected q row');
                        my $kid = $targetdb->{$atarget};
                        $kid->{dbname} = $atarget;
                        $self->{kidcheckq} = 1;
                        $self->create_newkid($sync,$kid);
                    }
                }
                $kid_check_abort = time();

            } ## end of aborted children

        } ## end !checked

        ## Check that our children are alive and healthy
        if (time() - $kidchecktime >= $config{ctl_checkonkids_time}) {
            for my $dbname (sort keys %$targetdb) {
                my $kid = $targetdb->{$dbname};
                next if ! $kid->{pid};
                my $pid = $kid->{pid};
                $count = kill 0 => $pid;
                if ($count != 1) {
                    ## Make sure this kid has cleaned up after themselves in the q table
                    $count = $sth{qupdateabortedpid}->execute('?',$syncname,$pid,$self->{ppid},$dbname);
                    if ($count >= 1) {
                        $self->glog("Rows updated child $pid to aborted in q: $count");
                    }
                    ## If they are finished, and kidsalive is false, then all is good.
                    $kid->{pid} = 0; ## No need to check it again

                    ## Also make a special exception for one-time-copy kids
                    if ($kid->{onetimecopy} or ($kid->{finished} and !$kidsalive)) {
                        $self->glog(qq{Kid $pid has died a natural death. Removing from list});
                        next;
                    }
                    $self->glog(qq{Warning! Kid $pid seems to have died. Sync "$syncname"});
                }
            } ## end each database / kid

            $kidchecktime = time();

        } ## end of time to check on our kids

        $maindbh->commit(); ## TODO: Possibly reposition this

        ## Redo if we are not kicking but are stayalive and the queue is clear
        if (! $kicked and $stayalive and $queueclear) {
            sleep $config{ctl_nothingfound_sleep};
            redo CONTROLLER;
        }

        ## If a custom code handler needs a database handle, create one
        our ($cc_sourcedbh,$safe_sourcedbh);

        ## Run all before_sync code
        for my $code (@{$sync->{code_before_sync}}) {
            my $result = run_ctl_custom_code($code, 'nostrict');
            if ($result eq 'redo') {
                redo CONTROLLER;
            }
        }


        sub run_ctl_custom_code {

            my $c = shift;
            my $strictness = shift || '';
            my $attempts = shift || 0;

            $self->glog("Running $c->{whenrun} controller custom code $c->{id}: $c->{name}");

            if (!defined $safe_sourcedbh) {
                $cc_sourcedbh = $self->connect_database($sync->{sourcedb});
                my $darg;
                for my $arg (sort keys %{$dbix{source}{notstrict}}) {
                    next if ! length $dbix{source}{notstrict}{$arg};
                    $darg->{$arg} = $dbix{source}{notstrict}{$arg};
                }
                $darg->{dbh} = $cc_sourcedbh;
                $safe_sourcedbh = DBIx::Safe->new($darg);
            }

            $input = {
                   sourcedbh  => $safe_sourcedbh,
                   synctype   => $sync->{synctype},
                   syncname   => $sync->{name},
                   goatlist   => $sync->{goatlist},
                   sourcename => $sync->{sourcedb},
                   targetname => '',
                   message    => '',
                   warning    => '',
                   error      => '',
                   nextcode   => '',
                   endsync    => '',
                   runagain   => 0, ## exception only
                   attempts   => $attempts, ## exception only
            };
            if ($c->{getrows}) {
                $input->{rows} = $rows_for_custom_code;
            }

            $maindbh->{InactiveDestroy} = 1;
            $cc_sourcedbh->{InactiveDestroy} = 1;
            &{$c->{coderef}}($input);
            $maindbh->{InactiveDestroy} = 0;
            $cc_sourcedbh->{InactiveDestroy} = 0;
            $self->glog("Finished custom code $c->{id}");
            if (length $input->{message}) {
                $self->glog("Message from $c->{whenrun} code $c->{id}: $input->{message}");
            }
            if (length $input->{warning}) {
                $self->glog("Warning! Code $c->{whenrun} $c->{id}: $input->{warning}");
            }
            if (length $input->{error}) {
                $self->glog("Warning! Code $c->{whenrun} $c->{id}: $input->{error}");
                die "Code $c->{whenrun} $c->{id} error: $input->{error}";
            }
            if (length $input->{nextcode}) { ## Mostly for conflict handlers
                return 'next';
            }
            if (length $input->{endsync}) {
                $self->glog("Code $c->{whenrun} requests a cancellation of the rest of the sync");
                ## before_txn and after_txn only should commit themselves
                $cc_sourcedbh->rollback();
                $maindbh->commit();
                sleep $config{endsync_sleep};
                return 'redo';
            }
            return 'normal';

        } ## end of run_ctl_custom_code

        ## Add kids to the queue if kicking
        if ($kicked) {
            ## TODO: For now, redo all targets
            $kicked = 1;
            for my $kid (keys %$targetdb) {
                if (1 == $kicked or $targetdb->{$kid}{kicked}) {
                    push @q, $kid;
                }
                $targetdb->{$kid}{kicked} = 0;
            }
            $kicked = 0;
        }

        ## If we are limiting, see who is currently busy
        $activecount=0;
        if ($limitdbs) { ## How busy is this sync?
            $activecount = $sth{qfree}->execute($syncname);
            $activecount = 0 if $activecount < 1;
            $sth{qfree}->finish;
        }
        if ($limitperdb) { ## How busy is each database?
            undef %dbinuse;
            for my $db (keys %$targetdb) {
                $dbinuse{target}{$db} = 0;
            }
            $dbinuse{source}{$sourcedb} = 0;

            $sth{qfreeall}->execute;
            for (@{$sth{qfreeall}->fetchall_arrayref()}) {
                $dbinuse{source}{$_->[0]}++;
                $dbinuse{target}{$_->[1]}++;
            }
        }

        ## Loop through the queue and see who we can add
        $queueclear = 1;
        undef %q;
        my $offset=0;

      Q: for my $dbname (@q) {
            next if $q{$dbname}++;
            my $kid = $targetdb->{$dbname};

            ## Can we add this one?
            my $ok2add = 0;
            if (! $limitdbs and ! $limitperdb) {
                $ok2add = 1;
            }
            ## Got any more slots for this sync?
            elsif ($limitdbs and $activecount >= $limitdbs) {
                $self->glog("No room in queue for $dbname ($syncname). Limit: $limitdbs. Used: $activecount Offset:$offset");
                shift @q for (1..$offset);
                ## Create a new queue!
                $queueclear = 0;
                last Q;
            }
            ## Got any more slots for this target db?
            elsif ($limitperdb and $dbinfo->{$dbname}{targetlimit} and $dbinuse{target}{$dbname} >= $dbinfo->{$dbname}{targetlimit}) {
                $self->glog(qq{No room in queue for target db "$dbname" ($syncname) Limit: $dbinfo->{$dbname}{targetlimit} Used: $dbinuse{target}{$dbname}});
                shift @q for (1..$offset);
                $queueclear = 0;
                next Q;
            }
            ## Got any more slots for this source db?
            elsif ($limitperdb and $dbinfo->{$sourcedb}{sourcelimit} and $dbinuse{source}{$sourcedb} >= $dbinfo->{$sourcedb}{sourcelimit}) {
                $self->glog(qq{No room in queue for source db "$dbname" ($syncname) Limit: $dbinfo->{$sourcedb}{sourcelimit} Used: $dbinuse{source}{$sourcedb}});
                shift @q for (1..$offset);
                $queueclear = 0;
                next Q;
            }
            else {
                $ok2add = 1;
                $activecount++;
                $self->glog(qq{Added "$dbname" to queue for sync $syncname, because we had free slots});
                $offset++;
            }

            if ($ok2add) {

                ## Free slots?
                $count = $sth{qcheck}->execute($syncname,$sourcedb,$dbname);
                $sth{qcheck}->finish();
                if ($count < 1) {
                    $count = $sth{qinsert}->execute($syncname,$self->{ppid},$sourcedb,$dbname,$synctype);
                }
                else {
                    $self->glog("Could not add to q sync=$syncname,source=$sourcedb,target=$dbname,count=$count. Sending manual notification", 7);
                }
                my $notifymsg = "bucardo_q_${syncname}_$dbname";
                $maindbh->do(qq{NOTIFY "$notifymsg"}) or die "NOTIFY $notifymsg failed";
                $maindbh->commit();

                ## Check if there is a kid alive for this database: spawn if needed
                if (! $kid->{pid} or ! (kill 0 => $kid->{pid})) {
                    $kid->{dbname} = $dbname;
                    $self->glog('Creating a kid');
                    $self->{kidcheckq} = 1; ## Since this kid will not get the above notice
                    $self->create_newkid($sync,$kid);
                }
            }
        } ## end each Q

        if ($queueclear) {
            ## We made it!
            undef @q;
        }

        sleep $config{ctl_nothingfound_sleep};
        redo CONTROLLER;

    } ## end CONTROLLER


    sub create_newkid {

        ## Fork and create a KID process

        my ($self,$kidsync,$kid) = @_;
        $self->{parent} = $$;

        ## Clear out any aborted kid entries, so the controller does not resurrect them.
        ## It's fairly sane to do this here, as we can assume a kid will be immediately created,
        ## and that kid will create a new aborted entry if it fails.
        ## We want to do it pre-fork, so we don't clear out a kid that aborts quickly.

        $sth{qclearaborted}->execute($self->{syncname},$kid->{dbname});
        $self->{masterdbh}->commit();

        my $newkid = fork;
        if (! defined $newkid) {
            die q{Fork failed for new kid in start_controller};
        }
        if (!$newkid) {
            sleep 0.05;
            $self->{masterdbh}->{InactiveDestroy} = 1;
            $self->{life} = ++$kid->{life};
            $self->start_kid($kidsync,$kid->{dbname});
            ## Should never return, but just in case:
            $self->{clean_exit} = 1;
            exit 0;
        }

        $self->glog(qq{Created new kid $newkid for sync "$self->{syncname}" to database "$kid->{dbname}"});
        $kid->{pid} = $newkid;
        $self->{kidpid}{$newkid} = $kid;
        $kid->{cdate} = time;
        $kid->{life}++;
        $kid->{finished} = 0;
        if ($kidsync->{onetimecopy_savepid}) {
            $kid->{onetimecopy} = 1;
        }
        sleep $config{ctl_createkid_time};
        return;

    } ## end of create_newkid

    die 'How did we reach outside of the main controller loop?';

} ## end of start_controller


sub cleanup_controller {

    ## Controller is shutting down
    ## Disconnect from the database
    ## Attempt to kill any 'kid' children
    ## Remove our PID file

    my ($self,$reason) = @_;

    if (exists $self->{cleanexit}) {
        $reason = 'Normal exit';
    }

    ## Disconnect from the database
    if ($self->{masterdbh}) {
        $self->{masterdbh}->rollback();
        $self->{masterdbh}->disconnect();
    }

    ## Kill all Bucardo children mentioned in the audit table for this sync
    if ($config{audit_pid}) {
        my ($finalbackend, $finaldbh) = $self->connect_database();
        $self->glog("Final database backend PID is $finalbackend");

        $SQL = q{
            SELECT pid
            FROM   bucardo.audit_pid
            WHERE  sync=?
            AND    type = 'KID'
            AND    killdate IS NULL
            AND    death IS NULL
        };
        $sth = $finaldbh->prepare($SQL);
        $sth->execute($self->{syncname});
        for my $pid (@{$sth->fetchall_arrayref()}) {
            my $kidpid = $pid->[0];
            ## TODO: Make sure these are Bucardo processes! - check for "Bucardo" string?
            $self->glog("Asking kid process $kidpid to terminate");
            kill $signumber{USR1} => $kidpid;
        }
        ## Asking them more than once is not going to do any good
        $SQL = q{
            UPDATE bucardo.audit_pid
            SET    death = ?
            WHERE  sync=?
            AND    type = 'KID'
            AND    killdate IS NULL
            AND    death IS NULL
        };
        my $now = scalar localtime;
        $sth = $finaldbh->prepare($SQL);
        $sth->execute("Sent kill request by $$ at $now", $self->{syncname});

        ## Update the audit_pid table
        $SQL = q{
            UPDATE bucardo.audit_pid
            SET    killdate = timeofday()::timestamp, death = ?
            WHERE  id = ?
            AND    killdate IS NULL
        };
        $sth = $finaldbh->prepare($SQL);
        $reason =~ s/\s+$//;
        $sth->execute($reason,$self->{ctlauditid});
        $finaldbh->commit();

    }

    ## Sleep a bit to let the processes clean up their own pid files
    sleep 0.3;

    ## Kill any children who have a pid file for this sync
    ## By kill, we mean "send a friendly USR1 signal"

    my $piddir = $config{piddir};
    opendir my $dh, $piddir or die qq{Could not opendir "$piddir" $!\n};
    my @pidfiles = readdir $dh;
    closedir $dh or warn qq{Could not closedir "$piddir": $!\n};
    for my $pidfile (sort @pidfiles) {
        my $sname = $self->{syncname};
        next unless $pidfile =~ /^bucardo\.kid\.sync\.$sname\..*\.pid$/;
        my $pfile = "$piddir/$pidfile";
        if (open my $fh, '<', $pfile) {
            my $pid = <$fh>;
            close $fh or warn qq{Could not close "$pfile": $!\n};
            if ($pid !~ /^\d+$/) {
                $self->glog("No PID found in file, so removing $pfile");
                unlink $pfile;
            }
            else {
                kill $signumber{USR1} => $pid;
                $self->glog("Sent USR1 signal to kid process $pid");
            }
        }
        else {
            $self->glog("Could not open file, so removing $pfile\n");
            unlink $pfile;
        }
    }

    $self->glog("Controller exiting at cleanup_controller. Reason: $reason");

    ## Remove the pid file
    if (unlink $self->{SYNCPIDFILE}) {
        $self->glog(qq{Removed pid file "$self->{SYNCPIDFILE}"});
    }
    else {
        $self->glog("Warning! Failed to remove pid file $self->{SYNCPIDFILE}");
    }

    return;

} ## end of cleanup_controller


sub get_deadlock_details {

    ## Given a database handle, extract deadlock details from it
    ## Returns a detailed string, or an empty one

    my ($self, $dldbh, $dlerr) = @_;
    return '' unless $dlerr =~ /Process \d+ waits for /;
    return '' unless defined $dldbh and $dldbh;

    $dldbh->rollback();
    my $pid = $dldbh->{pg_pid};
    while ($dlerr =~ /Process (\d+) waits for (.+) on relation (\d+) of database (\d+); blocked by process (\d+)/g) {
        next if $1 == $pid;
        my ($process,$locktype,$relation) = ($1,$2,$3);
        ## Fetch the relation name
        my $getname = $dldbh->prepare('SELECT relname FROM pg_class WHERE oid = ?');
        $getname->execute($relation);
        my $relname = $getname->fetchall_arrayref()->[0][0];

        ## Fetch information about the conflicting process
        my $queryinfo =$dldbh->prepare(q{
SELECT
  current_query AS query,
  datname AS database,
  TO_CHAR(timeofday()::timestamptz, 'HH24:MI:SS (YYYY-MM-DD)') AS current_time,
  TO_CHAR(backend_start, 'HH24:MI:SS (YYYY-MM-DD)') AS backend_started,
  TO_CHAR(timeofday()::timestamptz - backend_start, 'HH24:MI:SS') AS backend_age,
  CASE WHEN query_start IS NULL THEN '?' ELSE
    TO_CHAR(query_start, 'HH24:MI:SS (YYYY-MM-DD)') END AS query_started,
  CASE WHEN query_start IS NULL THEN '?' ELSE
    TO_CHAR(timeofday()::timestamptz - query_start, 'HH24:MI:SS') END AS query_age
  COALESCE(host(client_addr)::text,''::text) AS ip,
  CASE WHEN client_port <= 0 THEN 0 ELSE client_port END AS port,
  usename AS user
FROM pg_stat_activity
WHERE procpid = ?
});
        $queryinfo->execute($process);
        my $q = $queryinfo->fetchall_arrayref({})->[0];
        my $ret = qq{Deadlock on "$relname"\nLocktype: $locktype\n};
        if (defined $q) {
            $ret .= qq{Blocker PID: $process $q->{ip} Database: $q->{database} User: $q->{user}\n}.
                qq{Query: $q->{query}\nQuery started: $q->{query_started}  Total time: $q->{query_age}\n}.
                    qq{Backend started: $q->{backend_started} Total time: $q->{backend_age}\n};
        }
        return $ret;
    }

    return;

} ## end of get_deadlock_details


sub start_kid {

    ## A single kid, in charge of doing a sync between exactly two databases

    our ($self,$sync,$targetdb) = @_;

    ## no critic (ProhibitHardTabs)
    our ($syncname, $synctype, $sourcedb, $goatlist, $kidsalive ) = @$sync{qw(
           name      synctype   sourcedb   goatlist   kidsalive )};
    ## use critic

    ## Adjust the process name, start logging
    $0 = qq{Bucardo Kid.$self->{extraname} Sync "$syncname": ($synctype) "$sourcedb" -> "$targetdb"};
    $self->{logprefix} = 'KID';
    $self->glog(qq{New kid, syncs "$sourcedb" to "$targetdb" for sync "$syncname" alive=$kidsalive Parent=$self->{parent} Type=$synctype});
    $self->glog("PID: $$");

    ## Store our PID into a file
    my $kidpidfile = "$config{piddir}/bucardo.kid.sync.$syncname.$targetdb.pid";
    open my $pid, '>', $kidpidfile or die qq{Cannot write to $kidpidfile: $!\n};
    print {$pid} "$$\n";
    close $pid or warn qq{Could not close "$kidpidfile": $!\n};
    $self->{KIDPIDFILE} = $kidpidfile;

    ## Establish these early so the DIE block can use them
    our ($maindbh,$sourcedbh,$targetdbh);
    our ($S,$T,$pkval) = ('?','?','?');

    ## Keep track of how many times this kid has done work
    our $kidloop = 0;

    ## Catch USR1 errors as a signal from the parent CTL process to exit right away
    $SIG{USR1} = sub {
        die "CTL request\n";
    };

    ## Fancy exception handler to clean things up before leaving.
    $SIG{__DIE__} = sub {

        ## The message we were passed in. Remove whitespace from the end.
        my ($msg) = @_;
        $msg =~ s/\s+$//g;

        ## Find any error messages/states for the master, source, or target databases.
        my ($merr,$serr,$terr, $mstate,$sstate,$tstate) = ('', '', '', '', '', '');
        if ($msg =~ /DBD::Pg/) {
            $merr = $maindbh->err || 'none';
            $serr = $sourcedbh->err || 'none';
            $terr = $targetdbh->err || 'none';
            $mstate = $maindbh->state;
            $sstate = $sourcedbh->state;
            $tstate = $targetdbh->state;
            $msg .= "\n main error: $merr source error: $serr target error: $terr States:$mstate/$sstate/$tstate\n";
        }

        ## If the error was because we could not serialize, maybe add a sleep time
        my $gotosleep = 0;
        if (($tstate eq '40001' or $sstate eq '40001') and $config{kid_serial_sleep}) {
            $gotosleep = $config{kid_serial_sleep};
            $self->glog("Could not serialize, sleeping for $gotosleep seconds");
        }

        ## If this was a deadlock problem, try and gather more information
        if ($tstate eq '40P01') {
            $msg .= $self->get_deadlock_details($targetdbh, $msg);
        }
        elsif ($sstate eq '40P01') {
            $msg .= $self->get_deadlock_details($sourcedbh, $msg);
        }
        elsif ($mstate eq '40P01') { ## very unlikely
            $msg .= $self->get_deadlock_details($maindbh, $msg);
        }

        ## Drop all open connections, reconnect to main for cleanup
        defined $sourcedbh and $sourcedbh and ($sourcedbh->rollback, $sourcedbh->disconnect);
        defined $targetdbh and $targetdbh and ($targetdbh->rollback, $targetdbh->disconnect);
        defined $maindbh   and $maindbh   and ($maindbh->rollback,   $maindbh->disconnect  );
        my ($finalbackend, $finaldbh) = $self->connect_database();
        $self->glog("Final database backend PID is $finalbackend");

        ## Let anyone listening know that this target and sync aborted
        $finaldbh->do(qq{NOTIFY "bucardo_synckill_${syncname}_$targetdb"}) or warn 'NOTIFY failed';
        $finaldbh->do(qq{NOTIFY "bucardo_synckill_$syncname"}) or warn 'NOTIFY failed';
        $finaldbh->commit();

        ## Mark ourself as aborted if we've started but not completed a job
        ## The controller is responsible for marking aborted entries as ended
        $SQL = q{
            UPDATE bucardo.q
            SET    aborted=timeofday()::timestamp, whydie=?
            WHERE  sync=?
            AND    sourcedb=?
            AND    targetdb=?
            AND    ppid=?
            AND    pid=?
            AND    ended IS NULL
            AND    aborted IS NULL
        };
        ## Note: we don't check for non-null started because it is never set without a pid
        $sth = $finaldbh->prepare($SQL);
        $sth->execute($msg,$syncname,$sourcedb,$targetdb,$self->{parent},$$);

        ## Clean up the audit_pid table
        if ($config{audit_pid}) {
            $SQL = q{
                UPDATE bucardo.audit_pid
                SET    killdate=timeofday()::timestamp, death=?
                WHERE  id = ?
            };
            $sth = $finaldbh->prepare($SQL);
            $sth->execute($msg,$self->{kidauditid});
        }

        ## Done with database cleanups, so disconnect
        $finaldbh->commit();
        $finaldbh->disconnect();

        ## Only done from serialize at the moment
        sleep $gotosleep if $gotosleep;

        ## Send an email as needed (never for clean exit)
        if (! $self->{clean_exit} and $self->{sendmail} or $self->{sendmail_file}) {
            my $warn = $msg =~ /CTL request/ ? '' : 'Warning! ';
            my $line = (caller)[2];
            $self->glog(qq{${warn}Child for sync "$syncname" ("$sourcedb" -> "$targetdb") was killed at line $line: $msg});

            ## Never display the database password
            for (values %{$self->{dbs}}) {
                $_->{dbpass} = '???';
            }
            $self->{dbpass} = '???';

            ## Create the body of the message to be mailed
            my $dump = Dumper $self;
            ## no critic (ProhibitHardTabs)
            my $body = qq{
            Kid $$ has been killed at line $line
            Error: $msg
            Possible suspects: $S.$T: $pkval
            Host: $hostname
            Sync name: $syncname
            Stats page: $config{stats_script_url}?host=$sourcedb&sync=$syncname
            Source database: $sourcedb
            Target database: $targetdb
            Parent process: $self->{ppid}
            Rows set to aborted: $count
            Version: $VERSION
            Loops: $kidloop
            };
            ## use critic
            $body =~ s/^\s+//gsm;
            my $moresub = '';
            if ($msg =~ /Found stopfile/) {
                $moresub = ' (stopfile)';
            }
            elsif ($tstate eq '40001' or $sstate eq '40001') {
                $moresub = ' (serialization)';
            }
            elsif ($mstate eq '40P04' or $sstate eq '40P04' or $tstate eq '40P04') {
                $moresub = ' (deadlock)';
            }
            elsif ($msg =~ /could not connect/) {
                $moresub = ' (no connection)';
            }
            my $subject = qq{Bucardo kid for "$syncname" killed on $shorthost$moresub};
            $self->send_mail({ body => "$body\n", subject => $subject });
        }

        my $extrainfo = sprintf '%s%s%s',
            qq{Sync "$syncname", Target "$targetdb"},
            $S eq '?' ? '' : " $S.$T",
            $pkval eq '?' ? '' : " pk: $pkval";

        $self->cleanup_kid($msg, $extrainfo);

        exit 1;

    }; ## end $SIG{__DIE__}

    ## Connect to the main database; overwrites previous handle from the controller
    my $kid_backend;
    ($kid_backend, $self->{masterdbh}) = $self->connect_database();
    $maindbh = $self->{masterdbh};
    $self->glog("Bucardo database backend PID is $kid_backend");

    ## Add ourself to the audit table
    if ($config{audit_pid}) {
        $SQL = q{INSERT INTO bucardo.audit_pid (type,parentid,familyid,sync,ppid,pid,birth,source,target)}.
            qq{ VALUES ('KID',?,?,?,$self->{ppid},$$,'Life: $self->{life}',?,?)};
        $sth = $maindbh->prepare($SQL);
        $sth->execute($self->{ctlauditid},$self->{mcpauditid},$syncname,$sourcedb,$targetdb);
        $SQL = q{SELECT currval('audit_pid_id_seq')};
        $self->{kidauditid} = $maindbh->selectall_arrayref($SQL)->[0][0];
    }

    ## Listen for important changes to the q table, if we are persistent
    my $listenq = "bucardo_q_${syncname}_$targetdb";
    if ($kidsalive) {
        $maindbh->do(qq{LISTEN "$listenq"}) or die "LISTEN $listenq failed";
    }

    ## Listen for a ping, even if not persistent
    $maindbh->do('LISTEN bucardo_kid_'.$$.'_ping');
    $maindbh->commit();

    ## Prepare to update the q table when we start...
    $SQL = q{
        UPDATE bucardo.q
        SET    started=timeofday()::timestamptz, pid = ?
        WHERE  sync=?
        AND    targetdb=?
        AND    started IS NULL
    };
    $sth{qsetstart} = $maindbh->prepare($SQL);

    ## .. and when we finish.
    $SQL = q{
        UPDATE bucardo.q
        SET    ended=timeofday()::timestamptz, updates=?, inserts=?, deletes=?
        WHERE  sync=?
        AND    targetdb=?
        AND    pid=?
        AND    started IS NOT NULL
        AND    ended IS NULL
        AND    aborted IS NULL
    };
    $sth{qend} = $maindbh->prepare($SQL);

    my $backend;

    ## Connect to the source database
    ($backend, $sourcedbh) = $self->connect_database($sourcedb);
    $self->glog("Source database backend PID is $backend");


    ## Connect to the target database
    ($backend, $targetdbh) = $self->connect_database($targetdb);
    $self->glog("Target database backend PID is $backend");

    ## Put our backend PIDs into the log
    $SQL = 'SELECT pg_backend_pid()';
    my $source_backend = $sourcedbh->selectall_arrayref($SQL)->[0][0];
    my $target_backend = $targetdbh->selectall_arrayref($SQL)->[0][0];
    $self->glog("Source backend PID: $source_backend. Target backend PID: $target_backend");

    ## Put the backend PIDs in place in the audit_pid table
    if ($config{audit_pid}) {
        $SQL = q{
            UPDATE bucardo.audit_pid
            SET    source_backend = ?, target_backend = ?
            WHERE  id = ?
        };
        $sth = $maindbh->prepare($SQL);
        $sth->execute($source_backend, $target_backend, $self->{kidauditid});
        $maindbh->commit();
    }

    ## If we are using delta tables, prepare all relevant SQL
    if ($synctype eq 'pushdelta') {

        ## Check for any unhandled truncates in general. If there are, no reason to even look at bucardo_delta
        $SQL = 'SELECT tablename, MAX(cdate) FROM bucardo.bucardo_truncate_trigger '
            . 'WHERE sync = ? AND replicated IS NULL GROUP BY 1';
        $sth{source}{checktruncate} = $sourcedbh->prepare($SQL) if $synctype eq 'pushdelta';

        ## Check for the latest truncate to this target for each table
        $SQL = 'SELECT 1 FROM bucardo.bucardo_truncate_trigger_log '
            . 'WHERE sync = ? AND targetdb=? AND tablename = ? AND replicated = ?';
        $sth{source}{checktruncatelog} = $sourcedbh->prepare($SQL) if $synctype eq 'pushdelta';

    }

    if ($synctype eq 'pushdelta' or $synctype eq 'swap') {

        for my $g (@$goatlist) {

            next if $g->{reltype} ne 'table';

            ($S,$T) = ($g->{safeschema},$g->{safetable});

            if ($g->{does_source_makedelta} or $g->{does_target_makedelta} or
                $sync->{does_source_makedelta} or $sync->{does_target_makedelta}) {
                my $rowid = 'rowid';
                my $vals = '?' . (',?' x $g->{pkcols});
                $x=0;
                for my $pk (@{$g->{pkey}}) {
                    $x++;
                    next if $x < 2;
                    $rowid .= ", rowid$x";
                }
                $SQL = qq{INSERT INTO bucardo.bucardo_delta(tablename,$rowid) VALUES ($vals)};
                if ($g->{does_source_makedelta} or $sync->{does_source_makedelta}) {
                    $sth{source}{$g}{insertdelta} = $sourcedbh->prepare($SQL);
                    $g->{source_makedelta_inserts} = 0;
                }
                if ($g->{does_target_makedelta} or $sync->{does_target_makedelta}) {
                    $sth{target}{$g}{insertdelta} = $targetdbh->prepare($SQL);
                    $g->{target_makedelta_inserts} = 0;
                }
            }

            if ($synctype eq 'swap') {
                my $safepks = join ',' => @{$g->{qpkey}};
                my $q = '';
                for my $pkb (@{$g->{binarypkey}}) {
                    $q .= $pkb ? q{DECODE(?,'base64'),} : '?,';
                }
                chop $q;
                if (length $g->{safecolumnlist}) {
                    $SQL = "INSERT INTO $S.$T ($safepks, $g->{safecolumnlist}) VALUES ($q,";
                    $SQL .= join ',' => map {'?'} @{$g->{cols}};
                    $SQL .= ')';
                }
                else {
                    $SQL = "INSERT INTO $S.$T ($safepks) VALUES ($q)";
                }
                $sth{target}{$g}{insertrow} = $targetdbh->prepare($SQL);
                $sth{source}{$g}{insertrow} = $sourcedbh->prepare($SQL);

                if (length $g->{safecolumnlist}) {
                    $SQL = "UPDATE $S.$T SET ";
                    $SQL .= join ',' => map { "$_=?" } @{$g->{safecols}};
                }
                else {
                    $SQL = "UPDATE $S.$T SET $g->{qpkey}[0]=$g->{qpkey}[0]";
                }

                my $drow = q{d.rowid AS "BUCARDO_ID"};
                $x=1;
                for my $qpk (@{$g->{qpkey}}) {
                    $SQL .= sprintf ' %s %s = ?',
                        $x>1 ? 'AND' : 'WHERE',
                            $g->{binarypkey}[$x-1] ? qq{ENCODE($qpk,'base64')} : $qpk;
                    $x > 1 and $drow .= qq{,d.rowid$x AS "BUCARDO_ID$x"};
                    $x++;
                }

                $sth{target}{$g}{updaterow} = $targetdbh->prepare($SQL);
                $sth{source}{$g}{updaterow} = $sourcedbh->prepare($SQL);
                if (exists $g->{binarycols}) {
                    for (@{$g->{binarycols}}) {
                        $sth{target}{$g}{insertrow}->bind_param($_ + $g->{pkcols}, undef, {pg_type => DBD::Pg::PG_BYTEA});
                        $sth{target}{$g}{updaterow}->bind_param($_, undef, {pg_type => DBD::Pg::PG_BYTEA});
                        $sth{source}{$g}{insertrow}->bind_param($_+1, undef, {pg_type => DBD::Pg::PG_BYTEA});
                        $sth{source}{$g}{updaterow}->bind_param($_, undef, {pg_type => DBD::Pg::PG_BYTEA});
                    }
                }

                my $aliaslist = join ',' => @{$g->{safecols}};
                if (length $aliaslist) {
                    $aliaslist = ",$aliaslist";
                }

                ## Note: column order important for splice and defined calls later
                $SQL{delta} = qq{
                SELECT    DISTINCT $drow,
                          BUCARDO_PK $aliaslist
                FROM      bucardo.bucardo_delta d
                LEFT JOIN $S.$T t ON BUCARDO_JOIN
                WHERE     d.tablename = \$1::oid
                AND       NOT EXISTS (
                                SELECT 1
                                FROM   bucardo.bucardo_track bt
                                WHERE  d.txntime = bt.txntime
                                AND    bt.targetdb = '\$2'::text
                                AND    bt.tablename = \$1::oid
                          )
                };

                my $clause = '';
                my $cols = '';
                $x = 0;
                for my $qpk (@{$g->{qpkey}}) {
                    $clause .= sprintf q{%s::%s = d.rowid%s::%s AND },
                        $g->{binarypkey}[$x] ? qq{ENCODE(t.$qpk,'base64')} : "t.$qpk",
# 8.2 can't cast ENCODE()'s TEXT return value to BYTEA; leaving at TEXT appears to work
                        $g->{binarypkey}[$x] ? 'text' : $g->{pkeytype}[$x],
                        $x ? $x+1 : '',
                        $g->{binarypkey}[$x] ? 'text' : $g->{pkeytype}[$x];
                    $cols .= $g->{binarypkey}[0] ? qq{ENCODE(t.$qpk,'base64'),} : "t.$qpk,";
                    $x++;
                }
                $clause =~ s/ AND $//;
                chop $cols;
                $SQL{delta} =~ s/BUCARDO_JOIN/($clause)/;
                $SQL{delta} =~ s/BUCARDO_PK/$cols/;

            }
            else { ## synctype eq 'pushdelta'

                my $rowids = 'rowid';
                for (2 .. $g->{pkcols}) {
                    $rowids .= ",rowid$_";
                }

                ## This is the main query: grab all changed rows since the last sync
                $SQL{delta} = qq{
                SELECT  DISTINCT $rowids
                FROM    bucardo.bucardo_delta d
                WHERE   d.tablename = \$1::oid
                AND     NOT EXISTS (
                           SELECT 1
                           FROM   bucardo.bucardo_track bt
                           WHERE  d.txntime = bt.txntime
                           AND    bt.targetdb = '\$2'::text
                           AND    bt.tablename = \$1::oid
                        )
                };

                if ($sync->{track_rates}) {
                    ## no critic (ProhibitInterpolationOfLiterals)
                    $SQL{deltarate} = qq{
                    SELECT  DISTINCT txntime
                    FROM    bucardo.bucardo_delta d
                    WHERE   d.tablename = \$1::oid
                    AND     NOT EXISTS (
                               SELECT 1
                               FROM   bucardo.bucardo_track bt
                               WHERE  d.txntime = bt.txntime
                               AND    bt.targetdb = '\$2'::text
                               AND    bt.tablename = \$1::oid
                            )
                    };
                    ## use critic
                }

            } ## end pushdelta

            ## Plug in the tablenames (oids) and the targetdb names
            ($SQL = $SQL{delta}) =~ s/\$1/$g->{oid}/go;
            (my $safedbname = $targetdb) =~ s/\'/''/go;
            $SQL =~ s/\$2/$safedbname/o;
            $sth{source}{$g}{getdelta} = $sourcedbh->prepare($SQL);
            my $safesourcedb;
            ($safesourcedb = $sourcedb) =~ s/\'/''/go;

            ## Plug in for rate measuring
            if ($sync->{track_rates}) {
                ($SQL = $SQL{deltarate}) =~ s/\$1/$g->{oid}/go;
                $SQL =~ s/\$2/$safedbname/o;
                $sth{source}{$g}{deltarate} = $sourcedbh->prepare($SQL);
            }

            ## Plug in again for the source database when doing a swap sync
            if ($synctype eq 'swap') {
                ($SQL = $SQL{delta}) =~ s/\$1/$g->{targetoid}{$targetdb}/g;
                $SQL =~ s/\$2/$safesourcedb/o;
                $sth{target}{$g}{getdelta} = $targetdbh->prepare($SQL);

                if ($sync->{track_rates}) {
                    ($SQL = $SQL{deltarate}) =~ s/\$1/$g->{targetoid}{$targetdb}/g;
                    $SQL =~ s/\$2/$safesourcedb/o;
                    $sth{target}{$g}{deltarate} = $targetdbh->prepare($SQL);
                }
            }

            ## Mark all unclaimed visible delta rows as done in the track table
            ## This must be called within the same transaction as the delta select
            ## no critic (ProhibitInterpolationOfLiterals)
            $SQL{track} = qq{
                INSERT INTO bucardo.bucardo_track (txntime,targetdb,tablename)
                SELECT DISTINCT txntime, '\$1'::text, \$2::oid
                FROM bucardo.bucardo_delta d
                WHERE d.tablename = \$2::oid
                AND NOT EXISTS (
                    SELECT 1
                    FROM   bucardo.bucardo_track t
                    WHERE  d.txntime = t.txntime
                    AND    t.targetdb = '\$1'::text
                    AND    t.tablename = \$2::oid
                );
            };
            ## use critic
            ($SQL = $SQL{track}) =~ s/\$1/$safedbname/go;
            $SQL =~ s/\$2/$g->{oid}/go;
            $sth{source}{$g}{track} = $sourcedbh->prepare($SQL);
            if ($synctype eq 'swap' or $sync->{does_target_makedelta}) {
                ($SQL = $SQL{track}) =~ s/\$1/$safesourcedb/go;
                $SQL =~ s/\$2/$g->{targetoid}{$targetdb}/go;
                $sth{target}{$g}{track} = $targetdbh->prepare($SQL);
            }
        } ## end each goat

    } ## end pushdelta or swap

    ## We disable and enable triggers and rules in one of two ways
    ## For old, pre 8.3 versions of Postgres, we manipulate pg_class
    ## This is not ideal, as we don't lock pg_class and thus risk problems
    ## because the system catalogs are not strictly MVCC. However, there is
    ## no other way to disable rules, which we must do.
    ## If we are 8.3 or higher, we simply use session_replication_role,
    ## which is completely safe, and faster (thanks Jan!)
    ## Note that the source and target may have different methods

    our $source_disable_trigrules = $sourcedbh->{pg_server_version} >= 80300 ? 'replica' : 'pg_class';
    our $target_disable_trigrules = $targetdbh->{pg_server_version} >= 80300 ? 'replica' : 'pg_class';
    my $source_modern_copy = $sourcedbh->{pg_server_version} >= 80200 ? 1 : 0;

    ## We only have to worry about makedelta_triggers in replica mode
    $sync->{does_source_makedelta_triggers} = 0 if $source_disable_trigrules ne 'replica';
    $sync->{does_target_makedelta_triggers} = 0 if $target_disable_trigrules ne 'replica';

    $SQL{disable_trigrules} = $SQL{enable_trigrules} = '';

    if (($synctype eq 'swap' and $source_disable_trigrules eq 'pg_class')
            or $target_disable_trigrules eq 'pg_class') {
        $SQL = q{
            UPDATE pg_catalog.pg_class
            SET    reltriggers = 0, relhasrules = false
            FROM   pg_catalog.pg_namespace
            WHERE  pg_catalog.pg_namespace.oid = relnamespace
            AND    (
        };
        $SQL .= join "OR\n"
            => map { "(nspname=$_->{safeschemaliteral} AND relname=$_->{safetableliteral})" }
            grep { $_->{reltype} eq 'table' }
            @$goatlist;
        $SQL .= ')';
        $SQL{disable_trigrules} .= ";\n" if $SQL{disable_trigrules};
        $SQL{disable_trigrules} .= $SQL;

        my $setclause =
            ## no critic (RequireInterpolationOfMetachars)
            q{reltriggers = }
            . q{(SELECT count(*) FROM pg_catalog.pg_trigger WHERE tgrelid = pg_catalog.pg_class.oid),}
            . q{relhasrules = }
            . q{CASE WHEN (SELECT COUNT(*) FROM pg_catalog.pg_rules WHERE schemaname=$1 AND tablename=$2) > 0 }
            . q{THEN true ELSE false END};
            ## use critic

        $SQL{etrig} = qq{
            UPDATE pg_catalog.pg_class
            SET    $setclause
            FROM   pg_catalog.pg_namespace
            WHERE  pg_catalog.pg_namespace.oid = relnamespace
            AND    nspname = \$1
            AND    relname = \$2
        };
        $SQL = join ";\n"
            => map {
                     my $sql = $SQL{etrig};
                     $sql =~ s/\$1/$_->{safeschemaliteral}/g;
                     $sql =~ s/\$2/$_->{safetableliteral}/g;
                     $sql;
                 }
                grep { $_->{reltype} eq 'table' }
                @$goatlist;

        $SQL{enable_trigrules} .= ";\n" if $SQL{enable_trigrules};
        $SQL{enable_trigrules} .= $SQL;

    }

    ## Common settings for the database handles. Set before passing to DBIx::Safe below
    ## These persist through all subsequent transactions
    $sourcedbh->do('SET statement_timeout = 0');
    $targetdbh->do('SET statement_timeout = 0');

    ## Note: no need to turn these back to what they were: we always want to stay in replica mode
    if ($target_disable_trigrules eq 'replica') {
        $targetdbh->do(q{SET session_replication_role = 'replica'});
    }
    if ($synctype eq 'swap' and $source_disable_trigrules eq 'replica') {
        $sourcedbh->do(q{SET session_replication_role = 'replica'});
    }

    if ($config{tcp_keepalives_idle}) { ## e.g. not 0, should always exist
        $sourcedbh->do("SET tcp_keepalives_idle = $config{tcp_keepalives_idle}");
        $sourcedbh->do("SET tcp_keepalives_interval = $config{tcp_keepalives_interval}");
        $sourcedbh->do("SET tcp_keepalives_count = $config{tcp_keepalives_count}");
        $targetdbh->do("SET tcp_keepalives_idle = $config{tcp_keepalives_idle}");
        $targetdbh->do("SET tcp_keepalives_interval = $config{tcp_keepalives_interval}");
        $targetdbh->do("SET tcp_keepalives_count = $config{tcp_keepalives_count}");
    }
    $sourcedbh->commit();
    $targetdbh->commit();

    my $lastpingcheck = 0;

    ## Everything below with "our" is used in custom code calls

    ## Summary information about our actions.
    our %deltacount;
    our %dmlcount;
    our %rowinfo;
    our $input = {};

    ## Custom code may require a copy of the rows
    our $rows_for_custom_code;

    ## Create safe versions of the database handles if we are going to need them
    our ($safe_sourcedbh, $safe_sourcedbh_strict, $safe_targetdbh, $safe_targetdbh_strict);

    if ($sync->{need_safe_dbh_strict}) {
        my $darg;
        for my $arg (sort keys %{$dbix{source}{strict}}) {
            next if ! length $dbix{source}{strict}{$arg};
            $darg->{$arg} = $dbix{source}{strict}{$arg};
        }
        $darg->{dbh} = $sourcedbh;
        $safe_sourcedbh_strict = DBIx::Safe->new($darg);

        undef $darg;
        for my $arg (sort keys %{$dbix{target}{strict}}) {
            next if ! length $dbix{target}{strict}{$arg};
            $darg->{$arg} = $dbix{target}{strict}{$arg};
        }
        $darg->{dbh} = $targetdbh;
        $safe_targetdbh_strict = DBIx::Safe->new($darg);
    }
    if ($sync->{need_safe_dbh}) {
        my $darg;
        for my $arg (sort keys %{$dbix{source}{notstrict}}) {
            next if ! length $dbix{source}{notstrict}{$arg};
            $darg->{$arg} = $dbix{source}{notstrict}{$arg};
        }
        $darg->{dbh} = $sourcedbh;
        $safe_sourcedbh = DBIx::Safe->new($darg);

        undef $darg;
        for my $arg (sort keys %{$dbix{target}{notstrict}}) {
            next if ! length $dbix{target}{notstrict}{$arg};
            $darg->{$arg} = $dbix{target}{notstrict}{$arg};
        }
        $darg->{dbh} = $targetdbh;
        $safe_targetdbh = DBIx::Safe->new($darg);
    }


    sub run_kid_custom_code {

        my $c = shift;
        my $strictness = shift || '';
        my $attempts = shift || 0;

        $self->glog("Running $c->{whenrun} custom code $c->{id}: $c->{name}");
        my $send_mail_ref = sub { $self->send_mail(@_) };
        $input = {
            synctype   => $synctype,
            syncname   => $syncname,
            goatlist   => $goatlist,
            sourcename => $sourcedb,
            targetname => $targetdb,
            kidloop    => $kidloop,
            deltacount => \%deltacount,
            dmlcount   => \%dmlcount,
            message    => '',
            warning    => '',
            error      => '',
            nextcode   => '',
            endsync    => '',
            rowinfo    => \%rowinfo,
            runagain   => 0, ## exception only
            attempts   => $attempts, ## exception only
            sendmail   => $send_mail_ref,
        };
        if ($c->{getrows}) {
            $input->{rows} = $rows_for_custom_code;
        }
        if ($c->{getdbh}) {
            $input->{sourcedbh} = $strictness eq 'nostrict' ? $safe_sourcedbh : $safe_sourcedbh_strict;
            $input->{targetdbh} = $strictness eq 'nostrict' ? $safe_targetdbh : $safe_targetdbh_strict;
        }
        ## In case the custom code wants to use other table's rules or triggers:
        if ($c->{trigrules}) {
            ## We assume the default is something other than replica, naturally
            if ($source_disable_trigrules eq 'replica') {
                $sourcedbh->do(q{SET session_replication_role = DEFAULT});
            }
            if ($target_disable_trigrules eq 'replica') {
                $targetdbh->do(q{SET session_replication_role = DEFAULT});
            }
        }
        $maindbh->{InactiveDestroy} = 1;
        $sourcedbh->{InactiveDestroy} = 1;
        $targetdbh->{InactiveDestroy} = 1;
        &{$c->{coderef}}($input);
        $maindbh->{InactiveDestroy} = 0;
        $sourcedbh->{InactiveDestroy} = 0;
        $targetdbh->{InactiveDestroy} = 0;
        if ($c->{trigrules}) {
            if ($source_disable_trigrules eq 'replica') {
                $sourcedbh->do(q{SET session_replication_role = 'replica'});
            }
            if ($target_disable_trigrules eq 'replica') {
                $targetdbh->do(q{SET session_replication_role = 'replica'});
            }
        }
        $self->glog("Finished custom code $c->{id}");
        if (length $input->{message}) {
            $self->glog("Message from $c->{whenrun} code $c->{id}: $input->{message}");
        }
        if (length $input->{warning}) {
            $self->glog("Warning! Code $c->{whenrun} $c->{id}: $input->{warning}");
        }
        if (length $input->{error}) {
            $self->glog("Warning! Code $c->{whenrun} $c->{id}: $input->{error}");
            die "Code $c->{whenrun} $c->{id} error: $input->{error}";
        }
        if (length $input->{nextcode}) { ## Mostly for conflict handlers
            return 'next'; ## try the next customcode
        }
        if (length $input->{endsync}) {
            $self->glog("Code $c->{whenrun} requests a cancellation of the rest of the sync");
            ## before_txn and after_txn should commit themselves
            $targetdbh->rollback();
            $sourcedbh->rollback();
            $sth{qend}->execute(0,0,0,$syncname,$targetdb,$$);
            $self->glog( "Called qend with $syncname and $targetdb and $$!\n");
            my $notify = "bucardo_syncdone_${syncname}_$targetdb";
            $maindbh->do(qq{NOTIFY "$notify"}) or warn "NOTIFY $notify failed";
            $maindbh->commit();
            sleep $config{endsync_sleep};
            return 'redo'; ## redo this entire sync
        }
        return 'normal';

    } ## end of run_kid_custom_code

    ## Have we found a reason to check the queue yet?
    my $checkq;

  KID: {

        $checkq = 0;

        if (-e $self->{stopfile}) {
            $self->glog(qq{Found stopfile "$self->{stopfile}": exiting\n});
            last KID;
        }

        ## If persistent, listen for messages and do an occasional ping.
        if ($kidsalive) {
            while (my $notify = $maindbh->func('pg_notifies')) {
                my ($name, $pid) = @$notify;
                if ($name eq $listenq) {
                    $self->glog("Got a notice for $syncname: $sourcedb -> $targetdb", 7);
                    $checkq = 1;
                }
                ## Got a ping?
                elsif ($name eq 'bucardo_kid_'.$$.'_ping') {
                    $self->glog('Got a ping, issuing pong');
                    $maindbh->do('NOTIFY bucardo_kid_'.$$.'_pong') or warn 'NOTIFY failed';
                    $maindbh->commit();
                }
            }
            if (time() - $lastpingcheck >= $config{kid_pingtime}) {
                ## If this fails, simply have the CTL restart it
                ## Other things match on this wording, so change carefully
                $maindbh->ping or die qq{Ping failed for main database\n};
                $sourcedbh->ping or die qq{Ping failed for source database $sourcedb\n};
                $sourcedbh->rollback();
                $targetdbh->ping or die qq{Ping failed for target database $targetdb\n};
                $targetdbh->rollback();
                $lastpingcheck = time();
            }
            $maindbh->rollback();
        }

        ## If we are short-lived, or were created with a mandate, force a q check
        if (!$kidsalive or $self->{kidcheckq}) {
            $self->{kidcheckq} = 0;
            $checkq = 2;
        }

        if (! $checkq) {
            sleep $config{kid_nothingfound_sleep};
            redo KID;
        }

        ## Is there an entry in the q table for us to claim (started is null)?
        $count = $sth{qsetstart}->execute($$,$syncname,$targetdb);
        if ($count != 1) {
            ## We can say != 1 here because of the unique constraint on q
            $self->glog('Nothing to do: no entry found in the q table for this sync', 7);
            $maindbh->rollback();
            redo KID if $kidsalive;
            last KID;
        }
        ## Stake our claim
        $maindbh->commit();

        $kidloop++;

        my $kid_start_time = time();

        ## Reset stuff that may be used by custom code
        undef %deltacount;
        $deltacount{all} = 0;

        undef %dmlcount;
        $dmlcount{allinserts}{target} = 0;
        $dmlcount{allinserts}{source} = 0;
        $dmlcount{allupdates}{target} = 0;
        $dmlcount{allupdates}{source} = 0;
        $dmlcount{alldeletes}{target} = 0;
        $dmlcount{alldeletes}{source} = 0;

        undef %rowinfo;

        ## Run all 'before_txn' code
        for my $code (@{$sync->{code_before_txn}}) {
            my $result = run_kid_custom_code($code, 'nostrict');
            if ($result eq 'redo') {
                redo KID if $kidsalive;
                last KID;
            }
            else {
                ## Just in case it left it in a funky state
                $sourcedbh->rollback();
                $targetdbh->rollback();
            }
        }

        ## Start the main transaction. From here on out, speed is key
        ## Note that all database handles are currently not in a txn (last action was commit or rollback)
        $targetdbh->do("SET TRANSACTION ISOLATION LEVEL $sync->{txnmode} READ WRITE");
        if ($synctype eq 'swap' or $synctype eq 'pushdelta') {
            $sourcedbh->do("SET TRANSACTION ISOLATION LEVEL $sync->{txnmode} READ WRITE");
        }

        ## We may want to lock all the tables
        ## TODO: alternate ways to trigger this
        my $lock_table_mode = '';
        my $force_lock_file = "/tmp/bucardo-force-lock-$syncname";
        if (-e $force_lock_file) {
            $lock_table_mode = 'EXCLUSIVE';
            if (-s _ and (open my $fh, '<', "$force_lock_file")) {
                my $newmode = <$fh>;
                close $fh or warn qq{Could not close "$force_lock_file": $!\n};
                if (defined $newmode) {
                    chomp $newmode;
                    $lock_table_mode = $newmode if $newmode =~ /^\s*\w[ \w]+\s*$/o;
                }
            }
            $self->glog(qq{Found lock control file "$force_lock_file". Mode: $lock_table_mode});
        }

        if ($lock_table_mode) {
            $self->glog("Locking all table in $lock_table_mode MODE");
            for my $g (@$goatlist) {
                next if $g->{reltype} ne 'table';
                my $com = "$g->{safeschema}.$g->{safetable} IN $lock_table_mode MODE";
                $self->glog("$sourcedb: Locking table $com");
                $sourcedbh->do("LOCK TABLE $com");
                $self->glog("$targetdb: Locking table $com");
                $targetdbh->do("LOCK TABLE $com");
            }
        }

        ## Run all 'before_check_rows' code
        for my $code (@{$sync->{code_before_check_rows}}) {
            my $result = run_kid_custom_code($code, 'strict');
            if ($result eq 'redo') {
                ## In case we locked above:
                $sourcedbh->rollback();
                $targetdbh->rollback();
                redo KID if $kidsalive;
                last KID;
            }
        }

        ## If doing a pushdelta or a swap, see if we have any delta rows to process
        if ($synctype eq 'pushdelta' or $synctype eq 'swap') {

            ## Check for truncate activity. If found, switch to fullcopy for a table as needed.
            ## For now, just do pushdelta
            if ($synctype eq 'pushdelta') {
                $deltacount{sourcetruncate} = $sth{source}{checktruncate}->execute($syncname);
                $sth{source}{checktruncate}->finish() if $deltacount{sourcetruncate} =~ s/0E0/0/o;
                $self->glog(qq{Source truncate count: $deltacount{sourcetruncate}}, 6);
                if ($deltacount{sourcetruncate}) {
                    ## For each table that was truncated, see if this target has already handled it
                    for my $row (@{$sth{source}{checktruncate}->fetchall_arrayref()}) {
                        $count = $sth{source}{checktruncatelog}->execute($syncname, $targetdb, @$row);
                        $sth{source}{checktruncatelog}->finish();
                        ($deltacount{source}{truncate}{$row->[0]} = $count) =~ s/0E0/0/o;
                        $deltacount{source}{truncatelog}{$row->[0]} = $row->[1];
                    }
                    ## Which of the tables we are tracking need truncation support?
                    $SQL = 'INSERT INTO bucardo.bucardo_truncate_trigger_log (tablename,sname,tname,sync,targetdb,replicated) '
                        . 'VALUES(?,?,?,?,?,?)';
                    for my $g (@$goatlist) {
                        next if $g->{reltype} ne 'table';
                        ## deltacount may not exist = no truncation needed
                        ## may exist but be zero = truncate!
                        ## may exists and be positive = no truncation needed
                        $g->{source}{needstruncation} =
                            (exists $deltacount{source}{truncate}{$g->{oid}} and !$deltacount{source}{truncate}{$g->{oid}})
                            ? 1 : 0;
                        if ($g->{source}{needstruncation}) {
                            $sth = $sourcedbh->prepare_cached($SQL);
                            $sth->execute($g->{oid},$g->{safeschema},$g->{safetable},$syncname,$targetdb,
                                        $deltacount{source}{truncatelog}{$g->{oid}});
                            $deltacount{truncates}++;
                            $self->glog('Marking this truncate as done in bucardo_truncate_trigger_log');
                        }
                    }
                }
            }

            ## For each table in this herd, grab a count of changes
            $deltacount{allsource} = $deltacount{alltarget} = 0;
            for my $g (@$goatlist) {

                ## If this table was truncated on the source, we do nothing here
                next if $g->{source}{needstruncation};

                ($S,$T) = ($g->{safeschema},$g->{safetable});

                ## We'll handle sequence changes here and now (pushdelta only)
                if ($synctype eq 'pushdelta' and $g->{reltype} eq 'sequence') {

                    $SQL = "SELECT last_value, is_called FROM $S.$T";
                    my ($lastval, $iscalled) = @{$sourcedbh->selectall_arrayref($SQL)->[0]};

                    ## Check our internal table to see if we really need to propagate this sequence
                    $SQL = 'SELECT value, iscalled FROM bucardo.bucardo_sequences WHERE tablename = ? AND syncname = ?';
                    $sth = $sourcedbh->prepare($SQL);
                    $count = $sth->execute($g->{oid}, $sync->{name});
                    my $newval = 0;
                    if ($count < 1) {
                        $newval = 1; ## Never before seen, so add to the table
                        $sth->finish();
                    }
                    else {
                        my ($oldval,$oldcalled) = @{$sth->fetchall_arrayref()->[0]};
                        if ($oldval != $lastval) {
                            $newval = 2; ## Value has changed
                        }
                        elsif ($oldcalled ne $iscalled) {
                            $newval = 3; ## is_called has changed
                        }
                    }
                    if ($newval) {
                        $self->glog("Setting sequence $S.$T to value of $lastval, is_called is $iscalled");
                        $SQL = "SELECT setval('$S.$T', $lastval, '$iscalled')";
                        $targetdbh->do($SQL);

                        ## Copy the change to our internal table
                        if ($newval == 1) {
                            $SQL = 'INSERT INTO bucardo.bucardo_sequences (tablename, syncname, value, iscalled) VALUES (?,?,?,?)';
                            $sth = $sourcedbh->prepare($SQL);
                            $sth->execute($g->{oid},$sync->{name},$lastval,$iscalled);
                        }
                        else {
                            $SQL = 'UPDATE bucardo.bucardo_sequences SET value=?, iscalled=? WHERE tablename=? AND syncname=?';
                            $sth = $sourcedbh->prepare($SQL);
                            $sth->execute($lastval,$iscalled,$g->{oid}, $sync->{name});
                        }

                        ## Internal note so we know things have changed
                        $deltacount{sequences}++;

                    }

                }

                ## No need to continue unless we are a table
                next if $g->{reltype} ne 'table';

                $deltacount{allsource} += $deltacount{source}{$S}{$T} = $sth{source}{$g}{getdelta}->execute();
                $sth{source}{$g}{getdelta}->finish() if $deltacount{source}{$S}{$T} =~ s/0E0/0/o;
                $self->glog(qq{Source delta count for $S.$T: $deltacount{source}{$S}{$T}}, 6);

                if ($synctype eq 'swap') {
                    $deltacount{alltarget} += $deltacount{target}{$S}{$T} = $sth{target}{$g}{getdelta}->execute();
                    $sth{target}{$g}{getdelta}->finish() if $deltacount{target}{$S}{$T} =~ s/0E0/0/o;
                    $self->glog(qq{Target delta count for $S.$T: $deltacount{target}{$S}{$T}}, 6);
                }
            }
            if ($synctype eq 'swap') {
                $self->glog("Total source delta count: $deltacount{allsource}");
                $self->glog("Total target delta count: $deltacount{alltarget}");
            }
            $deltacount{all} = $deltacount{allsource} + $deltacount{alltarget};
            $self->glog("Total delta count: $deltacount{all}", 6);

            ## If no changes, rollback dbs, close out q, notify listeners, and leave or reloop
            if (! $deltacount{all} and ! $deltacount{truncates}) {
                $targetdbh->rollback();
                $sourcedbh->rollback();
                $sth{qend}->execute(0,0,0,$syncname,$targetdb,$$);
                $maindbh->do(qq{NOTIFY "bucardo_syncdone_${syncname}_$targetdb"})
                    or die qq{NOTIFY failed: bucardo_syncdone_${syncname}_$targetdb};
                $maindbh->commit();
                sleep $config{kid_nodeltarows_sleep};
                redo KID if $kidsalive;
                last KID;
            }
        } ## end count delta rows

        ## Run all 'before_trigger_drop' code
        for my $code (@{$sync->{code_before_trigger_drop}}) {
            my $result = run_kid_custom_code($code, 'strict');
            if ($result eq 'redo') { ## redo rollsback source and target
                redo KID if $kidsalive;
                last KID;
            }
        }

        ## Disable rules and triggers on target (all) and source (swap sync)
        if ($target_disable_trigrules ne 'replica') {
            $self->glog(qq{Disabling triggers and rules on $targetdb via pg_class});
            $targetdbh->do($SQL{disable_trigrules});
        }
        if ($synctype eq 'swap' and $source_disable_trigrules ne 'replica') {
            $self->glog(qq{Disabling triggers and rules on $sourcedb via pg_class});
            $sourcedbh->do($SQL{disable_trigrules});
        }

        ## FULLCOPY
        if ($synctype eq 'fullcopy' or $deltacount{truncates}) {

            for my $g (@$goatlist) {

                ($S,$T) = ($g->{safeschema},$g->{safetable});

                next if $deltacount{truncates} and ! $g->{source}{needstruncation};

                if ($g->{ghost}) {
                    $self->glog("Skipping ghost table $S.$T");
                    next;
                }

                ## Handle sequences first, by simply forcing a setval
                if ($g->{reltype} eq 'sequence') {
                    $SQL = "SELECT last_value, is_called FROM $S.$T";
                    my ($lastval, $iscalled) = @{$sourcedbh->selectall_arrayref($SQL)->[0]};

                    $self->glog("Setting sequence $S.$T to value of $lastval, is_called is $iscalled");
                    $SQL = "SELECT setval('$S.$T', $lastval, '$iscalled')";
                    $targetdbh->do($SQL);

                    ## No need to continue any further
                    next;
                }

                ## If doing a one-time-copy and using empty mode, leave if the target has rows
                if ($sync->{onetimecopy} == 2) {
                    $SQL = "SELECT 1 FROM $S.$T LIMIT 1";
                    $sth = $targetdbh->prepare($SQL);
                    $count = $sth->execute();
                    $sth->finish();
                    if ($count >= 1) {
                        $g->{onetimecopy_ifempty} = 1;
                        $self->glog(qq{Target table "$S.$T" has rows and we are in onetimecopy if empty mode, so we will not COPY});
                        next;
                    }

                    ## Just in case, verify that we aren't at zero rows due to nothing on the source
                    $sth = $sourcedbh->prepare($SQL);
                    $count = $sth->execute();
                    $sth->finish();
                    if ($count < 1) {
                        $g->{onetimecopy_ifempty} = 1;
                        $self->glog(qq{Source table "$S.$T" has no rows and we are in onetimecopy if empty mode, so we will not COPY});
                        next;
                    }
                }

                my $hasindex = 0;
                if ($g->{rebuild_index}) {
                    ## TODO: Cache this information earlier if feasible
                    $SQL = "SELECT relhasindex FROM pg_class WHERE oid = $g->{targetoid}{$targetdb}";
                    $hasindex = $targetdbh->selectall_arrayref($SQL)->[0][0];
                    if ($hasindex) {
                        $self->glog("Turning off indexes for $S.$T on $targetdb");
                        ## TODO: Do this without pg_class manipulation if possible
                        $SQL = "UPDATE pg_class SET relhasindex = 'f' WHERE oid = $g->{targetoid}{$targetdb}";
                        $targetdbh->do($SQL);
                    }
                }

                $self->glog("Emptying out target table $S.$T using $sync->{deletemethod}");
                my $empty_by_delete = 1;
                if ($sync->{deletemethod} =~ /^truncate/o) {
                    ## Temporarily override our kid-level handler due to the eval
                    local $SIG{__DIE__} = sub {};
                    my $cascade = $sync->{deletemethod} =~ /cascade/ ? ' CASCADE' : '';
                    $targetdbh->do('SAVEPOINT truncate_attempt');
                    eval {
                        $targetdbh->do("TRUNCATE TABLE $S.$T $cascade");
                    };
                    if ($@) {
                        $self->glog("Truncation of $S.$T failed, so we will try a delete");
                        $targetdbh->do('ROLLBACK TO truncate_attempt');
                        $empty_by_delete = 2;
                    }
                    else {
                        $targetdbh->do('RELEASE truncate_attempt');
                        $empty_by_delete = 0;
                    }
                }

                if ($empty_by_delete) {
                    ($dmlcount{D}{target}{$S}{$T} = $targetdbh->do("DELETE FROM $S.$T")) =~ s/0E0/0/o;
                    $dmlcount{alldeletes}{target} += $dmlcount{D}{target}{$S}{$T};
                    $self->glog("Rows deleted from $S.$T: $dmlcount{D}{target}{$S}{$T}");
                }

                my ($srccmd,$tgtcmd);
                if ($sync->{usecustomselect} and $g->{customselect}) {
                    ## TODO: Use COPY () format if 8.2 or greater
                    $g->{cs_temptable} = "bucardo_temp_$g->{tablename}_$$"; ## Raw version, not "safetable"
                    $self->glog("Creating temp table $g->{cs_temptable} for custom select on $S.$T");
                    $sourcedbh->do("CREATE TEMP TABLE $g->{cs_temptable} AS $g->{customselect}");
                    $srccmd = "COPY $g->{cs_temptable} TO STDOUT $sync->{copyextra}";
                    $tgtcmd = "COPY $S.$T($g->{safecolumnlist}) FROM STDIN $sync->{copyextra}";
                }
                else {
                    $srccmd = "COPY $S.$T TO STDOUT $sync->{copyextra}";
                    $tgtcmd = "COPY $S.$T FROM STDIN $sync->{copyextra}";
                }

                $self->glog("Running on $sourcedb: $srccmd");
                $sourcedbh->do($srccmd);

                $self->glog("Running on $targetdb: $tgtcmd");
                my $startotc = $sync->{onetimecopy} ? time : 0;
                $targetdbh->do($tgtcmd);
                my $buffer='';
                $dmlcount{I}{target}{$S}{$T} = 0;
                while ($sourcedbh->pg_getcopydata($buffer) >= 0) {
                    $targetdbh->pg_putcopydata($buffer);
                    $dmlcount{I}{target}{$S}{$T}++;
                }
                $targetdbh->pg_putcopyend();
                my $otc = $startotc ? (sprintf '(OTC: %ds) ', time-$startotc) : '';
                $self->glog(qq{${otc}End COPY of $S.$T, rows inserted: $dmlcount{I}{target}{$S}{$T}});
                $dmlcount{allinserts}{target} += $dmlcount{I}{target}{$S}{$T};

                if ($hasindex) {
                    $SQL = "UPDATE pg_class SET relhasindex = 't' WHERE oid = $g->{targetoid}{$targetdb}";
                    $targetdbh->do($SQL);
                    $self->glog("Reindexing table $S.$T on $targetdb");
                    $targetdbh->do("REINDEX TABLE $S.$T");
                    if ($otc) {
                        $self->glog(sprintf(qq{(OTC: %ds) REINDEX TABLE $S.$T}, time-$startotc));
                    }
                }

                ## If we just did a fullcopy, but the table is pushdelta or swap,
                ## we can clean out any older bucardo_delta entries
                if ($sync->{onetimecopy} or $deltacount{truncates}) {
                    $SQL = "DELETE FROM bucardo.bucardo_delta WHERE txntime <= now() AND tablename = $g->{oid}";
                    $sth = $sourcedbh->prepare($SQL);
                    $count = $sth->execute();
                    $sth->finish();
                    $count =~ s/0E0/0/o;
                    $self->glog("Rows removed from bucardo_delta on source for $S.$T: $count");
                    ## Swap? Other side(s) as well
                    if ($synctype eq 'swap') {
                        $SQL = "DELETE FROM bucardo.bucardo_delta WHERE txntime <= now() AND tablename = $g->{targetoid}{$targetdb}";
                        $sth = $targetdbh->prepare($SQL);
                        $count = $sth->execute();
                        $sth->finish();
                        $count =~ s/0E0/0/o;
                        $self->glog("Rows removed from bucardo_delta on target for $S.$T: $count");
                    }
                }
            } ## end each goat

            if ($sync->{deletemethod} ne 'truncate') {
                $self->glog("Total target rows deleted: $dmlcount{alldeletes}{target}");
            }
            $self->glog("Total target rows copied: $dmlcount{allinserts}{target}");

        } ## end of synctype fullcopy

        ## PUSHDELTA
        if ($synctype eq 'pushdelta') {

            ## Do each goat in turn, ordered by descending priority and ascending id
          PUSHDELTA_GOAT: for my $g (@$goatlist) {

                ($S,$T) = ($g->{safeschema},$g->{safetable});

                ## Skip if we've already handled this via fullcopy
                next if $g->{source}{needstruncation};

                ## No need to proceed unless we're a table
                next if $g->{reltype} ne 'table';

                ## Skip this table if no rows have changed on the source
                next unless $deltacount{source}{$S}{$T};

                ## The target table's OID
                my $toid = $g->{targetoid}{$targetdb};

                ## If requested, disable all indexes, then enable and rebuild them after we COPY
                my $hasindex = 0;
                if ($g->{rebuild_index} == 2) {
                    $SQL = "SELECT relhasindex FROM pg_class WHERE oid = $toid";
                    $hasindex = $targetdbh->selectall_arrayref($SQL)->[0][0];
                    if ($hasindex) {
                        $self->glog("Turning off indexes for $S.$T on $targetdb");
                        $SQL = "UPDATE pg_class SET relhasindex = 'f' WHERE oid = $toid";
                        $targetdbh->do($SQL);
                    }
                }

                ## How many times this goat has handled an exception
                $g->{exceptions} ||= 0;

                ## The list of primary key columns
                if (! $g->{pkeycols}) {
                    $g->{pkeycols} = '';
                    $x=0;
                    for my $qpk (@{$g->{qpkey}}) {
                        $g->{pkeycols} .= sprintf '%s,', $g->{binarypkey}[$x] ? qq{ENCODE($qpk,'base64')} : $qpk;
                    }
                    chop $g->{pkeycols};
                    $g->{pkcols} > 1 and $g->{pkeycols} = "($g->{pkeycols})";
                    ## Example: id
                    ## Example MCPK: (id,"space bar",cdate)
                }

                ## Figure out if we have enough rows to trigger a delta_bypass
                ## We cannot do a delta_bypass in makedelta mode
                $g->{does_delta_bypass} = 0;
                if ($g->{delta_bypass} and ! $g->{does_source_makedelta} and ! $g->{does_target_makedelta}) {
                    if ($g->{delta_bypass_count}
                            and $deltacount{source}{$S}{$T} >= $g->{delta_bypass_count}) {
                        $g->{does_delta_bypass} = 'count';
                        $self->glog("Activating delta_bypass for $S.$T. Count of $deltacount{source}{$S}{$T} >= $g->{delta_bypass_count}");
                    }
                    elsif ($g->{delta_bypass_percent} and $deltacount{source}{$S}{$T} >= $g->{delta_bypass_min}) {
                        ## Depends on a recent analyze, of course...
                        $SQL = "SELECT reltuples::bigint FROM pg_class WHERE oid = $g->{oid}";
                        my $total_rows = $sourcedbh->selectall_arrayref($SQL)->[0][0];
                        my $percent = $deltacount{source}{$S}{$T}*100/$total_rows;
                        if ($percent > $g->{delta_bypass_percent}) {
                            $g->{does_delta_bypass} = 'percent';
                            $self->glog("Activating delta_bypass for $S.$T. Count of $deltacount{source}{$S}{$T} for $total_rows total rows is $percent percent, which is >= $g->{delta_bypass_percent}%");
                        }
                    }
                }

                if ($g->{does_delta_bypass}) {
                    $self->glog('Forcing a onetimecopy due to delta_bypass');
                    my $srccmd = "COPY $S.$T TO STDOUT $sync->{copyextra}";
                    my $tgtcmd = "COPY $S.$T FROM STDIN $sync->{copyextra}";
                    ## Attempt to truncate the target table. If it fails, delete
                    my $empty_by_delete = 1;
                    ## Temporarily override our kid-level handler due to the eval
                    local $SIG{__DIE__} = sub {};
                    $targetdbh->do('SAVEPOINT truncate_attempt');
                    eval {
                        $targetdbh->do("TRUNCATE TABLE $S.$T");
                    };
                    if ($@) {
                        $self->glog("Truncation of $S.$T failed, so we will try a delete");
                        $targetdbh->do('ROLLBACK TO truncate_attempt');
                        $empty_by_delete = 2;
                    }
                    else {
                        $targetdbh->do('RELEASE truncate_attempt');
                        $empty_by_delete = 0;
                    }
                    if ($empty_by_delete) {
                        ($dmlcount{D}{target}{$S}{$T} = $targetdbh->do("DELETE FROM $S.$T")) =~ s/0E0/0/o;
                        $dmlcount{alldeletes}{target} += $dmlcount{D}{target}{$S}{$T};
                        $self->glog("Rows deleted from $S.$T: $dmlcount{D}{target}{$S}{$T}");
                    }

                    $self->glog("Running on $sourcedb: $srccmd");
                    $sourcedbh->do($srccmd);

                    $self->glog("Running on $targetdb: $tgtcmd");
                    $targetdbh->do($tgtcmd);
                    my $buffer='';
                    $dmlcount{I}{target}{$S}{$T} = 0;
                    while ($sourcedbh->pg_getcopydata($buffer) >= 0) {
                        $targetdbh->pg_putcopydata($buffer);
                        $dmlcount{I}{target}{$S}{$T}++;
                    }
                    $targetdbh->pg_putcopyend();
                    $self->glog(qq{End delta_bypass COPY of $S.$T, rows inserted: $dmlcount{I}{target}{$S}{$T}});
                    $dmlcount{allinserts}{target} += $dmlcount{I}{target}{$S}{$T};

                    ## If we disabled the indexes earlier, flip them on and run a REINDEX
                    if ($hasindex) {
                        $self->glog("Re-enabling indexes for table $S.$T on $targetdb");
                        $SQL = "UPDATE pg_class SET relhasindex = 't' WHERE oid = $toid";
                        $targetdbh->do($SQL);
                        $self->glog("Reindexing table $S.$T on $targetdb");
                        $targetdbh->do("REINDEX TABLE $S.$T");
                    }

                    ## Remove older bucardo_delta entries that are now irrelevant
                    $SQL = "DELETE FROM bucardo.bucardo_delta WHERE txntime <= now() AND tablename = $g->{oid}";
                    $sth = $sourcedbh->prepare($SQL);
                    $count = $sth->execute();
                    $sth->finish();
                    $count =~ s/0E0/0/o;
                    $self->glog("Rows removed from bucardo_delta on source for $S.$T: $count");

                    next PUSHDELTA_GOAT;

                } ## end of delta_bypass

                ## How many times have we done the loop below?
                my $pushdelta_attempts = 0;

                ## This is where we want to 'rewind' to on a handled exception
                ## We choose this point as its possible the custom code has a different getdelta result
              PUSHDELTA_SAVEPOINT: {

                    $pushdelta_attempts++;

                    ## From bucardo_delta, grab all distinct pks for this table that have not been already pushed
                    my $info = $sth{source}{$g}{getdelta}->fetchall_arrayref();

                    ## Reset the counts to zero
                    $dmlcount{I}{target}{$S}{$T} = $dmlcount{D}{target}{$S}{$T} = 0;

                    ## Prepare row information if any custom codes need it
                    if ($sync->{need_rows}) {
                        $rows_for_custom_code->{$S}{$T} =
                            {
                                source    => $info,
                                pkeyname  => $g->{pkey},
                                qpkeyname => $g->{qpkey},
                                pkeytype  => $g->{pkeytype},
                            };
                    }

                    ## Build a list of all PK values
                    my $pkvals = '';
                    for my $row (@$info) {
                        my $inner = join ',' => map { s/\'/''/go; qq{'$_'}; } @$row;
                        $pkvals .= $g->{pkcols} > 1 ? "($inner)," : "$inner,";
                    }
                    chop $pkvals;
                    ## Example: 1234, 221
                    ## Example MCPK: ('1234','Don''t Stop','2008-01-01'),('221','foobar','2008-11-01')

                    ## If this goat is set to makedelta, add rows to bucardo_delta to simulate the
                    ##   normal action of a trigger.
                    if ($g->{does_target_makedelta}) {
                        ## In rare cases, we want triggers and rules on bucardo_delta to fire
                        ## Check it for this database only
                        if ($sync->{does_target_makedelta_triggers}{$targetdb}) {
                            $targetdbh->do(q{SET session_replication_role = 'origin'});
                        }
                        for (@$info) {
                            $sth{target}{$g}{insertdelta}->execute($toid,@{$_}[0..($g->{pkcols}-1)]);
                            $g->{target_makedelta_inserts}++;
                        }
                        ## The bucardo_track table will be inserted to later on

                        if ($sync->{does_target_makedelta_triggers}{$targetdb}) {
                            $targetdbh->do(q{SET session_replication_role = 'replica'});
                        }
                        $self->glog("Total makedelta rows added for $S.$T on $targetdb: $count");
                    }

                    ## From here on out, we're making changes on the target that may trigger an exception
                    ## Thus, if we have exception handling code, we create a savepoint to rollback to
                    if ($g->{has_exception_code}) {
                        $self->glog('Creating savepoint on target for exception handler(s)');
                        $targetdbh->pg_savepoint("bucardo_$$") or die qq{Savepoint creation failed for bucardo_$$};
                    }

                    ## This label is solely to localize the DIE signal handler
                  LOCALDIE: {

                        ## Temporarily override our kid-level handler due to the eval
                        local $SIG{__DIE__} = sub {};

                        ## Everything before this point should work, so we delay the eval until right before
                        ##   our first (non-makedelta) data changes on the target
                        eval {

                            ## Delete any of these rows that may exist on the target
                            ## If rows were deleted from source, we are also deleting from target
                            ## If rows were inserted to source, they won't be on the target anyway
                            ## If rows were updated on source, we'll insert later (update = delete + insert)
                            $self->glog(qq{Deleting rows from $S.$T});

                            ## If we've got a very large number of values, break the DELETEs into multiples
                            my @delchunks;
                            if (length $pkvals > 100_000) {
                                ## How many items in the IN () clause
                                my $deletebatch = 10_000;
                                my $dcount = 0;
                                my $delcount = 0;
                                for my $row (@$info) {
                                    my $inner = join ',' => map { s/\'/''/go; qq{'$_'}; } @$row;
                                    ## Put this group of pks into a temporary array
                                    $delchunks[$delcount] .= $g->{pkcols} > 1 ? "($inner)," : "$inner,";
                                    ## Once we reach out limit, start appending to the next bit of the array
                                    if ($dcount++ >= $deletebatch) {
                                        $delcount++;
                                        $dcount = 0;
                                    }
                                }
                                $dcount = 1;
                                for my $chunk (@delchunks) {
                                    ## Remove the trailing comma
                                    chop $chunk;
                                    $SQL = "DELETE /* chunk $dcount */ FROM $S.$T WHERE $g->{pkeycols} IN ($chunk)";
                                    $self->glog("Deleting chunk $dcount");
                                    ($count = $targetdbh->do($SQL)) =~ s/0E0/0/o;
                                    $dmlcount{alldeletes}{target} += $dmlcount{D}{target}{$S}{$T} = $count;
                                    $dcount++;
                                }
                            }
                            else {
                                $SQL = "DELETE FROM $S.$T WHERE $g->{pkeycols} IN ($pkvals)";
                                ($count = $targetdbh->do($SQL)) =~ s/0E0/0/o;
                                $dmlcount{alldeletes}{target} += $dmlcount{D}{target}{$S}{$T} = $count;
                            }

                            ## COPY over all affected rows from source to target

                            ## Old versions of Postgres don't support "COPY (query)"
                            my ($srccmd,$temptable);
                            if (! $source_modern_copy) {
                                $temptable = "bucardo_tempcopy_$$";
                                $self->glog("Creating temporary table $temptable for copy on $S.$T, and savepoint bucardo_$$ along with it");
                                $sourcedbh->pg_savepoint("bucardo_$$");
                                $srccmd = "CREATE TEMP TABLE $temptable AS SELECT * FROM $S.$T WHERE $g->{pkeycols} IN ($pkvals)";

                                $sourcedbh->do($srccmd);
                                $srccmd = "COPY $temptable TO STDOUT";
                            }
                            elsif (! @delchunks) {
                                $srccmd = "COPY (SELECT * FROM $S.$T WHERE $g->{pkeycols} IN ($pkvals)) TO STDOUT";
                            }

                            my $tgtcmd = "COPY $S.$T FROM STDIN";
                            $targetdbh->do($tgtcmd);
                            my $buffer = '';
                            $self->glog(qq{Begin COPY to $S.$T});

                            if ($source_modern_copy and @delchunks) {
                                my $dcount = 1;
                                for my $chunk (@delchunks) {
                                    $srccmd = "COPY /* chunk $dcount */ (SELECT * FROM $S.$T WHERE $g->{pkeycols} IN ($chunk)) TO STDOUT";
                                    $sourcedbh->do($srccmd);
                                    $self->glog("Copying chunk $dcount");
                                    $dcount++;
                                    while ($sourcedbh->pg_getcopydata($buffer) >= 0) {
                                        $targetdbh->pg_putcopydata($buffer);
                                    }
                                }
                            }
                            else {
                                $sourcedbh->do($srccmd);
                                while ($sourcedbh->pg_getcopydata($buffer) >= 0) {
                                    $targetdbh->pg_putcopydata($buffer);
                                }
                            }

                            $targetdbh->pg_putcopyend();
                            $self->glog(qq{End COPY to $S.$T});
                            $dmlcount{allinserts}{target} += $dmlcount{I}{target}{$S}{$T} = @$info;

                            if (! $source_modern_copy) {
                                $self->glog("Dropping temporary table $temptable");
                                $sourcedbh->do("DROP TABLE $temptable");
                            }

                            ## If we disabled the indexes earlier, flip them on and run a REINDEX
                            if ($hasindex) {
                                $self->glog("Re-enabling indexes for table $S.$T on $targetdb");
                                $SQL = "UPDATE pg_class SET relhasindex = 't' WHERE oid = $toid";
                                $targetdbh->do($SQL);
                                $self->glog("Reindexing table $S.$T on $targetdb");
                                $targetdbh->do("REINDEX TABLE $S.$T");
                            }

                        }; ## end of eval

                    } ## end of LOCALDIE label: die will now revert to its previous behavior

                    ## If we failed the eval, and have no exception code, let the kid handle
                    ##   the exception as it normally would
                    if (!$g->{has_exception_code}) {
                        if ($@) {
                            chomp $@;
                            (my $err = $@) =~ s/\n/\\n/g;
                            $self->glog("Warning! Aborting due to exception for $S.$T:$pkval Error was $err");
                            die $@;
                        }
                    }
                    elsif ($@) {
                        chomp $@;
                        (my $err = $@) =~ s/\n/\\n/g;
                        $self->glog("Exception caught: $err");

                        ## Bail if we've already tried to handle this goat via an exception
                        if ($g->{exceptions} > 1) {
                            $self->glog("Warning! Exception custom code did not work for $S.$T:$pkval");
                            die qq{Error: too many exceptions to handle for $S.$T:$pkval};
                        }

                        ## Time to let the exception handling custom code do its work
                        ## First, we rollback any changes we've made on the target
                        $self->glog("Rolling back to target savepoint, due to database error: $err");
                        $targetdbh->pg_rollback_to("bucardo_$$");
                        if (! $source_modern_copy) {
                            # Also roll back to source savepoint, so we can try
                            # creating the temp table again
                            $self->glog('Rolling back to source savepoint as well, to remove temp table');
                            $sourcedbh->pg_rollback_to("bucardo_$$");
                        }

                        ## Now run one or more exception handlers
                        my $runagain = 0;
                        for my $code (@{$g->{code_exception}}) {
                            $self->glog("Trying exception code $code->{id}: $code->{name}");
                            my $result = run_kid_custom_code($code, 'strict', $pushdelta_attempts);
                            if ($result eq 'next') {
                                $self->glog('Going to next available exception code');
                                next;
                            }

                            ## A request to redo the entire sync
                            ## Note that 'redo' always rolls back both source and target, so we don't have to do it here
                            ## It also cleans up the q table and sends a sync done NOTIFY
                            if ($result eq 'redo') {
                                $self->glog('Exception handler requested redoing the entire sync');
                                redo KID;
                            }

                            ## A request to run the same goat again.
                            if ($input->{runagain}) {
                                $self->glog('Exception handler thinks we can try again');
                                $runagain = 1;
                                last;
                            }
                        }

                        ## If not running again, we simply give up and throw an exception to the kid
                        if (!$runagain) {
                            $self->glog('No exception handlers were able to help, so we are bailing out');
                            die qq{No exception handlers were able to help, so we are bailing out\n};
                        }

                        ## The custom code wants to try again

                        ## Make sure the database connections are still clean
                        my $sourceping = $sourcedbh->ping();
                        if ($sourceping !~ /^[13]$/o) {
                            $self->glog("Warning! Source ping after exception handler was $sourceping");
                        }
                        my $targetping = $targetdbh->ping();
                        if ($targetping !~ /^[13]$/o) {
                            $self->glog("Warning! Target ping after exception handler was $targetping");
                        }

                        ## As the bucardo_delta and source rows may have changed, we need to reset the counts
                        ##   and pull a fresh copy of the interesting rows from the database
                        $deltacount{allsource} -= $deltacount{source}{$S}{$T};
                        $deltacount{allsource} += $deltacount{source}{$S}{$T} = $sth{source}{$g}{getdelta}->execute();

                        ## Now jump back and try this goat again!
                        redo PUSHDELTA_SAVEPOINT;

                    } ## end of handled exception
                    else {
                        ## Got exception handlers, but no exceptions, so reset the count:
                        $g->{exceptions} = 0;
                    }

                } ## end of PUSHDELTA_SAVEPONT

            } ## end each goat

            $self->glog("Pushdelta counts: deletes=$dmlcount{alldeletes}{target} inserts=$dmlcount{allinserts}{target}");

        } ## end pushdelta

        ## SWAP
        if ($synctype eq 'swap') {


            ## Do each table in turn, ordered by descending priority and ascending id
            for my $g (@$goatlist) {

                ($S,$T) = ($g->{safeschema},$g->{safetable});

                if ($g->{reltype} eq 'sequence') {
                    my $action = 0; ## 0 = skip, 1 = source->target, 2 = target->source
                    $g->{tempschema} = {};
                    my $SEQUENCESQL = "SELECT last_value, is_called FROM $S.$T";
                    if (exists $g->{code_conflict}) {
                        $self->glog('No support for custom conflict handlers for sequences yet!');
                    }
                    else {
                        my $sc = $g->{standard_conflict};
                        if ('skip' eq $sc) {
                            $action = 0;
                        }
                        elsif ('source' eq $sc) {
                            $action = 1;
                        }
                        elsif ('target' eq $sc) {
                            $action = 2;
                        }
                        elsif ('lowest' eq $sc or 'highest' eq $sc) {
                            ($g->{tempschema}{s}{lastval},$g->{tempschema}{s}{iscalled}) =
                                @{$sourcedbh->selectall_arrayref($SEQUENCESQL)->[0]};
                            ($g->{tempschema}{t}{lastval},$g->{tempschema}{t}{iscalled}) =
                                @{$targetdbh->selectall_arrayref($SEQUENCESQL)->[0]};
                            if ($g->{tempschema}{s}{lastval} > $g->{tempschema}{t}{lastval}) {
                                $action = 'lowest' eq $sc ? 2 : 1;
                            }
                            elsif ($g->{tempschema}{s}{lastval} < $g->{tempschema}{t}{lastval}) {
                                $action = 'lowest' eq $sc ? 1 : 2;
                            }
                            else {
                                $action = 0;
                            }
                        }
                        else {
                            die "Unknown conflict type for sequence: $sc\n";
                        }
                    }

                    if (0 == $action) {
                        $self->glog("No action taken for sequence $S.$T");
                        next;
                    }

                    ## Internal note so we know things have changed
                    $deltacount{sequences}++;

                    ## Get the last seen value
                    my $LASTSEQUENCESQL = 'SELECT value, iscalled FROM bucardo.bucardo_sequences WHERE tablename = ? AND syncname = ?';

                    ## Source wins - copy its value to the target
                    if (1 == $action) {
                        $self->glog("Copying value of $S.$T from source to target");

                        if (! exists $g->{tempschema}{s}) {
                            ($g->{tempschema}{s}{lastval},$g->{tempschema}{s}{iscalled}) =
                                @{$sourcedbh->selectall_arrayref($SEQUENCESQL)->[0]};
                        }

                        my $lastval = $g->{tempschema}{s}{lastval};
                        my $iscalled = $g->{tempschema}{s}{iscalled};

                        ## Has it changed since last visit?
                        $sth = $sourcedbh->prepare($LASTSEQUENCESQL);
                        $count = $sth->execute($g->{oid}, $sync->{name});
                        my $newval = 0;
                        if ($count < 1) {
                            $newval = 1; ## Never before seen, so add to the table
                            $sth->finish();
                        }
                        else {
                            my ($oldval,$oldcalled) = @{$sth->fetchall_arrayref()->[0]};
                            if ($oldval != $lastval) {
                                $newval = 2; ## Value has changed
                            }
                            elsif ($oldcalled ne $iscalled) {
                                $newval = 3; ## is_called has changed
                            }
                        }
                        ## Has not changed, so we simply move on to the next goat
                        next if ! $newval;

                        ## Apply to the target
                        $self->glog("Setting sequence $S.$T on target to value of $lastval, is_called is $iscalled");
                        $SQL = "SELECT setval('$S.$T', $lastval, '$iscalled')";
                        $targetdbh->do($SQL);

                        ## Save to the target's internal table
                        ## Rather than worry about upserts, we'll just delete/insert every time
                        $SQL = 'DELETE FROM bucardo.bucardo_sequences WHERE tablename = ? AND syncname = ?';
                        $sth = $sourcedbh->prepare($SQL);
                        $sth->execute($g->{targetoid}{$targetdb}, $sync->{name});
                        $SQL = 'INSERT INTO bucardo.bucardo_sequences (tablename, syncname, value, iscalled) VALUES (?,?,?,?)';
                        $sth = $sourcedbh->prepare($SQL);
                        $sth->execute($g->{targetoid}{$targetdb},$sync->{name},$lastval,$iscalled);

                        ## Internal note so we know things have changed
                        $deltacount{sequences}++;

                        ## Done: jump to the next goat
                        next;
                    }

                    ## Target wins - copy its value to the source
                    $self->glog("Copying value of $S.$T from target to source");

                    if (! exists $g->{tempschema}{t}) {
                        ($g->{tempschema}{t}{lastval},$g->{tempschema}{t}{iscalled}) =
                            @{$targetdbh->selectall_arrayref($SEQUENCESQL)->[0]};
                    }

                    my $lastval = $g->{tempschema}{t}{lastval};
                    my $iscalled = $g->{tempschema}{t}{iscalled};

                    ## Has it changed since last visit?
                    $sth = $sourcedbh->prepare($LASTSEQUENCESQL);
                    $count = $sth->execute($g->{oid}, $sync->{name});
                    my $newval = 0;
                    if ($count < 1) {
                        $newval = 1; ## Never before seen, so add to the table
                        $sth->finish();
                    }
                    else {
                        my ($oldval,$oldcalled) = @{$sth->fetchall_arrayref()->[0]};
                        if ($oldval != $lastval) {
                            $newval = 2; ## Value has changed
                        }
                        elsif ($oldcalled ne $iscalled) {
                            $newval = 3; ## is_called has changed
                        }
                    }
                    ## Has not changed, so we simply move on to the next goat
                    next if ! $newval;

                    ## Apply to the source
                    $self->glog("Setting sequence $S.$T on source to value of $lastval, is_called is $iscalled");
                    $SQL = "SELECT setval('$S.$T', $lastval, '$iscalled')";
                    $sourcedbh->do($SQL);

                    ## Save to the source's internal table
                    ## Rather than worry about upserts, we'll just delete/insert every time
                    $SQL = 'DELETE FROM bucardo.bucardo_sequences WHERE tablename = ? AND syncname = ?';
                    $sth = $targetdbh->prepare($SQL);
                    $sth->execute($g->{oid}, $sync->{name});
                    $SQL = 'INSERT INTO bucardo.bucardo_sequences (tablename, syncname, value, iscalled) VALUES (?,?,?,?)';
                    $sth = $targetdbh->prepare($SQL);
                    $sth->execute($g->{oid},$sync->{name},$lastval,$iscalled);

                    ## Proceed to the next goat
                    next;
                }

                ## Skip if neither source not target has changes for this table
                next unless $deltacount{source}{$S}{$T} or $deltacount{target}{$S}{$T};

                ## Use copies as rollback/redo may change the originals
                ## TODO: pushdelta doesn't need this, so why do we?
                $deltacount{src2}{$S}{$T} = $deltacount{source}{$S}{$T};
                $deltacount{tgt2}{$S}{$T} = $deltacount{target}{$S}{$T};

                ## Get target table's oid, set index disable requests to zero
                my ($toid,$hasindex_src,$hasindex_tgt) = ($g->{targetoid}{$targetdb},0,0);

                ## If requested, turn off indexes before making changes
                if ($g->{rebuild_index} == 2) {
                    $SQL = "SELECT relhasindex FROM pg_class WHERE oid = $g->{oid}";
                    $hasindex_src = $sourcedbh->selectall_arrayref($SQL)->[0][0];
                    if ($hasindex_src) {
                        $self->glog("Turning off indexes for $S.$T on $sourcedb");
                        $SQL = "UPDATE pg_class SET relhasindex = 'f' WHERE oid = $toid";
                        $sourcedbh->do($SQL);
                    }
                    $SQL = "SELECT relhasindex FROM pg_class WHERE oid = $toid";
                    $hasindex_tgt = $targetdbh->selectall_arrayref($SQL)->[0][0];
                    if ($hasindex_tgt) {
                        $self->glog("Turning off indexes for $S.$T on $targetdb");
                        $SQL = "UPDATE pg_class SET relhasindex = 'f' WHERE oid = $toid";
                        $targetdbh->do($SQL);
                    }
                }

                ## Keep track of how many times this goat has handled an exception
                $g->{exceptions} = 0;

                ## How many times have we done the loop below?
                my $swap_attempts = 0;

                ## This is where we want to 'rewind' to on a handled exception
                ## We choose this point as its possible the custom code has a different getdelta result
              SWAP_SAVEPOINT: {

                $swap_attempts++;

                ## Reset all IUD counters to 0
                $dmlcount{I}{source}{$S}{$T} = $dmlcount{U}{source}{$S}{$T} = $dmlcount{D}{source}{$S}{$T} =
                $dmlcount{I}{target}{$S}{$T} = $dmlcount{U}{target}{$S}{$T} = $dmlcount{D}{target}{$S}{$T} = 0;

                ## The actual data from the large join of bucardo_delta + original table for source and target
                my ($info1,$info2)= ({},{});

                ## Single PK cols are easy, we can just use _hashref
                if ($g->{pkcols} == 1) {
                     $deltacount{src2}{$S}{$T} >= 1 and $info1 = $sth{source}{$g}{getdelta}->fetchall_hashref('BUCARDO_ID');
                     $deltacount{tgt2}{$S}{$T} >= 1 and $info2 = $sth{target}{$g}{getdelta}->fetchall_hashref('BUCARDO_ID');
                }
                else {
                    ## For multi-col PKs, we join all pk values together into a single scalar
                    if ($deltacount{src2}{$S}{$T} >= 1) {
                        for my $row (@{$sth{source}{$g}{getdelta}->fetchall_arrayref({})}) {
                            my $key = $row->{BUCARDO_ID};
                            push @{$row->{BUCARDO_PKVALS}} => $row->{BUCARDO_ID};
                            for (2..$g->{pkcols}) {
                                $key .= '|' . $row->{"BUCARDO_ID$_"};
                                push @{$row->{BUCARDO_PKVALS}} => $row->{"BUCARDO_ID$_"};
                            }
                            $info1->{$key} = $row;
                        }
                    }
                    if ($deltacount{tgt2}{$S}{$T} >= 1) {
                        for my $row (@{$sth{target}{$g}{getdelta}->fetchall_arrayref({})}) {
                            my $key = $row->{BUCARDO_ID};
                            push @{$row->{BUCARDO_PKVALS}} => $row->{BUCARDO_ID};
                            for (2..$g->{pkcols}) {
                                $key .= '|' . $row->{"BUCARDO_ID$_"};
                                push @{$row->{BUCARDO_PKVALS}} => $row->{"BUCARDO_ID$_"};
                            }
                            $info2->{$key} = $row;
                        }
                    }
                }

                ## Store this info away for use by a custom code hook
                if ($sync->{need_rows}) {
                    $rows_for_custom_code->{$S}{$T} =
                        {
                         ## hashref of hashrefs of individual rows from giant join:
                         source    => $info1,
                         target    => $info2,
                         ## arrayrefs:
                         pkeyname  => $g->{pkey},
                         qpkeyname => $g->{qpkey},
                         pkeytype  => $g->{pkeytype},
                         };
                }

                ## Go through all keys and resolve any conflicts. Bitmap action:
                ## 1 = Add source row to the target db
                ## 2 = Add target row to the source db
                ## 4 = Add source row to the source db
                ## 8 = Add target row to the target db

                my $qnamepk = $g->{qpkeyjoined};
                ## TODO: Consider removing the sort for speed on large sets
                ## First, we loop through all changed row on the source
                for my $temp_pkval (sort keys %$info1) {
                    $pkval = $temp_pkval;
                    ## No problem if it only changed on the source
                    if (! exists $info2->{$pkval}) {
                        $self->glog("No conflict, source only for $S.$T.$qnamepk: $pkval");
                        $info1->{$pkval}{BUCARDO_ACTION} = 1; ## copy source to target
                        next;
                    }
                    ## At this point, it's on both source and target. Don't panic.

                    ## Write detailed information to the conflict_file if requested
                    if ($config{log_conflict_details}) {
                        my $header = "$g->{pkey},";
                        my $srcrow = "$pkval,";
                        my $tgtrow = "$pkval,";
                        for my $column (@{$g->{cols}}) {
                            $header .= $column . ',';
                            $srcrow .= $info1->{$pkval}{$column} . ',';
                            $tgtrow .= $info2->{$pkval}{$column} . ',';
                        }
                        $self->clog("conflict,$S,$T");
                        $self->clog('timestamp,' . localtime());
                        $self->clog('header,' . substr($header, 0, -1));
                        $self->clog('source,' . substr($srcrow, 0, -1));
                        $self->clog('target,' . substr($tgtrow, 0, -1));
                        $self->glog("Logged details of conflict to $config{log_conflict_file}");
                    }

                    ## Standard conflict handlers don't need info to make a decision
                    if (!exists $g->{code_conflict}) {
                        my $sc = $g->{standard_conflict};
                        $self->glog(qq{Conflict detected for $S.$T:$pkval. Using standard conflict "$sc"});
                        if ('source' eq $sc) {
                            $info1->{$pkval}{BUCARDO_ACTION} = 1; ## copy source to target
                        }
                        elsif ('target' eq $sc) {
                            $info1->{$pkval}{BUCARDO_ACTION} = 2; ## copy target to source
                        }
                        elsif ('random' eq $sc) {
                            $info1->{$pkval}{BUCARDO_ACTION} = rand 2 > 1 ? 1 : 2;
                        }
                        elsif ('abort' eq $sc) {
                            die qq{Aborting sync $syncname due to conflict for $S:$T:$pkval\n};
                        }
                        elsif ('latest' eq $sc) {
                            if (!exists $sth{sc_latest_src}{$g->{pkcols}}) {
                                $SQL =
                                    q{SELECT extract(epoch FROM MAX(txntime)) FROM bucardo.bucardo_delta WHERE tablename=? AND rowid=?};
                                for (2..$g->{pkcols}) {
                                    $SQL .= " AND rowid$_=?";
                                }
                                $sth{sc_latest_src}{$g->{pkcols}} = $sourcedbh->prepare($SQL);
                                $sth{sc_latest_tgt}{$g->{pkcols}} = $targetdbh->prepare($SQL);
                            }
                            if ($g->{pkcols} > 1) {
                                $sth{sc_latest_src}{$g->{pkcols}}->execute($g->{oid},@{$info1->{$pkval}{BUCARDO_PKVALS}});
                            }
                            else {
                                $sth{sc_latest_src}{$g->{pkcols}}->execute($g->{oid},$pkval);
                            }
                            my $srctime = $sth{sc_latest_src}{$g->{pkcols}}->fetchall_arrayref()->[0][0];
                            if ($g->{pkcols} > 1) {
                                $sth{sc_latest_tgt}{$g->{pkcols}}->execute($toid,@{$info2->{$pkval}{BUCARDO_PKVALS}});
                            }
                            else {
                                $sth{sc_latest_tgt}{$g->{pkcols}}->execute($toid,$pkval);
                            }
                            my $tgttime = $sth{sc_latest_tgt}{$g->{pkcols}}->fetchall_arrayref()->[0][0];
                            $self->glog(qq{Delta source time: $srctime Target time: $tgttime});
                            $info1->{$pkval}{BUCARDO_ACTION} = $srctime >= $tgttime ? 1 : 2;
                        } ## end 'latest'
                        else {
                            die qq{Unknown standard conflict for sync $syncname on $T.$S: $sc\n};
                        }
                        next;
                    } ## end standard conflict

                    ## Custom conflict handler. Gather up info to pass to it.
                    %rowinfo = (
                        sourcerow  => $info1->{$pkval},
                        targetrow  => $info2->{$pkval},
                        schema     => $S,
                        table      => $T,
                        pkeyname   => $g->{pkey},
                        pkeytype   => $g->{pkeytype},
                        pkey       => $pkval,
                        action     => 0,
                        );

                    ## Run the custom conflict handler(s)
                    for my $code (@{$g->{code_conflict}}) {
                        my $result = run_kid_custom_code($code, 'strict');
                        if ($result eq 'next') {
                            $self->glog('Going to next available conflict code');
                            next;
                        }
                        if ($result eq 'redo') { ## ## redo rollsback source and target
                            $self->glog('Custom conflict handler has requested we redo this sync');
                            redo KID if $kidsalive;
                            last KID;
                        }

                        $self->glog("Conflict handler action: $rowinfo{action}");

                        ## Check for conflicting actions
                        if ($rowinfo{action} & 2 and $rowinfo{action} & 4) {
                            $self->glog('Warning! Conflict handler cannot return 2 and 4. Ignoring 4');
                            $rowinfo{action} -= 4;
                        }
                        if ($rowinfo{action} & 1 and $rowinfo{action} & 8) {
                            $self->glog('Warning! Conflict handler cannot return 1 and 8. Ignoring 8');
                            $rowinfo{action} -= 8;
                        }

                        $info1->{$pkval}{BUCARDO_ACTION} = $rowinfo{action};

                        last;

                    } ## end custom conflict

                } ## end each key in source delta list

                ## Since we've already handled conflicts, simply mark "target only" rows
                for my $tpkval (keys %$info2) {
                    next if exists $info1->{$tpkval};
                    $self->glog("No conflict, target only for $S.$T.$qnamepk: $tpkval");
                    $info1->{$tpkval}{BUCARDO_ACTION} = 2; ## copy target to source
                    $info1->{$tpkval}{BUCARDO_PKVALS} ||= $info2->{$tpkval}{BUCARDO_PKVALS};
                }

                ## Give some summary statistics
                my %actionstat;
                for (values %$info1) {
                    $actionstat{$_->{BUCARDO_ACTION}}++ if exists $_->{BUCARDO_ACTION};
                }
                $self->glog('Action summary: ' . join ' ' => map { "$_:$actionstat{$_}" } sort keys %actionstat);

                ## For each key, either mark as deleted, or mark as needing to be checked
                my (@srcdelete,@tgtdelete,@srccheck,@tgtcheck);

                ## Used for makedelta:
                my (@srcdelete2,@tgtdelete2);

                ## How many rows are we upserting?
                my $changecount = 0;

                ## We only need the first regardless of pkcols: it always null or not null
                my $namepk = $g->{pkey}[0];

                for my $temp_pkval (keys %$info1) {
                    $pkval = $temp_pkval;
                    my $action = $info1->{$pkval}{BUCARDO_ACTION};
                    if (! $action) {
                        $dmlcount{N}{source}{$S}{$T}++;
                        $dmlcount{N}{target}{$S}{$T}++;
                        $self->glog("No action for $S.$T:$pkval");
                        next;
                    }

                    ## We are manually building lists, so we may need to escape the pkeys
                    my @safepk;
                    if ($g->{pkcols} <= 1) {
                        if ($g->{pkeytype}[0] =~ /int$/o) {
                            push @safepk => $pkval;
                        }
                        else {
                            (my $safepkval = $pkval) =~ s/\'/''/go;
                            push @safepk => qq{'$safepkval'};
                        }
                    }
                    else {
                        $x=0;
                        for my $pk (@{$info1->{$pkval}{BUCARDO_PKVALS}}) {
                            if ($g->{pkeytype}[0] =~ /int$/o) {
                                push @safepk => $pk;
                            }
                            else {
                                (my $safepkval = $pk) =~ s/\'/''/go;
                                push @safepk => qq{'$safepkval'};
                            }
                        }
                    }

                    ## Delete from source if going to source and has been deleted
                    if (($action & 2 and ! defined $info2->{$pkval}{$namepk}) ## target to source
                     or ($action & 4 and ! defined $info1->{$pkval}{$namepk})) { ## source to source
                        push @srcdelete, \@safepk;
                        ## 1=source 2=target 3=both
                        if ($g->{does_source_makedelta}) {
                            if ($g->{pkcols} <= 1) {
                                push @srcdelete2, [$pkval];
                            }
                            else {
                                push @srcdelete2, $info1->{$pkval}{BUCARDO_PKEYS};
                            }
                        }
                        ## Strip out this action as done (2 and 4 are mutually exclusive)
                        $info1->{$pkval}{BUCARDO_ACTION} -= ($action & 2) ? 2 : 4;
                        $action = $info1->{$pkval}{BUCARDO_ACTION};
                    }

                    ## Delete from target if going to target and has been deleted
                    if (($action & 1 and ! defined $info1->{$pkval}{$namepk}) ## source to target
                     or ($action & 8 and ! defined $info2->{$pkval}{$namepk})) { ## target to target
                        push @tgtdelete, \@safepk;
                        ## 1=source 2=target 3=both
                        if ($g->{does_target_makedelta}) {
                            if ($g->{pkcols} <= 1) {
                                push @tgtdelete2, [$pkval];
                            }
                            else {
                                push @tgtdelete2, $info1->{$pkval}{BUCARDO_PKEYS};
                            }
                        }
                        ## Strip out this action as done (1 and 8 are mutually exclusive)
                        $info1->{$pkval}{BUCARDO_ACTION} -= ($action & 1) ? 1 : 8;
                        $action = $info1->{$pkval}{BUCARDO_ACTION};
                    }

                    next if ! $action; ## Stop if delete only

                    $changecount++;

                    ## If going from target to source, verify if it exists on source or not
                    if (($action & 2) and !defined $info1->{$pkval}{$namepk}) {
                        push @srccheck, \@safepk;
                    }

                    ## If going from source to target, verify it it exists on target or not
                    if (($action & 1) and !defined $info2->{$pkval}{$namepk}) {
                        push @tgtcheck, \@safepk;
                    }

                }

                ## Add in the makedelta rows as needed
                if ($g->{does_source_makedelta}) {
                    ## If makedelta is 2, we temporarily allow triggers and rules,
                    ## for cases when have them on bucardo_delta or bucardo_track
                    if ($sync->{does_source_makedelta_triggers}) {
                        $sourcedbh->do(q{SET session_replication_role = 'origin'});
                    }
                    for (@srcdelete2) {
                        $sth{source}{$g}{insertdelta}->execute($g->{oid},@$_);
                        $g->{source_makedelta_inserts}++;
                    }
                    if ($sync->{does_source_makedelta_triggers}) {
                        $sourcedbh->do(q{SET session_replication_role = 'replica'});
                    }
                }
                if ($g->{does_target_makedelta}) {
                    if ($sync->{does_target_makedelta_triggers}{$targetdb}) {
                        $targetdbh->do(q{SET session_replication_role = 'origin'});
                    }
                    for (@tgtdelete2) {
                        $sth{target}{$g}{insertdelta}->execute($toid,@$_);
                        $g->{target_makedelta_inserts}++;
                        $self->glog("Adding in target bucardo_delta row (delete) for $toid and $_");
                    }
                    if ($sync->{does_target_makedelta_triggers}{$targetdb}) {
                        $targetdbh->do(q{SET session_replication_role = 'replica'});
                    }
                }

                ## If we have exception handling code, create a savepoint to rollback to
                if ($g->{has_exception_code}) {
                    $self->glog('Creating savepoints on source and target for exception handler(s)');
                    $sourcedbh->pg_savepoint("bucardo_$$") or die qq{Savepoint creation failed for bucardo_$$};
                    $targetdbh->pg_savepoint("bucardo_$$") or die qq{Savepoint creation failed for bucardo_$$};
                }

                ## Do deletions in chunks
                if (! $g->{pkeycols}) {
                    $g->{pkeycols} = '';
                    $x=0;
                    for my $qpk (@{$g->{qpkey}}) {
                        $g->{pkeycols} .= sprintf '%s,', $g->{binarypkey}[$x] ? qq{ENCODE($qpk,'base64')} : $qpk;
                    }
                    chop $g->{pkeycols};
                    $g->{pkcols} > 1 and $g->{pkeycols} = "($g->{pkeycols})";
                }
                $SQL = $g->{pkeycols};

                $SQL = "DELETE FROM $S.$T WHERE $SQL IN";
                while (@srcdelete) {
                    $x=0;
                    my $list = '';
                  LOOP: {
                        my $row = shift @srcdelete;
                        last LOOP if ! defined $row or ! defined $row->[0];
                        if ($g->{pkcols} > 1) {
                            $list .= sprintf '(%s),' => join ',' => @$row;
                        }
                        else {
                            $list .= "$row->[0],";
                        }
                        last LOOP if $x++ >= $config{max_delete_clause};
                        redo LOOP;
                    }
                    chop $list;
                    if (length $list) {
                        $self->glog("Deleting from source: $SQL ($list)");
                        $dmlcount{D}{source}{$S}{$T} += $sourcedbh->do("$SQL ($list)");
                    }
                }
                if ($dmlcount{D}{source}{$S}{$T}) {
                    $self->glog(qq{Rows deleted from source "$S.$T": $dmlcount{D}{source}{$S}{$T}/$count});
                }

                while (@tgtdelete) {
                    $x=0;
                    my $list = '';
                  LOOP: {
                        my $row = shift @tgtdelete;
                        last LOOP if ! defined $row or ! defined $row->[0];
                        if ($g->{pkcols} > 1) {
                            $list .= sprintf '(%s),' => join ',' => @$row;
                        }
                        else {
                            $list .= "$row->[0],";
                        }
                        last LOOP if $x++ >= $config{max_delete_clause};
                        redo LOOP;
                    }
                    chop $list;
                    if (length $list) {
                        $self->glog("Deleting from target: $SQL ($list)");
                        $dmlcount{D}{target}{$S}{$T} += $targetdbh->do("$SQL ($list)");
                    }
                }
                if ($dmlcount{D}{target}{$S}{$T}) {
                    $self->glog(qq{Rows deleted from target "$S.$T": $dmlcount{D}{target}{$S}{$T}/$count});
                }
                ## Get authoritative existence information for all undefined keys
                ## Before this point, the lack of a matching record from the left join
                ## only tells us that the real row *might* exist.
                ## And upserts are too expensive here :)
                $x=0;
                my $list = '';
                my $pre = '';
                for my $q (@{$g->{qpkey}}) {
                    $list .= sprintf '%s,',
                        $g->{binarypkey}[$x++] ? "ENCODE($q,'base64')" : $q;
                    $pre .= sprintf q{%s||'|'||},
                        $g->{binarypkey}[$x++] ? "ENCODE($q,'base64')" : "${q}::text";
                }
                ## We are pulling back a combined scalar, not necessarily the exact primary key
                $pre =~ s/.......$/ AS id/;
                $list =~ s/,$//;
                $SQL = "SELECT $pre FROM $S.$T WHERE ($list) IN ";
                while (@srccheck) {
                    $x=0;
                    $list = '';
                  LOOP: {
                        my $row = shift @srccheck;
                        last LOOP if ! defined $row or ! defined $row->[0];
                        if ($g->{pkcols} > 1) {
                            $list .= sprintf '(%s),' => join ',' => @$row;
                        }
                        else {
                            $list .= "$row->[0],";
                        }
                        $list =~ s/,$//;
                        last LOOP if $x++ >= $config{max_select_clause};
                        for (@{$sourcedbh->selectall_arrayref("$SQL ($list)")}) {
                            $info1->{$_->[0]}{$namepk} = 1;
                        }
                    }
                }
                while (@tgtcheck) {
                    $x=0;
                    $list = '';
                  LOOP: {
                        my $row = shift @tgtcheck;
                        last LOOP if ! defined $row or ! defined $row->[0];
                        if ($g->{pkcols} > 1) {
                            $list .= sprintf '(%s),' => join ',' => @$row;
                        }
                        else {
                            $list .= "$row->[0],";
                        }
                        $list =~ s/,$//;
                        last LOOP if $x++ >= $config{max_select_clause};
                        for (@{$targetdbh->selectall_arrayref("$SQL ($list)")}) {
                            $info2->{$_->[0]}{$namepk} = 1;
                        }
                    }
                }

                ## Do inserts and updates on source and target
                $pkval = 0;
                my $row = 0;
              PKEY: for my $temp_pkval (sort keys %$info1) {
                    $pkval = $temp_pkval;
                    my $action = $info1->{$pkval}{BUCARDO_ACTION};

                    if (! $action) {
                        $self->glog("No action for $S.$T:$pkval\n");
                        next;
                    }

                    ## Eight possibilities:
                    ## From info1: update source, insert source, update target, insert target
                    ## From info2: update source, insert source, update target, insert target

                    ## Populate arrays only if we need them
                    my (@srcrow,@tgtrow);
                    if ($action & 1 or $action & 4) { ## source to target / source to source
                        @srcrow = @{$info1->{$pkval}}{@{$g->{cols}}};
                    }
                    if ($action & 2 or $action & 8) { ## target to source / target to target
                        @tgtrow = @{$info2->{$pkval}}{@{$g->{cols}}};
                    }

                    $row++;
                    my $prefix = "[$row/$changecount] $S.$T";

                  GENX: {
                        ## Temporarily override our kid-level handler due to the eval
                        local $SIG{__DIE__} = sub {};

                        ## This eval block needed for potential error handling
                        eval {

                            my $srcpks = $g->{pkcols} <= 1 ? [$pkval] : $info1->{$pkval}{BUCARDO_PKVALS};
                            my $tgtpks = $g->{pkcols} <= 1 ? [$pkval] : $info2->{$pkval}{BUCARDO_PKVALS};
                            if ($action & 1) { ## Source to target
                                if (defined $info2->{$pkval}{$namepk}) {
                                    $self->glog("$prefix UPDATE source to target pk $pkval");
                                    $count = $sth{target}{$g}{updaterow}->execute(@srcrow,@$srcpks);
                                    $dmlcount{U}{target}{$S}{$T}++;
                                }
                                else {
                                    $self->glog("$prefix INSERT source to target pk $pkval");
                                    $count = $sth{target}{$g}{insertrow}->execute(@$srcpks,@srcrow);
                                    $dmlcount{I}{target}{$S}{$T}++;
                                }
                            }
                            if ($action & 2) { ## Target to source
                                if (defined $info1->{$pkval}{$namepk}) {
                                    $self->glog("$prefix UPDATE target to source pk $pkval");
                                    $count = $sth{source}{$g}{updaterow}->execute(@tgtrow,@$tgtpks);
                                    $dmlcount{U}{source}{$S}{$T}++;
                                }
                                else {
                                    $self->glog("$prefix INSERT target to source pk $pkval");
                                    $count = $sth{source}{$g}{insertrow}->execute(@$tgtpks,@tgtrow);
                                    $dmlcount{I}{source}{$S}{$T}++;
                                }
                            }
                            if ($action & 4) { ## Source to source
                                if (defined $info1->{$pkval}{$namepk}) {
                                    $self->glog("$prefix UPDATE source to source pk $pkval");
                                    $count = $sth{source}{$g}{updaterow}->execute(@srcrow,@$srcpks);
                                    $dmlcount{U}{source}{$S}{$T}++;
                                }
                                else {
                                    $self->glog("$prefix INSERT source to source pk $pkval");
                                    $count = $sth{source}{$g}{insertrow}->execute(@$srcpks,@srcrow);
                                    $dmlcount{I}{source}{$S}{$T}++;
                                }
                            }
                            if ($action & 8) { ## Target to target
                                if (defined $info2->{$pkval}{$namepk}) {
                                    $self->glog("$prefix UPDATE target to target pk $pkval");
                                    $count = $sth{target}{$g}{updaterow}->execute(@tgtrow,@$tgtpks);
                                    $dmlcount{U}{target}{$S}{$T}++;
                                }
                                else {
                                    $self->glog("$prefix INSERT target to target pk $pkval");
                                    $count = $sth{target}{$g}{insertrow}->execute(@$tgtpks,@tgtrow);
                                    $dmlcount{I}{target}{$S}{$T}++;
                                }
                            }
                            if ($g->{does_source_makedelta}) {
                                if ($sync->{does_source_makedelta_triggers}) {
                                    $sourcedbh->do(q{SET session_replication_role = 'origin'});
                                }
                                if ($action & 2 or $action & 4) {
                                    $sth{source}{$g}{insertdelta}->execute($g->{oid},@$srcpks);
                                    $g->{source_makedelta_inserts}++
                                }
                                if ($sync->{does_source_makedelta_triggers}) {
                                    $sourcedbh->do(q{SET session_replication_role = 'replica'});
                                }
                            }
                            if ($g->{does_target_makedelta}) {
                                if ($sync->{does_target_makedelta_triggers}{$targetdb}) {
                                    $targetdbh->do(q{SET session_replication_role = 'origin'});
                                }
                                if ($action & 1 or $action & 8) {
                                    $sth{target}{$g}{insertdelta}->execute($toid,@$tgtpks);
                                    $g->{target_makedelta_inserts}++
                                }
                                if ($sync->{does_target_makedelta_triggers}{$targetdb}) {
                                    $targetdbh->do(q{SET session_replication_role = 'replica'});
                                }
                            }
                        }; ## end eval block
                    } ## end GENX block

                    if (!$g->{has_exception_code}) {
                        if ($@) {
                            chomp $@;
                            (my $err = $@) =~ s/\n/\\n/g;
                            $self->glog("Warning! Aborting due to exception for $S.$T.$qnamepk: $pkval Error was $err");
                            die $@;
                        }
                    }
                    elsif ($@) {
                        chomp $@;
                        (my $err = $@) =~ s/\n/\\n/g;
                        $self->glog("Exception caught: $err");

                        ## Bail if we've called one exception for every (original) row
                        ## TODO: Develop better metrics here
                        if ($g->{exceptions} > $deltacount{source}{$S}{$T} and $g->{exceptions} > $deltacount{target}{$S}{$T}) {
                            $self->glog("Warning! Exception count=$g->{exceptions}, source=$deltacount{source}{$S}{$T}, target=$deltacount{target}{$S}{$T}");
                            die qq{Error: too many exceptions to handle for $S.$T:$pkval};
                        }

                        ## Prepare information to hand to our exception handler
                        %rowinfo = (
                                    sourcerow    => $info1->{$pkval},
                                    targetrow    => $info2->{$pkval},
                                    schema       => $S,
                                    table        => $T,
                                    pkeyname     => $g->{pkey},
                                    qpkeyname    => $g->{qpkey},
                                    pkeytype     => $g->{pkeytype},
                                    pkey         => $pkval,
                                    action       => 0,
                                    dbi_error    => $err,
                                    source_error => $sourcedbh->err ? 1 : 0,
                                    target_error => $targetdbh->err ? 1 : 0,
                                );

                        $self->glog("Rolling back to savepoints, due to database error: $err");
                        $sourcedbh->pg_rollback_to("bucardo_$$");
                        $targetdbh->pg_rollback_to("bucardo_$$");

                        ## Run the exception handler(s)
                        my $runagain = 0;
                        for my $code (@{$g->{code_exception}}) {
                            $self->glog("Trying exception code $code->{id}: $code->{name}");
                            my $result = run_kid_custom_code($code, 'strict', $swap_attempts);
                            if ($result eq 'next') {
                                $self->glog('Going to next available exception code');
                                next;
                            }
                            if ($result eq 'redo') { ## redo rollsback source and target
                                $self->glog('Exception handler requested redoing the sync');
                                redo KID;
                            }
                            if ($input->{runagain}) {
                                $self->glog('Exception handler thinks we can try again');
                                $runagain = 1;
                                last;
                            }
                        }

                        if (!$runagain) {
                            $self->glog('No exception handlers were able to help, so we are bailing out');
                            die qq{No exception handlers were able to help, so we are bailing out\n};
                        }

                        ## Make sure the database connections are still clean
                        my $sourceping = $sourcedbh->ping();
                        if ($sourceping !~ /^[13]$/o) {
                            $self->glog("Warning! Source ping after exception handler was $sourceping");
                        }
                        my $targetping = $targetdbh->ping();
                        if ($targetping !~ /^[13]$/o) {
                            $self->glog("Warning! Target ping after exception handler was $targetping");
                        }

                        ## This table gets another chance
                        $deltacount{src2}{$g} = $sth{source}{$g}{getdelta}->execute();
                        $deltacount{tgt2}{$g} = $sth{target}{$g}{getdelta}->execute();

                        $g->{exceptions}++;
                        redo SWAP_SAVEPOINT;

                    } ## end exception and savepointing
                } ## end each PKEY

                if ($g->{has_exception_code}) {
                    $sourcedbh->pg_release("bucardo_$$");
                    $targetdbh->pg_release("bucardo_$$");
                }

                $dmlcount{allinserts}{source} += $dmlcount{I}{source}{$S}{$T};
                $dmlcount{allupdates}{source} += $dmlcount{U}{source}{$S}{$T};
                $dmlcount{alldeletes}{source} += $dmlcount{D}{source}{$S}{$T};
                $dmlcount{allinserts}{target} += $dmlcount{I}{target}{$S}{$T};
                $dmlcount{allupdates}{target} += $dmlcount{U}{target}{$S}{$T};
                $dmlcount{alldeletes}{target} += $dmlcount{D}{target}{$S}{$T};

            } ## end SWAP_SAVEPOINT

                if ($hasindex_src) {
                    $SQL = "UPDATE pg_class SET relhasindex = 't' WHERE oid = $g->{oid}";
                    $sourcedbh->do($SQL);
                    $self->glog("Reindexing table $S.$T on $sourcedb");
                    $sourcedbh->do("REINDEX TABLE $S.$T");
                }
                if ($hasindex_tgt) {
                    $SQL = "UPDATE pg_class SET relhasindex = 't' WHERE oid = $toid";
                    $targetdbh->do($SQL);
                    $self->glog("Reindexing table $S.$T on $targetdb");
                    $targetdbh->do("REINDEX TABLE $S.$T");
                }

            } ## end each goat

        } ## end swap

        ## Update bucardo_track table so that the bucardo_delta rows we just processed
        ##  are marked as "done" and ignored by subsequent runs
        ## We also rely on this section to do makedelta related bucardo_track inserts
        if ($synctype eq 'pushdelta' or $synctype eq 'swap') {
            for my $g (@$goatlist) {
                next if $g->{reltype} ne 'table';
                ($S,$T) = ($g->{safeschema},$g->{safetable});
                delete $g->{rateinfo};
                ## Gather up our rate information - just store for now, we can write it after the commits
                if ($deltacount{source}{$S}{$T} and $sync->{track_rates}) {
                    $self->glog('Gathering source rate information');
                    my $sth = $sth{source}{$g}{deltarate};
                    $count = $sth->execute();
                    $g->{rateinfo}{source} = $sth->fetchall_arrayref();
                }
                if ($deltacount{source}{$S}{$T} or $g->{source_makedelta_inserts}) {
                    $self->glog("Updating bucardo_track for $S.$T on $sourcedb", 6);
                    $sth{source}{$g}{track}->execute();
                }
                if ($deltacount{target}{$S}{$T} and $sync->{track_rates}) {
                    $self->glog('Gathering target rate information');
                    my $sth = $sth{target}{$g}{deltarate};
                    $count = $sth->execute();
                    $g->{rateinfo}{target} = $sth->fetchall_arrayref();
                }
                if ($deltacount{target}{$S}{$T} or $g->{target_makedelta_inserts}) {
                    $self->glog("Updating bucardo_track for $S.$T on $targetdb");
                    $sth{target}{$g}{track}->execute();
                }
            }
        }

        ## Run all 'before_trigger_enable' code
        for my $code (@{$sync->{code_before_trigger_enable}}) {
            my $result = run_kid_custom_code($code, 'strict');
            if ($result eq 'redo') { ## redo rollsback source and target
                redo KID if $kidsalive;
                last KID;
            }
        }

        if ($target_disable_trigrules ne 'replica') {
            $self->glog(q{Enabling triggers and rules on target via pg_class});
            $targetdbh->do($SQL{enable_trigrules});
        }
        if ($synctype eq 'swap' and $source_disable_trigrules ne 'replica') {
            $self->glog(q{Enabling triggers and rules on source via pg_class});
            $sourcedbh->do($SQL{enable_trigrules});
        }

        # Run all 'after_trigger_enable' code
        for my $code (@{$sync->{code_after_trigger_enable}}) {
            my $result = run_kid_custom_code($code, 'strict');
            if ($result eq 'redo') { ## redo rollsback source and target
                redo KID if $kidsalive;
                last KID;
            }
        }

        if ($self->{dryrun}) {
            $self->glog('Dryrun, rolling back...');
            $targetdbh->rollback();
            $sourcedbh->rollback();
            $maindbh->rollback();
        }
        else {
            $self->glog('Issuing final commit for source and target',6);
            $sourcedbh->commit();
            $targetdbh->commit();
            if ($sync->{usecustomselect}) {
                for my $g (@$goatlist) {
                    next if ! $g->{cs_temptable};
                    $self->glog("Dropping temp table $g->{cs_temptable} created for customselect");
                    $sourcedbh->do("DROP TABLE $g->{cs_temptable}");
                    $g->{cs_temptable} = '';
                }
            }
        }

        ## Capture the current time. now() is good enough as we just committed or rolled back
        my $source_commit_time = $sourcedbh->selectall_arrayref('SELECT now()')->[0][0];
        my $target_commit_time = $targetdbh->selectall_arrayref('SELECT now()')->[0][0];
        $sourcedbh->commit();
        $targetdbh->commit();

        ## Mark as done in the q table, and notify the parent directly
        $self->glog('Marking as done in the q table, notifying controller', 6);
        $sth{qend}->execute($dmlcount{allupdates}{source}+$dmlcount{allupdates}{target},
                            $dmlcount{allinserts}{source}+$dmlcount{allinserts}{target},
                            $dmlcount{alldeletes}{source}+$dmlcount{alldeletes}{target},
                            $syncname,$targetdb,$$);
        my $notify = "bucardo_syncdone_${syncname}_$targetdb";
        $maindbh->do(qq{NOTIFY "$notify"}) or die "NOTIFY $notify failed!";
        $maindbh->commit();

        ## Update our rate information as needed
        if ($sync->{track_rates}) {
            $SQL = 'INSERT INTO bucardo_rate(sync,goat,target,mastercommit,slavecommit,total) VALUES (?,?,?,?,?,?)';
            $sth = $maindbh->prepare($SQL);
            for my $g (@$goatlist) {
                next if ! exists $g->{rateinfo} or $g->{reltype} ne 'table';
                ($S,$T) = ($g->{safeschema},$g->{safetable});
                if ($deltacount{source}{$S}{$T}) {
                    for my $time (@{$g->{rateinfo}{source}}) {
                        $sth->execute($syncname,$g->{id},$targetdb,$time,$source_commit_time,$deltacount{source}{$S}{$T});
                    }
                }
                if ($deltacount{target}{$S}{$T}) {
                    for my $time (@{$g->{rateinfo}{target}}) {
                        $sth->execute($syncname,$g->{id},$sourcedb,$time,$source_commit_time,$deltacount{target}{$S}{$T});
                    }
                }
            }
            $maindbh->commit();
        }

        if ($synctype eq 'fullcopy'
            and $sync->{analyze_after_copy}
            and !$self->{dryrun}) {
            for my $g (@$goatlist) {
                next if ! $g->{analyze_after_copy} or $g->{reltype} ne 'table';
                if ($g->{onetimecopy_ifempty}) {
                    $g->{onetimecopy_ifempty} = 0;
                    next;
                }
                ($S,$T) = ($g->{safeschema},$g->{safetable});
                my $total_time = time() - $kid_start_time;
                $self->glog("Analyzing $S.$T on $targetdb. Time: $total_time");
                $targetdbh->do("ANALYZE $S.$T");
                $targetdbh->commit();
            }
        }

        my $total_time = time() - $kid_start_time;
        if ($synctype eq 'swap') {
            $self->glog("Finished syncing. Time: $total_time. Updates: $dmlcount{allupdates}{source}+$dmlcount{allupdates}{target} Inserts: $dmlcount{allinserts}{source}+$dmlcount{allinserts}{target} Deletes: $dmlcount{alldeletes}{source}+$dmlcount{alldeletes}{target} Sync: $syncname. Keepalive: $kidsalive");
        }
        else {
            $self->glog("Finished syncing. Time: $total_time. Updates: $dmlcount{allupdates}{target} Inserts: $dmlcount{allinserts}{target} Deletes: $dmlcount{alldeletes}{target} Sync: $syncname. Keepalive: $kidsalive");
        }

        ## Remove lock file if we used it
        if ($lock_table_mode and -e $force_lock_file) {
            $self->glog("Removing lock control file $force_lock_file");
            unlink $force_lock_file or $self->glog("Warning! Failed to unlink $force_lock_file");
        }

        # Run all 'after_txn' code
        for my $code (@{$sync->{code_after_txn}}) {
            my $result = run_kid_custom_code($code, 'nostrict');
            ## In case we want to bypass other after_txn code
            if ($result eq 'redo') {
                redo KID if $kidsalive;
                last KID;
            }
            ## Just in case
            $sourcedbh->rollback();
            $targetdbh->rollback();
        }

        if (! $kidsalive) {
            last KID;
        }

        redo KID;

    } ## end KID

    ## Cleanup and exit
    if ($config{audit_pid}) {
        $SQL = q{
            UPDATE bucardo.audit_pid
            SET    killdate = timeofday()::timestamp, death = 'END'
            WHERE  id = ?
            AND    killdate IS NULL
        };
        $sth = $maindbh->prepare($SQL);
        $sth->execute($self->{kidauditid});
    }
    $maindbh->commit();
    $maindbh->disconnect();

    $sourcedbh->rollback();
    $sourcedbh->disconnect();
    $targetdbh->rollback();
    $targetdbh->disconnect();

    $self->cleanup_kid('Normal exit', '');

    exit 0;

} ## end of start_kid


sub cleanup_kid {

    ## Kid is shutting down
    ## Remove our PID file

    my ($self,$reason,$extrainfo) = @_;

    $self->glog("Kid exiting at cleanup_kid. $extrainfo Reason: $reason");

    ## Remove the pid file
    if (unlink $self->{KIDPIDFILE}) {
        $self->glog(qq{Removed pid file "$self->{KIDPIDFILE}"});
    }
    else {
        $self->glog("Warning! Failed to remove pid file $self->{KIDPIDFILE}");
    }

    return;

} ## end of cleanup_kid

sub send_mail {

    ## Send out an email message
    ## Expects a hashref with mandatory args 'body' and 'subject'
    ## Optional args: 'to'

    my ($self,$arg) = @_;

    return if ! $self->{sendmail} and ! $self->{sendmail_file};

    ## If 'default_email_from' is not set, we default to currentuser@currenthost
    my $from = $config{default_email_from} || (getpwuid($>) . '@' . $hostname);

    $arg->{to} ||= $config{default_email_to};
    $arg->{subject} ||= 'Bucardo Mail!';
    if (! $arg->{body}) {
        $self->glog('ERROR: Cannot send mail, no body message');
        return;
    }

    my $smtphost = $config{default_email_host} || 'localhost';

    if ($self->{sendmail} and $arg->{to} ne 'nobody@example.com') {
        eval {
            my $smtp = Net::SMTP->new(
                Host    => $smtphost,
                Hello   => $hostname,
                Timeout => 15
                );
            $smtp->mail($from);
            $smtp->to($arg->{to});
            $smtp->data();
            $smtp->datasend("From: $from\n");
            $smtp->datasend("To: $arg->{to}\n");
            $smtp->datasend("Subject: $arg->{subject}\n");
            $smtp->datasend("\n");
            $smtp->datasend($arg->{body});
            $smtp->dataend;
            $smtp->quit;
        };
        if ($@) {
            my $error = $@ || '???';
            $self->glog("Warning: Error sending email to $arg->{to}: $error");
        }
        else {
            $self->glog("Sent an email to $arg->{to} from $from: $arg->{subject}");
        }
    }

    if ($self->{sendmail_file}) {
        my $fh;
        ## This happens rare enough to not worry about caching the file handle
        if (! open $fh, '>>', $self->{sendmail_file}) {
            $self->glog(qq{Warning: Could not open sendmail file "$self->{sendmail_file}": $!\n});
            return;
        }
        my $now = scalar localtime;
        print {$fh} qq{
==========================================
To: $arg->{to}
From: $from
Subject: $arg->{subject}
Date: $now
$arg->{body}

};
        close $fh or warn qq{Could not close "$self->{sendmail_file}": $!\n};
    }

    return;

} ## end of send_mail

1;


__END__

=pod

=head1 NAME

Bucardo - Postgres multi-master replication system

=head1 VERSION

This document describes version 4.5.0 of Bucardo

=head1 WEBSITE

The latest news and documentation can always be found at:

http://bucardo.org/

=head1 DESCRIPTION

Bucardo is a Perl module that replicates Postgres databases using a combination 
of Perl, a custom database schema, Pl/Perlu, and Pl/Pgsql.

Bucardo is unapologetically extremely verbose in its logging.

Full documentation can be found on the website, or in the files that came with 
this distribution. See also the documentation for the bucardo_ctl program.

=head1 DEPENDENCIES

* DBI (1.51 or better)
* DBD::Pg (2.0.0 or better)
* Sys::Hostname
* Sys::Syslog
* DBIx::Safe    ## Try 'yum install perl-DBIx-Safe' or visit bucardo.org

=head1 BUGS

Bugs should be reported to bucardo-general@bucardo.org. A list of bugs can be found at 
http://bucardo.org/bugs.html

=head1 CREDITS

Bucardo was originally developed and funded by Backcountry.com, who have been using versions 
of it in production since 2002. Jon Jensen <jon@endpoint.com> wrote the original version.

=head1 AUTHOR

Greg Sabino Mullane <greg@endpoint.com>

=head1 LICENSE AND COPYRIGHT

Copyright (c) 2005-2010 Greg Sabino Mullane <greg@endpoint.com>.

This software is free to use: see the LICENSE file for details.

=cut
