#!perl
# -*-mode:cperl; indent-tabs-mode: nil; cperl-indent-level: 4-*-

## The main Bucardo program
##
## This script should only be called via the 'bucardo' program
##
## Copyright 2006-2020 Greg Sabino Mullane <greg@turnstep.com>
##
## Please visit https://bucardo.org for more information

package Bucardo;
use 5.008003;
use strict;
use warnings;
use utf8;
use open qw( :std :utf8 );

our $VERSION = '5.6.0';

use DBI 1.51;                               ## How Perl talks to databases
use DBD::Pg 2.0   qw( :async             ); ## How Perl talks to Postgres databases
use DBIx::Safe '1.2.4';                     ## Filter out what DB calls customcode may use

use sigtrap       qw( die normal-signals ); ## Call die() on HUP, INT, PIPE, or TERM
use Config        qw( %Config            ); ## Used to map signal names
use File::Spec    qw(                    ); ## For portable file operations
use Data::Dumper  qw( Dumper             ); ## Used to dump information in email alerts
use POSIX         qw( strftime strtod    ); ## For grabbing the local timezone, and forcing to NV
use Sys::Hostname qw( hostname           ); ## Used for host safety check, and debugging/mail sending
use IO::Handle    qw( autoflush          ); ## Used to prevent stdout/stderr buffering
use Sys::Syslog   qw( openlog syslog     ); ## In case we are logging via syslog()
use Net::SMTP     qw(                    ); ## Used to send out email alerts
use List::Util    qw( first              ); ## Better than grep
use MIME::Base64  qw( encode_base64
                      decode_base64      ); ## For making text versions of bytea primary keys

use Time::HiRes   qw( sleep gettimeofday
                      tv_interval        ); ## For better resolution than the built-in sleep
                                            ## and for timing of events

## Formatting of Data::Dumper() calls:
$Data::Dumper::Varname = 'BUCARDO';
$Data::Dumper::Indent = 1;

## Common variables we don't want to declare over and over:
use vars qw(%SQL $sth %sth $count $info);

## Logging verbosity control
## See also the 'log_level_number' inside the config hash
use constant {
    LOG_WARN    => 0,  ## Always shown
    LOG_TERSE   => 1,  ## Bare minimum
    LOG_NORMAL  => 2,  ## Normal messages
    LOG_VERBOSE => 3,  ## Many more details
    LOG_DEBUG   => 4,  ## Firehose: rarely needed
    LOG_DEBUG2  => 5,  ## Painful level of detail
};

## Map system signal numbers to standard names
## This allows us to say kill $signumber{HUP} => $pid
my $i = 0;
my %signumber;
for (split(' ', $Config{sig_name})) {
    $signumber{$_} = $i++;
}

## Prevent buffering of output:
*STDOUT->autoflush(1);
*STDERR->autoflush(1);

## Configuration of DBIx::Safe
## Specify exactly what database handles are allowed to do within custom code
## Here, 'strict' means 'inside the main transaction that Bucardo uses to make changes'
my $strict_allow = 'SELECT INSERT UPDATE DELETE quote quote_identifier';
my $nostrict_allow = "$strict_allow COMMIT ROLLBACK NOTIFY SET pg_savepoint pg_release pg_rollback_to";

my %dbix = (
    source => {
        strict => {
            allow_command   => $strict_allow,
            allow_attribute => '',
            allow_regex     => '', ## Must be qr{} if not empty
            deny_regex      => '',
        },
        notstrict => {
            allow_command   => $nostrict_allow,
            allow_attribute => 'RaiseError PrintError',
            allow_regex     => [qr{CREATE TEMP TABLE},qr{CREATE(?: UNIQUE)? INDEX}],
            deny_regex      => '',
        },
    },
    target => {
        strict => {
            allow_command   => $strict_allow,
            allow_attribute => '',
            allow_regex     => '', ## Must be qr{} if not empty
            deny_regex      => '',
        },
        notstrict => {
            allow_command   => $nostrict_allow,
            allow_attribute => 'RaiseError PrintError',
            allow_regex     => [qr{CREATE TEMP TABLE}],
            deny_regex      => '',
        },
    }
);

## Grab our full and shortened host name:
## Used for the host_safety_check as well as for emails
my $hostname = hostname;
my $shorthost = $hostname;
$shorthost =~ s/^(.+?)\..*/$1/;

## Items pulled from bucardo_config and shared everywhere:
our %config;
our %config_about;

## Set a default in case we call glog before we load the configs:
$config{log_level_number} = LOG_NORMAL;

## Sequence columns we care about and how to change them via ALTER:
my @sequence_columns = (
    ['last_value'   => ''],
    ['start_value'  => 'START WITH'],
    ['increment_by' => 'INCREMENT BY'],
    ['max_value'    => 'MAXVALUE'],
    ['min_value'    => 'MINVALUE'],
    ['is_cycled'    => 'BOOL CYCLE'],
    ['is_called'    => ''],
);

my $sequence_columns = join ',' => map { $_->[0] } @sequence_columns;

## Default statement chunk size in case config does not have it
my $default_statement_chunk_size = 10_000;

## Output messages per language
our %msg = (
'en' => {
    'time-day'           => q{day},
    'time-days'          => q{days},
    'time-hour'          => q{hour},
    'time-hours'         => q{hours},
    'time-minute'        => q{minute},
    'time-minutes'       => q{minutes},
    'time-month'         => q{month},
    'time-months'        => q{months},
    'time-second'        => q{second},
    'time-seconds'       => q{seconds},
    'time-week'          => q{week},
    'time-weeks'         => q{weeks},
    'time-year'          => q{year},
    'time-years'         => q{years},
},
'fr' => {
    'time-day'           => q{jour},
    'time-days'          => q{jours},
    'time-hour'          => q{heure},
    'time-hours'         => q{heures},
    'time-minute'        => q{minute},
    'time-minutes'       => q{minutes},
    'time-month'         => q{mois},
    'time-months'        => q{mois},
    'time-second'        => q{seconde},
    'time-seconds'       => q{secondes},
    'time-week'          => q{semaine},
    'time-weeks'         => q{semaines},
    'time-year'          => q{année},
    'time-years'         => q{années},
},
'de' => {
    'time-day'           => q{Tag},
    'time-days'          => q{Tag},
    'time-hour'          => q{Stunde},
    'time-hours'         => q{Stunden},
    'time-minute'        => q{Minute},
    'time-minutes'       => q{Minuten},
    'time-month'         => q{Monat},
    'time-months'        => q{Monate},
    'time-second'        => q{Sekunde},
    'time-seconds'       => q{Sekunden},
    'time-week'          => q{Woche},
    'time-weeks'         => q{Woche},
    'time-year'          => q{Jahr},
    'time-years'         => q{Jahr},
},
'es' => {
    'time-day'           => q{día},
    'time-days'          => q{días},
    'time-hour'          => q{hora},
    'time-hours'         => q{horas},
    'time-minute'        => q{minuto},
    'time-minutes'       => q{minutos},
    'time-month'         => q{mes},
    'time-months'        => q{meses},
    'time-second'        => q{segundo},
    'time-seconds'       => q{segundos},
    'time-week'          => q{semana},
    'time-weeks'         => q{semanas},
    'time-year'          => q{año},
    'time-years'         => q{años},
},
);
## use critic

## Figure out which language to use for output
our $lang = $ENV{LC_ALL} || $ENV{LC_MESSAGES} || $ENV{LANG} || 'en';
$lang = substr($lang,0,2);


##
## Everything else is subroutines
##

sub new {

    ## Create a new Bucardo object and return it
    ## Takes a hashref of options as the only argument

    my $class = shift;
    my $params = shift || {};

    ## The hash for this object, with default values:
    my $self = {
        created      => scalar localtime,
        mcppid       => $$,
        verbose      => 1,
        quickstart   => 0,
        logdest      => ['.'],
        warning_file => '',
        logseparate  => 0,
        logextension => '',
        logclean     => 0,
        dryrun       => 0,
        sendmail     => 1,
        extraname    => '',
        logprefix    => 'BC!',
        version      => $VERSION,
        listening    => {},
        pidmap       => {},
        exit_on_nosync => 0,
        sqlprefix    => "/* Bucardo $VERSION */",
    };

    ## Add any passed-in parameters to our hash:
    for (keys %$params) {
        $self->{$_} = $params->{$_};
    }

    ## Transform our hash into a genuine 'Bucardo' object:
    bless $self, $class;

    ## Remove any previous log files if requested
    if ($self->{logclean} && (my @dirs = grep {
        $_ !~ /^(?:std(?:out|err)|none|syslog)/
    } @{ $self->{logdest} }) ) {
        ## If the dir does not exists, silently proceed
        for my $dir (@dirs) {
            opendir my $dh, $dir or next;
            ## We look for any files that start with 'log.bucardo' plus another dot
            for my $file (grep { /^log\.bucardo\./ } readdir $dh) {
                my $fullfile = File::Spec->catfile( $dir => $file );
                unlink $fullfile or warn qq{Could not remove "$fullfile": $!\n};
            }
            closedir $dh or warn qq{Could not closedir "$dir": $!\n};
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
        $self->glog(q{** DRYRUN - Syncs will not be committed! **}, LOG_WARN);
    }

    ## This gets appended to the process description ($0)
    if ($self->{extraname}) {
        $self->{extraname} = " ($self->{extraname})";
    }

    ## Connect to the main Bucardo database
    $self->{masterdbh} = $self->connect_database();

    ## Load in the configuration information
    $self->reload_config_database();

    ## Figure out if we are writing emails to a file
    $self->{sendmail_file} = $ENV{BUCARDO_EMAIL_DEBUG_FILE} || $config{email_debug_file} || '';

    ## Where to store our PID:
    $self->{pid_file} = File::Spec->catfile( $config{piddir} => 'bucardo.mcp.pid' );

    ## The file to ask all processes to stop:
    $self->{stop_file} = File::Spec->catfile( $config{piddir} => $config{stopfile} );

    ## Send all log lines starting with "Warning" to a separate file
    $self->{warning_file} ||= $config{warning_file};

    ## Make sure we are running where we are supposed to be
    ## This prevents items in bucardo.db that reference production
    ## systems from getting run on QA!
    ## ...or at least makes sure people have to work a lot harder
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
        elsif ($safe eq $hostname) {
            $ok = 1;
        }

        if (! $ok) {
            warn qq{Cannot start: configured to only run on "$osafe". This is "$hostname"\n};
            warn qq{  This is usually done to prevent a configured Bucardo from running\n};
            warn qq{  on the wrong host. Please verify the 'db' settings by doing:\n};
            warn qq{bucardo list dbs\n};
            warn qq{  Once you are sure the bucardo.db table has the correct values,\n};
            warn qq{  you can adjust the 'host_safety_check' value\n};
            exit 2;
        }
    }

    return $self;

} ## end of new


sub start_mcp {

    ## Start the Bucardo daemon. Called by bucardo after setsid()
    ## Arguments: one
    ## 1. Arrayref of command-line options.
    ## Returns: never (exit 0 or exit 1)

    my ($self, $opts) = @_;

    ## Store the original invocation string, then modify it
    my $old0 = $0;
    ## May not work on all platforms, of course, but we're gonna try
    $0 = "Bucardo Master Control Program v$VERSION.$self->{extraname}";

    ## Prefix all lines in the log file with this TLA (until overriden by a forked child)
    $self->{logprefix} = 'MCP';

    ## If the standard pid file [from new()] already exists, cowardly refuse to run
    if (-e $self->{pid_file}) {
        ## Grab the PID from the file if we can for better output
        my $extra = '';

        ## Failing to open is not fatal here, just means no PID shown
        my $oldpid;
        if (open my $fh, '<', $self->{pid_file}) {
            if (<$fh> =~ /(\d+)/) {
                $oldpid = $1;
                $extra = " (PID=$oldpid)";
            }
            close $fh or warn qq{Could not close "$self->{pid_file}": $!\n};
        }

        ## Output to the logfile, to STDERR, then exit
        if ($oldpid != $$) {
            my $msg = qq{File "$self->{pid_file}" already exists$extra: cannot run until it is removed};
            $self->glog($msg, LOG_WARN);
            warn $msg;

            exit 1;
        }
    }

    ## We also refuse to run if the global stop file exists
    if (-e $self->{stop_file}) {
        my $msg = qq{Cannot run while this file exists: "$self->{stop_file}"};
        $self->glog($msg, LOG_WARN);
        warn $msg;

        ## Failure to open this file is not fatal
        if (open my $fh, '<', $self->{stop_file}) {
            ## Read in up to 10 lines from the stopfile and output them
            while (<$fh>) {
                $msg = "Line $.: $_";
                $self->glog($msg, LOG_WARN);
                warn $msg;
                last if $. > 10;
            }
            close $fh or warn qq{Could not close "$self->{stop_file}": $!\n};
        }

        exit 1;
    }

    ## We are clear to start. Output a quick hello and version to the logfile
    $self->glog("Starting Bucardo version $VERSION", LOG_WARN);
    $self->glog("Log level: $config{log_level}", LOG_WARN);

    ## Close unused file handles.
    unless (grep { $_ eq 'stderr' } @{ $self->{logdest} }) {
        close STDERR or warn "Could not close STDERR\n";
    }
    unless (grep { $_ eq 'stdout' } @{ $self->{logdest} }) {
        close STDOUT or warn "Could not close STDOUT\n";
    }

    ## Create a new (but very temporary) PID file
    ## We will overwrite later with a new PID once we do the initial fork
    $self->create_mcp_pid_file($old0);

    ## Send an email message with details about this invocation
    if ($self->{sendmail} or $self->{sendmail_file}) {
        ## Create a pretty Dumped version of the current $self object, with the password elided

        ## Squirrel away the old password
        my $oldpass = $self->{dbpass};
        ## Set to something else
        $self->{dbpass} = '<not shown>';
        ## Dump the entire object with Data::Dumper (with custom config variables)
        my $dump = Dumper $self;
        ## Put the password back in place
        $self->{dbpass} = $oldpass;

        ## Prepare to send an email letting people know we have started up
        my $body = qq{
        Master Control Program $$ was started on $hostname
        Args: $old0
        Version: $VERSION
        };
        my $subject = qq{Bucardo $VERSION started on $shorthost};

        ## If someone left a message in the reason file, append it, then delete the file
        my $reason = get_reason('delete');
        if ($reason) {
            $body .= "Reason: $reason\n";
            $subject .= " ($reason)";
        }
        ## Strip leading whitespace from the body (from the qq{} above)
        $body =~ s/^\s+//gsm;

        ## Send out the email (if sendmail or sendmail_file is enabled)
        $self->send_mail({ body => "$body\n\n$dump", subject => $subject });
    }

    ## Drop the existing database connection, fork, and get a new one
    ## This self-fork helps ensure our survival
    my $disconnect_ok = 0;
    eval {
        ## This connection was set in new()
        $self->{masterdbh}->disconnect();
        $disconnect_ok = 1;
    };
    $disconnect_ok or $self->glog("Warning! Disconnect failed $@", LOG_WARN);

    my $seeya = fork;
    if (! defined $seeya) {
        die q{Could not fork mcp!};
    }
    ## Immediately close the child process (one side of the fork)
    if ($seeya) {
        exit 0;
    }

    ## Now that we've forked, overwrite the PID file with our new value
    $self->create_mcp_pid_file($old0);

    ## Reconnect to the master database
    ($self->{mcp_backend}, $self->{masterdbh}) = $self->connect_database();
    my $masterdbh = $self->{masterdbh};

    ## Let any listeners know we have gotten this far
    ## (We do this nice and early for impatient watchdog programs)
    $self->db_notify($masterdbh, 'boot', 1);

    ## Store the function to use to generate clock timestamps
    ## We greatly prefer clock_timestamp,
    ## but fallback to timeofday() for 8.1 and older
    $self->{mcp_clock_timestamp} =
        $masterdbh->{pg_server_version} >= 80200
            ? 'clock_timestamp()'
            : 'timeofday()::timestamptz';

    ## Start outputting some interesting things to the log
    $self->show_db_version_and_time($masterdbh, $self->{mcp_backend}, 'Master DB ');
    $self->glog("PID: $$", LOG_WARN);
    $self->glog('Postgres library version: ' . $masterdbh->{pg_lib_version}, LOG_WARN);
    $self->glog("bucardo: $old0", LOG_WARN);
    $self->glog('Bucardo.pm: ' . $INC{'Bucardo.pm'}, LOG_WARN);
    $self->glog((sprintf 'OS: %s  Perl: %s %vd', $^O, $^X, $^V), LOG_WARN);

    ## Get an integer version of the DBD::Pg version, for later comparisons
    if ($DBD::Pg::VERSION !~ /(\d+)\.(\d+)\.(\d+)/) {
        die "Could not parse the DBD::Pg version: was $DBD::Pg::VERSION\n";
    }
    $self->{dbdpgversion} = int (sprintf '%02d%02d%02d', $1,$2,$3);
    $self->glog((sprintf 'DBI version: %s  DBD::Pg version: %s (%d) DBIx::Safe version: %s',
                 $DBI::VERSION,
                 $DBD::Pg::VERSION,
                 $self->{dbdpgversion},
                 $DBIx::Safe::VERSION),
                LOG_WARN);

    ## Attempt to print the git hash to help with debugging if running a dev version
    if (-d '.git') {
        my $COM = 'git log -1';
        my $log = '';
        eval { $log = qx{$COM}; };
        if ($log =~ /^commit ([a-f0-9]{40}).+Date:\s+(.+?)$/ms) {
            $self->glog("Last git commit sha and date: $1 $2", LOG_NORMAL);
        }
    }

    ## Store some PIDs for later debugging use
    $self->{pidmap}{$$} = 'MCP';
    $self->{pidmap}{$self->{mcp_backend}} = 'Bucardo DB';

    ## Get the maximum key length of the "self" hash for pretty formatting
    my $maxlen = 5;
    for (keys %$self) {
        $maxlen = length($_) if length($_) > $maxlen;
    }

    ## Print each object, aligned, and show 'undef' for undefined values
    ## Yes, this prints things like HASH(0x8fbfc84), but we're okay with that
    $Data::Dumper::Indent = 0;
    $Data::Dumper::Terse = 1;
    my $objdump = "Bucardo object:\n";
    for my $key (sort keys %$self) {
        my $value = $key eq 'dbpass' ? '<not shown>' : $self->{$key};
        $objdump .= sprintf " %-*s => %s\n", $maxlen, $key,
            (defined $value) ?
                (ref $value eq 'ARRAY') ? Dumper($value)
                    : qq{'$value'} : 'undef';
    }
    $Data::Dumper::Indent = 1;
    $Data::Dumper::Terse = 0;
    $self->glog($objdump, LOG_TERSE);

    ## Dump all configuration variables to the log
    $self->log_config();

    ## Any other files we find in the piddir directory should be considered old
    ## Thus, we can remove them
    my $piddir = $config{piddir};
    opendir my $dh, $piddir or die qq{Could not opendir "$piddir": $!\n};

    ## Nothing else should really be in here, but we will limit with a regex anyway
    my @pidfiles = grep { /^bucardo.*\.pid$/ } readdir $dh;
    closedir $dh or warn qq{Could not closedir "$piddir" $!\n};

    ## Loop through and remove each file found, making a note in the log
    for my $pidfile (sort @pidfiles) {
        my $fullfile = File::Spec->catfile( $piddir => $pidfile );
        ## Do not erase our own file
        next if $fullfile eq $self->{pid_file};
        ## Everything else can get removed
        if (-e $fullfile) {
            if (unlink $fullfile) {
                $self->glog("Warning: removed old pid file $fullfile", LOG_VERBOSE);
            }
            else {
                ## This will cause problems, but we will drive on
                $self->glog("Warning: failed to remove pid file $fullfile", LOG_TERSE);
            }
        }
    }

    ## We use a USR2 signal to indicate that the logs should be reopened
    local $SIG{USR2} = sub {

        $self->glog("Received USR2 from pid $$, who is a $self->{logprefix}", LOG_DEBUG);

        ## Go through and reopen anything that needs reopening
        ## For now, that is only plain text files
        for my $logdest (sort keys %{$self->{logcodes}}) {
            my $loginfo = $self->{logcodes}{$logdest};

            next if $loginfo->{type} ne 'textfile';

            my $filename = $loginfo->{filename};

            ## Reopen the same (named) file with a new filehandle
            my $newfh;
            if (! open $newfh, '>>', $filename) {
                $self->glog("Warning! Unable to open new filehandle for $filename", LOG_WARN);
                next;
            }

            ## Turn off buffering on this handle
            $newfh->autoflush(1);

            ## Overwrite the old sub and point to the new filehandle
            my $oldfh = $loginfo->{filehandle};

            $self->glog("Switching to new filehandle for log file $filename", LOG_NORMAL);
            $loginfo->{code} = sub { print {$newfh} @_, $/ };
            $self->glog("Completed reopen of file $filename", LOG_NORMAL);

            ## Close the old filehandle, then remove it from our records
            close $oldfh or warn "Could not close old filehandle for $filename: $!\n";
            $loginfo->{filehandle} = $newfh;

        }

     }; ## end of handling USR2 signals

    ## From this point forward, we want to die gracefully
    ## We setup our own subroutine to catch any die signals
    local $SIG{__DIE__} = sub {

        ## Arguments: one
        ## 1. The error message
        ## Returns: never (exit 1 or exec new process)

        my $msg = shift;
        my $line = (caller)[2];
        $self->glog("Warning: Killed (line $line): $msg", LOG_WARN);

        ## Was this a database problem?
        ## We can carefully handle certain classes of errors
        if ($msg =~ /DBI|DBD/) {

            ## How many bad databases we found
            my $bad = 0;
            for my $db (sort keys %{ $self->{sdb} }) { ## need a better name!
                if (! exists $self->{sdb}{$db}{dbh} ) {
                    $self->glog("Database $db has no database handle", LOG_NORMAL);
                    $bad++;
                }
                elsif (! $self->{sdb}{$db}{dbh}->ping()) {
                    $self->glog("Database $db failed ping check", LOG_NORMAL);
                    $msg = 'Ping failed';
                    $bad++;
                }
            }

            if ($bad) {
                my $changes = $self->check_sync_health();
                if ($changes) {
                    ## If we already made a MCP label, go there
                    ## Else fallthrough and assume our bucardo.sync changes stick!
                    if ($self->{mcp_loop_started}) {
                        $self->glog('Going to restart the MCP loop, as syncs have changed', LOG_VERBOSE);
                        die 'We are going to redo the MCP loop'; ## goes to end of mcp main eval
                    }
                }
            }
        }

        ## The error message determines if we try to resurrect ourselves or not
        my $respawn = (
            $msg =~  /DBI connect/         ## From DBI
                or $msg =~ /Ping failed/       ## Set below
        ) ? 1 : 0;

        ## Sometimes we don't want to respawn at all (e.g. during some tests)
        if (! $config{mcp_dbproblem_sleep}) {
            $self->glog('Database problem, but will not attempt a respawn due to mcp_dbproblem_sleep=0', LOG_TERSE);
            $respawn = 0;
        }

        ## Create some output for the mail message
        my $diesubject = "Bucardo MCP $$ was killed";
        my $diebody = "MCP $$ was killed: $msg";

        ## Most times we *do* want to respawn
        if ($respawn) {
            $self->glog("Database problem, will respawn after a short sleep: $config{mcp_dbproblem_sleep}", LOG_TERSE);
            $diebody .= " (will attempt respawn in $config{mcp_dbproblem_sleep} seconds)";
            $diesubject .= ' (respawning)';
        }

        ## Callers can prevent an email being sent by setting this before they die
        if (! $self->{clean_exit}) {
            $self->send_mail({ body => $diebody, subject => $diesubject });
        }

        ## Kill kids, remove pidfile, update tables, etc.
        $self->cleanup_mcp("Killed: $msg");

        ## If we are not respawning, simply exit right now
        exit 1 if ! $respawn;

        ## We will attempt a restart, but sleep a while first to avoid constant restarts
        $self->glog("Sleep time: $config{mcp_dbproblem_sleep}", LOG_TERSE);
        sleep($config{mcp_dbproblem_sleep});

        ## Do a quick check for a stopfile
        ## Bail if the stopfile exists
        if (-e $self->{stop_file}) {
            $self->glog(qq{Found stopfile "$self->{stop_file}": exiting}, LOG_WARN);
            my $message = 'Found stopfile';

            ## Grab the reason, if it exists, so we can propagate it onward
            my $mcpreason = get_reason(0);
            if ($mcpreason) {
                $message .= ": $mcpreason";
            }

            ## Stop controllers, disconnect, remove PID file, etc.
            $self->cleanup_mcp("$message\n");

            $self->glog('Exiting', LOG_WARN);
            exit 0;
        }

        ## We assume this is bucardo, and that we are in same directory as when called
        my $RUNME = $old0;
        ## Check to see if $RUNME is executable as is, before we assume we're in the same directory
        if (! -x $RUNME) {
            $RUNME = "./$RUNME" if index ($RUNME,'.') != 0;
        }

        my $mcpreason = 'Attempting automatic respawn after MCP death';
        $self->glog("Respawn attempt: $RUNME @{ $opts } start '$mcpreason'", LOG_TERSE);

        ## Replace ourselves with a new process running this command
        { exec $RUNME, @{ $opts }, 'start', $mcpreason };
        $self->glog("Could not exec $RUNME: $!", LOG_WARN);

    }; ## end SIG{__DIE__} handler sub

    ## This resets listeners, kills kids, and loads/activates syncs
    my $active_syncs = $self->reload_mcp();

    if (!$active_syncs && $self->{exit_on_nosync}) {
        ## No syncs means no reason for us to hang around, so we exit
        $self->glog('No active syncs were found, so we are exiting', LOG_WARN);
        $self->db_notify($masterdbh, 'nosyncs', 1);
        $self->cleanup_mcp('No active syncs');
        exit 1;
    }

    ## Report which syncs are active
    $self->glog("Active syncs: $active_syncs", LOG_TERSE);

    ## We want to reload everything if someone HUPs us
    local $SIG{HUP} = sub {
        $self->reload_mcp();
    };

    ## We need KIDs to tell us their PID so we can deregister them
    $self->{kidpidlist} = {};

    ## Let any listeners know we have gotten this far
    $self->db_notify($masterdbh, 'started', 1);

    ## For optimization later on, we need to know which syncs are 'fullcopy'
    for my $syncname (keys %{ $self->{sync} }) {

        my $s = $self->{sync}{$syncname};

        ## Skip inactive or paused syncs
        next if !$s->{mcp_active} or $s->{paused};

        ## Walk through each database and check the roles, discarding inactive dbs
        my %rolecount;
        for my $db (values %{ $s->{db} }) {
            next if $db->{status} ne 'active';
            $rolecount{$db->{role}}++;
        }

        ## Default to being fullcopy
        $s->{fullcopy} = 1;

        ## We cannot be a fullcopy sync if:
        if ($rolecount{'target'}           ## there are any target dbs
            or $rolecount{'source'} > 1    ## there is more than one source db
            or ! $rolecount{'fullcopy'}) { ## there are no fullcopy dbs
            $s->{fullcopy} = 0;
        }
    }


    ## Because a sync may have gotten a notice while we were down,
    ## we auto-kick all eligible syncs
    ## We also need to see if we can prevent the VAC daemon from running,
    ## if there are no databases with bucardo schemas
    $self->{needsvac} = 0;
    for my $syncname (keys %{ $self->{sync} }) {

        my $s = $self->{sync}{$syncname};

        ## Default to starting in a non-kicked mode
        $s->{kick_on_startup} = 0;

        ## Skip inactive or paused syncs
        next if  !$s->{mcp_active} or $s->{paused};

        ## Skip fullcopy syncs
        next if $s->{fullcopy};

        ## Right now, the vac daemon is only useful for source Postgres databases
        ## Of course, it is not needed for fullcopy syncs
        for my $db (values %{ $s->{db} }) {
            if ($db->{status} eq 'active'
                and $db->{dbtype} eq 'postgres'
                and $db->{role} eq 'source') {
                ## We need to increment it for any matches in sdb, regardless of which sync initially set it!
                $self->{sdb}{ $db->{name} }{needsvac} = 2;
                $self->{needsvac} = 1;
            }
        }

        ## Skip if autokick is false
        next if ! $s->{autokick};

        ## Kick it!
        $s->{kick_on_startup} = 1;
    }

    ## Start the main loop
    {
        my $value = $self->mcp_main();
        redo if $value;
    }

    return; ## no critic

} ## end of start_mcp


sub create_mcp_pid_file {

    ## Create a file containing the PID of the current MCP,
    ## plus a few other details
    ## Arguments: one
    ## 1. Message (usually just the original invocation line)
    ## Returns: undef

    my $self = shift;
    my $message = shift || '';

    open my $pidfh, '>', $self->{pid_file}
        or die qq{Cannot write to $self->{pid_file}: $!\n};

    ## Inside our newly created PID file, print out PID on the first line
    ##  - print how the script was originally invoked on the second line (old $0),
    ##  - print the current time on the third line
    my $now = scalar localtime;
    print {$pidfh} "$$\n$message\n$now\n";
    close $pidfh or warn qq{Could not close "$self->{pid_file}": $!\n};

    return;

} ## end of create_mcp_pid_file


sub mcp_main {

    ## The main MCP process
    ## Arguments: none
    ## Returns: undef (but almost always just exits with 0 or 1)

    my $self = shift;

    my $maindbh = $self->{masterdbh};
    my $sync = $self->{sync};

    my $SQL;

    ## Used to gather up and handle any notices received via the listen/notify system
    my $notice;

    ## Used to keep track of the last time we pinged the databases
    my $lastpingcheck = 0;

    ## Keep track of how long since we checked on the VAC daemon
    my $lastvaccheck = 0;

    $self->glog('Entering main loop', LOG_TERSE);

    $self->{mcp_loop_started} = 1;

  MCP: {

        ## We eval the whole loop so we can cleanly redo it if needed
        my $mcp_loop_finished = 0;
        eval {

        ## Bail if the stopfile exists
        if (-e $self->{stop_file}) {
            $self->glog(qq{Found stopfile "$self->{stop_file}": exiting}, LOG_WARN);
            my $msg = 'Found stopfile';

            ## Grab the reason, if it exists, so we can propagate it onward
            my $mcpreason = get_reason(0);
            if ($mcpreason) {
                $msg .= ": $mcpreason";
            }

            ## Stop controllers, disconnect, remove PID file, etc.
            $self->cleanup_mcp("$msg\n");

            $self->glog('Exiting', LOG_WARN);
            exit 0;
        }

        ## Startup the VAC daemon as needed
        ## May be off via user configuration, or because of no valid databases
        if ($config{bucardo_vac} and $self->{needsvac}) {

            ## Check on it occasionally (different than the running time)
            if (time() - $lastvaccheck >= $config{mcp_vactime}) {

                ## Is it alive? If not, spawn
                my $pidfile = "$config{piddir}/bucardo.vac.pid";
                if (! -e $pidfile) {
                    $self->fork_vac();
                }

                $lastvaccheck = time();

            } ## end of time to check vac

        } ## end if bucardo_vac

        ## Every once in a while, make sure our database connections are still there
        if (time() - $lastpingcheck >= $config{mcp_pingtime}) {

            ## This message must have "Ping failed" to match the $respawn above
            $maindbh->ping or die qq{Ping failed for main database!\n};

            ## Check each (pingable) remote database in undefined order
            for my $dbname (keys %{ $self->{sdb} }) {

                my $d = $self->{sdb}{$dbname};

                next if $d->{dbtype} =~ /flat|mongo|redis/o;

                my $try_reconnect = 0;
                if ($d->{status} eq 'stalled') {
                    $self->glog("Trying to connect to stalled database $dbname", LOG_VERBOSE);
                    $try_reconnect = 1;
                }
                elsif (! $d->{dbh}->ping) {
                    $self->glog("Ping failed for database $dbname, trying to reconnect", LOG_NORMAL);
                }

                if ($try_reconnect) {

                    ## Sleep a hair so we don't reloop constantly
                    sleep 0.5;
                    undef $d->{backend};
                    {
                        local $SIG{__DIE__} = 'IGNORE';
                        eval {
                            ($d->{backend}, $d->{dbh}) = $self->connect_database($dbname);
                        };
                    }
                    if (defined $d->{backend}) {
                        $self->show_db_version_and_time($d->{dbh}, $d->{backend}, qq{Database "$dbname" });
                        $d->{status} = 'active'; ## In case it was stalled
                    }
                    else {
                        $self->glog("Unable to reconnect to database $dbname!", LOG_WARN);
                        ## We may want to throw an exception if this keeps happening
                        ## We may also want to adjust lastpingcheck so we check more often
                    }
                }
            }

            ## Reset our internal counter to 'now'
            $lastpingcheck = time();

        } ## end of checking database connections

        ## Add in any messages from the main database and reset the notice hash
        ## Ignore things we may have sent ourselves
        $notice = $self->db_get_notices($maindbh, $self->{mcp_backend});

        ## Add in any messages from each remote database
        for my $dbname (keys %{ $self->{sdb} }) {

            my $d = $self->{sdb}{$dbname};

            next if $d->{dbtype} ne 'postgres';

            next if $d->{status} eq 'stalled';

            my $nlist = $self->db_get_notices($d->{dbh});
            $d->{dbh}->rollback();
            for my $name (keys %{ $nlist } ) {
                if (! exists $notice->{$name}) {
                    $notice->{$name} = $nlist->{$name};
                }
                else {
                    for my $pid (keys %{ $nlist->{$name}{pid} }) {
                        $notice->{$name}{pid}{$pid}++;
                    }
                }
            }
        }

        ## Handle each notice one by one
        for my $name (sort keys %{ $notice }) {

            my $npid = $notice->{$name}{firstpid};

            ## Request to stop everything
            if ('mcp_fullstop' eq $name) {
                $self->glog("Received full stop notice from PID $npid, leaving", LOG_TERSE);
                $self->cleanup_mcp("Received stop NOTICE from PID $npid");
                exit 0;
            }

            ## Request that a named sync get kicked
            elsif ($name =~ /^kick_sync_(.+)/o) {
                my $syncname = $1;

                ## Prepare to send some sort of log message
                my $msg = '';

                ## We will not kick if this sync does not exist or it is inactive
                if (! exists $self->{sync}{$syncname}) {
                    $msg = qq{Warning: Unknown sync to be kicked: "$syncname"\n};
                }
                elsif (! $self->{sync}{$syncname}{mcp_active}) {
                    $msg = qq{Cannot kick inactive sync "$syncname"};
                }
                elsif ($self->{sync}{$syncname}{paused}) {
                    $msg = qq{Cannot kick paused sync "$syncname"};
                }
                ## We also won't kick if this was created by a kid
                ## This can happen as our triggerkicks may be set to 'always'
                elsif (exists $self->{kidpidlist}{$npid}) {
                    $self->glog(qq{Not kicking sync "$syncname" as it came from KID $npid}, LOG_DEBUG);
                }
                else {
                    ## Kick it!
                    $sync->{$syncname}{kick_on_startup} = 1;
                }

                if ($msg) {
                    $self->glog($msg, $msg =~ /Unknown/ ? LOG_TERSE : LOG_VERBOSE);
                    ## As we don't want people to wait around for a syncdone...
                    $self->db_notify($maindbh, "syncerror_$syncname", 1);
                }
            }

            ## A sync has finished
            elsif ($name =~ /^syncdone_(.+)/o) {
                my $syncdone = $1;
                $self->glog("Sync $syncdone has finished", LOG_DEBUG);

                ## Echo out to anyone listening
                $self->db_notify($maindbh, $name, 1);

                ## If this was a onetimecopy sync, flip it off
                $sync->{$syncdone}{onetimecopy} = 0;
            }
            ## A sync has been killed
            elsif ($name =~ /^synckill_(.+)/o) {
                my $syncdone = $1;
                $self->glog("Sync $syncdone has been killed", LOG_DEBUG);
                ## Echo out to anyone listening
                $self->db_notify($maindbh, $name, 1);
                ## Check on the health of our databases, in case that was the reason the sync was killed
                $self->check_sync_health();
            }
            ## Request to pause a sync
            elsif ($name =~ /^pause_sync_(.+)/o) {
                my $syncname = $1;
                my $msg;

                ## We will not pause if this sync does not exist or it is inactive
                if (! exists $self->{sync}{$syncname}) {
                    $msg = qq{Warning: Unknown sync to be paused: "$syncname"\n};
                }
                elsif (! $self->{sync}{$syncname}{mcp_active}) {
                    $msg = qq{Cannot pause inactive sync "$syncname"};
                }
                else {
                    ## Mark it as paused, stop the kids and controller
                    $sync->{$syncname}{paused} = 1;
                    my $stopsync = "stopsync_$syncname";
                    $self->db_notify($maindbh, "kid_$stopsync");
                    $self->db_notify($maindbh, "ctl_$stopsync");
                    $maindbh->commit();
                    $self->glog(qq{Set sync "$syncname" as paused}, LOG_VERBOSE);
                }
                if (defined $msg) {
                    $self->glog($msg, LOG_TERSE);
                }
            }
            ## Request to resume a sync
            elsif ($name =~ /^resume_sync_(.+)/o) {
                my $syncname = $1;
                my $msg;

                ## We will not resume if this sync does not exist or it is inactive
                if (! exists $self->{sync}{$syncname}) {
                    $msg = qq{Warning: Unknown sync to be resumed: "$syncname"\n};
                }
                elsif (! $self->{sync}{$syncname}{mcp_active}) {
                    $msg = qq{Cannot resume inactive sync "$syncname"};
                }
                else {
                    ## Mark it as resumed
                    my $s = $sync->{$syncname};
                    $s->{paused} = 0;
                    ## Since we may have accumulated deltas while pasued, set to autokick if needed
                    if (!$s->{fullcopy} and $s->{autokick}) {
                        $s->{kick_on_startup} = 1;
                    }
                    $self->glog(qq{Set sync "$syncname" as resumed}, LOG_VERBOSE);
                    ## MCP will restart the CTL on next loop around
                }
                if (defined $msg) {
                    $self->glog($msg, LOG_TERSE);
                }
            }
            ## Request to reload the configuration file
            elsif ('reload_config' eq $name) {
                $self->glog('Reloading configuration table', LOG_TERSE);
                $self->reload_config_database();

                ## Output all values to the log file again
                $self->log_config();

                ## We need to reload ourself as well
                ## XXX Not needed for some items! e.g. mcp_pingtime
                $self->reload_mcp();

                ## Let anyone listening know we are done
                $self->db_notify($maindbh, 'reload_config_finished', 1);
            }

            ## Request to reload the MCP
            elsif ('mcp_reload' eq $name) {
                $self->glog('Reloading MCP', LOG_TERSE);
                $self->reload_mcp();

                ## Let anyone listening know we are done
                $self->db_notify($maindbh, 'reloaded_mcp', 1);
            }

            ## Request for a ping via listen/notify
            elsif ('mcp_ping' eq $name) {
                $self->glog("Got a ping from PID $npid, issuing pong", LOG_DEBUG);
                $self->db_notify($maindbh, 'mcp_pong', 1);
            }

            ## Request that we parse and empty the log message table
            elsif ('log_message' eq $name) {
                $self->glog('Checking for log messages', LOG_DEBUG);
                $SQL = 'SELECT msg,cdate FROM bucardo_log_message ORDER BY cdate';
                my $sth = $maindbh->prepare_cached($SQL);
                $count = $sth->execute();
                if ($count ne '0E0') {
                    for my $row (@{$sth->fetchall_arrayref()}) {
                        $self->glog("MESSAGE ($row->[1]): $row->[0]", LOG_TERSE);
                    }
                    $maindbh->do('DELETE FROM bucardo_log_message');
                    $maindbh->commit();
                }
                else {
                    $sth->finish();
                }
            }

            ## Request that a named sync get reloaded
            elsif ($name =~ /^reload_sync_(.+)/o) {
                my $syncname = $1;
                my $succeeded = 0;

                ## Skip if the sync does not exist or is inactive
                if (! exists $sync->{$syncname}) {
                    $self->glog(qq{Invalid sync reload: "$syncname"}, LOG_TERSE);
                }
                elsif (!$sync->{$syncname}{mcp_active}) {
                    $self->glog(qq{Cannot reload: sync "$syncname" is not active}, LOG_TERSE);
                }
                else {

                    ## reload overrides a pause
                    if ($sync->{$syncname}{paused}) {
                        $self->glog(qq{Resuming paused sync "$syncname"}, LOG_TERSE);
                        $sync->{$syncname}{paused} = 0;
                    }

                    $self->glog(qq{Deactivating sync "$syncname"}, LOG_TERSE);
                    $self->deactivate_sync($sync->{$syncname});

                    ## Reread from the database
                    $SQL = q{SELECT *, }
                        . q{COALESCE(EXTRACT(epoch FROM checktime),0) AS checksecs, }
                            . q{COALESCE(EXTRACT(epoch FROM lifetime),0) AS lifetimesecs }
                                . q{FROM bucardo.sync WHERE name = ?};
                    my $sth = $maindbh->prepare($SQL);
                    $count = $sth->execute($syncname);
                    if ($count eq '0E0') {
                        $sth->finish();
                        $self->glog(qq{Warning! Cannot reload sync "$syncname": no longer in the database!}, LOG_WARN);
                        $maindbh->commit();
                        next; ## Handle the next notice
                    }

                    ## XXX: Actually do a full disconnect and redo all the items in here

                    my $info = $sth->fetchall_arrayref({})->[0];
                    $maindbh->commit();

                    ## Only certain things can be changed "on the fly"
                    for my $val (qw/checksecs stayalive deletemethod status autokick
                                    analyze_after_copy vacuum_after_copy targetgroup targetdb
                                    onetimecopy lifetimesecs maxkicks rebuild_index
                                   conflict_strategy/) {
                        $sync->{$syncname}{$val} = $self->{sync}{$syncname}{$val} = $info->{$val};
                    }

                    ## XXX: Todo: Fix those double assignments

                    ## Empty all of our custom code arrays
                    for my $key (grep { /^code_/ } sort keys %{ $self->{sync}{$syncname} }) {
                        $sync->{$syncname}{$key} = $self->{sync}{$syncname}{$key} = [];
                    }

                    sleep 2; ## XXX TODO: Actually wait somehow, perhaps fork

                    $self->glog("Reactivating sync $syncname", LOG_TERSE);
                    $sync->{$syncname}{mcp_active} = 0;
                    if (! $self->activate_sync($sync->{$syncname})) {
                        $self->glog(qq{Warning! Reactivation of sync "$syncname" failed}, LOG_WARN);
                    }
                    else {
                        ## Let anyone listening know the sync is now ready
                        $self->db_notify($maindbh, "reloaded_sync_$syncname", 1);
                        $succeeded = 1;
                    }
                    $maindbh->commit();

                    $self->glog("Succeeded: $succeeded", LOG_WARN);
                }
                $self->db_notify($maindbh, "reload_error_sync_$syncname", 1)
                    if ($succeeded != 1);
            }

            ## Request that a named sync get activated
            elsif ($name =~ /^activate_sync_(.+)/o) {
                my $syncname = $1;
                if (! exists $sync->{$syncname}) {
                    $self->glog(qq{Invalid sync activation: "$syncname"}, LOG_TERSE);
                }
                elsif ($sync->{$syncname}{mcp_active}) {
                    $self->glog(qq{Sync "$syncname" is already activated}, LOG_TERSE);
                    $self->db_notify($maindbh, "activated_sync_$syncname", 1);
                }
                elsif ($self->activate_sync($sync->{$syncname})) {
                    $sync->{$syncname}{mcp_active} = 1;
                    ## Just in case:
                    $sync->{$syncname}{paused} = 0;
                    $maindbh->do(
                        'UPDATE sync SET status = ? WHERE name = ?',
                        undef, 'active', $syncname
                    );
                }
            }
            ## Request that a named sync get deactivated
            elsif ($name =~ /^deactivate_sync_(.+)/o) {
                my $syncname = $1;
                if (! exists $sync->{$syncname}) {
                    $self->glog(qq{Invalid sync "$syncname"}, LOG_TERSE);
                }
                elsif (! $sync->{$syncname}{mcp_active}) {
                    $self->glog(qq{Sync "$syncname" is already deactivated}, LOG_TERSE);
                    $self->db_notify($maindbh, "deactivated_sync_$syncname", 1);
                }
                elsif ($self->deactivate_sync($sync->{$syncname})) {
                    $sync->{$syncname}{mcp_active} = 0;
                    $maindbh->do(
                        'UPDATE sync SET status = ? WHERE name = ?',
                        undef, 'inactive', $syncname
                    );
                }
            }

            # Serialization/deadlock problems; now the child is gonna sleep.
            elsif ($name =~ /^syncsleep_(.+)/o) {
                my $syncname = $1;
                $self->glog("Sync $syncname could not serialize, will sleep", LOG_DEBUG);

                ## Echo out to anyone listening
                $self->db_notify($maindbh, $name, 1);
            }

            ## A kid reporting in. We just store the PID
            elsif ('kid_pid_start') {
                for my $lpid (keys %{ $notice->{$name}{pid} }) {
                    $self->{kidpidlist}{$lpid} = 1;
                }
            }

            ## A kid leaving. We remove the stored PID.
            elsif ('kid_pid_stop') {
                for my $lpid (keys %{ $notice->{$name}{pid} }) {
                    delete $self->{kidpidlist}{$lpid};
                }
            }

            ## Someone giving us a hint that a database may be down
            elsif ($name =~ /dead_db_(.+)/) {
                my $dbname = $1;
                $self->glog(qq{Got a hint that database "$dbname" may be down. Let's check it out!}, LOG_NORMAL);
                my $changes = $self->check_sync_health($dbname);
            }

            ## Should not happen, but let's at least log it
            else {
                $self->glog("Warning: received unknown message $name from $npid!", LOG_TERSE);
            }

        } ## end each notice

        $maindbh->commit();

        ## Just in case this changed behind our back:
        $sync = $self->{sync};

        ## Startup controllers for all eligible syncs
      SYNC: for my $syncname (keys %$sync) {

            my $s = $sync->{$syncname};

            ## Skip if this sync has not been activated
            next if ! $s->{mcp_active};

            ## Skip if this one is paused
            next if $s->{paused};

            ## Skip is this one is stalled
            next if $s->{status} eq 'stalled';

            ## If this is not a stayalive, AND is not being kicked, skip it
            next if ! $s->{stayalive} and ! $s->{kick_on_startup};

            ## If this is a fullcopy sync, skip unless it is being kicked
            next if $s->{fullcopy} and ! $s->{kick_on_startup};

            ## If this is a previous stayalive, see if it is active, kick if needed
            if ($s->{stayalive} and $s->{controller}) {
                $count = kill 0 => $s->{controller};
                ## If kill 0 returns nothing, the controller is gone, so create a new one
                if (! $count) {
                    $self->glog("Could not find controller $s->{controller}, will create a new one. Kicked is $s->{kick_on_startup}", LOG_TERSE);
                    $s->{controller} = 0;
                }
                else { ## Presume it is alive and listening to us, restart and kick as needed
                    if ($s->{kick_on_startup}) {
                        ## See if controller needs to be killed, because of time limit or job count limit
                        my $restart_reason = '';

                        ## We can kill and restart a controller after a certain number of kicks
                        if ($s->{maxkicks} > 0 and $s->{ctl_kick_counts} >= $s->{maxkicks}) {
                            $restart_reason = "Total kicks ($s->{ctl_kick_counts}) >= limit ($s->{maxkicks})";
                        }

                        ## We can kill and restart a controller after a certain amount of time
                        elsif ($s->{lifetimesecs} > 0) {
                            my $thistime = time();
                            my $timediff = $thistime - $s->{start_time};
                            if ($thistime - $s->{start_time} > $s->{lifetimesecs}) {
                                $restart_reason = "Time is $timediff, limit is $s->{lifetimesecs} ($s->{lifetime})";
                            }
                        }

                        if ($restart_reason) {
                            ## Kill and restart controller
                            $self->glog("Restarting controller for sync $syncname. $restart_reason", LOG_TERSE);
                            kill $signumber{USR1} => $s->{controller};

                            ## Create a new controller
                            $self->fork_controller($s, $syncname);
                        }
                        else {
                            ## Perform the kick
                            my $notify = "ctl_kick_$syncname";
                            $self->db_notify($maindbh, $notify);
                            $self->glog(qq{Sent a kick to controller $s->{controller} for sync "$syncname"}, LOG_DEBUG);
                        }

                        ## Reset so we don't kick the next round
                        $s->{kick_on_startup} = 0;

                        ## Track how many times we've kicked
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
                $self->glog(qq{Checking for existing controllers for sync "$syncname"}, LOG_VERBOSE);
            }

            if (-e $pidfile and ! $s->{mcp_problemchild}) {
                $self->glog("File exists staylive=$s->{stayalive} controller=$s->{controller}", LOG_TERSE);
                my $pid;
                if (!open $pid, '<', $pidfile) {
                    $self->glog(qq{Warning: Could not open file "$pidfile": $!}, LOG_WARN);
                    $s->{mcp_problemchild} = 1;
                    next SYNC;
                }
                my $oldpid = <$pid>;
                chomp $oldpid;
                close $pid or warn qq{Could not close "$pidfile": $!\n};
                ## We don't need to know about this every time
                if ($s->{mcp_changed}) {
                    $self->glog(qq{Found previous controller $oldpid from "$pidfile"}, LOG_TERSE);
                }
                if ($oldpid !~ /^\d+$/) {
                    $self->glog(qq{Warning: Invalid pid found inside of file "$pidfile" ($oldpid)}, LOG_WARN);
                    $s->{mcp_changed} = 0;
                    $s->{mcp_problemchild} = 2;
                    next SYNC;
                }
                ## Is it still alive?
                $count = kill 0 => $oldpid;
                if ($count==1) {
                    if ($s->{mcp_changed}) {
                        $self->glog(qq{Skipping sync "$syncname", seems to be already handled by $oldpid}, LOG_VERBOSE);
                        ## Make sure this kid is still running
                        $count = kill 0 => $oldpid;
                        if (!$count) {
                            $self->glog(qq{Warning! PID $oldpid was not found. Removing PID file}, LOG_WARN);
                            unlink $pidfile or $self->glog("Warning! Failed to unlink $pidfile", LOG_WARN);
                            $s->{mcp_problemchild} = 3;
                            next SYNC;
                        }
                        $s->{mcp_changed} = 0;
                    }
                    if (! $s->{stayalive}) {
                        $self->glog(qq{Non stayalive sync "$syncname" still active - sending it a notify}, LOG_NORMAL);
                    }
                    my $notify = "ctl_kick_$syncname";
                    $self->db_notify($maindbh, $notify);
                    $s->{kick_on_startup} = 0;
                    next SYNC;
                }
                $self->glog("No active pid $oldpid found. Killing just in case, and removing file", LOG_TERSE);
                $self->kill_bucardo_pid($oldpid => 'normal');
                unlink $pidfile or $self->glog("Warning! Failed to unlink $pidfile", LOG_WARN);
                $s->{mcp_changed} = 1;
            } ## end if pidfile found for this sync

            ## We may have found an error in the pid file detection the first time through
            $s->{mcp_problemchild} = 0;

            ## Fork off the controller, then clean up the $s hash
            $self->{masterdbh}->commit();
            $self->fork_controller($s, $syncname);
            $s->{kick_on_startup} = 0;
            $s->{mcp_changed} = 1;

        } ## end each sync

        sleep $config{mcp_loop_sleep};

        $mcp_loop_finished = 1;

        }; # end of eval

        redo MCP if $mcp_loop_finished;

        ## We may want to redo if the error was not *that* fatal
        if ($@ =~ /redo/) {
            $self->glog('Going to restart the main MCP loop', LOG_VERBOSE);
            redo MCP;
        }

    } ## end of MCP loop

    return;

} ## end of mcp_main


sub check_sync_health {

    ## Check every database used by a sync
    ## Typically called on demand when we know something is wrong
    ## Marks any unreachable databases, and their syncs, as stalled
    ## Arguments: zero or one
    ## 1. Optional name of database to hone in on
    ## Returns: number of bad databases detected

    my $self = shift;
    my $dbnamematch = shift || '';

    my $SQL;

    $self->glog('Starting check_sync_health', LOG_NORMAL);

    ## How many bad databases did we find?
    my $bad_dbs = 0;

    ## No need to check databases more than once, as they can span across syncs
    my $db_checked = {};

    ## Do this at the sync level, rather than 'sdb', as we don't
    ## want to check non-active syncs at all
  SYNC: for my $syncname (sort keys %{ $self->{sync} }) {

        my $sync = $self->{sync}{$syncname};

        if ($sync->{status} ne 'active') {
            $self->glog("Skipping $sync->{status} sync $syncname", LOG_NORMAL);
            next SYNC;
        }

        ## Walk through each database used by this sync
      DB: for my $dbname (sort keys %{ $sync->{db} }) {

            ## Only check each database (by name) once
            next if $db_checked->{$dbname}++;

            ## If limiting to a single database, only check that one
            next if $dbnamematch and $dbnamematch ne $dbname;

            $self->glog("Checking database $dbname for sync $syncname", LOG_DEBUG);

            my $dbinfo = $sync->{db}{$dbname};

            ## We only bother checking ones that are currently active
            if ($dbinfo->{status} ne 'active') {
                $self->glog("Skipping $dbinfo->{status} database $dbname for sync $syncname", LOG_NORMAL);
                next DB;
            }

            ## Is this database valid or not?
            my $isbad = 0;

            my $dbh = $dbinfo->{dbh};

            if (! ref $dbh) {
                $self->glog("Database handle for database $dbname does not look valid", LOG_NORMAL);
                if ($dbinfo->{dbtype} eq 'postgres') {
                    $isbad = 1;
                }
                else {
                    ## TODO: Account for other non dbh types
                    next DB;
                }
            }
            elsif (ref $dbh =~ /DBI/ and ! $dbh->ping() ) {
                $isbad = 1;
                $self->glog("Database $dbname failed ping", LOG_NORMAL);
            }

            ## If not marked as bad, assume good and move on
            next DB unless $isbad;

            ## Retry connection afresh: wrap in eval as one of these is likely to fail!
            undef $dbinfo->{dbh};

            eval {
                ($dbinfo->{backend}, $dbinfo->{dbh}) = $self->connect_database($dbname);
                $self->show_db_version_and_time($dbinfo->{dbh}, $dbinfo->{backend}, qq{Database "$dbname" });
            };

            ## If we cannot connect, mark the db (and the sync) as stalled
            if (! defined $dbinfo->{dbh}) {
                $self->glog("Database $dbname is unreachable, marking as stalled", LOG_NORMAL);
                $dbinfo->{status} = 'stalled';
                $bad_dbs++;
                if ($sync->{status} ne 'stalled') {
                    $self->glog("Marked sync $syncname as stalled", LOG_NORMAL);
                    $sync->{status} = 'stalled';
                    $SQL = 'UPDATE bucardo.sync SET status = ? WHERE name = ?';
                    eval {
                        my $sth = $self->{masterdbh}->prepare($SQL);
                        $sth->execute('stalled',$syncname);
                    };
                    if ($@) {
                        $self->glog("Failed to set sync $syncname as stalled: $@", LOG_WARN);
                        $self->{masterdbh}->rollback();
                    }
                }
                $SQL = 'UPDATE bucardo.db SET status = ? WHERE name = ?';
                my $sth = $self->{masterdbh}->prepare($SQL);
                eval {
                    $sth->execute('stalled',$dbname);
                    $self->{masterdbh}->commit();
                };
                if ($@) {
                    $self->glog("Failed to set db $dbname as stalled: $@", LOG_WARN);
                    $self->{masterdbh}->rollback();
                }

            }

        } ## end each database in this sync

    } ## end each sync

    ## If any databases were marked as bad, go ahead and stall other syncs that are using them
    ## (todo)

    return $bad_dbs;

} ## end of check_sync_health


sub restore_syncs {

    ## Try to restore stalled syncs by checking its stalled databases
    ## Arguments: none
    ## Returns: number of syncs restored

    my $self = shift;

    my $SQL;

    $self->glog('Starting restore_syncs', LOG_DEBUG);

    ## How many syncs did we restore?
    my $restored_syncs = 0;

    ## No need to check databases more than once, as they can span across syncs
    my $db_checked = {};

    ## If a sync is stalled, check its databases
  SYNC: for my $syncname (sort keys %{ $self->{sync} }) {

        my $sync = $self->{sync}{$syncname};

        next SYNC if $sync->{status} ne 'stalled';

        $self->glog("Checking stalled sync $syncname", LOG_DEBUG);

        ## Number of databases restored for this sync only
        my $restored_dbs = 0;

        ## Walk through each database used by this sync
      DB: for my $dbname (sort keys %{ $sync->{db} }) {

            ## Only check each database (by name) once
            next if $db_checked->{$dbname}++;

            $self->glog("Checking database $dbname for sync $syncname", LOG_DEBUG);

            my $dbinfo = $sync->{db}{$dbname};

            ## All we need to worry about are stalled ones
            next DB if $dbinfo->{status} ne 'stalled';

            ## Just in case, remove the database handle
            undef $dbinfo->{dbh};
            eval {
                ($dbinfo->{backend}, $dbinfo->{dbh}) = $self->connect_database($dbname);
                $self->show_db_version_and_time($dbinfo->{dbh}, $dbinfo->{backend}, qq{Database "$dbname" });
            };

            if (defined $dbinfo->{dbh}) {
                $dbinfo->{status} = 'active';
                $SQL = 'UPDATE bucardo.db SET status = ? WHERE name = ?';
                my $sth = $self->{masterdbh}->prepare($SQL);
                $sth->execute('active',$dbname);
                $self->{masterdbh}->commit();
                $restored_dbs++;
                $self->glog("Sucessfully restored database $dbname: no longer stalled", LOG_NORMAL);
            }

        } ## end each database

        ## If any databases were restored, restore the sync too
        if ($restored_dbs) {
            $sync->{status} = 'stalled';
            $SQL = 'UPDATE bucardo.sync SET status = ? WHERE name = ?';
            my $sth = $self->{masterdbh}->prepare($SQL);
            $sth->execute('active',$syncname);
            $self->{masterdbh}->commit();
            $restored_syncs++;
            $self->glog("Sucessfully restored sync $syncname: no longer stalled", LOG_NORMAL);
        }

    } ## end each sync

    return $restored_syncs;

} ## end of restore_syncs


sub start_controller {

    ## For a particular sync, does all the listening and creation of KIDs
    ## aka the CTL process
    ## Why not just spawn KIDs? Someday the CTL may have multiple kids again...
    ## Arguments: one
    ## 1. Hashref of sync information
    ## Returns: never

    our ($self,$sync) = @_;

    my $SQL;

    $self->{ctlpid} = $$;
    $self->{syncname} = $sync->{name};

    ## Prefix all log lines with this TLA (was MCP)
    $self->{logprefix} = 'CTL';

    ## Extract some of the more common items into local vars
    my ($syncname,$kidsalive,$dbinfo, $kicked,) = @$sync{qw(
           name    kidsalive  dbs     kick_on_startup)};

    ## Set our process name
    $0 = qq{Bucardo Controller.$self->{extraname} Sync "$syncname" for relgroup "$sync->{herd}" to dbs "$sync->{dbs}"};

    ## Upgrade any specific sync configs to global configs
    if (exists $config{sync}{$syncname}) {
        while (my ($setting, $value) = each %{$config{sync}{$syncname}}) {
            $config{$setting} = $value;
            $self->glog("Set sync-level config setting $setting: $value", LOG_TERSE);
        }
    }

    ## Store our PID into a file
    ## Save the complete returned name for later cleanup
    $self->{ctlpidfile} = $self->store_pid( "bucardo.ctl.sync.$syncname.pid" );

    ## Start normal log output for this controller: basic facts
    my $msg = qq{New controller for sync "$syncname". Relgroup is "$sync->{herd}", dbs is "$sync->{dbs}". PID=$$};
    $self->glog($msg, LOG_TERSE);

    ## Log some startup information, and squirrel some away for later emailing
    my $mailmsg = "$msg\n";
    $msg = qq{  stayalive: $sync->{stayalive} checksecs: $sync->{checksecs} kicked: $kicked};
    $self->glog($msg, LOG_NORMAL);
    $mailmsg .= "$msg\n";

    $msg = sprintf q{  kidsalive: %s onetimecopy: %s lifetimesecs: %s (%s) maxkicks: %s},
        $kidsalive,
        $sync->{onetimecopy},
        $sync->{lifetimesecs},
        $sync->{lifetime} || 'NULL',
        $sync->{maxkicks};
    $self->glog($msg, LOG_NORMAL);
    $mailmsg .= "$msg\n";

    ## Allow the MCP to signal us (request to exit)
    local $SIG{USR1} = sub {
        ## Do not change this message: looked for in the controller DIE sub
        die "MCP request\n";
    };

    ## From this point forward, we want to die gracefully
    local $SIG{__DIE__} = sub {

        ## Arguments: one
        ## 1. Error message
        ## Returns: never (exit 0)

        my ($diemsg) = @_;

        ## Store the line that did the actual exception
        my $line = (caller)[2];

        ## Don't issue a warning if this was simply a MCP request
        my $warn = $diemsg =~ /MCP request/ ? '' : 'Warning! ';
        $self->glog(qq{${warn}Controller for "$syncname" was killed at line $line: $diemsg}, LOG_WARN);

        ## We send an email if it's enabled
        if ($self->{sendmail} or $self->{sendmail_file}) {

            ## Never email passwords
            my $oldpass = $self->{dbpass};
            $self->{dbpass} = '???';

            ## Create a text version of our $self to email out
            my $dump = Dumper $self;

            my $body = qq{
                Controller $$ has been killed at line $line
                Host: $hostname
                Sync name: $syncname
                Relgroup: $sync->{herd}
                Databases: $sync->{dbs}
                Error: $diemsg
                Parent process: $self->{mcppid}
                Stats page: $config{stats_script_url}?sync=$syncname
                Version: $VERSION
            };

            ## Whitespace cleanup
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

            ## Send the mail, but not for a normal shutdown
            if ($moresub !~ /stopfile/) {
                my $subject = qq{Bucardo "$syncname" controller killed on $shorthost$moresub};
                $self->send_mail({ body => "$body\n", subject => $subject });
            }

            ## Restore the password for the final cleanup connection
            $self->{dbpass} = $oldpass;

        } ## end sending email

        ## Cleanup the controller by killing kids, cleaning database tables and removing the PID file.
        $self->cleanup_controller(0, $diemsg);

        exit 0;

    }; ## end SIG{__DIE__} handler sub

    ## Connect to the master database
    ($self->{master_backend}, $self->{masterdbh}) = $self->connect_database();
    my $maindbh = $self->{masterdbh};
    $self->glog("Bucardo database backend PID: $self->{master_backend}", LOG_VERBOSE);

    ## Map the PIDs to common names for better log output
    $self->{pidmap}{$$} = 'CTL';
    $self->{pidmap}{$self->{master_backend}} = 'Bucardo DB';

    ## Listen for kick requests from the MCP for this sync
    my $kicklisten = "kick_$syncname";
    $self->db_listen($maindbh, "ctl_$kicklisten");

    ## Listen for a controller ping request
    my $pinglisten = "${$}_ping";
    $self->db_listen($maindbh, "ctl_$pinglisten");

    ## Commit so we start listening right away
    $maindbh->commit();

    ## SQL to update the syncrun table's status only
    ## This is currently unused, but no harm in leaving it in place.
    ## It would be nice to syncrun the before_sync and after_sync
    ## custom codes. If we reintroduce the multi-kid 'gang' concept,
    ## that changes things radically as well.
    $SQL = q{
        UPDATE bucardo.syncrun
        SET    status=?
        WHERE  sync=?
        AND    ended IS NULL
    };
    $sth{ctl_syncrun_update_status} = $maindbh->prepare($SQL);

    ## SQL to update the syncrun table on startup
    ## Returns the insert (start) time
    $SQL = q{
        UPDATE    bucardo.syncrun
        SET       ended=now(), status=?
        WHERE     sync=?
        AND       ended IS NULL
        RETURNING started
    };
    $sth{ctl_syncrun_end_now} = $maindbh->prepare($SQL);

    ## At this point, this controller must be authoritative for its sync
    ## Thus, we want to stop/kill any other CTL or KID processes that exist for this sync
    ## The first step is to send a friendly notice asking them to leave gracefully

    my $stopsync = "stopsync_$syncname";
    ## This will commit after the notify:
    $self->db_notify($maindbh, "kid_$stopsync");
    ## We also want to force other controllers of this sync to leave
    $self->db_notify($maindbh, "ctl_$stopsync");

    ## Now we can listen for it ourselves in case the MCP requests it
    $self->db_listen($maindbh, "ctl_$stopsync");

    ## Now we look for any PID files for this sync and send them a HUP
    $count = $self->send_signal_to_PID( {sync => $syncname} );

    ## Next, we want to interrupt any long-running queries a kid may be in the middle of
    ## If they are, they will not receive the message above until done, but we can't wait
    ## If we stopped anyone, sleep a bit to allow them to exit and remove their PID files
    $self->terminate_old_goats($syncname) and sleep 1;

    ## Clear out any old entries in the syncrun table
    $sth = $sth{ctl_syncrun_end_now};
    $count = $sth->execute("Old entry ended (CTL $$)", $syncname);
    if (1 == $count) {
        $info = $sth->fetchall_arrayref()->[0][0];
        $self->glog("Ended old syncrun entry, start time was $info", LOG_NORMAL);
    }
    else {
        $sth->finish();
    }

    ## Listen for a kid letting us know the sync has finished
    my $syncdone = "syncdone_$syncname";
    $self->db_listen($maindbh, "ctl_$syncdone");

    ## Determine the last time this sync fired, if we are using "checksecs"
    if ($sync->{checksecs}) {

        ## The handy syncrun table tells us the time of the last good run
        $SQL = q{
            SELECT CEIL(EXTRACT(epoch FROM ended))
            FROM bucardo.syncrun
            WHERE sync=?
            AND lastgood IS TRUE
            OR  lastempty IS TRUE
        };
        $sth = $maindbh->prepare($SQL);
        $count = $sth->execute($syncname);

        ## Got a match? Use that
        if (1 == $count) {
            $sync->{lastheardfrom} = $sth->fetchall_arrayref()->[0][0];
        }
        else {
            ## We default to "now" if we cannot find an earlier time
            $sth->finish();
            $sync->{lastheardfrom} = time();
        }
        $maindbh->commit();
    }

    ## If running an after_sync customcode, we need a timestamp
    if (exists $sync->{code_after_sync}) {
        $SQL = 'SELECT now()';
        $sync->{starttime} = $maindbh->selectall_arrayref($SQL)->[0][0];
        ## Rolling back as all we did was the SELECT
        $maindbh->rollback();
    }

    ## Reconnect to all databases we care about: overwrites existing dbhs
    for my $dbname (sort keys %{ $sync->{db} }) {

        my $d = $sync->{db}{$dbname};

        if ($d->{dbtype} =~ /flat/o) {
            $self->glog(qq{Not connecting to flatfile database "$dbname"}, LOG_NORMAL);
            next;
        }

        ## Do not need non-Postgres handles for the controller
        next if $d->{dbtype} ne 'postgres';

        ## Establish a new database handle
        ($d->{backend}, $d->{dbh}) = $self->connect_database($dbname);
        $self->glog(qq{Database "$dbname" backend PID: $d->{backend}}, LOG_NORMAL);
        $self->{pidmap}{$d->{backend}} = "DB $dbname";
    }

    ## Adjust the target table names as needed and store in the goat hash

    ## New table name regardless of syncs or databases
    $SQL = 'SELECT newname FROM bucardo.customname WHERE goat=? AND db IS NULL and sync IS NULL';
    my $sth_custom1 = $maindbh->prepare($SQL);
    ## New table name for this sync only
    $SQL = 'SELECT newname FROM bucardo.customname WHERE goat=? AND sync=? AND db IS NULL';
    my $sth_custom2 = $maindbh->prepare($SQL);
    ## New table name for a specific database only
    $SQL = 'SELECT newname FROM bucardo.customname WHERE goat=? AND db=? AND sync IS NULL';
    my $sth_custom3 = $maindbh->prepare($SQL);
    ## New table name for this sync and a specific database
    $SQL = 'SELECT newname FROM bucardo.customname WHERE goat=? AND sync=? AND db=?';
    my $sth_custom4 = $maindbh->prepare($SQL);

    ## Adjust the target table columns as needed and store in the goat hash

    ## New table cols regardless of syncs or databases
    $SQL = 'SELECT clause FROM bucardo.customcols WHERE goat=? AND db IS NULL and sync IS NULL';
    my $sth_customc1 = $maindbh->prepare($SQL);
    ## New table cols for this sync only
    $SQL = 'SELECT clause FROM bucardo.customcols WHERE goat=? AND sync=? AND db IS NULL';
    my $sth_customc2 = $maindbh->prepare($SQL);
    ## New table cols for a specific database only
    $SQL = 'SELECT clause FROM bucardo.customcols WHERE goat=? AND db=? AND sync IS NULL';
    my $sth_customc3 = $maindbh->prepare($SQL);
    ## New table cols for this sync and a specific database
    $SQL = 'SELECT clause FROM bucardo.customcols WHERE goat=? AND sync=? AND db=?';
    my $sth_customc4 = $maindbh->prepare($SQL);

    for my $g (@{ $sync->{goatlist} }) {

        ## We only transform tables for now
        next if $g->{reltype} ne 'table';

        my ($S,$T) = ($g->{safeschema},$g->{safetable});

        ## See if we have any custom names or columns. Each level overrides the last
        my $customname = '';
        my $customcols = '';

        ## Just this goat
        $count = $sth_custom1->execute($g->{id});
        if ($count < 1) {
            $sth_custom1->finish();
        }
        else {
            $customname = $sth_custom1->fetchall_arrayref()->[0][0];
        }
        $count = $sth_customc1->execute($g->{id});
        if ($count < 1) {
            $sth_customc1->finish();
        }
        else {
            $customcols = $sth_customc1->fetchall_arrayref()->[0][0];
        }

        ## Just this goat and this sync
        $count = $sth_custom2->execute($g->{id}, $syncname);
        if ($count < 1) {
            $sth_custom2->finish();
        }
        else {
            $customname = $sth_custom2->fetchall_arrayref()->[0][0];
        }
        $count = $sth_customc2->execute($g->{id}, $syncname);
        if ($count < 1) {
            $sth_customc2->finish();
        }
        else {
            $customcols = $sth_customc2->fetchall_arrayref()->[0][0];
        }

        ## Need to pick one source at random to extract the list of columns from
        my $saved_sourcedbh = '';

        ## Set for each target db
        $g->{newname}{$syncname} = {};
        $g->{newcols}{$syncname} = {};
        for my $dbname (sort keys %{ $sync->{db} }) {

            my $d = $sync->{db}{$dbname};

            my $type= $d->{dbtype};

            my $cname;
            my $ccols = '';

            ## We only ever change table names (or cols) for true targets
            if ($d->{role} ne 'source') {

                ## Save local copies for this database only
                $cname = $customname;
                $ccols = $customcols;

                ## Anything for this goat and this database?
                $count = $sth_custom3->execute($g->{id}, $dbname);
                if ($count < 1) {
                    $sth_custom3->finish();
                }
                else {
                    $cname = $sth_custom3->fetchall_arrayref()->[0][0];
                }
                $count = $sth_customc3->execute($g->{id}, $dbname);
                if ($count < 1) {
                    $sth_customc3->finish();
                }
                else {
                    $ccols = $sth_customc3->fetchall_arrayref()->[0][0];
                }

                ## Anything for this goat, this sync, and this database?
                $count = $sth_custom4->execute($g->{id}, $syncname, $dbname);
                if ($count < 1) {
                    $sth_custom4->finish();
                }
                else {
                    $cname = $sth_custom4->fetchall_arrayref()->[0][0];
                }
                $count = $sth_customc4->execute($g->{id}, $syncname, $dbname);
                if ($count < 1) {
                    $sth_customc4->finish();
                }
                else {
                    $ccols = $sth_customc4->fetchall_arrayref()->[0][0];
                }
            }

            ## Got a new name match? Just use that for everything
            if (defined $cname and $cname) {
                $g->{newname}{$syncname}{$dbname} = $cname;
            }
            ## Only a few use schemas:
            elsif ($d->{dbtype} eq 'postgres'
                   or $d->{dbtype} eq 'flatpg') {
                $g->{newname}{$syncname}{$dbname} = "$S.$T";
            }
            ## Some always get the raw table name
            elsif ($d->{dbtype} eq 'redis' or $d->{dbtype} eq 'mongo') {
                $g->{newname}{$syncname}{$dbname} = $g->{tablename};
            }
            else {
                $g->{newname}{$syncname}{$dbname} = $T;
            }

            ## Set the columns for this combo: empty for no change
            $g->{newcols}{$syncname}{$dbname} = $ccols;

            ## If we do not have a source database handle yet, grab one
            if (! $saved_sourcedbh) {
                for my $dbname (sort keys %{ $sync->{db} }) {

                    next if $sync->{db}{$dbname}{role} ne 'source';

                    ## All we need is the handle, nothing more
                    $saved_sourcedbh = $sync->{db}{$dbname}{dbh};

                    ## Leave this loop, we got what we came for
                    last;
                }
            }

            ## We either get the specific columns, or use a '*' if no customcols
            my $SELECT = $ccols || 'SELECT *';

            ## Run a dummy query against the source to pull back the column names
            ## This is particularly important for customcols of course!
            $sth = $saved_sourcedbh->prepare("SELECT * FROM ($SELECT FROM $S.$T LIMIT 0) AS foo LIMIT 0");
            $sth->execute();

            ## Store the arrayref of column names for this goat and this select clause
            $g->{tcolumns}{$SELECT} = $sth->{NAME};
            $sth->finish();
            $saved_sourcedbh->rollback();

            ## Make sure none of them are un-named, which Postgres outputs as ?column?
            if (grep { /^\?.+\?$/ } @{ $g->{tcolumns}{$SELECT} }) {
                die "Invalid customcols given: must give an alias to all columns! ($g->{tcolumns}{$SELECT}) for $SELECT\n";
            }

        }
    }

    ## Set to true if we determine the kid(s) should make a run
    ## Can be set by:
    ##   kick notice from the MCP for this sync
    ##   'checksecs' timeout
    ##   if we are just starting up (now)
    my $kick_request = 1;

    ## How long it has been since we checked on our kids
    my $kidchecktime = 0;

    ## For custom code:
    our $input = {}; ## XXX still needed?

    ## We are finally ready to enter the main loop

  CONTROLLER: {

        ## Bail if the stopfile exists
        if (-e $self->{stop_file}) {
            $self->glog(qq{Found stopfile "$self->{stop_file}": exiting}, LOG_TERSE);
            ## Do not change this message: looked for in the controller DIE sub
            my $stopmsg = 'Found stopfile';

            ## Grab the reason, if it exists, so we can propagate it onward
            my $ctlreason = get_reason(0);
            if ($ctlreason) {
                $stopmsg .= ": $ctlreason";
            }

            ## This exception is caught by the controller's __DIE__ sub above
            die "$stopmsg\n";
        }

        ## Process any notifications from the main database
        ## Ignore things we may have sent ourselves
        my $nlist = $self->db_get_notices($maindbh, $self->{master_backend});

      NOTICE: for my $name (sort keys %{ $nlist }) {

            my $npid = $nlist->{$name}{firstpid};

            ## Strip prefix so we can easily use both pre and post 9.0 versions
            $name =~ s/^ctl_//o;

            ## Kick request from the MCP?
            if ($name eq $kicklisten) {
                $kick_request = 1;
                next NOTICE;
            }

            ## Request for a ping via listen/notify
            if ($name eq $pinglisten) {

                $self->glog('Got a ping, issuing pong', LOG_DEBUG);
                $self->db_notify($maindbh, "ctl_${$}_pong");

                next NOTICE;
            }

            ## Another controller has asked us to leave as we are no longer The Man
            if ($name eq $stopsync) {
                $self->glog('Got a stop sync request, so exiting', LOG_TERSE);
                die 'Stop sync request';
            }

            ## A kid has just finished syncing
            if ($name eq $syncdone) {
                $self->{syncdone} = time;
                $self->glog("Kid $npid has reported that sync $syncname is done", LOG_DEBUG);
                ## If this was a onetimecopy sync, flip the bit (which should be done in the db already)
                if ($sync->{onetimecopy}) {
                    $sync->{onetimecopy} = 0;
                }
                next NOTICE;
            }

            ## Someone else's sync is getting kicked, finishing up, or stopping
            next NOTICE if
                (index($name, 'kick_') == 0)
                or
                (index($name, 'syncdone_') == 0)
                or
                (index($name, 'stopsync_') == 0);


            ## Ignore any messages sent to a kid
            next NOTICE if 0 == index($name, 'kid_');

            ## Should not happen, but let's at least log it
            $self->glog("Warning: received unknown message $name from $npid!", LOG_TERSE);

        } ## end of each notification

        ## To ensure we can receive new notifications next time:
        $maindbh->commit();

        if ($self->{syncdone}) {

            ## Reset the notice
            $self->{syncdone} = 0;

            ## Run all after_sync custom codes
            if (exists $sync->{code_after_sync}) {
                for my $code (@{$sync->{code_after_sync}}) {
                    #$sth{ctl_syncrun_update_status}->execute("Code after_sync (CTL $$)", $syncname);
                    $maindbh->commit();
                    my $result = $self->run_ctl_custom_code($sync,$input,$code, 'nostrict');
                    $self->glog("End of after_sync $code->{id}", LOG_VERBOSE);
                } ## end each custom code
            }

            ## Let anyone listening know that this sync is complete. Global message
            my $notifymsg = "syncdone_$syncname";
            $self->db_notify($maindbh, $notifymsg);

            ## If we are not a stayalive, this is a good time to leave
            if (! $sync->{stayalive} and ! $kidsalive) {
                $self->cleanup_controller(1, 'Kids are done');
                exit 0;
            }

            ## XXX: re-examine
            # If we ran an after_sync and grabbed rows, reset the time
            # if (exists $rows_for_custom_code->{source}) {
            #     $SQL = "SELECT $self->{mcp_clock_timestamp}";
            #     $sync->{starttime} = $maindbh->selectall_arrayref($SQL)->[0][0];
            # }

        } ## end if sync done

        ## If we are using checksecs, possibly force a kick
        if ($sync->{checksecs}) {

            ## Already being kicked? Reset the clock
            if ($kick_request) {
                $sync->{lastheardfrom} = time();
            }
            elsif (time() - $sync->{lastheardfrom} >= $sync->{checksecs}) {
                if ($sync->{onetimecopy}) {
                    $self->glog(qq{Timed out, but in onetimecopy mode, so not kicking, for "$syncname"}, LOG_DEBUG);
                }
                else {
                    $self->glog(qq{Timed out - force a sync for "$syncname"}, LOG_VERBOSE);
                    $kick_request = 1;
                }

                ## Reset the clock
                $sync->{lastheardfrom} = time();
            }
        }

        ## XXX What about non stayalive kids?
        ## XXX This is called too soon - recently created kids are not there yet!

        ## Check that our kids are alive and healthy
          ## XXX Skip if we know the kids are busy? (cannot ping/pong!)
        ## XXX Maybe skip this entirely and just check on a kick?
        if ($sync->{stayalive}      ## CTL must be persistent
            and $kidsalive          ## KID must be persistent
            and $self->{kidpid}     ## KID must have been created at least once
            and time() - $kidchecktime >= $config{ctl_checkonkids_time}) {

            my $pidfile = "$config{piddir}/bucardo.kid.sync.$syncname.pid";

            ## If we find a problem, set this to true
            my $resurrect = 0;
            ## Make sure the PID file exists
            if (! -e $pidfile) {
                $self->glog("PID file missing: $pidfile", LOG_DEBUG);
                $resurrect = 1;
            }
            else {
                ## Make sure that a kill 0 sees it
                ## XXX Use ping/pong?
                my $pid = $self->{kidpid};
                $count = kill 0 => $pid;
                if ($count != 1) {
                    $self->glog("Warning: Kid $pid is not responding, will respawn", LOG_TERSE);
                    $resurrect = 2;
                }
            }

            ## At this point, the PID file does not exist or the kid is not responding
            if ($resurrect) {
                ## XXX Try harder to kill it?
                ## First clear out any old entries in the syncrun table
                $sth = $sth{ctl_syncrun_end_now};
                $count = $sth->execute("Old entry died (CTL $$)", $syncname);
                if (1 == $count) {
                    $info = $sth->fetchall_arrayref()->[0][0];
                    $self->glog("Old syncrun entry removed during resurrection, start time was $info", LOG_NORMAL);
                }
                else {
                    $sth->finish();
                }
                $self->glog("Resurrecting kid $syncname, resurrect was $resurrect", LOG_DEBUG);
                $self->{kidpid} = $self->create_newkid($sync);

                ## Sleep a little here to prevent runaway kid creation
                sleep $config{kid_restart_sleep};
            }

            ## Reset the time
            $kidchecktime = time();

        } ## end of time to check on our kid's health

        ## Redo if we are not kicking but are stayalive and the queue is clear
        if (! $kick_request and $sync->{stayalive}) {
            sleep $config{ctl_sleep};
            redo CONTROLLER;
        }

        ## Reset the kick_request for the next run
        $kick_request = 0;

        ## At this point, we know we are about to run a sync
        ## We will either create the kid(s), or signal the existing one(s)

        ## XXX If a custom code handler needs a database handle, create one
        our ($cc_sourcedbh,$safe_sourcedbh);

        ## Run all before_sync code
        ## XXX Move to kid? Do not want to run over and over if something is queued
        if (exists $sync->{code_before_sync}) {
            #$sth{ctl_syncrun_update_status}->execute("Code before_sync (CTL $$)", $syncname);
            $maindbh->commit();
            for my $code (@{$sync->{code_before_sync}}) {
                my $result = $self->run_ctl_custom_code($sync,$input,$code, 'nostrict');
                if ($result eq 'redo') {
                    redo CONTROLLER;
                }
            }
        }

        $maindbh->commit();

        if ($self->{kidpid}) {
            ## Tell any listening kids to go ahead and start
            $self->db_notify($maindbh, "kid_run_$syncname");
        }
        else {
            ## Create any kids that do not exist yet (or have been killed, as detected above)
            $self->glog("Creating a new kid for sync $syncname", LOG_VERBOSE);
            $self->{kidpid} = $self->create_newkid($sync);
        }

        sleep $config{ctl_sleep};
        redo CONTROLLER;

    } ## end CONTROLLER

    die 'How did we reach outside of the main controller loop?';

} ## end of start_controller


sub start_kid {

    ## A single kid, in charge of doing a sync between two or more databases
    ## aka the KID process
    ## Arguments: one
    ## 1. Hashref of sync information
    ## Returns: never (exits)

    my ($self,$sync) = @_;

    my $SQL;

    ## Prefix all log lines with this TLA
    $self->{logprefix} = 'KID';

    ## Extract some of the more common items into local vars
    my ($syncname, $goatlist, $kidsalive, $dbs, $kicked) = @$sync{qw(
          name      goatlist   kidsalive   dbs kick_on_startup)};

    ## Adjust the process name, start logging
    $0 = qq{Bucardo Kid.$self->{extraname} Sync "$syncname"};
    my $extra = $sync->{onetimecopy} ? "OTC: $sync->{onetimecopy}" : '';
    if ($config{log_showsyncname}) {
        $self->{logprefix} .= " ($syncname)";
    }

    $self->glog(qq{New kid, sync "$syncname" alive=$kidsalive Parent=$self->{ctlpid} PID=$$ kicked=$kicked $extra}, LOG_TERSE);

    ## Store our PID into a file
    ## Save the complete returned name for later cleanup
    $self->{kidpidfile} = $self->store_pid( "bucardo.kid.sync.$syncname.pid" );

    ## Establish these early so the DIE block can use them
    my ($S,$T,$pkval) = ('?','?','?');

    ## Keep track of how many times this kid has done work
    my $kidloop = 0;

    ## Catch USR1 errors as a signal from the parent CTL process to exit right away
    local $SIG{USR1} = sub {
        ## Mostly so we do not send an email:
        $self->{clean_exit} = 1;
        die "CTL request\n";
    };

    ## Set up some common groupings of the databases inside sync->{db}
    ## Also setup common attributes
    my (@dbs, @dbs_source, @dbs_target, @dbs_delta, @dbs_fullcopy,
        @dbs_connectable, @dbs_dbi, @dbs_write, @dbs_non_fullcopy,
        @dbs_postgres, @dbs_drizzle, @dbs_firebird, @dbs_mongo, @dbs_mysql, @dbs_oracle,
        @dbs_redis, @dbs_sqlite);

    ## Used to weed out all but one source if in onetimecopy mode
    my $found_first_source = 0;

    for my $dbname (sort keys %{ $sync->{db} }) {

        my $d = $sync->{db}{$dbname};

        ## All databases start with triggers enabled
        $d->{triggers_enabled} = 1;

        ## First, do some exclusions

        ## If this is a onetimecopy sync, the fullcopy targets are dead to us
        next if $sync->{onetimecopy} and $d->{role} eq 'fullcopy';

        ## If this is a onetimecopy sync, we only need to connect to a single source
        if ($sync->{onetimecopy} and $d->{role} eq 'source') {
            next if $found_first_source;
            $found_first_source = 1;
        }

        ## If this is inactive, we've already checked that if it is a source in validate_sync
        ## Thus, if we made it this far, it is a target and should be skipped
        if ($d->{status} eq 'inactive') {
            $self->glog(qq{Skipping inactive database "$dbname" entirely}, LOG_NORMAL);
            ## Don't just skip it: nuke it from orbit! It's the only way to be sure.
            delete $sync->{db}{$dbname};
            next;
        }

        ## Now set the default attributes

        ## Is this a SQL database?
        $d->{does_sql} = 0;

        ## Do we have a DBI-based driver?
        $d->{does_dbi} = 0;

        ## Can it do truncate?
        $d->{does_truncate} = 0;

        ## Does it support asynchronous queries well?
        $d->{does_async} = 0;

        ## Does it have good support for ANY()?
        $d->{does_ANY_clause} = 0;

        ## Can it do savepoints (and roll them back)?
        $d->{does_savepoints} = 0;

        ## Does it support truncate cascade?
        $d->{does_cascade} = 0;

        ## Does it support a LIMIT clause?
        $d->{does_limit} = 0;

        ## Can it be queried?
        $d->{does_append_only} = 0;

        ## List of tables in this database that need makedelta inserts
        $d->{does_makedelta} = {};

        ## Does it have that annoying timestamp +dd bug?
        $d->{has_mysql_timestamp_issue} = 0;

        ## Start clumping into groups and adjust the attributes

        ## Postgres
        if ('postgres' eq $d->{dbtype}) {
            push @dbs_postgres => $dbname;
            $d->{does_sql}        = 1;
            $d->{does_truncate}   = 1;
            $d->{does_savepoints} = 1;
            $d->{does_cascade}    = 1;
            $d->{does_limit}      = 1;
            $d->{does_async}      = 1;
            $d->{does_ANY_clause} = 1;
        }

        ## Drizzle
        if ('drizzle' eq $d->{dbtype}) {
            push @dbs_drizzle => $dbname;
            $d->{does_sql}        = 1;
            $d->{does_truncate}   = 1;
            $d->{does_savepoints} = 1;
            $d->{does_limit}      = 1;
            $d->{has_mysql_timestamp_issue} = 1;
        }

        ## MongoDB
        if ('mongo' eq $d->{dbtype}) {
            push @dbs_mongo => $dbname;
        }

        ## MySQL (and MariaDB)
        if ('mysql' eq $d->{dbtype} or 'mariadb' eq $d->{dbtype}) {
            push @dbs_mysql => $dbname;
            $d->{does_sql}        = 1;
            $d->{does_truncate}   = 1;
            $d->{does_savepoints} = 1;
            $d->{does_limit}      = 1;
            $d->{has_mysql_timestamp_issue} = 1;
        }

        ## Firebird
        if ('firebird' eq $d->{dbtype}) {
            push @dbs_firebird => $dbname;
            $d->{does_sql}        = 1;
            $d->{does_truncate}   = 1;
            $d->{does_savepoints} = 1;
            $d->{does_limit}      = 1;
            $d->{has_mysql_timestamp_issue} = 1;
        }

        ## Oracle
        if ('oracle' eq $d->{dbtype}) {
            push @dbs_oracle => $dbname;
            $d->{does_sql}        = 1;
            $d->{does_truncate}   = 1;
            $d->{does_savepoints} = 1;
        }

        ## Redis
        if ('redis' eq $d->{dbtype}) {
            push @dbs_redis => $dbname;
        }

        ## SQLite
        if ('sqlite' eq $d->{dbtype}) {
            push @dbs_sqlite => $dbname;
            $d->{does_sql}        = 1;
            $d->{does_truncate}   = 1;
            $d->{does_savepoints} = 1;
            $d->{does_limit}      = 1;
        }

        ## Flat files
        if ($d->{dbtype} =~ /flat/) {
            $d->{does_append_only} = 1;
        }

        ## Everyone goes into this bucket
        push @dbs => $dbname;

        ## Databases we read data from
        push @dbs_source => $dbname
            if $d->{role} eq 'source';

        ## Target databases
        push @dbs_target => $dbname
            if $d->{role} ne 'source';

        ## Databases that (potentially) get written to
        ## This is all of them, unless we are a source
        ## and a fullcopy sync or in onetimecopy mode
        push @dbs_write => $dbname
            if (!$sync->{fullcopy} and !$sync->{onetimecopy})
                or $d->{role} ne 'source';

        ## Databases that get deltas
        ## If in onetimecopy mode, this is always forced to be empty
        ## Likewise, no point in populating if this is a fullcopy sync
        push @dbs_delta => $dbname
            if $d->{role} eq 'source'
                and ! $sync->{onetimecopy}
                    and ! $sync->{fullcopy};

        ## Databases that get the full monty
        ## In normal mode, this means a role of 'fullcopy'
        ## In onetimecopy mode, this means a role of 'target'
        push @dbs_fullcopy => $dbname
            if ($sync->{onetimecopy} and $d->{role} eq 'target')
                or ($sync->{fullcopy} and $d->{role} eq 'fullcopy');

        ## Non-fullcopy databases. Basically dbs_source + dbs_target
        push @dbs_non_fullcopy => $dbname
            if $d->{role} ne 'fullcopy';

        ## Databases with Perl DBI support
        if ($d->{dbtype} eq 'postgres'
                or $d->{dbtype} eq 'drizzle'
                or $d->{dbtype} eq 'firebird'
                or $d->{dbtype} eq 'mariadb'
                or $d->{dbtype} eq 'mysql'
                or $d->{dbtype} eq 'oracle'
                or $d->{dbtype} eq 'sqlite') {
            push @dbs_dbi => $dbname;
            $d->{does_dbi} = 1;
        }

        ## Things we can connect to. Almost everything
        push @dbs_connectable => $dbname
            if $d->{dbtype} !~ /flat/;
    }

    ## Connect to the main database
    ($self->{master_backend}, $self->{masterdbh}) = $self->connect_database();

    ## Set a shortcut for this handle, and log the details
    my $maindbh = $self->{masterdbh};
    $self->glog("Bucardo database backend PID: $self->{master_backend}", LOG_VERBOSE);

    ## Setup mapping so we can report in the log which things came from this backend
    $self->{pidmap}{$self->{master_backend}} = 'Bucardo DB';

    ## SQL to enter a new database in the dbrun table
    $SQL = q{
        INSERT INTO bucardo.dbrun(sync,dbname,pgpid)
        VALUES (?,?,?)
    };
    $sth{dbrun_insert} = $maindbh->prepare($SQL);

    ## SQL to remove a database from the dbrun table
    $SQL{dbrun_delete} = q{
        DELETE FROM bucardo.dbrun
        WHERE sync = ? AND dbname = ?
    };
    $sth{dbrun_delete} = $maindbh->prepare($SQL{dbrun_delete});

    ## Disable the CTL exception handler.


    ## Fancy exception handler to clean things up before leaving.
    my $err_handler = sub {

        ## Arguments: one
        ## 1. Error message
        ## Returns: never (exit 1)

        ## Trim whitespace from our message
        my ($msg) = @_;
        $msg =~ s/\s+$//g;

        ## Where did we die?
        my $line = (caller)[2];
        $msg .= "\nLine: $line";

        ## Subject line tweaking later on
        my $moresub = '';

        ## Find any error messages/states for all databases
        if ($msg =~ /DBD::Pg/) {
           $msg .= "\nMain DB state: " . ($maindbh->state || '?');
           $msg .= ' Error: ' . ($maindbh->err || 'none');
           for my $dbname (@dbs_dbi) {

               my $d = $sync->{db}{$dbname};

               my $dbh = $d->{dbh};
               my $state = $dbh->state || '?';
               $msg .= "\nDB $dbname state: $state";
               $msg .= ' Error: ' . ($dbh->err || 'none');
               ## If this was a deadlock problem, try and gather more information
               if ($state eq '40P01' and $d->{dbtype} eq 'postgres') {
                   $msg .= $self->get_deadlock_details($dbh, $msg);
                   $moresub = ' (deadlock)';
                   last;
               }
            }
        }
        $msg .= "\n";

        (my $flatmsg = $msg) =~ s/\n/ /g;
        $self->glog("Kid has died, error is: $flatmsg", LOG_TERSE);

        ## Drop connection to the main database, then reconnect
        if (defined $maindbh and $maindbh) {
            $maindbh->rollback;
            $_->finish for values %{ $maindbh->{CachedKids} };
            $maindbh->disconnect;
        }
        my ($finalbackend, $finaldbh) = $self->connect_database();
        $self->glog("Final database backend PID: $finalbackend", LOG_VERBOSE);
        $sth{dbrun_delete} = $finaldbh->prepare($SQL{dbrun_delete});

        $self->db_notify($finaldbh, 'kid_pid_stop', 1);

        ## Drop all open database connections, clear out the dbrun table
        for my $dbname (@dbs_dbi) {

            my $d = $sync->{db}{$dbname};

            my $dbh = $d->{dbh} or do {
                $self->glog("Missing $dbname database handle", LOG_WARN);
                next;
            };

            ## Is this still around?
            if (!$dbh->ping) {
                $self->glog("Ping failed for database $dbname", LOG_TERSE);
                ## We want to give the MCP a hint that something is wrong
                $self->db_notify($finaldbh, "dead_db_$dbname", 1);
                ## We'll assume no disconnect is necessary - but we'll undef it below just in case
            }
            else {
                ## Rollback, finish all statement handles, and disconnect
                $dbh->rollback();
                $self->glog("Disconnecting from database $dbname", LOG_DEBUG);
                $_->finish for values %{ $dbh->{CachedKids} };
                $dbh->disconnect();
            }

            ## Make sure we don't think we are still in the middle of an async query
            $d->{async_active} = 0;

            ## Make sure we never access this connection again
            undef $dbh;

            ## Clear out the entry from the dbrun table
            $sth = $sth{dbrun_delete};
            $sth->execute($syncname, $dbname);
            $finaldbh->commit();
        }

        ## If using semaphore tables, mark the status as 'failed'
        ## At least in the Mongo case, it's pretty safe to do this,
        ## as it is unlikely the error came from Mongo Land
        if ($config{semaphore_table}) {
            my $tname = $config{semaphore_table};
            for my $dbname (@dbs_connectable) {

                my $d = $sync->{db}{$dbname};

                if ($d->{dbtype} eq 'mongo') {
                    $self->update_mongo_status( $d, $syncname, $tname, 'failed' );
                }
            }
        }

        ## Mark this syncrun as aborted if needed, replace the 'lastbad'
        my $status = "Failed : $flatmsg (KID $$)";
        $self->end_syncrun($finaldbh, 'bad', $syncname, $status);
        $finaldbh->commit();

        ## Update the dbrun table as needed
        $SQL = q{DELETE FROM bucardo.dbrun WHERE sync = ?};
        $sth = $finaldbh->prepare($SQL);
        $sth->execute($syncname);

        ## Let anyone listening know that this target sync aborted. Global message.
        $self->db_notify($finaldbh, "synckill_${syncname}");

        ## Done with database cleanups, so disconnect
        $finaldbh->disconnect();

        ## Send an email as needed (never for clean exit)
        if (! $self->{clean_exit} and $self->{sendmail} or $self->{sendmail_file}) {
            my $warn = $msg =~ /CTL.+request/ ? '' : 'Warning! ';
            $self->glog(qq{${warn}Child for sync "$syncname" was killed at line $line: $msg}, LOG_WARN);

            ## Never display the database passwords
            for (values %{$self->{dbs}}) {
                $_->{dbpass} = '???';
            }
            $self->{dbpass} = '???';

            ## Create the body of the message to be mailed
            my $dump = Dumper $self;

            my $body = qq{
            Kid $$ has been killed at line $line
            Error: $msg
            Possible suspects: $S.$T: $pkval
            Host: $hostname
            Sync name: $syncname
            Stats page: $config{stats_script_url}?sync=$syncname
            Parent process: $self->{mcppid} -> $self->{ctlpid}
            Rows set to aborted: $count
            Version: $VERSION
            Loops: $kidloop
            };

            $body =~ s/^\s+//gsm;
            if ($msg =~ /Found stopfile/) {
                $moresub = ' (stopfile)';
            }
            elsif ($msg =~ /could not connect/) {
                $moresub = ' (no connection)';
            }
            my $subject = qq{Bucardo kid for "$syncname" killed on $shorthost$moresub};
            $self->send_mail({ body => "$body\n", subject => $subject });

        } ## end sending email

        my $extrainfo = sprintf '%s%s%s',
            qq{Sync "$syncname"},
            $S eq '?' ? '' : " $S.$T",
            $pkval eq '?' ? '' : " pk: $pkval";

        $self->cleanup_kid($flatmsg, $extrainfo);

        exit 1;

    }; ## end $err_handler

    my $stop_sync_request = "stopsync_$syncname";
    ## Tracks how long it has been since we last ran a ping against our databases
    my $lastpingcheck = 0;

    ## Row counts from the delta tables:
    my %deltacount;

    ## Count of changes made (inserts,deletes,truncates,conflicts handled):
    my %dmlcount;

    my $did_setup = 0;
    local $@;
    eval {
        ## Listen for the controller asking us to go again if persistent
        if ($kidsalive) {
            $self->db_listen( $maindbh, "kid_run_$syncname" );
        }

        ## Listen for a kid ping, even if not persistent
        my $kidping = "${$}_ping";
        $self->db_listen( $maindbh, "kid_$kidping" );

        ## Listen for a sync-wide exit signal
        $self->db_listen( $maindbh, "kid_$stop_sync_request" );

        ## Prepare all of our SQL
        ## Note that none of this is actually 'prepared' until the first execute

        ## SQL to add a new row to the syncrun table
        $SQL = 'INSERT INTO bucardo.syncrun(sync,status) VALUES (?,?)';
        $sth{kid_syncrun_insert} = $maindbh->prepare($SQL);

        ## SQL to update the syncrun table's status only
        $SQL = q{
            UPDATE bucardo.syncrun
            SET    status=?
            WHERE  sync=?
            AND    ended IS NULL
        };
        $sth{kid_syncrun_update_status} = $maindbh->prepare($SQL);

        ## SQL to set the syncrun table as ended once complete
        $SQL = q{
            UPDATE bucardo.syncrun
            SET    deletes=deletes+?, inserts=inserts+?, truncates=truncates+?,
                   conflicts=?, details=?, status=?
            WHERE  sync=?
            AND    ended IS NULL
        };
        $sth{kid_syncrun_end} = $maindbh->prepare($SQL);

        ## Connect to all (connectable) databases we are responsible for
        ## This main list has already been pruned by the controller as needed
        for my $dbname (@dbs_connectable) {

            my $d = $sync->{db}{$dbname};

            ($d->{backend}, $d->{dbh}) = $self->connect_database($dbname);
            $self->glog(qq{Database "$dbname" backend PID: $d->{backend}}, LOG_VERBOSE);

            ## Register ourself with the MCP (if we are Postgres)
            if ($d->{dbtype} eq 'postgres') {
                $self->db_notify($maindbh, 'kid_pid_start', 1, $dbname);
            }
        }

        ## Set the maximum length of the $dbname.$S.$T string.
        ## Used for logging output
        $self->{maxdbname} = 1;
        for my $dbname (keys %{ $sync->{db} }) {
            $self->{maxdbname} = length $dbname if length $dbname > $self->{maxdbname};
        }
        my $maxst = 3;
        for my $g (@$goatlist) {
            next if $g->{reltype} ne 'table';
            ($S,$T) = ($g->{safeschema},$g->{safetable});
            $maxst = length "$S.$T" if length ("$S.$T") > $maxst;
        }
        $self->{maxdbstname} = $self->{maxdbname} + 1 + $maxst;

        ## If we are using delta tables, prepare all relevant SQL
        if (@dbs_delta) {

            ## Prepare the SQL specific to each table
            for my $g (@$goatlist) {

                ## Only tables get all this fuss: sequences are easy
                next if $g->{reltype} ne 'table';

                ## This is the main query: grab all unique changed primary keys since the last sync
                $SQL{delta}{$g} = qq{
                    SELECT  DISTINCT $g->{pklist}
                    FROM    bucardo.$g->{deltatable} d
                    WHERE   NOT EXISTS (
                               SELECT 1
                               FROM   bucardo.$g->{tracktable} t
                               WHERE  d.txntime = t.txntime
                               AND    (t.target = DBGROUP::text)
                            )
                    };

                ## We also need secondary queries to catch the case of partial replications
                ## This is a per-target check
                $SQL{deltatarget}{$g} = qq{
                    SELECT  DISTINCT $g->{pklist}
                    FROM    bucardo.$g->{deltatable} d
                    WHERE   NOT EXISTS (
                               SELECT 1
                               FROM   bucardo.$g->{tracktable} t
                               WHERE  d.txntime = t.txntime
                               AND    (t.target = TARGETNAME::text)
                            )
                    };

                ## Mark all unclaimed visible delta rows as done in the track table
                $SQL{track}{$g} = qq{
                    INSERT INTO bucardo.$g->{tracktable} (txntime,target)
                    SELECT DISTINCT txntime, DBGROUP::text
                    FROM bucardo.$g->{deltatable} d
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM   bucardo.$g->{tracktable} t
                        WHERE  d.txntime = t.txntime
                        AND    (t.target = DBGROUP::text)
                    );
                };

                ## The same thing, but to the staging table instead, as we have to
                ## wait for all targets to succesfully commit in multi-source situations
                ($SQL{stage}{$g} = $SQL{track}{$g}) =~ s/$g->{tracktable}/$g->{stagetable}/;


            } ## end each table

            ## For each source database, prepare the queries above
            for my $dbname (@dbs_source) {

                my $d = $sync->{db}{$dbname};

                ## Set the DBGROUP for each database: the bucardo.track_* target entry
                $d->{DBGROUPNAME} = "dbgroup $dbs";

                for my $g (@$goatlist) {

                    next if $g->{reltype} ne 'table';

                    ($S,$T) = ($g->{safeschema},$g->{safetable});

                    ## Replace with the target name for source delta querying
                    ($SQL = $SQL{delta}{$g}) =~ s/DBGROUP/'$d->{DBGROUPNAME}'/o;

                    ## As these can be expensive, make them asynchronous
                    $sth{getdelta}{$dbname}{$g} = $d->{dbh}->prepare($SQL, {pg_async => PG_ASYNC});

                    ## We need to update either the track table or the stage table
                    ## There is no way to know beforehand which we will need, so we prepare both

                    ## Replace with the target name for source track updating
                    ($SQL = $SQL{track}{$g}) =~ s/DBGROUP/'$d->{DBGROUPNAME}'/go;
                    ## Again, async as they may be slow
                    $sth{track}{$dbname}{$g} = $d->{dbh}->prepare($SQL, {pg_async => PG_ASYNC});

                    ## Same thing for stage
                    ($SQL = $SQL{stage}{$g}) =~ s/DBGROUP/'$d->{DBGROUPNAME}'/go;
                    $sth{stage}{$dbname}{$g} = $d->{dbh}->prepare($SQL, {pg_async => PG_ASYNC});

                } ## end each table

            } ## end each source database


            ## Set all makedelta tables (target databases can have them too, as another sync may have them as a source)
            for my $dbname (@dbs) {

                my $d = $sync->{db}{$dbname};

                for my $g (@$goatlist) {

                    next if $g->{reltype} ne 'table';
                    ($S,$T) = ($g->{safeschema},$g->{safetable});
                    ## Set the per database/per table makedelta setting now
                    if (1 == $d->{makedelta} or $g->{makedelta} eq 'on' or $g->{makedelta} =~ /\b$dbname\b/) {
                        $d->{does_makedelta}{"$S.$T"} = 1;
                        $self->glog("Set table $dbname.$S.$T to makedelta", LOG_NORMAL);
                    }

                } ## end each table

            } ## end all databases

        } ## end if delta databases

        ## Create safe versions of the database handles if we are going to need them
        if ($sync->{need_safe_dbh_strict} or $sync->{need_safe_dbh}) {

            for my $dbname (@dbs_postgres) {

                my $d = $sync->{db}{$dbname};

                my $darg;
                if ($sync->{need_safe_dbh_strict}) {
                    for my $arg (sort keys %{ $dbix{ $d->{role} }{strict} }) {
                        next if ! length $dbix{ $d->{role} }{strict}{$arg};
                        $darg->{$arg} = $dbix{ $d->{role} }{strict}{$arg};
                    }
                    $darg->{dbh} = $d->{dbh};
                    $self->{safe_dbh_strict}{$dbname} = DBIx::Safe->new($darg);
                }

                if ($sync->{need_safe_dbh}) {
                    undef $darg;
                    for my $arg (sort keys %{ $dbix{ $d->{role} }{notstrict} }) {
                        next if ! length $dbix{ $d->{role} }{notstrict}{$arg};
                        $darg->{$arg} = $dbix{ $d->{role} }{notstrict}{$arg};
                    }
                    $darg->{dbh} = $d->{dbh};
                    $self->{safe_dbh}{$dbname} = DBIx::Safe->new($darg);
                }
            }

        } ## end DBIX::Safe creations
        $did_setup = 1;
    };
    $err_handler->($@) if !$did_setup;

    ## Begin the main KID loop
    my $didrun = 0;
    my $runkid = sub {
      KID: {
        ## Leave right away if we find a stopfile
        if (-e $self->{stop_file}) {
            $self->glog(qq{Found stopfile "$self->{stop_file}": exiting}, LOG_WARN);
            last KID;
        }

        ## Should we actually do something this round?
        my $dorun = 0;

        ## If we were just created or kicked, go ahead and start a run.
        if ($kicked) {
            $dorun = 1;
            $kicked = 0;
        }

        ## If persistent, listen for messages and do an occasional ping of all databases
        if ($kidsalive) {

            my $nlist = $self->db_get_notices($maindbh);

            for my $name (sort keys %{ $nlist }) {

                my $npid = $nlist->{$name}{firstpid};

                ## Strip the prefix
                $name =~ s/^kid_//o;

                ## The controller wants us to exit
                if ( $name eq $stop_sync_request ) {
                    $self->glog('Got a stop sync request, so exiting', LOG_TERSE);
                    die 'Stop sync request';
                }

                ## The controller has told us we are clear to go
                elsif ($name eq "run_$syncname") {
                    $dorun = 1;
                }

                ## Got a ping? Respond with a pong.
                elsif ($name eq "${$}_ping") {
                    $self->glog('Got a ping, issuing pong', LOG_DEBUG);
                    $self->db_notify($maindbh, "kid_${$}_pong");
                }

                ## Someone else's sync is running
                elsif (index($name, 'run_') == 0) {
                }
                ## Someone else's sync is stopping
                elsif (index($name, 'stopsync_') == 0) {
                }
                ## Someone else's kid is getting pinged
                elsif (index($name, '_ping') > 0) {
                }

                ## Should not happen, but let's at least log it
                else {
                    $self->glog("Warning: received unknown message $name from $npid!", LOG_TERSE);
                }

            } ## end each notice

            ## Now that we've read in any notices, simply rollback
            $maindbh->rollback();

            ## Periodically verify connections to all databases
            if (time() - $lastpingcheck >= $config{kid_pingtime}) {
                ## If this fails, simply have the CTL restart it
                ## Other things match on the exception wording below, so change carefully
                $maindbh->ping or die qq{Ping failed for main database\n};
                for my $dbname (@dbs_dbi) {

                    my $d = $sync->{db}{$dbname};

                    $d->{dbh}->ping or die qq{Ping failed for database "$dbname"\n};
                    $d->{dbh}->rollback();
                }
                $lastpingcheck = time();
            }

        } ## end if kidsalive

        ## If we are not doing anything this round, sleep and start over
        ## We will only ever hit this on the second go around, as kids
        ## start as autokicked
        if (! $dorun) {
            sleep $config{kid_sleep};
            redo KID;
        }

        ## From this point on, we are a live kid that is expected to run the sync

        ## Used to report on total times for the long-running parts, e.g. COPY
        my $kid_start_time = [gettimeofday];

        ## Create an entry in the syncrun table to let people know we've started
        $self->glog('Adding entry to syncrun table', LOG_DEBUG);
        $sth{kid_syncrun_insert}->execute($syncname, "Started (KID $$)");

        ## Increment our count of how many times we have been here before
        $kidloop++;

        ## Reset the numbers to track total bucardo_delta matches
        undef %deltacount;
        $deltacount{all} = 0;
        $deltacount{alltables} = 0;
        $deltacount{table} = {};

        ## Reset our counts of total inserts, deletes, truncates, and conflicts
        undef %dmlcount;
        $dmlcount{deletes} = 0;
        $dmlcount{inserts} = 0;
        $dmlcount{truncates} = 0;
        $dmlcount{conflicts} = 0;

        ## Reset all of our truncate stuff
        $self->{has_truncation} = 0;
        delete $self->{truncateinfo};

        ## Reset some things at the per-database level
        for my $dbname (keys %{ $sync->{db} }) {

            my $d = $sync->{db}{$dbname};

            ## This must be set, as it is used by the conflict_strategy below
            $deltacount{$dbname} = 0;
            $dmlcount{allinserts}{$dbname} = 0;
            $dmlcount{alldeletes}{$dbname} = 0;

            delete $d->{truncatewinner};

        }

        ## Reset things at the goat level
        for my $g (@$goatlist) {
            delete $g->{truncatewinner};
        }

        ## Run all 'before_txn' code
        if (exists $sync->{code_before_txn}) {
            ## Let external people know where we are
            $sth{kid_syncrun_update_status}->execute("Code before_txn (KID $$)", $syncname);
            $maindbh->commit();
            for my $code (@{$sync->{code_before_txn}}) {
                ## Check if the code has asked us to skip other before_txn codes
                last if 'last' eq $self->run_kid_custom_code($sync, $code);
            }
        }

        ## Populate the dbrun table so others know we are using these databases
        $self->glog('Populating the dbrun table', LOG_DEBUG);
        for my $dbname (@dbs_connectable) {

            my $d = $sync->{db}{$dbname};

            $sth{dbrun_insert}->execute($syncname, $dbname, $d->{backend});
        }

        ## Add a note to the syncrun table
        $self->glog('Adding note to the syncrun table', LOG_DEBUG);
        $sth{kid_syncrun_update_status}->execute("Begin txn (KID $$)", $syncname);

        ## Figure out our isolation level. Only used for Postgres
        ## All others are hard-coded as 'serializable'
        $self->{pg_isolation_level} = defined $sync->{isolation_level} ? $sync->{isolation_level} :
            $config{isolation_level} || 'serializable';

        ## Commit so our dbrun and syncrun stuff is visible to others
        ## This should be done just before we start transactions on all dbs
        $self->glog('Doing final maindbh commit', LOG_DEBUG);
        $maindbh->commit();

        ## Start the main transaction and do things such as setting isolation levels
        $self->start_main_transaction({ sync => $sync, databases => \@dbs_connectable});

        ## We may have a request to lock all the tables
        $self->lock_all_tables({ sync => $sync, databases => \@dbs_write, tables => $goatlist});

        ## Do all the delta (non-fullcopy) targets
        if (@dbs_delta) {

            ## We will never reach this while in onetimecopy mode as @dbs_delta is emptied

            ## Run all 'before_check_rows' code
            if (exists $sync->{code_before_check_rows}) {
                $sth{kid_syncrun_update_status}->execute("Code before_check_rows (KID $$)", $syncname);
                $maindbh->commit();
                for my $code (@{$sync->{code_before_check_rows}}) {
                    ## Check if the code has asked us to skip other before_check_rows codes
                    last if 'last' eq $self->run_kid_custom_code($sync, $code);
                }
            }

            ## Check if any tables were truncated on all source databases
            ## If so, set $self->{has_truncation}; store results in $self->{truncateinfo}
            ## First level keys are schema then table name
            ## Third level is maxtime and maxdb, showing the "winner" for each table

            $SQL = 'SELECT quote_ident(sname), quote_ident(tname), MAX(EXTRACT(epoch FROM cdate))'
                   . ' FROM bucardo.bucardo_truncate_trigger '
                   . ' WHERE sync = ? AND replicated IS NULL GROUP BY 1,2';

            for my $dbname (@dbs_source) {

                my $d = $sync->{db}{$dbname};

                ## Grab the latest truncation time for each table, for this source database
                $self->glog(qq{Checking truncate_trigger table on database "$dbname"}, LOG_VERBOSE);
                $sth = $d->{dbh}->prepare($SQL);
                $self->{has_truncation} += $sth->execute($syncname);
                for my $row (@{ $sth->fetchall_arrayref() }) {
                    my ($s,$t,$time) = @{ $row };
                    ## Store if this is the new winner
                    if (! exists $self->{truncateinfo}{$s}{$t}{maxtime}
                            or $time > $self->{truncateinfo}{$s}{$t}{maxtime}) {
                        $self->{truncateinfo}{$s}{$t}{maxtime} = $time;
                        $self->{truncateinfo}{$s}{$t}{maxdb} = $dbname;
                    }
                }

            } ## end each source database, checking for truncations

            ## Now go through and mark the winner within the "x" hash, for easy skipping later on
            if ($self->{has_truncation}) {
                for my $s (keys %{ $self->{truncateinfo} }) {
                    for my $t (keys %{ $self->{truncateinfo}{$s} }) {
                        my $dbname = $self->{truncateinfo}{$s}{$t}{maxdb};
                        my $d = $sync->{db}{$dbname};
                        $d->{truncatewinner}{$s}{$t} = 1;
                        $self->glog("Truncate winner for $s.$t is database $dbname", LOG_DEBUG);
                    }
                }
                ## Set the truncate count
                my $number = @dbs_non_fullcopy; ## not the best estimate: corner cases
                $dmlcount{truncate} = $number - 1;

                ## Now map this back to our goatlist
                for my $g (@$goatlist) {
                    next if $g->{reltype} ne 'table';
                    ($S,$T) = ($g->{safeschema},$g->{safetable});
                    if (exists $self->{truncateinfo}{$S}{$T}) {
                        $g->{truncatewinner} = $self->{truncateinfo}{$S}{$T}{maxdb};
                    }
                }
            }

            ## Next, handle all the sequences
            for my $g (@$goatlist) {

                next if $g->{reltype} ne 'sequence';

                ($S,$T) = ($g->{safeschema},$g->{safetable});

                ## Grab the sequence information from each database
                ## Figure out which source one is the highest
                ## Right now, this is the only sane option.
                ## In the future, we might consider coupling tables and sequences and
                ## then copying sequences based on the 'winning' underlying table
                $SQL = "SELECT * FROM $S.$T";
                my $maxvalue = -1;
                for my $dbname (@dbs_non_fullcopy) {

                    my $d = $sync->{db}{$dbname};

                    next if $d->{dbtype} ne 'postgres';

                    $sth = $d->{dbh}->prepare($SQL);
                    $sth->execute();
                    my $info = $sth->fetchall_arrayref({})->[0];
                    $g->{sequenceinfo}{$dbname} = $info;

                    ## Only the source databases matter for the max value comparison
                    next if $d->{role} ne 'source';

                    if ($info->{last_value} > $maxvalue) {
                        $maxvalue = $info->{last_value};
                        $g->{winning_db} = $dbname;
                    }
                }

                $self->glog("Sequence $S.$T from db $g->{winning_db} is the highest", LOG_DEBUG);

                ## Now that we have a winner, apply the changes to every other (non-fullcopy) PG database
                for my $dbname (@dbs_non_fullcopy) {

                    my $d = $sync->{db}{$dbname};

                    next if $d->{dbtype} ne 'postgres';

                    $d->{adjustsequence} = 1;
                }

                $deltacount{sequences} += $self->adjust_sequence($g, $sync, $S, $T, $syncname);

            } ## end of handling sequences

            ## We want to line up all the delta count numbers in the logs,
            ## so this tracks the largest number returned
            my $maxcount = 0;

            ## Use the bucardo_delta_check function on each database, which gives us
            ## a quick summary of whether each table has any active delta rows
            ## This is a big win on slow networks!
            if ($config{quick_delta_check}) {
                for my $dbname (@dbs_source) {

                    my $d = $sync->{db}{$dbname};

                    $sth{kid_syncrun_update_status}->execute("delta_check on db $dbname",$syncname);
                    $maindbh->commit();

                    $SQL = 'SELECT * FROM bucardo.bucardo_delta_check(?,?)';
                    $sth = $d->{dbh}->prepare($SQL);
                    $sth->execute($syncname, $d->{DBGROUPNAME});
                    $d->{deltazero} = $d->{deltatotal} = 0;
                    for my $row (@{$sth->fetchall_arrayref()}) {
                        my ($number,$tablename) = split /,/ => $row->[0], 2;
                        $d->{deltaquick}{$tablename} = $number;
                        if ($number) {
                            $d->{deltatotal}++;
                            $deltacount{table}{$tablename}++;
                        }
                        else {
                            $d->{deltazero}++;
                        }
                    }
                    $self->glog("Tables with deltas on $dbname: $d->{deltatotal} Without: $d->{deltazero}", LOG_VERBOSE);

                } ## end quick delta check for each database

            } ## end quick delta check

            ## Grab the delta information for each table from each source database
            ## While we could do this as per-db/per-goat instead of per-goat/per-db,
            ## we want to take advantage of the async requests as much as possible,
            ## and we'll get the best benefit by hitting each db in turn

            for my $g (@$goatlist) {

                ## Again, this is only for tables
                next if $g->{reltype} ne 'table';

                ## Populate the global vars
                ($S,$T) = ($g->{safeschema},$g->{safetable});

                ## This is the meat of Bucardo:
                for my $dbname (@dbs_source) {

                    ## If we had a truncation, we only get deltas from the "winning" source
                    ## We still need these, as we want to respect changes made after the truncation!
                    next if exists $g->{truncatewinner} and $g->{truncatewinner} ne $dbname;

                    my $d = $sync->{db}{$dbname};

                    ## No need to grab information if we know there are no deltas for this table
                    if ($config{quick_delta_check}) {
                        next if ! $d->{deltaquick}{"$S.$T"};
                    }

                    $sth{kid_syncrun_update_status}->execute("Counting all deltas on db $dbname",$syncname);
                    $maindbh->commit();

                    ## Gets all relevant rows from bucardo_deltas: runs asynchronously
                    $d->{async_active} = time;
                    $sth{getdelta}{$dbname}{$g}->execute();
                }

                ## Grab all results as they finish.
                ## Order does not really matter here, except for consistency in the logs
                for my $dbname (@dbs_source) {

                    ## Skip if truncating and this one is not the winner
                    next if exists $g->{truncatewinner} and $g->{truncatewinner} ne $dbname;

                    my $d = $sync->{db}{$dbname};

                    ## If we skipped this, set the deltacount to zero and move on
                    if ($config{quick_delta_check}) {
                        if (! $d->{deltaquick}{"$S.$T"}) {
                            $deltacount{dbtable}{$dbname}{$S}{$T} = 0;
                            next;
                        }
                    }

                    ## pg_result tells us to wait for the query to finish
                    $count = $d->{dbh}->pg_result();
                    $d->{async_active} = 0;

                    ## Call finish() and change the ugly 0E0 to a true zero
                    $sth{getdelta}{$dbname}{$g}->finish() if $count =~ s/0E0/0/o;

                    ## Store counts globally (per sync), per DB, per table, and per table/DB
                    $deltacount{all} += $count;
                    $deltacount{db}{$dbname} += $count;
                    $deltacount{table}{$S}{$T} += $count;
                    $deltacount{dbtable}{$dbname}{$S}{$T} = $count; ## NOT a +=

                    ## Special versions for FK checks below
                    if ($count) {
                        $deltacount{tableoid}{$g->{oid}}{$dbname} = $count;
                    }

                    ## For our pretty output below
                    $maxcount = $count if $count > $maxcount;

                } ## end each database

            } ## end each table (deltacount)

            ## Output the counts, now that we know the widths
            for my $g (@$goatlist) {

                ## Only for tables
                next if $g->{reltype} ne 'table';

                ## Populate the global vars
                ($S,$T) = ($g->{safeschema},$g->{safetable});

                for my $dbname (@dbs_source) {

                    ## Skip if truncating and this one is not the winner
                    next if exists $g->{truncatewinner} and $g->{truncatewinner} ne $dbname;

                    $self->glog((sprintf q{Delta count for %-*s : %*d},
                                 $self->{maxdbstname},
                                 "$dbname.$S.$T",
                                 length $maxcount,
                                 $deltacount{dbtable}{$dbname}{$S}{$T}), LOG_VERBOSE);
                } ## end each db

            } ## end each table

            ## Report on the total number of deltas found
            $self->glog("Total delta count: $deltacount{all}", LOG_VERBOSE);

            ## Reset our list of possible FK issues
            $sync->{fkcheck} = {};

            ## If more than one total source db, break it down at that level
            ## We also check for foreign key dependencies here
            if (keys %{ $deltacount{db} } > 1) {

                ## Figure out the width for the per-db breakdown below
                my $maxdbcount = 0;
                for my $dbname (sort keys %{ $sync->{db} }) {
                    $maxdbcount = $deltacount{db}{$dbname}
                        if exists $deltacount{db}{$dbname}
                            and $deltacount{db}{$dbname} > $maxdbcount;
                }

                for my $dbname (@dbs_source) {

                    ## Skip if truncating and deltacount is thus not set
                    next if ! exists $deltacount{db}{$dbname};

                    $self->glog((sprintf q{Delta count for %-*s: %*d},
                                $self->{maxdbname} + 2,
                                qq{"$dbname"},
                                 length $maxdbcount,
                                $deltacount{db}{$dbname}), LOG_VERBOSE);
                }

                ## Since we have changes appearing on more than one database,
                ## we need to see if any of the database-spanning tables involved
                ## are linked via foreign keys. If they are, we may have to
                ## change our replication strategy so that the foreign keys are
                ## still intact at the end of our operation.
                ## If we find tables that need to be checked, we add them to $self->{fkcheck}

                ## Walk through each table with changes
                for my $toid (sort keys %{ $deltacount{tableoid} }) {

                    my $t1 = $deltacount{tableoid}{$toid};
                    my $tname1 = $sync->{tableoid}{$toid}{name};

                    ## Find all tables that this table references
                    my $info = $sync->{tableoid}{$toid};
                    ## Note that we really only need to check one of references or referencedby
                  REFFER: for my $reftable (sort keys %{ $info->{references} } ) {

                        ## Skip if it has no changes
                        next if ! exists $deltacount{tableoid}{$reftable};

                        ## At this point, we know that both linked tables have at
                        ## least one source change. We also know that at least two
                        ## source databases are involved in this sync.

                        my $t2 = $deltacount{tableoid}{$reftable};
                        my $tname2 = $sync->{tableoid}{$reftable}{name};

                        ## The danger is if the changes come from different databases
                        ## If this happens, the foreign key relationship may be violated
                        ## when we push the changes both ways.

                        ## Check if any of the dbs are mismatched. If so, instant FK marking
                        for my $db1 (sort keys %$t1) {
                            if (! exists $t2->{$db1}) {
                                $self->glog("Table $tname1 and $tname2 may have FK issues", LOG_DEBUG);
                                $sync->{fkcheck}{$tname1}{$tname2} = 1;
                                next REFFER;
                            }
                        }

                        ## So both tables have changes on the same source databases.
                        ## Now the only danger is if either has more than one source
                        if (keys %$t1 > 1 or keys %$t2 > 1) {
                            $self->glog("Table $tname1 and $tname2 may have FK issues", LOG_DEBUG);
                            $sync->{fkcheck}{$tname1}{$tname2} = 1;
                            $sync->{fkcheck}{$tname2}{$tname1} = 2;
                        }

                    } ## end each reffed table

                } ## end each changed table

            } ## end if more than one source database has changes

            ## If there were no changes on any sources, rollback all databases,
            ## update the syncrun and dbrun tables, notify listeners,
            ## then either re-loop or leave

            if (! $deltacount{all} and ! $self->{has_truncation}) {

               ## If we modified the bucardo_sequences table, save the change
                if ($deltacount{sequences}) {
                    #die "fixme";
                    #$sourcedbh->commit();
                }

                ## Just to be safe, rollback everything
                for my $dbname (@dbs_dbi) {

                    my $d = $sync->{db}{$dbname};

                    $d->{dbh}->rollback();
                }

                ## Clear out the entries from the dbrun table
                for my $dbname (@dbs_connectable) {

                    my $d = $sync->{db}{$dbname};

                    ## We never do native fullcopy targets here
                    next if $d->{role} eq 'fullcopy';

                    $sth = $sth{dbrun_delete};
                    $sth->execute($syncname, $dbname);
                    $maindbh->commit();
                }

                ## Clear the syncrun table
                my $msg = "No delta rows found (KID $$)";
                $self->end_syncrun($maindbh, 'empty', $syncname, $msg);

                $maindbh->commit();

                ## Let the CTL know we are done
                $self->db_notify($maindbh, "ctl_syncdone_${syncname}");
                $maindbh->commit();

                ## Even with no changes, we like to know how long this took
                my $synctime = sprintf '%.2f', tv_interval($kid_start_time);
                $self->glog((sprintf 'Total time for sync "%s" (no rows): %s%s',
                    $syncname,
                    pretty_time($synctime),
                    $synctime < 120 ? '' : " ($synctime seconds)",),
                    LOG_DEBUG);

                ## Sleep a hair
                sleep $config{kid_nodeltarows_sleep};

                redo KID if $kidsalive;
                last KID;

            } ## end no deltas

            ## Only need to turn off triggers and rules once via pg_class
            my $disabled_via_pg_class = 0;

            ## Reset all of our non-persistent conflict information
            $self->{conflictinfo} = {};

            ## Custom conflict handler may have told us to always use the same winner
            if (exists $self->{conflictinfo}{winneralways}) {
                $self->{conflictinfo}{winners} = $self->{conflictinfo}{winneralways};
            }

            ## Do each goat in turn

          PUSHDELTA_GOAT: for my $g (@$goatlist) {

                ## No need to proceed unless we're a table
                next if $g->{reltype} ne 'table';

                ## Skip if we've already handled this via fullcopy
                next if $g->{source}{needstruncation};

                ($S,$T) = ($g->{safeschema},$g->{safetable});

                ## Skip this table if no source rows have changed
                ## However, we still need to go on in the case of a truncation
                next if ! $deltacount{table}{$S}{$T} and ! exists $g->{truncatewinner};

                ## How many times this goat has handled an exception?
                $g->{exceptions} ||= 0;

                ## The list of primary key columns
                if (! $g->{pkeycols}) { ## only do this once
                    $g->{pkeycols} = '';
                    $i=0;
                    for my $qpk (@{$g->{qpkey}}) {
                        $g->{pkeycols} .= sprintf '%s,', $g->{binarypkey}{$i} ? qq{ENCODE($qpk,'base64')} : $qpk;
                        $i++;
                    }
                    chop $g->{pkeycols};
                    $g->{numpkcols} > 1 and $g->{pkeycols} = "($g->{pkeycols})";
                    ## Example: id
                    ## Example MCPK: (id,"space bar",cdate)

                    ## Store a raw version for some non-Postgres targets
                    $g->{pkeycolsraw} = join ',' => @{ $g->{pkey} };

                }

                ## How many times have we done the loop below?
                my $delta_attempts = 0;

                ## For each source database, grab all distinct pks for this table
                ## from bucardo_delta (that have not already been pushed to the targetname)
                ## We've already executed and got a count from these queries:
                ## it's now time to gather the actual data
                my %deltabin;

                ## Customcode may need to know which rows we have changed: reset it here
                $sync->{deltarows} = {};

                for my $dbname (@dbs_source) {

                    ## Skip if we are truncating and this is not the winner
                    next if exists $g->{truncatewinner} and $g->{truncatewinner} ne $dbname;

                    ## If this is a truncation, we always want the deltabin to exist, even if empty!
                    if (exists $g->{truncatewinner}) {
                        $deltabin{$dbname} = {};
                    }

                    ## Skip if we know we have no rows - and thus have issued a finish()
                    next if ! $deltacount{dbtable}{$dbname}{$S}{$T};

                    ## Create an empty hash to hold the primary key information
                    $deltabin{$dbname} = {};

                    $sth{kid_syncrun_update_status}->execute("Get deltas from db $dbname",$syncname);
                    $maindbh->commit();

                    while (my $y = $sth{getdelta}{$dbname}{$g}->fetchrow_arrayref()) {
                        ## Join all primary keys together with \0, put into hash as key
                        ## XXX: Using \0 is not unique for binaries
                        if (!$g->{hasbinarypk}) {
                            $deltabin{$dbname}{join "\0" => @$y} = 1;
                        }
                        else {
                            my $decodename = '';

                            my @pk;
                            for my $row (@$y) {
                                push @pk => $row;
                            }
                            $deltabin{$dbname}{join "\0" => @pk} = 1;
                        }
                    }

                } ## end getting pks from each db for this table

                ## Walk through and make sure we have only one source for each primary key

                ## Simple map of what we've already compared:
                my %seenpair;

                ## Hash indicating which databases have conflicts:
                $self->{db_hasconflict} = {};

                ## Hash of all conflicts for this goat
                ## Key is the primary key value
                ## Value is a list of all databases containing this value
                my %conflict;

                for my $dbname1 (sort keys %deltabin) {

                   for my $dbname2 (sort keys %deltabin) {

                        ## Don't compare with ourselves
                        next if $dbname1 eq $dbname2;

                        ## Skip if we've already handled this pair the reverse way
                        next if exists $seenpair{$dbname2}{$dbname1};
                        $seenpair{$dbname1}{$dbname2} = 1;

                        ## Loop through all rows from database 1 and see if they exist on 2
                        ## If they do, it's a conflict, and one of them must win
                        ## Store in the conflict hash for processing below
                        for my $key (keys %{ $deltabin{$dbname1} }) {
                            next if ! exists $deltabin{$dbname2}{$key};

                            ## Got a conflict! Same pkey updated on both sides
                            $conflict{$key}{$dbname1} = 1;
                            $conflict{$key}{$dbname2} = 1;

                            ## Build a list of which databases have conflicts
                            $self->{db_hasconflict}{$dbname1} = 1;
                            $self->{db_hasconflict}{$dbname2} = 1;
                        }
                    }
                }

                ## If we had any conflicts, handle them now
                $count = keys %conflict;
                if ($count) {

                    ## Increment count across all tables
                    $dmlcount{conflicts} += $count;

                    $self->glog("Conflicts for $S.$T: $count", LOG_NORMAL);

                    ## If we have a custom conflict handler for this goat, invoke it
                    if ($g->{code_conflict}) {

                        ## We can safely skip this if we already have the winners list in some format
                        if (exists $self->{conflictinfo}{tablewinner_always}{$g}) {
                            $self->glog('Using previous tablewinner_always winner', LOG_DEBUG);
                        }
                        elsif (exists $self->{conflictinfo}{syncwinner}) {
                            $self->glog('Using previous syncwinner winner', LOG_DEBUG);
                        }
                        elsif (exists $self->{conflictinfo}{syncwinner_always}) {
                            $self->glog('Using previous syncwinner_always winner', LOG_DEBUG);
                        }
                        else {
                            $self->glog('Starting code_conflict', LOG_VERBOSE);

                            ## Give each piece of code a chance to resolve the conflict
                            for my $code (@{ $g->{code_conflict} }) {

                                ## The all important conflict hash, which the caller may change
                                $code->{info}{conflicts} = \%conflict;

                                ## Provide the current schema and table name
                                $code->{info}{schemaname} = $S;
                                $code->{info}{tablename} = $T;

                                ## Provide detailed information on all databases, but elide the dbh
                                for my $dbname (@dbs_connectable) {

                                    my $d = $sync->{db}{$dbname};

                                    ## Make a shallow copy, excluding the actual dbh handle
                                    for my $name (keys %$d) {

                                        ## We provide DBIx::Safe versions elsewhere
                                        next if $name eq 'dbh';

                                        $code->{info}{dbinfo}{$dbname}{$name} = $d->{$name};
                                    }
                                }

                                my $cname = $code->{name};

                                ## Run the conflict handler customcode, get the result
                                my $result = $self->run_kid_custom_code($sync, $code);
                                $self->glog("Result of custom code $cname is $result", LOG_DEBUG);

                                ## Code has asked us to do nothing
                                next if 'skip' eq $result;

                                ## How to handle conflicts for this table right now only:
                                if ($result =~ /tablewinner: (.+)/o) {
                                    my $winlist = $1;
                                    $self->glog("Custom code $cname says table winners should be: $winlist", LOG_VERBOSE);
                                    $self->{conflictinfo}{tablewinner}{$g} = $winlist;
                                    last;
                                }

                                ## How to handle conflicts for this table until the sync restarts:
                                if ($result =~ /tablewinner_always: (.+)/o) {
                                    my $winlist = $1;
                                    $self->glog("Custom code $cname says table winners should always be: $winlist", LOG_VERBOSE);
                                    $self->{conflictinfo}{tablewinner_always}{$g} = $winlist;
                                    last;
                                }

                                ## How to handle conflicts for all tables in this sync:
                                if ($result =~ /syncwinner: (.+)/o) {
                                    my $winlist = $1;
                                    $self->glog("Custom code $cname says all table winners should be: $winlist", LOG_VERBOSE);
                                    $self->{conflictinfo}{syncwinner} = $winlist;
                                    last;
                                }

                                ## How to handle conflicts for all tables in this sync, until the sync restarts:
                                if ($result =~ /syncwinner_always: (.+)/o) {
                                    my $winlist = $1;
                                    $self->glog("Custom code $cname says all table winners should always be: $winlist", LOG_VERBOSE);
                                    $self->{conflictinfo}{syncwinner_always} = $winlist;
                                    last;
                                }

                                ## We assume that some or all keys in %conflict have been changed,
                                ## from a hashref to a scalar.
                                ## We don't do checks here, as it will get caught down below.

                                ## If info->{lastcode} has been set, we don't call any other codes
                                last if $result eq 'last';

                            } ## end each code_conflict
                        }
                    }
                    ## If conflict_strategy is abort, simply die right away
                    elsif ('bucardo_abort' eq $g->{conflict_strategy}) {
                        $self->pause_and_exit(qq{Aborting sync due to conflict of $S.$T});
                    }
                    ## If we require a custom code, also die
                    elsif ('bucardo_custom' eq $g->{conflict_strategy}) {
                        $self->pause_and_exit(qq{Aborting sync due to lack of custom conflict handler for $S.$T});
                    }
                    elsif ($g->{conflict_strategy} =~ /^bucardo_latest/o) {

                        ## For bucardo_latest*, we want to check the transaction times across
                        ## all databases in this sync that may conflict - in other words,
                        ## source databases that have deltas. We then sort that list and set it
                        ## as the list of preferred databases
                        ## There are two variants:
                        ## bucardo_latest: check this table only
                        ## bucardo_latest_all_tables: check all tables in the sync
                        ## These get internally mapped to tablewinner and syncwinner respectively

                        $self->glog(qq{Starting conflict strategy $g->{conflict_strategy}}, LOG_VERBOSE);

                        ## If we are doing all tables, we only run it once, then save the information
                        if (exists $self->{conflictinfo}{syncwinner}) {
                            $self->glog("Using previous conflict winner $self->{conflictinfo}{syncwinner}", LOG_DEBUG);
                        }
                        else {
                            my $maxsql = 'SELECT COALESCE(extract(epoch FROM MAX(txntime)),0) FROM';

                            ## Find the maximum txntime across all databases for this table
                            if ($g->{conflict_strategy} eq 'bucardo_latest') {
                                $SQL = "$maxsql bucardo.$g->{deltatable}";
                            }
                            ## Same, but also across all tables in the sync
                            elsif ($g->{conflict_strategy} eq 'bucardo_latest_all_tables') {
                                $SQL = join " UNION\n" =>
                                    map { "$maxsql bucardo.$_->{deltatable}" }
                                        grep { $_->{reltype} eq 'table'}
                                            @$goatlist;
                            }
                            else {
                                ## Sanity check in case something got misspelled
                                $self->pause_and_exit(qq{Unknown conflict_strategy $g->{conflict_strategy}!});
                            }

                            $SQL .= ' ORDER BY 1 DESC LIMIT 1';

                            ## Check every database that generates deltas
                            for my $dbname (@dbs_delta) {

                                my $d = $sync->{db}{$dbname};

                                $d->{sth} = $d->{dbh}->prepare($SQL, {pg_async => PG_ASYNC});
                                $d->{async_active} = time;
                                $d->{sth}->execute();
                            }
                            for my $dbname (@dbs_delta) {

                                my $d = $sync->{db}{$dbname};

                                $d->{dbh}->pg_result();
                                $d->{async_active} = 0;
                                $d->{lastmod} = $d->{sth}->fetchall_arrayref()->[0][0] || 0;
                            }

                            ## Now we can put them in rank order
                            ## The last modification time is the main key
                            ## In the unlikely chance of a tie, we go by alphabetical database name
                            my $winner =
                                join ' ' =>
                                    map { $_->[0] }
                                        sort { $b->[1] <=> $a->[1] or $a->[0] cmp $b->[0] }
                                            map { [$_, $sync->{db}{$_}{lastmod} ] }
                                                @dbs_delta;

                            $self->glog("Set conflict winners to: $winner", LOG_VERBOSE);

                            ## Store it away
                            $self->{conflictinfo}{tablewinner}{$g} = $winner;
                            if ($g->{conflict_strategy} eq 'bucardo_latest_all_tables') {
                                $self->{conflictinfo}{syncwinner} = $winner;
                            }
                        }

                    } ## end of bucardo_latest*
                    else {
                        ## Not a built-in, so assume a list of databases:
                        $self->{conflictinfo}{winners} = $g->{conflict_strategy};
                    }

                    ## At this point, we should have enough information to solve the conflict
                    ## Either conflictinfo{winners} will have a list of databases,
                    ## or we will have a per-table or per-sync list
                    if (! exists $self->{conflictinfo}{winners}) {
                        if (exists $self->{conflictinfo}{tablewinner}{$g}) {
                            $self->{conflictinfo}{winners} = $self->{conflictinfo}{tablewinner}{$g};
                        }
                        if (exists $self->{conflictinfo}{tablewinner_always}{$g}) {
                            $self->{conflictinfo}{winners} = $self->{conflictinfo}{tablewinner_always}{$g};
                        }
                        if (exists $self->{conflictinfo}{syncwinner}) {
                            $self->{conflictinfo}{winners} = $self->{conflictinfo}{syncwinner};
                        }
                        if (exists $self->{conflictinfo}{syncwinner_alwyas}) {
                            $self->{conflictinfo}{winners} = $self->{conflictinfo}{syncwinner_always};
                        }
                    }

                    if (exists $self->{conflictinfo}{winners}) {
                        ## We walk through all of the conflicting rows, and set the winner as the
                        ## database highest in the supplied list
                        my $sc = $self->{conflictinfo}{winners}
                            or $self->pause_and_exit(q{Invalid conflict winners list given});
                        if (index($sc, ' ') < 1) {
                            ## Sanity check
                            if (! exists $deltacount{$sc}) {
                                $self->pause_and_exit(qq{Invalid conflict strategy '$sc' used for $S.$T: no such database});
                            }
                            for my $pkval (keys %conflict) {
                                ## May have already been set by customcode, so only change if a ref
                                $conflict{$pkval} = $sc if ref $conflict{$pkval};
                            }
                        }
                        else {
                            ## Have more than one, so figure out the best one to use
                            my @mdbs = split / +/ => $sc;
                            ## Make sure they all exist
                            for my $dbname (@mdbs) {
                                if (! exists $deltacount{$dbname}) {
                                    $self->pause_and_exit(qq{Invalid conflict strategy '$sc' used for $S.$T: no such database '$dbname'});;
                                }
                            }

                            ## Fill in each conflict with first found database
                            for my $pkval (keys %conflict) {
                                ## As above, we only change if currently a ref
                                next if ! ref $conflict{$pkval};
                                $conflict{$pkval} = first { exists $conflict{$pkval}{$_} } split ' ' => $sc;
                            }
                        }
                    }

                    ## Delete our old conflict resolution information so we don't use it again
                    delete $self->{conflictinfo}{winners};

                    ## At this point, the conflict hash should consist of keys with
                    ## the winning database as the value
                    ## Walk through and apply to the %deltabin hash

                    for my $pkey (keys %conflict) {

                        ## Delete everyone for this primary key
                        for my $dbname (keys %deltabin) {
                            delete $deltabin{$dbname}{$pkey};
                        }

                        ## Add (or re-add) the winning one
                        ## We do it this way as we cannot be sure that the combo existed.
                        ## It could be the case that the winning database made
                        ## no changes to this table!
                        $deltabin{ $conflict{$pkey} }{$pkey} = 1;
                    }

                    $self->glog('Conflicts have been resolved', LOG_NORMAL);

                } ## end if have conflicts

                ## Create filehandles for any flatfile databases
                for my $dbname (keys %{ $sync->{db} }) {

                    my $d = $sync->{db}{$dbname};

                    next if $d->{dbtype} !~ /flat/o;

                    ## Figure out and set the filename
                    my $date = strftime('%Y%m%d_%H%M%S', localtime());
                    $d->{filename} = "$config{flatfile_dir}/bucardo.flatfile.$self->{syncname}.$date.sql";

                    ## Does this already exist? It's possible we got so quick the old one exists
                    ## Since we want the names to be unique, come up with a new name
                    if (-e $d->{filename}) {
                        my $tmpfile;
                        my $extension = 1;
                        {
                            $tmpfile = "$d->{filename}.$extension";
                            last if -e $tmpfile;
                            $extension++;
                            redo;
                        }
                        $d->{filename} = $tmpfile;
                    }
                    $d->{filename} .= '.tmp';

                    open $d->{filehandle}, '>>', $d->{filename}
                        or die qq{Could not open flatfile "$d->{filename}": $!\n};
                }

                ## Populate the semaphore table if the setting is non-empty
                if ($config{semaphore_table}) {
                    my $tname = $config{semaphore_table};
                    for my $dbname (@dbs_connectable) {

                        my $d = $sync->{db}{$dbname};

                        if ($d->{dbtype} eq 'mongo') {
                            $self->update_mongo_status( $d, $syncname, $tname, 'started' );
                        }
                    }
                }

                ## At this point, %deltabin should contain a single copy of each primary key
                ## It may even be empty if we are truncating

                ## We need to figure out how many sources we have for some later optimizations
                my $numsources = keys %deltabin;

                ## Figure out which databases are getting written to
                ## If there is only one source, then it will *not* get written to
                ## If there is more than one source, then everyone gets written to!
                for my $dbname (keys %{ $sync->{db} }) {

                    my $d = $sync->{db}{$dbname};

                    ## Again: everyone is written to unless there is a single source
                    ## A truncation source may have an empty deltabin, but it will exist
                    $d->{writtento} = (1==$numsources and exists $deltabin{$dbname}) ? 0 : 1;
                    next if ! $d->{writtento};

                    ## Should we use the stage table for this database?
                    $d->{trackstage} = ($numsources > 1 and exists $deltabin{$dbname}) ? 1 : 0;

                    ## Disable triggers as needed
                    $self->disable_triggers($sync, $d);

                    ## Disable indexes as needed (will be rebuilt after data is copied)
                    $self->disable_indexes($sync, $d, $g);

                } ## end setting up each database


                ## This is where we want to 'rewind' to on a handled exception
              PUSH_SAVEPOINT: {

                    $delta_attempts++;

                    ## From here on out, we're making changes that may trigger an exception
                    ## Thus, if we have exception handling code, we create savepoints to rollback to
                    if ($g->{has_exception_code}) {
                        for my $dbname (keys %{ $sync->{db} }) {

                            my $d = $sync->{db}{$dbname};

                            ## No need to rollback if we didn't make any changes
                            next if ! $d->{writtento};

                            $self->glog(qq{Creating savepoint on database "$dbname" for exception handler(s)}, LOG_DEBUG);
                            $d->{dbh}->do("SAVEPOINT bucardo_$$")
                                or die qq{Savepoint creation failed for bucardo_$$};
                        }
                    }

                    ## This var gets set to true at the end of the eval
                    ## Safety check as $@ alone is not enough
                    my $evaldone = 0;

                    ## This label is solely to localize the DIE signal handler
                  LOCALDIE: {

                        $sth{kid_syncrun_update_status}->execute("Sync $S.$T (KID $$)", $syncname);
                        $maindbh->commit();

                        ## Everything before this point should work, so we delay the eval until right before
                        ##   our first actual data change on a target

                        eval {

                            ## Walk through each database in %deltabin, and push its contents
                            ## to all other databases for this sync
                            for my $dbname1 (sort keys %deltabin) {

                                ## If we are doing a truncate, delete everything from all other dbs!
                                if (exists $g->{truncatewinner}) {

                                    for my $dbnamet (@dbs) {

                                        ## Exclude ourselves, which should be the only thing in deltabin!
                                        next if $dbname1 eq $dbnamet;

                                        ## Set the real target name
                                        $g->{tablename} = $g->{newname}{$syncname}{$dbnamet};

                                        my $d = $sync->{db}{$dbnamet};

                                        my $do_cascade = 0;
                                        $self->truncate_table($d, $g, $do_cascade);

                                        ## Do not keep this around, as it is sync and db specific!
                                        delete $g->{tablename};

                                    }
                                    ## We keep going, in case the source has post-truncation items
                                }

                                ## How many rows are we pushing around? If none, we done!
                                my $rows = keys %{ $deltabin{$dbname1} };
                                $self->glog("Rows to push from $dbname1.$S.$T: $rows", LOG_VERBOSE);
                                ## This also exits us if we are a truncate with no source rows
                                next if ! $rows;
                                $deltacount{alltables}++;

                                ## Build the list of target databases we are pushing to
                                my @pushdbs;
                                for my $dbname2 (@dbs_non_fullcopy) {

                                    ## Don't push to ourselves!
                                    next if $dbname1 eq $dbname2;

                                    ## No %seenpair is needed: this time we *do* go both ways (A->B, then B->A)

                                    push @pushdbs => $sync->{db}{$dbname2};
                                }

                                my $sourcedb = $sync->{db}{$dbname1};

                                ## Here's the real action: delete/truncate from target, then copy from source to target

                                ## For this table, delete all rows that may exist on the target(s)
                                $sth{kid_syncrun_update_status}->execute("Deleting based on $dbname1.$S.$T",$syncname);
                                $maindbh->commit();
                                $dmlcount{deletes} += $self->delete_rows(
                                    $deltabin{$dbname1}, $g, $sync, \@pushdbs);

                                ## For this table, copy all rows from source to target(s)
                                $sth{kid_syncrun_update_status}->execute("Copying from $dbname1.$S.$T",$syncname);
                                $maindbh->commit();
                                $dmlcount{inserts} += $self->push_rows(
                                    $deltabin{$dbname1}, $g, $sync, $sourcedb, \@pushdbs, 'copy');

                                ## Store references to the list of changes in case custom code needs them
                                $sync->{deltarows}{$S}{$T} = $deltabin{$dbname1};

                            } ## end copying data from each source database

                            ## Enable indexes and run REINDEX as needed
                            $self->enable_indexes($sync, $g);

                            ## We set this as we cannot rely on $@ alone
                            $evaldone = 1;

                        }; ## end of eval

                    } ## end of LOCALDIE

                    ## Got exception handlers, but no exceptions, so reset the count:
                    if ($evaldone) {
                        $g->{exceptions} = 0;
                    }
                    ## Did we fail the eval?
                    else {

                        chomp $@;
                        (my $err = $@) =~ s/\n/\\n/g;

                        ## If we have no exception code, we simply die to pass control to $err_handler.
                        ## XXX If no handler, we want to rewind and try again ourselves
                        ## XXX But this time, we want to enter a more aggressive conflict resolution mode
                        ## XXX Specifically, we need to ensure that a single database "wins" and that
                        ## XXX all table changes therein come from that database.
                        ## XXX No need if we only have a single table, of course, or if there were
                        ## XXX no possible conflicting changes.
                        ## XXX Finally, we skip if the first run already had a canonical winner
                        if (!$g->{has_exception_code}) {
                            $self->glog("Warning! Aborting due to exception for $S.$T:$pkval Error was $err",
                                        $err =~ /serialize|deadlock/ ? LOG_VERBOSE : LOG_WARN);
                            ## If this was a serialization error, we will not need to use pg_cancel
                            if ($err =~ /serialize/) {
                                $g->{async_active} = 0;
                            }
                            die "$err\n";
                        }

                        ## We have an exception handler
                        $self->glog("Exception caught: $err", LOG_WARN);

                        ## Bail if we've already tried to handle this goat via an exception
                        if ($g->{exceptions}++ > 1) {
                            ## XXX Does this get properly reset on a redo?
                            $self->glog("Warning! Exception custom code did not work for $S.$T:$pkval", LOG_WARN);
                            die qq{Error: too many exceptions to handle for $S.$T:$pkval};
                        }

                        ## Time to let the exception handling custom code do its work
                        ## First, we rollback to our savepoint on all databases that are using them
                        for my $dbname (keys %{ $sync->{db} }) {

                            my $d = $sync->{db}{$dbname};

                            next if ! $d->{writtento};

                            ## Just in case, clear out any existing async queries
                            if ($d->{async_active}) {
                                $d->{dbh}->pg_cancel();
                                $d->{async_active} = 0;
                            }

                            $self->glog("Rolling back to savepoint on database $dbname", LOG_DEBUG);
                            $d->{dbh}->do("ROLLBACK TO SAVEPOINT bucardo_$$");
                        }

                        ## Prepare information to pass to the handler about this run
                        my $codeinfo = {
                            version      => $VERSION,
                            schemaname   => $S,
                            tablename    => $T,
                            error_string => $err,
                            deltabin     => \%deltabin,
                            attempts     => $delta_attempts,
                        };

                        ## Set if any handlers think we should try again
                        my $runagain = 0;

                        for my $code (@{$g->{code_exception}}) {

                            $self->glog("Trying exception code $code->{id}: $code->{name}", LOG_TERSE);

                            ## Pass in the information above about the current state
                            $code->{info} = $codeinfo;

                            my $result = $self->run_kid_custom_code($sync, $code);

                            ## A request to run the same goat again.
                            if ('retry' eq $result) {
                                $self->glog('Exception handler thinks we can try again', LOG_NORMAL);
                                $runagain = 1;
                                last;
                            }

                            ## Request to skip any other codes
                            last if $result eq 'last';

                            $self->glog('Going to next available exception code', LOG_VERBOSE);
                            next;
                        }

                        ## If not running again, we simply give up and throw an exception to the kid
                        if (!$runagain) {
                            $self->glog('No exception handlers were able to help, so we are bailing out', LOG_WARN);
                            die qq{No exception handlers were able to help, so we are bailing out\n};
                        }

                        ## The custom code wants to try again
                        ## XXX Should probably reset session_replication_role

                        ## Make sure the Postgres database connections are still clean
                        for my $dbname (@dbs_postgres) {

                            my $ping = $sync->{db}{$dbname}{dbh}->ping();
                            if ($ping !~ /^[123]$/o) {
                                $self->glog("Warning! Ping on database $dbname after exception handler was $ping", LOG_WARN);
                            }
                        }

                        ## Now jump back and try this goat again!
                        redo PUSH_SAVEPOINT;

                    } ## end of handled exception

                } ## end of PUSH_SAVEPOINT

            } ## end each goat

            $self->glog("Totals: deletes=$dmlcount{deletes} inserts=$dmlcount{inserts} conflicts=$dmlcount{conflicts}",
                        LOG_VERBOSE);

            ## Update bucardo_track table so that the bucardo_delta rows we just processed
            ##  are marked as "done" and ignored by subsequent runs

            ## Reset our pretty-printer count
            $maxcount = 0;

            for my $g (@$goatlist) {

                next if $g->{reltype} ne 'table';

                ($S,$T) = ($g->{safeschema},$g->{safetable});
                delete $g->{rateinfo};

                ## Gather up our rate information - just store for now, we can write it after the commits
                ## XX Redo with sourcename etc.
                ## Skip as {deltarate} is not even defined!
                if (0) {
                    if ($deltacount{source}{$S}{$T} and $sync->{track_rates}) {
                        $self->glog('Gathering source rate information', LOG_VERBOSE);
                        my $sth = $sth{source}{$g}{deltarate};
                        $count = $sth->execute();
                        $g->{rateinfo}{source} = $sth->fetchall_arrayref();
                    }

                    for my $dbname (@dbs_source) {

                        if ($deltacount{dbtable}{$dbname}{$S}{$T} and $sync->{track_rates}) {
                            $self->glog('Gathering target rate information', LOG_VERBOSE);
                            my $sth = $sth{target}{$g}{deltarate};
                            $count = $sth->execute();
                            $g->{rateinfo}{target} = $sth->fetchall_arrayref();
                        }

                    }
                }
                ## For each database that had delta changes, insert rows to bucardo_track
                for my $dbname (@dbs_source) {

                    my $d = $sync->{db}{$dbname};

                    $d->{needs_track} = 0;

                    if ($deltacount{dbtable}{$dbname}{$S}{$T}) {
                        $d->{needs_track} = 1;
                        ## Kick off the track or stage update asynchronously
                        if ($d->{trackstage}) {
                            ## The stage table can only have rows if a previous version failed
                            ## This can happen if this kid committed, but another failed
                            ## Thus, we always want to make sure the stage table is empty:
                            $SQL = "DELETE FROM bucardo.$g->{stagetable}";
                            $d->{dbh}->do($SQL);
                            $sth{stage}{$dbname}{$g}->execute();
                        }
                        else {
                            $sth{track}{$dbname}{$g}->execute();
                        }
                        $d->{async_active} = time;
                    }
                }

                ## Loop through again and let everyone finish
                for my $dbname (@dbs_source) {

                    my $d = $sync->{db}{$dbname};

                    if ($d->{needs_track}) {
                        ($count = $d->{dbh}->pg_result()) =~ s/0E0/0/o;
                        $d->{async_active} = 0;
                        $self->{insertcount}{dbname}{$S}{$T} = $count;
                        $maxcount = $count if $count > $maxcount;
                    }
                }

            } ## end each goat

            ## Get sizing for the next printout
            my $maxsize = 10;
            my $maxcount2 = 1;

            for my $g (@$goatlist) {
                next if $g->{reltype} ne 'table';
                ($S,$T) = ($g->{safeschema},$g->{safetable});
                for my $dbname (keys %{ $sync->{db} }) {
                    next if ! $deltacount{dbtable}{$dbname}{$S}{$T};
                    $maxsize = length " $dbname.$S.$T" if length " $dbname.$S.$T" > $maxsize;
                    $maxcount2 = length $count if length $count > $maxcount2;
                }
            }

            ## Pretty print the number of rows per db/table
            for my $g (@$goatlist) {
                next if $g->{reltype} ne 'table';
                ($S,$T) = ($g->{safeschema},$g->{safetable});

                for my $dbname (keys %{ $sync->{db} }) {

                    my $d = $sync->{db}{$dbname};

                    if ($deltacount{dbtable}{$dbname}{$S}{$T}) {
                        $count = $self->{insertcount}{dbname}{$S}{$T};
                        $self->glog((sprintf 'Rows inserted to bucardo_%s for %-*s: %*d',
                             $d->{trackstage} ? 'stage' : 'track',
                             $maxsize,
                             "$dbname.$S.$T",
                             length $maxcount2,
                             $count),
                             LOG_DEBUG);
                    }
                } ## end each db
            } ## end each table

        } ## end if dbs_delta

        ## Handle all the fullcopy targets
        if (@dbs_fullcopy) {

            ## We only need one of the sources, so pull out the first one
            ## (dbs_source should only have a single entry anyway)
            my ($sourcename, $sourcedbh, $sourcex);
            for my $dbname (@dbs_source) {

                my $d = $sync->{db}{$dbname};

                $sourcename = $dbname;
                $sourcedbh = $d->{dbh};
                $sourcex = $d;
                $self->glog(qq{For fullcopy, we are using source database "$sourcename"}, LOG_VERBOSE);
                last;

            }

            ## Temporary hash to store onetimecopy information
            $sync->{otc} = {};

            ## Walk through and handle each goat
          GOAT: for my $g (@$goatlist) {

                ($S,$T) = ($g->{safeschema},$g->{safetable});

                ## Handle sequences first
                ## We always do these, regardless of onetimecopy
                if ($g->{reltype} eq 'sequence') {
                    $SQL = "SELECT * FROM $S.$T";
                    $sth = $sourcedbh->prepare($SQL);
                    $sth->execute();
                    $g->{sequenceinfo}{$sourcename} = $sth->fetchall_arrayref({})->[0];
                    $g->{winning_db} = $sourcename;

                    ## We want to modify all fullcopy targets only
                    for my $dbname (@dbs_fullcopy) {
                        $sync->{db}{$dbname}{adjustsequence} = 1;
                    }
                    $self->adjust_sequence($g, $sync, $S, $T, $syncname);

                    next;
                }

                ## Some tables exists just to be examined but not pushed to
                if ($g->{ghost}) {
                    $self->glog("Skipping ghost table $S.$T", LOG_VERBOSE);
                    next;
                }

                ## If doing a one-time-copy and using empty mode, skip this table if it has rows
                ## This is done on a per table / per target basis
                if (2 == $sync->{onetimecopy}) {

                    ## Also make sure we have at least one row on the source
                    my $tname = $g->{newname}{$syncname}{$sourcename};
                    if (! $self->table_has_rows($sourcex, $tname)) {
                        $self->glog(qq{Source table "$sourcename.$S.$T" has no rows and we are in onetimecopy if empty mode, so we will not COPY}, LOG_NORMAL);
                        ## No sense in going any further
                        next GOAT;
                    }

                    ## Check each fullcopy target to see if it is empty and thus ready to COPY
                    my $have_targets = 0;
                    for my $dbname (@dbs_fullcopy) {

                        ## Reset this in case a previous loop changed it
                        $sync->{otc}{skip}{$dbname} = 0;

                        my $d = $sync->{db}{$dbname};

                        my $targetname = $g->{newname}{$syncname}{$dbname};

                        ## If this target table has rows, skip it
                        if ($self->table_has_rows($d, $targetname)) {
                            $sync->{otc}{skip}{$dbname} = 1;
                            $self->glog(qq{Target table "$dbname.$targetname" has rows and we are in onetimecopy if empty mode, so we will not COPY}, LOG_NORMAL);
                        }
                        else {
                            $have_targets = 1;
                        }
                    }

                    ## If we have no valid targets at all, skip this goat
                    next GOAT if ! $have_targets;

                } ## end onetimecopy of 2

                ## The list of targets we will be fullcopying to
                ## This is a subset of dbs_fullcopy, and may be less due
                ## to the target having rows and onetimecopy being set
                my @dbs_copytarget;

                for my $dbname (@dbs_fullcopy) {

                    ## Skip if onetimecopy was two and this target had rows
                    next if 2 == $sync->{onetimecopy} and $sync->{otc}{skip}{$dbname};

                    push @dbs_copytarget => $dbname;

                }

                ## Truncate the table on all target databases, and fallback to delete if that fails
                for my $dbname (@dbs_copytarget) {

                    my $d = $sync->{db}{$dbname};

                    ## Nothing to do here for flatfiles
                    next if $d->{dbtype} =~ /flat/;

                    ## Disable triggers as needed
                    $self->disable_triggers($sync, $d);

                    ## Disable indexes as needed
                    $self->disable_indexes($sync, $d, $g);

                    $self->glog(qq{Emptying out $dbname.$S.$T using $sync->{deletemethod}}, LOG_VERBOSE);
                    my $use_delete = 1;

                    ## By hook or by crook, empty this table

                    my $tname = $g->{tablename} = $g->{newname}{$syncname}{$dbname};

                    if ($sync->{deletemethod} =~ /truncate/io) {
                        my $do_cascade = $sync->{deletemethod} =~ /cascade/io ? 1 : 0;
                        if ($self->truncate_table($d, $g, $do_cascade)) {
                            $self->glog("Truncated table $tname", LOG_VERBOSE);
                            $use_delete = 0;
                        }
                        else {
                            $self->glog("Truncation of table $tname failed, so we will try a delete", LOG_VERBOSE);
                        }
                    }

                    if ($use_delete) {

                        ## This may take a while, so we update syncrun
                        $sth{kid_syncrun_update_status}->execute("DELETE $tname (KID $$)", $syncname);
                        $maindbh->commit();

                        ## Note: even though $tname is the actual name, we still track stats with $S.$T
                        $dmlcount{D}{target}{$S}{$T} = $self->delete_table($d, $g);
                        $dmlcount{alldeletes}{target} += $dmlcount{D}{target}{$S}{$T};
                        $self->glog("Rows deleted from $tname: $dmlcount{D}{target}{$S}{$T}", LOG_VERBOSE);
                    }

                    ## This needs to not stick around
                    delete $g->{tablename};

                } ## end each database to be truncated/deleted


                ## For this table, copy all rows from source to target(s)
                $dmlcount{inserts} += $dmlcount{I}{target}{$S}{$T} = $self->push_rows(
                    {}, $g, $sync, $sourcex,
                    ## We need an array of database objects here:
                    [ map { $sync->{db}{$_} } @dbs_copytarget ], 'fullcopy');

                ## Add to our cross-table tally
                $dmlcount{allinserts}{target} += $dmlcount{I}{target}{$S}{$T};

                ## Restore the indexes and run REINDEX where needed
                $self->enable_indexes($sync, $g);

                ## TODO: logic to clean out delta rows is this was a onetimecopy

            } ## end each goat

            if ($sync->{deletemethod} ne 'truncate') {
                $self->glog("Total target rows deleted: $dmlcount{alldeletes}{target}", LOG_NORMAL);
            }
            $self->glog("Total target rows copied: $dmlcount{allinserts}{target}", LOG_NORMAL);

        } ## end have some fullcopy targets

        ## Close filehandles for any flatfile databases
        for my $dbname (keys %{ $sync->{db} }) {

            my $d = $sync->{db}{$dbname};

            next if $d->{dbtype} !~ /flat/o;

            close $d->{filehandle}
                or warn qq{Could not close flatfile "$d->{filename}": $!\n};
            ## Atomically rename it so other processes can pick it up
            (my $newname = $d->{filename}) =~ s/\.tmp$//;
            rename $d->{filename}, $newname;

            ## Remove the old ones, just in case
            delete $d->{filename};
            delete $d->{filehandle};
        }

        ## If using semaphore tables, mark the status as 'complete'
        if ($config{semaphore_table}) {

            my $tname = $config{semaphore_table};

            for my $dbname (@dbs_connectable) {

                my $d = $sync->{db}{$dbname};

                if ($d->{dbtype} eq 'mongo') {
                    $self->update_mongo_status( $d, $syncname, $tname, 'complete' );
                }
            }
        }

        ## If doing truncate, do some cleanup
        if (exists $self->{truncateinfo}) {
            ## For each source database that had a truncate entry, mark them all as done
            $SQL  = 'UPDATE bucardo.bucardo_truncate_trigger SET replicated = now() WHERE sync = ? AND replicated IS NULL';
            for my $dbname (@dbs_source) {

                my $d = $sync->{db}{$dbname};

                $d->{sth} = $d->{dbh}->prepare($SQL, {pg_async => PG_ASYNC});
                $d->{sth}->execute($syncname);
                $d->{async_active} = time;

            }
            for my $dbname (@dbs_source) {

                my $d = $sync->{db}{$dbname};

                $d->{dbh}->pg_result();
                $d->{async_active} = 0;

            }
        }

        ## Run all 'before_trigger_enable' code
        if (exists $sync->{code_before_trigger_enable}) {
            $sth{kid_syncrun_update_status}->execute("Code before_trigger_enable (KID $$)", $syncname);
            $maindbh->commit();
            for my $code (@{$sync->{code_before_trigger_enable}}) {
                last if 'last' eq $self->run_kid_custom_code($sync, $code);
            }
        }

        ## Turn triggers and rules back on as needed
        $self->enable_triggers($sync);

        ## Bring the db back to normal
        for my $dbname (@dbs_write) {

            my $d = $sync->{db}{$dbname};

            next if ! $d->{writtento};

            if ($d->{dbtype} eq 'mysql' or $d->{dbtype} eq 'mariadb') {

                $self->glog(qq{Turning foreign key checks back on for $dbname}, LOG_VERBOSE);
                $d->{dbh}->do('SET foreign_key_checks = 1');
            }
        }

        if ($self->{dryrun}) {
            $self->glog('Dryrun, rolling back...', LOG_TERSE);
            for my $dbname (@dbs_dbi) {
                $sync->{db}{$dbname}{dbh}->rollback();
            }
            for my $dbname (@dbs_redis) {
                ## Implement DISCARD when the client supports it
                ##$sync->{db}{$dbname}{dbh}->discard();
            }
            $maindbh->rollback();
        }
        else {
            $self->glog(q{Issuing final commit for all databases}, LOG_VERBOSE);
            ## This is a tricky bit: all writeable databases *must* go first
            ## If we only have a single source, this ensures we don't mark rows as done
            ## in the track tables before everyone has reported back
            for my $dbname (@dbs_dbi) {

                my $d = $sync->{db}{$dbname};

                next if ! $d->{writtento};

                $d->{dbh}->commit();
            }
            ## Now we can commit anyone else
            for my $dbname (@dbs_dbi) {

                my $d = $sync->{db}{$dbname};

                next if $d->{writtento};

                $d->{dbh}->commit();
            }
            for my $dbname (@dbs_redis) {
                ## Implement EXEC when the client supports it
                ## $sync->{db}{$dbname}{dbh}->exec();
            }
            $self->glog(q{All databases committed}, LOG_VERBOSE);
        }

        ## If we used a staging table for the tracking info, do the final inserts now
        ## This is the safest way to ensure we never miss any changes
        for my $dbname (@dbs_dbi) {

            my $d = $sync->{db}{$dbname};

            next if ! $d->{trackstage};

            my $dbh = $d->{dbh};

            for my $g (@$goatlist) {

                next if $g->{reltype} ne 'table';

                next if ! $deltacount{dbtable}{$dbname}{$g->{safeschema}}{$g->{safetable}};

                $SQL = "INSERT INTO bucardo.$g->{tracktable} SELECT * FROM bucardo.$g->{stagetable}";
                $dbh->do($SQL);
                $SQL = "DELETE FROM bucardo.$g->{stagetable}";
                $dbh->do($SQL);
                $self->glog("Populated $dbname.$g->{tracktable}", LOG_DEBUG);
            }
            $dbh->commit();
        }

        ## Capture the current time. now() is good enough as we just committed or rolled back
        ## XXX used for track below
        #my $source_commit_time = $sourcedbh->selectall_arrayref('SELECT now()')->[0][0];
        #my $target_commit_time = $targetdbh->selectall_arrayref('SELECT now()')->[0][0];
        #$sourcedbh->commit();
        #$targetdbh->commit();
        #my ($source_commit_time, $target_commit_time);

        ## Update the syncrun table, including the delete and insert counts
        my $reason = "Finished (KID $$)";
        my $details = '';
        $count = $sth{kid_syncrun_end}->execute(
            $dmlcount{deletes}, $dmlcount{inserts}, $dmlcount{truncates}, $dmlcount{conflicts},
            $details, $reason, $syncname);

        ## Change this row to the latest good or empty
        my $action = ($dmlcount{deletes} or $dmlcount{inserts} or $dmlcount{truncates})
            ? 'good' : 'empty';
        $self->end_syncrun($maindbh, $action, $syncname, "Complete (KID $$)");
        $maindbh->commit();

        ## Just in case, report on failure to update
        if ($count != 1) {
            $self->glog("Unable to correctly update syncrun table! (count was $count)", LOG_TERSE);
        }

        ## Put a note in the logs for how long this took
        my $synctime = sprintf '%.2f', tv_interval($kid_start_time);
        $self->glog((sprintf 'Total time for sync "%s" (%s %s, %s %s): %s%s',
                    $syncname,
                    $dmlcount{inserts},
                    (1==$dmlcount{inserts} ? 'row' : 'rows'),
                    $deltacount{alltables},
                    (1== $deltacount{alltables} ? 'table' : 'tables'),
                    pretty_time($synctime),
                    $synctime < 120 ? '' : " ($synctime seconds)",), LOG_VERBOSE);

        ## Update our rate information as needed
        if (0 and $sync->{track_rates}) {
            $SQL = 'INSERT INTO bucardo_rate(sync,goat,target,mastercommit,slavecommit,total) VALUES (?,?,?,?,?,?)';
            $sth = $maindbh->prepare($SQL);
            for my $g (@$goatlist) {
                next if ! exists $g->{rateinfo} or $g->{reltype} ne 'table';
                ($S,$T) = ($g->{safeschema},$g->{safetable});
                if ($deltacount{source}{$S}{$T}) {
                    for my $time (@{$g->{rateinfo}{source}}) {
                        #$sth->execute($syncname,$g->{id},$targetname,$time,$source_commit_time,$deltacount{source}{$S}{$T});
                    }
                }
                if ($deltacount{target}{$S}{$T}) {
                    for my $time (@{$g->{rateinfo}{target}}) {
                        # fixme
                        #$sth->execute($syncname,$g->{id},$sourcename,$time,$source_commit_time,$deltacount{target}{$S}{$T});
                    }
                }
            }
            $maindbh->commit();

        } ## end of track_rates

        if (@dbs_fullcopy and !$self->{dryrun}) {
            if ($sync->{vacuum_after_copy}) {
                ## May want to break this output down by table
                $sth{kid_syncrun_update_status}->execute("VACUUM (KID $$)", $syncname);
                $maindbh->commit();
                for my $dbname (@dbs_fullcopy) {

                    my $d = $sync->{db}{$dbname};

                    for my $g (@$goatlist) {
                        next if ! $g->{vacuum_after_copy} or $g->{reltype} ne 'table';
                        my $tablename = $g->{newname}{$syncname}{$dbname};
                        $self->vacuum_table($kid_start_time, $d->{dbtype}, $d->{dbh}, $d->{name}, $tablename);
                    }
                }
            }
            if ($sync->{analyze_after_copy}) {
                $sth{kid_syncrun_update_status}->execute("ANALYZE (KID $$)", $syncname);
                $maindbh->commit();
                for my $dbname (@dbs_fullcopy) {

                    my $d = $sync->{db}{$dbname};

                    for my $g (@$goatlist) {
                        next if ! $g->{analyze_after_copy} or $g->{reltype} ne 'table';
                        if ($g->{onetimecopy_ifempty}) {
                            $g->{onetimecopy_ifempty} = 0;
                            next;
                        }
                        my $tablename = $g->{newname}{$syncname}{$dbname};
                        $self->analyze_table($kid_start_time, $d->{dbtype}, $d->{dbh}, $d->{name}, $tablename);
                    }
                }
            }
        }

        my $total_time = sprintf '%.2f', tv_interval($kid_start_time);

        ## Remove lock file if we used it
        $self->remove_lock_file();

        ## Run all 'after_txn' code
        if (exists $sync->{code_after_txn}) {
            $sth{kid_syncrun_update_status}->execute("Code after_txn (KID $$)", $syncname);
            $maindbh->commit();
            for my $code (@{$sync->{code_after_txn}}) {
                last if 'last' eq $self->run_kid_custom_code($sync, $code);
            }
        }

        ## Clear out the entries from the dbrun table
        for my $dbname (@dbs_connectable) {
            $sth = $sth{dbrun_delete};
            $sth->execute($syncname, $dbname);
            $maindbh->commit();
        }

        ## Notify the parent that we are done
        $self->db_notify($maindbh, "ctl_syncdone_${syncname}");
        $maindbh->commit();

        ## If this was a onetimecopy, leave so we don't have to rebuild dbs_fullcopy etc.
        if ($sync->{onetimecopy}) {
            $self->glog('Turning onetimecopy back to 0', LOG_VERBOSE);
            $SQL = 'UPDATE sync SET onetimecopy=0 WHERE name = ?';
            $sth = $maindbh->prepare($SQL);
            $sth->execute($syncname);
            $maindbh->commit();
            ## This gets anything loaded from scratch from this point
            ## The CTL knows to switch onetimecopy off because it gets a syncdone signal
            last KID;
        }

        if (! $kidsalive) {
            $self->glog('Kid is not kidsalive, so exiting', LOG_DEBUG);
            last KID;
        }

        redo KID;

    } ## end KID

        ## Disconnect from all the databases used in this sync
        for my $dbname (@dbs_dbi) {
            my $dbh = $sync->{db}{$dbname}{dbh};
            $dbh->rollback();
            $_->finish for values %{ $dbh->{CachedKids} };
            $dbh->disconnect();
        }

        if ($sync->{onetimecopy}) {
            ## XXX
            ## We need the MCP and CTL to pick up the new setting. This is the
            ## easiest way: First we sleep a second, to make sure the CTL has
            ## picked up the syncdone signal. It may resurrect a kid, but it
            ## will at least have the correct onetimecopy
            #sleep 1;
            #$maindbh->do("NOTIFY reload_sync_$syncname");
            #$maindbh->commit();
        }

        ## Disconnect from the main database
        $maindbh->disconnect();

        $self->cleanup_kid('Normal exit', '');

        $didrun = 1;
    }; ## end $runkid

    ## Do the actual work.
    RUNKID: {
        $didrun = 0;
        eval { $runkid->() };
        exit 0 if $didrun;

        my $err = $@;

        ## Bail out unless this error came from DBD::Pg
        $err_handler->($err) if $err !~ /DBD::Pg/;

        eval {
            ## We only do special things for certain errors, so check for those.
            my ($sleeptime, $fail_msg) = (0,'');
            my @states = map { $sync->{db}{$_}{dbh}->state } @dbs_dbi;
            if (first { $_ eq '40001' } @states) {
                $sleeptime = $config{kid_serial_sleep};
                ## If set to -1, this means we never try again
                if ($sleeptime < 0) {
                    $self->glog('Could not serialize, will not retry', LOG_VERBOSE);
                    $err_handler->($err);
                }
                elsif ($sleeptime) {
                    $self->glog((sprintf 'Could not serialize, will sleep for %s %s',
                                 $sleeptime, 1==$sleeptime ? 'second' : 'seconds'), LOG_NORMAL);
                }
                else {
                    $self->glog('Could not serialize, will try again', LOG_NORMAL);
                }
                $fail_msg = 'Serialization failure';
            }
            elsif (first { $_ eq '40P01' } @states) {
                $sleeptime = $config{kid_deadlock_sleep};
                ## If set to -1, this means we never try again
                if ($sleeptime < 0) {
                    $self->glog('Encountered a deadlock, will not retry', LOG_VERBOSE);
                    $err_handler->($err);
                }
                elsif ($sleeptime) {
                    $self->glog((sprintf 'Encountered a deadlock, will sleep for %s %s',
                                 $sleeptime, 1==$sleeptime ? 'second' : 'seconds'), LOG_NORMAL);
                }
                else {
                    $self->glog('Encountered a deadlock, will try again', LOG_NORMAL);
                }
                $fail_msg = 'Deadlock detected';
                ## TODO: Get more information via get_deadlock_details()
            }
            else {
                $err_handler->($err);
            }

            if ($config{log_level_number} >= LOG_VERBOSE) {
                ## Show complete error information in debug mode.
                for my $dbh (map { $sync->{db}{$_}{dbh} } @dbs_dbi) {
                    $self->glog(
                        sprintf('*  %s: %s - %s', $dbh->{Name}, $dbh->state, $dbh->errstr),
                        LOG_VERBOSE
                    ) if $dbh->err;
                }
            }

            ## Roll everyone back
            for my $dbname (@dbs_dbi) {

                my $d = $sync->{db}{$dbname};

                my $dbh = $d->{dbh};

                ## If we are async, clear it out - if the connection is still valid!
                if ($d->{async_active}) {
                    my $state = $dbh->state;
                    if ($state eq '' or $state eq '25P01') {
                        $dbh->pg_cancel();
                    }
                    $d->{async_active} = 0;
                }

                ## Mark triggers as enabled, since we are also rolling back our trigger disabling magic
                $d->{triggers_enabled} = 1;

                ## Seperate eval{} for the rollback as we are probably still connected to the transaction.
                eval { $dbh->rollback; };
                if ($@) {
                    $self->glog("Result of eval for rollback: $@", LOG_DEBUG);
                    die $@;
                }
            }

            # End the syncrun.
            $self->end_syncrun($maindbh, 'bad', $syncname, "Failed : $fail_msg (KID $$)" );
            $maindbh->commit;

            ## Tell listeners we are about to sleep
            ## TODO: Add some sweet payload information: sleep time, which dbs/tables failed, etc.
            $self->db_notify($maindbh, "syncsleep_${syncname}", 0, "$fail_msg. Sleep=$sleeptime");

            ## Sleep and try again.
            sleep $sleeptime if $sleeptime;
            $kicked = 1;
        };
        if ($@) {
            # Our recovery failed. :-(
            $err_handler->($@);
        }
        else {
            redo RUNKID;
        }

    }

} ## end of start_kid


sub start_main_transaction {

    ## Prepare each database for the final work of copying data
    ## This is the time when we do things such as set the isolation level
    ## From this point on, we are in the "main" transaction and speed is important
    ## Arguments: one hashref
    ## sync: the sync object
    ## databases: arrayref of all databases that have been connected to
    ## Returns: undef

    my ($self, $info) = @_;

    my $sync      = $info->{sync}      or die qq{Required arg 'sync' missing\n};
    my $databases = $info->{databases} or die qq{Required arg 'databases' missing\n};

    for my $dbname (@$databases) {

        my $d = $sync->{db}{$dbname};
        my $dbh = exists $d->{dbh} ? $d->{dbh} : '';

        if ($d->{does_dbi}) {
            ## Just in case:
            $dbh->rollback();
        }

        if ('postgres' eq $d->{dbtype}) {
            ## We never want to timeout!
            $dbh->do('SET statement_timeout = 0');
            ## Using the same time zone everywhere keeps us sane
            $dbh->do(q{SET TIME ZONE 'GMT'});
            ## Rare, but allow for tcp fiddling
            for my $var (qw/ idle interval count /) {
                my $name = "tcp_keepalives_$var";

                ## Should always exist, but:
                next if ! exists $config{$name};

                ## Quick sanity checks:
                next if ! defined $config{$name} or $config{$name} !~ /^\d+$/;

                ## A setting of zero means leave it alone
                next if ! $config{$name};

                $dbh->do("SET $name = $config{$name}");

                $self->glog("Set $name to $config{$name} for database $dbname", LOG_DEBUG);
            }

            $dbh->do(qq{SET TRANSACTION ISOLATION LEVEL $self->{pg_isolation_level} READ WRITE});
            $self->glog(qq{Set database "$dbname" to $self->{pg_isolation_level} read write}, LOG_DEBUG);
        }

        if ('mysql' eq $d->{dbtype} or 'mariadb' eq $d->{dbtype}) {

            ## ANSI mode: mostly because we want ANSI_QUOTES
            $dbh->do(q{SET sql_mode = 'ANSI'});
            ## Use the same time zone everywhere
            $dbh->do(q{SET time_zone = '+0:00'});

            $dbh->do('SET TRANSACTION ISOLATION LEVEL SERIALIZABLE'); ## READ WRITE appears in MySQL 5.6.5
            $self->glog(qq{Set database "$dbname" to serializable}, LOG_DEBUG);
        }

        if ('drizzle' eq $d->{dbtype}) {
            ## Drizzle does not appear to have anything to control this yet
        }

        if ('oracle' eq $d->{dbtype}) {
            $dbh->do('SET TRANSACTION READ WRITE');
            $dbh->do(q{SET TRANSACTION ISOLATION LEVEL SERIALIZABLE NAME 'bucardo'});
            $self->glog(qq{Set database "$dbname" to serializable and read write}, LOG_DEBUG);
        }

        if ('sqlite' eq $d->{dbtype}) {
            ## Defer all foreign key checking until the very end
            $dbh->do('PRAGMA defer_foreign_keys = 1');
        }

        if ('redis' eq $d->{dbtype}) {
            ## Implement MULTI, when the driver supports it
            ##$dbh->multi();
        }

    }

    return undef;

} ## end of start_main_transaction


sub lock_all_tables {

    ## If requested, lock all the tables used in the sync
    ## Arguments: one hashref
    ## sync: sync object
    ## tables: arrayref of table objects
    ## databases: arrayref of database names
    ## Returns: undef

    my ($self, $info) = @_;

    my $sync      = $info->{sync}      or die qq{Required arg 'sync' missing\n};
    my $tables    = $info->{tables}    or die qq{Required arg 'tables' missing\n};
    my $databases = $info->{databases} or die qq{Required arg 'databases' missing\n};

    ## The final mode we choose
    my $lock_table_mode = '';

    my $syncname = $sync->{name};

    ## Check if the filesystem has a lock file request
    my $force_lock_file = File::Spec->catfile( $config{piddir} => "bucardo-force-lock-$syncname" );
    ## Cache that

    ## Currently, a file is the only way to trigger this rather severe action
    return undef if ! -e $force_lock_file;

    $self->{force_lock_file} = $force_lock_file;

    ## If the file exists, pull the mode from inside it. Default to EXCLUSIVE mode
    $lock_table_mode = 'EXCLUSIVE';
    if (-s _ and (open my $fh, '<', "$force_lock_file")) {
        my $newmode = <$fh>;
        close $fh or warn qq{Could not close "$force_lock_file": $!\n};
        if (defined $newmode) {
            chomp $newmode;
            ## Quick sanity check: only set if looks like normal words
            $lock_table_mode = $newmode if $newmode =~ /^\s*\w[ \w]+\s*$/o;
        }
    }
    $self->glog(qq{Found lock control file "$force_lock_file". Mode: $lock_table_mode}, LOG_TERSE);

    $self->glog("Locking all writeable tables in $lock_table_mode MODE", LOG_TERSE);
    for my $dbname (@$databases) {

        my $d = $sync->{db}{$dbname};

        for my $g (@$tables) {

            next if $g->{reltype} ne 'table';

            ## Figure out which table name to use
            my $tname = $g->{newname}{$syncname}{$dbname};

            if ('postgres' eq $d->{dbtype}) {
                my $com = "$tname IN $lock_table_mode MODE";
                $self->glog("Database $dbname: Locking table $com", LOG_TERSE);
                $d->{dbh}->do("LOCK TABLE $com");
            }
            elsif ('mysql' eq $d->{dbtype } or 'drizzle' eq $d->{dbtype} or 'mariadb' eq $d->{dbtype}) {
                my $com = "$tname WRITE";
                $self->glog("Database $dbname: Locking table $com", LOG_TERSE);
                $d->{dbh}->do("LOCK TABLE $com");
            }
            elsif ('oracle' eq $d->{dbtype}) {
                my $com = "$tname IN EXCLUSIVE MODE";
                $self->glog("Database $dbname: Locking table $com", LOG_TERSE);
                $d->{dbh}->do("LOCK TABLE $com");
            }
            elsif ('sqlite' eq $d->{dbtype}) {
                $d->{dbh}->do('BEGIN EXCLUSIVE TRANSACTION');
            }
        }
    }

    return undef;

} ## end of lock_all_tables


sub remove_lock_file {

    ## Remove a lock file that was used above in the remove_lock_file sub
    ## Arguments: none
    ## Returns: undef

    my $self = shift;

    if (exists $self->{force_lock_file} and -e $self->{force_lock_file}) {
        $self->glog("Removing lock control file $self->{force_lock_file}", LOG_VERBOSE);
        unlink $self->{force_lock_file}
            or $self->glog("Warning! Failed to unlink $self->{force_lock_file}", LOG_WARN);
    }

    return undef;

} ## end of remove_lock_file


sub update_mongo_status {

    ## Update the Mongo semaphore table
    ## Arguments: four
    ## 1. Database object
    ## 2. Name of the sync
    ## 3. Name of the table
    ## 3. New status

    my ($self, $d, $syncname, $tablename, $status) = @_;

    my $collection = $d->{dbh}->get_collection($tablename);

    my @args = (
        { sync => $syncname },
        { '$set' => {
                        sync => $syncname,
                        status => $status,
                        endtime => scalar gmtime,
                    }
        },
        { upsert => 1, safe => 1 }
    );

    $self->{oldmongo} ? $collection->update(@args) : $collection->update_one(@args);

    return;


} ## end of update_mongo_status



sub disable_triggers {

    ## Disable triggers and rules for all tables in a sync, for the given database.
    ## This gets all tables at once, so it only needs to be called once for each database.
    ## Arguments: two
    ## 1. Sync object
    ## 2. Database object
    ## Returns: undef

    my ($self, $sync, $db) = @_;

    my $SQL;

    ## Are triggers already disabled for this database? Return and do nothing
    return undef if ! $db->{triggers_enabled};

    my $dbh = $db->{dbh};

    if ('mysql' eq $db->{dbtype} or 'mariadb' eq $db->{dbtype}) {
        ## Do not worry about checking foreign keys
        $dbh->do('SET foreign_key_checks = 0');
        ## Do not worry about uniqueness of unique indexes
        $dbh->do('SET unique_checks = 0');

        $db->{triggers_enabled} = 0;
        return undef;
    }

    ## From this point on we are doing Postgres
    return undef if $db->{dbtype} ne 'postgres';

    ## Can we do this the easy way? Thanks to Jan for srr!
    my $dbname = $db->{name};
    if ($dbh->{pg_server_version} >= 80300) {
        $self->glog("Setting session_replication_role to replica for database $dbname", LOG_VERBOSE);
        $dbh->do(q{SET session_replication_role = 'replica'});

        $db->{triggers_enabled} = 0;
        return undef;
    }

    ## Okay, the old and ugly way: pg_class table manipulation
    ## First, create the SQL as needed
    if (! $sync->{SQL_disable_trigrules}) {

        ## The SQL to disable all triggers and rules for the tables in this sync
        $SQL = q{
                UPDATE pg_class
                SET    reltriggers = 0, relhasrules = false
                WHERE  (
            };
        $SQL .= join "OR\n"
            => map { "(oid = '$_->{safeschema}.$_->{safetable}'::regclass)" }
                grep { $_->{reltype} eq 'table' }
                    @{ $sync->{goatlist} };
        $SQL .= ')';

        $sync->{SQL_disable_trigrules} = $SQL;
    }

    ## Now run the SQL and mark that we have been here
    $self->glog(qq{Disabling triggers and rules on database "$dbname" via pg_class}, LOG_VERBOSE);
    $dbh->do($sync->{SQL_disable_trigrules});

    $db->{triggers_enabled} = 0;

    return undef;

} ## end of disable_triggers


sub enable_triggers {

    ## Restore any previously disabled triggers and rules for all databases
    ## Arguments: one
    ## 1. Sync object
    ## Returns: undef

    my ($self, $sync) = @_;

    my $SQL;

    ## Walk through each database in this sync and enable triggers as needed
    for my $dbname (sort keys %{ $sync->{db} }) {

        my $db = $sync->{db}{$dbname};

        ## Do nothing unless triggers are disabled
        next if $db->{triggers_enabled};

        my $dbh = $db->{dbh};

        if ('mysql' eq $db->{dbtype} or 'mariadb' eq $db->{dbtype}) {
            $dbh->do('SET foreign_key_checks = 1');
            $dbh->do('SET unique_checks = 1');
            $db->{triggers_enabled} = time;
            next;
        }

        ## Past here is Postgres

        ## If we are using srr, just flip it back to the default
        if ($db->{dbh}{pg_server_version} >= 80300) {
            $self->glog("Setting session_replication_role to default for database $dbname", LOG_VERBOSE);
            $dbh->do(q{SET session_replication_role = default}); ## Assumes a sane default!
            $dbh->commit();
            $db->{triggers_enabled} = time;
            next;
        }

        ## Okay, the old and ugly way: pg_class table manipulation
        ## First, create the SQL as needed
        if (! $sync->{SQL_enable_trigrules}) {

            my $setclause =
                ## no critic (RequireInterpolationOfMetachars)
                q{reltriggers = }
                . q{(SELECT count(*) FROM pg_catalog.pg_trigger WHERE tgrelid = pg_catalog.pg_class.oid),}
                . q{relhasrules = }
                . q{CASE WHEN (SELECT COUNT(*) FROM pg_catalog.pg_rules WHERE schemaname=SNAME AND tablename=TNAME) > 0 }
                . q{THEN true ELSE false END};
                ## use critic

            my $tempsql = qq{
                UPDATE pg_class
                SET    $setclause
                WHERE  oid = 'SCHEMANAME.TABLENAME'::regclass
                };
            $SQL = join ";\n"
                => map {
                    my $sql = $tempsql;
                    $sql =~ s/SNAME/$_->{safeschemaliteral}/g;
                    $sql =~ s/TNAME/$_->{safetableliteral}/g;
                    $sql =~ s/SCHEMANAME/$_->{safeschema}/g;
                    $sql =~ s/TABLENAME/$_->{safetable}/g;
                    $sql;
                }
                    grep { $_->{reltype} eq 'table' }
                        @{ $sync->{goatlist} };

            $sync->{SQL_enable_trigrules} = $SQL;
        }

        ## Now run the SQL and mark that we have been here
        $self->glog(qq{Enabling triggers and rules on database "$dbname" via pg_class}, LOG_VERBOSE);
        $db->{dbh}->do($sync->{SQL_enable_trigrules});

        $db->{triggers_enabled} = time;

    }

    return undef;

} ## end of enable_triggers


sub disable_indexes {

    ## Disable indexes on a specific table in a specific database for faster copying
    ## Obviously, the index will get enabled and rebuilt later on
    ## If you want finer tuning, such as only disabling the same table for some databases,
    ## then it is up to the caller to tweak {rebuild_index} before calling.
    ## Arguments: three
    ## 1. Sync object
    ## 2. Database object
    ## 3. Table object
    ## Returns: undef

    my ($self, $sync, $db, $table) = @_;

    my $SQL;

    ## Do nothing unless rebuild_index has been set for this table
    return undef if ! $table->{rebuild_index};

    ## The only system we do this with is Postgres
    return undef if $db->{dbtype} ne 'postgres';

    ## Grab the actual target table name
    my $tablename = $table->{newname}{$sync->{name}}{$db->{name}};

    ## Have we already disabled triggers on this table? Return but make a note
    my $dbname = $db->{name};
    if ($table->{"db:$dbname"}{indexes_disabled}) {
        $self->glog("Warning: tried to disable indexes twice for $db->{name}.$tablename", LOG_WARN);
        return undef;
    }

    ## We need to know if this table has indexes or not
    if (! exists $table->{"db:$dbname"}{has_indexes}) {
        $SQL = qq{SELECT relhasindex FROM pg_class WHERE oid = '$tablename'::regclass};
        ## relhasindex is a boolean 't' or 'f', but DBD::Pg will return it as 1 or 0
        $table->{"db:$dbname"}{has_indexes} = $db->{dbh}->selectall_arrayref($SQL)->[0][0];
    }

    ## If the table has no indexes, then we don't need to worry about disabling them
    return undef if ! $table->{"db:$dbname"}{has_indexes};

    ## Now we can proceed with the disabling, by monkeying with the system catalog
    $self->glog("Disabling indexes for $dbname.$tablename", LOG_NORMAL);
    $SQL = qq{UPDATE pg_class SET relhasindex = 'f' WHERE oid = '$tablename'::regclass};
    $count = $db->{dbh}->do($SQL);
    ## Safety check:
    if ($count < 1) {
        $self->glog("Warning: disable index failed for $dbname.$tablename", LOG_WARN);
    }

    ## This is mostly here to tell enable_indexes to proceed
    $table->{"db:$dbname"}{indexes_disabled} = 1;

    return undef;

} ## end of disable_indexes


sub enable_indexes {

    ## Make indexes live again, and rebuild if needed
    ## Walks through all the databases itself
    ## Arguments: two
    ## 1. Sync object
    ## 2. Table object
    ## Returns: undef

    my ($self, $sync, $table) = @_;

    my $SQL;

    ## Walk through each database in this sync and reapply indexes as needed
    for my $dbname (sort keys %{ $sync->{db} }) {

        my $db = $sync->{db}{$dbname};

        ## Do nothing unless we are sure indexes have been disabled
        next if ! $table->{"db:$dbname"}{indexes_disabled};

        ## This all assumes the database is Postgres

        ## Grab the actual target table name
        my $tablename = $table->{newname}{$sync->{name}}{$db->{name}};

        ## Turn the indexes back on
        $self->glog("Enabling indexes for $dbname.$tablename", LOG_NORMAL);
        ## We set this to 'f' earlier, so flip it back now
        $SQL = qq{UPDATE pg_class SET relhasindex = 't' WHERE oid = '$tablename'::regclass};
        $count = $db->{dbh}->do($SQL);
        ## Safety check:
        if ($count < 1) {
            $self->glog("Warning: enable index failed for $dbname.$tablename", LOG_WARN);
        }
        $table->{"db:$dbname"}{indexes_disabled} = 0;

        ## Rebuild all the indexes on this table
        $self->glog("Reindexing table $dbname.$tablename", LOG_NORMAL);
        ## We do this asynchronously so we don't wait on each db
        $db->{async_active} = time;
        $db->{dbh}->do( "REINDEX TABLE $tablename", {pg_async => PG_ASYNC} );

        ## Very short-lived variable to help the loop below
        $db->{rebuild_index_active} = 1;
    }

    ## Now walk through and let each one finish
    for my $dbname (sort keys %{ $sync->{db} }) {

        my $db = $sync->{db}{$dbname};

        if ($db->{rebuild_index_active}) {
            ## Waits for the REINDEX to finish:
            $db->{dbh}->pg_result();
            $db->{async_active} = 0;
        }
        delete $db->{rebuild_index_active};

    }

    return undef;

} ## end of enable_indexes


sub pause_and_exit {

    ## Usually called by a kid, dies and pauses the sync before it leaves
    ## This prevents infinite loops because something went wrong with the kid
    ## Arguments: one
    ## 1. Message to give (LOG_WARN)
    ## Returns: never, dies.

    my ($self, $message) = @_;

    $self->glog($message, LOG_WARN);

    my $syncname = $self->{sync}{name};
    $self->glog("Pausing sync $syncname", LOG_TERSE);

    $self->db_notify($self->{masterdbh}, "pause_sync_$syncname", 1);

    die $message;

} ## end of pause_and_exit


sub connect_database {

    ## Connect to the given database
    ## Arguments: one
    ## 1. The id of the database
    ##   If the database id is blank or zero, we return the main database
    ## Returns:
    ## - the database handle and the backend PID
    ##   OR
    ## - the string 'inactive' if set as such in the db table
    ##   OR
    ## - the string 'flat' if this is a flatfile 'database'

    my $self = shift;

    my $id = shift || 0;

    my ($dsn,$dbh,$user,$pass,$ssp,$dbname,$SQL);

    my $dbtype = 'postgres';

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
        $dbtype = $d->{dbtype};
        $dbname = $d->{dbname};
        if ($d->{status} eq 'inactive') {
            return 0, 'inactive';
        }

        ## Flat files do not actually get connected to, of course
        if ($dbtype =~ /flat/o) {
            return 0, 'flat';
        }

        if ('postgres' eq $dbtype) {
            $dsn = 'dbi:Pg:';
            $dsn .= join ';', map {
                ($_ eq 'dbservice' ? 'service' : $_ ) . "=$d->{$_}";
            } grep { defined $d->{$_} and length $d->{$_} } qw/dbname dbservice/;
        }
        elsif ('drizzle' eq $dbtype) {
            $dsn = "dbi:drizzle:database=$dbname";
        }
        elsif ('mongo' eq $dbtype) {

            ## For now, we simply require it
            require MongoDB;

            ## We also need some specific Perl modules we do not want all of Bucardo to require
            ## In this case, we want to generate our own error message:
            my $module_loaded_ok = 0;
            eval { require boolean; $module_loaded_ok = 1; };
            $module_loaded_ok or die qq{Unable to load the Perl 'boolean' module: needed for MongoDB support\n};

            $module_loaded_ok = 0;
            eval { require Date::Parse; $module_loaded_ok = 1; };
            $module_loaded_ok or die qq{Unable to load the Perl 'Date::Parse' module: needed for MongoDB support\n};

            $module_loaded_ok = 0;
            eval { require DateTime; $module_loaded_ok = 1; };
            $module_loaded_ok or die qq{Unable to load the Perl 'DateTime' module: needed for MongoDB support\n};

            ## Are we using the old "point-zero" version?
            my $mongoversion = $MongoDB::VERSION;
            $self->{oldmongo} = $mongoversion =~ /^0\./ ? 1 : 0;

            my $mongoURI = 'mongodb://';
            my $dbdsn = $d->{dbdsn} || '';

            if (length $dbdsn) {
                $dbdsn =~ s/^DSN://;
                if ($dbdsn !~ /^mongodb:/) {
                    $mongoURI .= $dbdsn;
                }
                else {
                    $mongoURI = $dbdsn;
                }
            }
            else {
                my $mongodsn = {};
                for my $name (qw/ dbhost dbport dbuser dbpass /) {
                    defined $d->{$name} and length $d->{$name} and $mongodsn->{$name} = $d->{$name};
                }
                if (exists $mongodsn->{dbuser}) {
                    my $pass = $mongodsn->{dbpass} || '';
                    $mongoURI .= "$mongodsn->{dbuser}:$pass\@";
                }
                $mongoURI .= $mongodsn->{dbhost} || 'localhost';
                $mongoURI .= ":$mongodsn->{dbport}" if exists $mongodsn->{dbport};
            }

            $self->glog("MongoDB connection URI to database $dbname: $mongoURI", LOG_DEBUG);
            my $conn = $self->{oldmongo} ? MongoDB::MongoClient->new(host => $mongoURI)
                : MongoDB->connect($mongoURI); ## no critic

            $dbh = $conn->get_database($dbname);
            my $backend = 0;
            if (! $self->{show_mongodb_version}++) {
                $self->glog("Perl module MongoDB loaded. Version $MongoDB::VERSION", LOG_NORMAL);
            }

            return $backend, $dbh;
        }
        elsif ('firebird' eq $dbtype) {
            $dsn = "dbi:Firebird:db=$dbname";
        }
        elsif ('mysql' eq $dbtype or 'mariadb' eq $dbtype) {
            $dsn = "dbi:mysql:database=$dbname";
        }
        elsif ('oracle' eq $dbtype) {
            $dsn = "dbi:Oracle:dbname=$dbname";
        }
        elsif ('redis' eq $dbtype) {
            my @dsn;
            my $server = '';
            if (defined $d->{dbhost} and length $d->{dbhost}) {
                $server = $d->{dbhost};
            }
            if (defined $d->{dbport} and length $d->{dbport}) {
                $server = ":$d->{dbport}";
            }
            if ($server) {
                push @dsn => 'server', $server;
            }

            my ($pass, $index);
            if (defined $d->{dbpass} and length $d->{dbpass}) {
                $pass = $d->{dbpass};
            }
            if (defined $d->{dbname} and length $d->{dbname} and $d->{dbname} !~ /\D/) {
                $index = $d->{dbname};
            }

            push @dsn => 'on_connect', sub {
                $_[0]->client_setname('bucardo');
                $_[0]->auth($pass) if $pass;
                $_[0]->select($index) if $index;
            };

            ## For now, we simply require it
            require Redis;
            $dbh = Redis->new(@dsn);
            if (! $self->{show_redis_version}++) {
                $self->glog("Perl module Redis loaded. Version $Redis::VERSION", LOG_NORMAL);
            }

            return 0, $dbh;
        }
        elsif ('sqlite' eq $dbtype) {
            $dsn = "dbi:SQLite:dbname=$dbname";
        }
        else {
            die qq{Cannot handle databases of type "$dbtype"\n};
        }

        if (defined $d->{dbdsn} and length $d->{dbdsn}) {
            $dsn = "TEST$d->{dbdsn}";
        }
        else {
            defined $d->{dbport} and length $d->{dbport} and $dsn .= ";port=$d->{dbport}";
            defined $d->{dbhost} and length $d->{dbhost} and $dsn .= ";host=$d->{dbhost}";
            length $d->{dbconn} and $dsn .= ";$d->{dbconn}";
        }
        $user = $d->{dbuser};
        $pass = $d->{dbpass} || '';
        $ssp = $d->{server_side_prepares};
    }

    $self->glog("DSN: $dsn", LOG_VERBOSE) if exists $config{log_level};

    $dbh = DBI->connect
        (
         $dsn,
         $user,
         $pass,
         {AutoCommit=>0, RaiseError=>1, PrintError=>0}
    );

    ## Register this database in our global list
    ## Note that we only worry about DBI-backed databases here,
    ## as there is no particular cleanup needed (e.g. InactiveDestroy)
    ## for other types.
    $self->{dbhlist}{$dbh} = $dbh;

    ## From here on out we are setting Postgres-specific items, so everyone else is done
    if ($dbtype ne 'postgres') {
        my $modname = "DBD::" . $dbh->{Driver}->{Name};
        if (! $self->{"show_${modname}_version"}++) {
            my $modver = $modname->VERSION;
            $self->glog("Perl module $modname loaded. Version $modver", LOG_NORMAL);
        }
        return 0, $dbh;
    }

    ## Set the application name if we can
    if ($dbh->{pg_server_version} >= 90000) {
        my $role = $self->{logprefix} || '???';
        $dbh->do("SET application_name='bucardo $role (PID $$)'");
        $dbh->commit();
    }

    ## If we are using something like pgbouncer, we need to tell Bucardo not to
    ## use server-side prepared statements, as they will not span commits/rollbacks.
    if (! $ssp) {
        $self->glog('Turning off server-side prepares for this database connection', LOG_TERSE);
        $dbh->{pg_server_prepare} = 0;
    }

    ## Grab the backend PID for this Postgres process
    ## Also a nice check that everything is working properly
    $SQL = 'SELECT pg_backend_pid()';

    my $backend = $dbh->selectall_arrayref($SQL)->[0][0];
    $dbh->rollback();

    ## If the main database, prepend 'bucardo' to the search path
    if (!$id) {
        $dbh->do(q{SELECT pg_catalog.set_config('search_path', 'bucardo,' || current_setting('search_path'), false)});
        $dbh->commit();
    }

    ## If this is not the main database, listen for a dead db hint
    if ($id and $self->{logprefix} eq 'MCP') {
        $self->db_listen($self->{masterdbh}, "dead_db_$id");
        $self->glog("Listening for dead_db_$id", LOG_DEBUG);
        $dbh->commit();
    }

    ## If this is a vacuum process, make sure it can write to the database!
    if ('VAC' eq $self->{logprefix}) {
        $dbh->do(qq{SET default_transaction_read_only = off});
        $dbh->commit();
    }

    return $backend, $dbh;

} ## end of connect_database


sub reload_config_database {

    ## Reload the %config and %config_about hashes from the bucardo_config table
    ## Calls commit on the masterdbh
    ## Arguments: none
    ## Returns: undef

    my $self = shift;

    my $SQL;

    undef %config;
    undef %config_about;

    my %log_level_number = (
        WARN    => 1, ## Yes, this is correct. Should not be able to set lower than 1
        TERSE   => 1,
        NORMAL  => 2,
        VERBOSE => 3,
        DEBUG   => 4,
    );

    $SQL = 'SELECT name,setting,about,type,name FROM bucardo_config';
    $sth = $self->{masterdbh}->prepare($SQL);
    $sth->execute();
    for my $row (@{$sth->fetchall_arrayref({})}) {
        ## Things from an rc file can override the value in the db
        my $setting = exists $self->{$row->{name}} ? $self->{$row->{name}} : $row->{setting};
        if ($row->{name} eq 'log_level') {
            my $newvalue = $log_level_number{uc $setting};
            if (! defined $newvalue) {
                die "Invalid log_level! ($setting)\n";
            }
            $config{log_level_number} = $newvalue;
        }
        if (defined $row->{type}) {
            $config{$row->{type}}{$row->{name}}{$row->{setting}} = $setting;
            $config_about{$row->{type}}{$row->{name}}{$row->{setting}} = $row->{about};
        }
        else {
            $config{$row->{name}} = $setting;
            $config_about{$row->{name}} = $row->{about};
        }
    }
    $self->{masterdbh}->commit();

    ## Allow certain command-line overrides
    my $loglevel = delete $self->{loglevel} || '';
    if (length $loglevel) {
        $config{log_level} = $loglevel;
        $config{log_level_number} = $log_level_number{uc $loglevel};
    }
    my $logshowline = delete $self->{logshowline} || '';
    if (length $logshowline) {
        $config{log_showline} = 1;
    }

    return;

} ## end of reload_config_database


sub log_config {

    ## Write the current contents of the config hash to the log
    ## Arguments: none
    ## Returns: undef

    my $self = shift;

    my $msg = "Bucardo config:\n";

    ## Figure out the longest key name for pretty formatting
    my $maxlen = 5;
    for (keys %config) {
        $maxlen = length($_) if length($_) > $maxlen;
    }

    ## Print each config name and setting in alphabetic order
    for (sort keys %config) {
        $msg .= sprintf " %-*s => %s\n", $maxlen, $_, (defined $config{$_}) ? qq{'$config{$_}'} : 'undef';
    }
    $self->glog($msg, LOG_WARN);

    return;

} ## end of log_config


sub _logto {

    my $self = shift;

    if ($self->{logpid} && $self->{logpid} != $$) {
        # We've forked! Get rid of any existing handles.
        delete $self->{logcodes};
    }

    return $self->{logcodes} if $self->{logcodes};

    # Do no logging if any destination is "none".
    if (grep { $_ eq 'none' } @{ $self->{logdest} }) {
        $self->{logcodes} = {};
        return $self->{logcodes};
    }

    $self->{logpid} = $$;
    my %logger;
    for my $dest (@{ $self->{logdest}} ) {

        next if exists $logger{$dest};

        if ($dest eq 'syslog') {
            ## Use Sys::Syslog to open a new syslog connection
            openlog 'Bucardo', 'pid nowait', $config{syslog_facility};
            ## Ignore the header argument for syslog output.
            $logger{syslog} = { type => 'syslog', code => sub { shift; syslog 'info', @_ } };
        }
        elsif ($dest eq 'stderr') {
            $logger{stderr} = { type => 'stderr', code => sub { print STDERR @_, $/ } };
        }
        elsif ($dest eq 'stdout') {
            $logger{stdout} = { type => 'stdout', code => sub { print STDOUT @_, $/ } };
        }
        else {
            ## Just a plain text file
            my $fn = File::Spec->catfile($dest, 'log.bucardo');
            $fn .= ".$self->{logextension}" if length $self->{logextension};

            ## If we are writing each process to a separate file,
            ## append the prefix (first three letters) and the PID to the file name
            my $tla = substr($self->{logprefix},0,3);
            $fn .= "$tla.$$"  if $self->{logseparate};

            open my $fh, '>>', $fn or die qq{Could not append to "$fn": $!\n};
            ## Turn off buffering on this handle
            $fh->autoflush(1);

            $logger{$dest} = {
                type       => 'textfile',
                code       => sub { print {$fh} @_, $/ },
                filename   => $fn,
                filehandle => $fh,
            };

        }
    }

    ## Store this away so the reopening via USR2 works
    $self->{logcodes} = \%logger;

    return \%logger;
}

sub glog { ## no critic (RequireArgUnpacking)

    ## Reformat and log internal messages to the correct place
    ## Arguments: two
    ## 1. the log message
    ## 2. the log level (defaults to 0)
    ## Returns: undef

    ## Quick shortcut if verbose is 'off' (which is not recommended!)
    return if ! $_[0]->{verbose};

    my $self = shift;
    my $msg = shift;

    ## Grab the log level: defaults to 0 (LOG_WARN)
    my $loglevel = shift || 0;

    ## Return and do nothing, if we have not met the minimum log level
    return if $loglevel > $config{log_level_number};

    ## Just return if there is no place to log to.
    my $logs = $self->_logto;
    return unless keys %$logs || ($loglevel == LOG_WARN && $self->{warning_file});

    ## Remove newline from the end of the message, in case it has one
    chomp $msg;

    ## We should always have a prefix, either BC!, MCP, CTL, KID, or VAC
    ## Prepend it to our message
    my $prefix = $self->{logprefix} || '???';
    $msg = "$prefix $msg";

    ## We may also show other optional things: log level, PID, timestamp, line we came from

    ## Optionally show the current time in some form
    my $showtime = '';
    if ($config{log_showtime}) {
        my ($sec,$msec) = gettimeofday;
        $showtime =
            1 == $config{log_showtime} ? $sec
            : 2 == $config{log_showtime} ? ($config{log_timer_format} ?
                                            strftime($config{log_timer_format}, gmtime($sec))
                                            : scalar gmtime($sec))
            : 3 == $config{log_showtime} ? ($config{log_timer_format} ?
                                            strftime($config{log_timer_format}, localtime($sec))
                                            : scalar localtime($sec))
            : '';
        if ($config{log_microsecond}) {
            $showtime =~ s/(:\d\d) /"$1." . substr($msec,0,3) . ' '/oe;
            $showtime =~ s/(:\d\d\.\d\d) /${1}0 /;
        }
    }

    ## Optionally show the PID (and set the time from above)
    ## Show which line we came from as well
    my $header = sprintf '%s%s%s',
        ($config{log_showpid} ? "($$) " : ''),
        ($showtime ? "[$showtime] " : ''),
        $config{log_showline} ? (sprintf '#%04d ', (caller)[2]) : '';

    ## Prepend the loglevel to the message
    if ($config{log_showlevel}) {
        $header = sprintf "%s $header", qw(WARN TERSE NORMAL VERBOSE DEBUG)[$loglevel];
    }

    ## Warning messages may also get written to a separate file
    ## Note that a 'warning message' is simply anything starting with "Warning"
    if ($self->{warning_file} and $loglevel == LOG_WARN) {
        my $file = $self->{warning_file};
        open my $fh, , '>>', $file or die qq{Could not append to "$file": $!\n};
        print {$fh} "$header$msg\n";
        close $fh or warn qq{Could not close "$file": $!\n};
    }

    # Send it to all logs.
    for my $log (sort keys %$logs) {
        next if ! exists $logs->{$log}{code};
        $logs->{$log}{code}->($header, $msg);
    }
    return;

} ## end of glog


sub conflict_log {

    ## Log a message to the conflict log file at config{log_conflict_file}
    ## Arguments: one
    ## 1. the log message
    ## Returns: undef

    my $self = shift;
    my $msg = shift;
    chomp $msg;

    my $cfile = $config{log_conflict_file};
    my $clog;
    if (! open $clog, '>>', $cfile) {
        warn qq{Could not append to file "$cfile": $!};
        return;
    }

    print {$clog} "$msg\n";
    close $clog or warn qq{Could not close "$cfile": $!\n};

    return;

} ## end of conflict_log


sub show_db_version_and_time {

    ## Output the time, timezone, and version information to the log
    ## Arguments: three
    ## 1. Database handle
    ## 2. Backend PID
    ## 3. A string indicating which database this is
    ## Returns: undef

    my ($self,$ldbh,$backend,$prefix) = @_;

    my $SQL;

    return if ! defined $ldbh;

    return if ref $ldbh ne 'DBI::db';

    return if $ldbh->{Driver}{Name} ne 'Pg';

    $self->glog(qq{${prefix}backend PID: $backend}, LOG_VERBOSE);

    ## Get the databases epoch, timestamp, and timezone
    $SQL = q{SELECT extract(epoch FROM now()), now(), current_setting('timezone')};
    my $sth = $ldbh->prepare($SQL);

    ## Get the system's time
    my $systemtime = Time::HiRes::time();

    ## Do the actual database call as close as possible to the system one
    $sth->execute();
    my $dbtime = $sth->fetchall_arrayref()->[0];

    $self->glog("${prefix}Local epoch: $systemtime  DB epoch: $dbtime->[0]", LOG_WARN);
    $systemtime = scalar localtime ($systemtime);
    $self->glog("${prefix}Local time: $systemtime  DB time: $dbtime->[1]", LOG_WARN);
    $systemtime = strftime('%Z (%z)', localtime());
    $self->glog("${prefix}Local timezone: $systemtime  DB timezone: $dbtime->[2]", LOG_WARN);
    $self->glog("${prefix}Postgres version: " . $ldbh->{pg_server_version}, LOG_WARN);
    $self->glog("${prefix}Database port: " . $ldbh->{pg_port}, LOG_WARN);
    $ldbh->{pg_host} and $self->glog("${prefix}Database host: " . $ldbh->{pg_host}, LOG_WARN);

    return;

} ## end of show_db_version_and_time

sub get_dbs {

    ## Fetch a hashref of everything in the db table
    ## Used by connect_database()
    ## Calls commit on the masterdbh
    ## Arguments: none
    ## Returns: hashref

    my $self = shift;

    my $SQL = 'SELECT * FROM bucardo.db';
    $sth = $self->{masterdbh}->prepare($SQL);
    $sth->execute();
    my $info = $sth->fetchall_hashref('name');
    $self->{masterdbh}->commit();

    return $info;

} ## end of get_dbs


sub get_goats {

    ## Fetch a hashref of everything in the goat table
    ## Used by find_goats()
    ## Calls commit on the masterdbh
    ## Arguments: none
    ## Returns: hashref

    my $self = shift;

    my $SQL = 'SELECT * FROM bucardo.goat';
    $sth = $self->{masterdbh}->prepare($SQL);
    $sth->execute();
    my $info = $sth->fetchall_hashref('id');
    $self->{masterdbh}->commit();

    return $info;

} ## end of get_goats


sub find_goats {

    ## Given a herd, return an arrayref of goats
    ## Used by validate_sync()
    ## Calls commit on the masterdbh
    ## Arguments: none
    ## Returns: hashref

    my ($self,$herd) = @_;

    my $goats = $self->get_goats();
    my $SQL = q{
        SELECT   goat
        FROM     bucardo.herdmap
        WHERE    herd = ?
        ORDER BY priority DESC, goat ASC
    };
    $sth = $self->{masterdbh}->prepare($SQL);
    $sth->execute($herd);
    my $newgoats = [];
    for (@{$sth->fetchall_arrayref()}) {
        push @$newgoats, $goats->{$_->[0]};
    }
    $self->{masterdbh}->commit();

    return $newgoats;

} ## end of find_goats


sub get_syncs {

    ## Fetch a hashref of everything in the sync table
    ## Used by reload_mcp()
    ## Calls commit on the masterdbh
    ## Arguments: none
    ## Returns: hashref

    my $self = shift;

    ## Grab all fields plus some computed ones from the sync table
    my $SQL = q{
        SELECT *,
            COALESCE(EXTRACT(epoch FROM checktime),0) AS checksecs,
            COALESCE(EXTRACT(epoch FROM lifetime),0) AS lifetimesecs
        FROM     bucardo.sync
    };
    $sth = $self->{masterdbh}->prepare($SQL);
    $sth->execute();

    ## Turn it into a hash based on the sync name, then return the ref
    my $info = $sth->fetchall_hashref('name');
    $self->{masterdbh}->commit();

    return $info;

} ## end of get_syncs


sub get_reason {

    ## Returns the current string (if any) in the reason file
    ## Arguments: one
    ## 1. Optional boolean: if true, the reason file is removed
    ## Returns: string

    my $delete = shift || 0;

    ## String to return
    my $reason = '';

    ## If we can't open the file, we simply return an empty string
    if (open my $fh, '<', $config{reason_file}) {
        ## Everything after the pipe is the reason. If no match, return empty string
        if (<$fh> =~ /\|\s*(.+)/o) {
            $reason = $1;
        }
        close $fh or warn qq{Could not close "$config{reason_file}": $!\n};

        ## Optionally delete the file after we've opened and closed it
        $delete and unlink $config{reason_file};
    }

    return $reason;

} ## end of get_reason


sub db_listen {

    ## Listen for specific messages. Does not commit.
    ## Arguments: two, three, or four
    ## 1. Database handle
    ## 2. String to listen for
    ## 3. Short name of the database (optional, for debug output, default to 'bucardo')
    ## 4. Whether to skip payloads. Optional boolean, defaults to false

    ## Returns: undef

    my $self = shift;
    my $ldbh = shift;
    my $string = shift;
    my $name = shift || 'bucardo';
    my $skip_payload = shift || 0;

    if (! ref $ldbh) {
        my $line = (caller)[2];
        $self->glog("Call to db_listen from an invalid database handle for $name, line $line", LOG_WARN);
        return;
    }

    ## If using payloads, we only need to listen for one thing
    if ($ldbh->{pg_server_version} >= 90000 and ! $skip_payload) {

        ## Do nothing if we are already listening
        return if $self->{listen_payload}{$ldbh};

        ## Mark this process as listening to this database.
        ## Get implicitly reset post-fork as new database handles are created
        $self->{listen_payload}{$ldbh} = 1;

        ## We use 'bucardo', 'bucardo_ctl', or 'bucardo_kid'
        my $suffix = $self->{logprefix} =~ /(KID|CTL)/ ? ('_' . lc $1) : '';
        $string = "bucardo$suffix";
    }
    elsif (exists $self->{listening}{$ldbh}{$string}) {
        ## Using old-style direct names and already listening? Just return
        return;
    }
    else {
        ## Mark it as already done
        $self->{listening}{$ldbh}{$string} = 1;
    }

    $string = "bucardo_$string" if index($string, 'bucardo');

    ## If log level low enough, show which line this call came from
    if ($config{log_level_number} <= LOG_DEBUG) {
        my $line = (caller)[2];
        $self->glog(qq{LISTEN for "$string" on "$name" (line $line)}, LOG_DEBUG);
    }

    $ldbh->do(qq{LISTEN "$string"})
        or die qq{LISTEN "$string" failed!\n};

    return;

} ## end of db_listen


sub db_unlisten {

    ## Stop listening for specific messages
    ## Arguments: four
    ## 1. Database handle
    ## 2. String to stop listening to
    ## 3. Short name of the database (for debug output)
    ## 4. Whether to skip payloads. Optional boolean, defaults to false
    ## Returns: undef

    my $self = shift;
    my $ldbh = shift;
    my $string = shift;
    my $name = shift || 'bucardo';
    my $skip_payload = shift || 0;

    ## If we are 9.0 or greater, we never stop listening
    if ($ldbh->{pg_server_version} >= 90000 and ! $skip_payload) {
        return;
    }

    my $original_string = $string;

    $string = "bucardo_$string";

    ## If log level low enough, show which line this call came from
    if ($config{log_level_number} <= LOG_DEBUG) {
        my $line = (caller)[2];
        $self->glog(qq{UNLISTEN for "$string" on "$name" (line $line)}, LOG_DEBUG);
    }

    ## We'll unlisten even if the hash indicates we are not
    $ldbh->do(qq{UNLISTEN "$string"});

    delete $self->{listening}{$ldbh}{$original_string};

    return;

} ## end of db_unlisten


sub db_unlisten_all {

    ## Stop listening to everything important
    ## Arguments: one
    ## 1. Database handle
    ## Returns: undef

    my $self = shift;
    my $ldbh = shift;

    ## If the log level is low enough, show the line that called this
    if ($config{log_level_number} <= LOG_DEBUG) {
        my $line = (caller)[2];
        $self->glog(qq{UNLISTEN * (line $line)}, LOG_DEBUG);
    }

    ## Do the deed
    $ldbh->do('UNLISTEN *');

    delete $self->{listening}{$ldbh};
    delete $self->{listen_payload}{$ldbh};

    return;

} ## end of db_unlisten_all


sub db_notify {

    ## Send an asynchronous notification into the DB aether, then commit
    ## Arguments: five
    ## 1. Database handle
    ## 2. The string to send
    ## 3. Whether to skip payloads. Optional boolean, defaults to false
    ## 4. Name of the database (as defined in bucardo.db). Optional
    ## 5. Whether we should skip the final commit or not. Defaults to false. Optional.
    ## Returns: undef

    my ($self, $ldbh, $string, $skip_payload, $dbname, $skip_commit) = @_;

    ## We make some exceptions to the payload system, mostly for early MCP notices
    ## This is because we don't want to complicate external clients with payload decisions
    $skip_payload = 0 if ! defined $skip_payload;

    $skip_commit = 0 if ! defined $skip_commit;

    if ($config{log_level_number} <= LOG_DEBUG) {
        my $line = (caller)[2];
        my $showdb = (defined $dbname and length $dbname) ? " to db $dbname" : '';
        $self->glog(qq{Sending NOTIFY "$string"$showdb (line $line) skip_commit=$skip_commit}, LOG_DEBUG);
    }

    if ($ldbh->{pg_server_version} < 90000 or $skip_payload) {
        ## Old-school notification system. Simply send the given string
        ## ...but prepend a 'bucardo_' to it first
        $string = "bucardo_$string";
        $ldbh->do(qq{NOTIFY "$string"})
            or $self->glog(qq{Warning: NOTIFY failed for "$string"}, LOG_DEBUG);
    }
    else {
        ## New-style notification system. The string becomes the payload

        ## The channel is always 'bucardo' based.
        my $channel = 'bucardo';
        ## Going to ctl?
        $channel = 'bucardo_ctl' if $string =~ s/^ctl_//o;
        ## Going to kid
        $channel = 'bucardo_kid' if $string =~ s/^kid_//o;

        $ldbh->do(qq{NOTIFY $channel, '$string'})
            or $self->glog(qq{Warning: NOTIFY failed for bucardo, '$string'}, LOG_DEBUG);
    }

    $ldbh->commit() if ! $skip_commit;

    return;

} ## end of db_notify


sub db_get_notices {

    ## Gather up and return a list of asynchronous notices received since the last check
    ## Arguments: one or two
    ## 1. Database handle
    ## 2. PID that can be ignored (optional)
    ## Returns: hash of notices, with the key as the name and then another hash with:
    ##   count: total number received
    ##   firstpid: the first PID for this notice
    ##   pids: hashref of all pids
    ## If using 9.0 or greater, the payload becomes the name

    my ($self, $ldbh, $selfpid) = @_;

    my ($n, %notice);

    while ($n = $ldbh->func('pg_notifies')) {

        my ($name, $pid, $payload) = @$n;

        ## Ignore certain PIDs (e.g. from ourselves!)
        next if defined $selfpid and $pid == $selfpid;

        if ($ldbh->{pg_server_version} >= 90000 and $payload) {
            $name = $payload; ## presto!
        }
        else {
            $name =~ s/^bucardo_//o;
        }

        if (exists $notice{$name}) {
            $notice{$name}{count}++;
            $notice{$name}{pid}{$pid}++;
        }
        else {
            $notice{$name}{count} = 1;
            $notice{$name}{pid}{$pid} = 1;
            $notice{$name}{firstpid} = $pid;
        }
    }

    ## Return right now if we had no notices,
    ## or if don't need lots of logging detail
    if (! keys %notice or $config{log_level_number} > LOG_DEBUG) {
        return \%notice;
    }

    ## TODO: Return if this was sent from us (usually PID+1)

    ## Always want to write the actual line these came from
    my $line = (caller)[2];

    ## Walk the list and show each unique message received
    for my $name (sort keys %notice) {
        my $pid = $notice{$name}{firstpid};
        my $prettypid = (exists $self->{pidmap}{$pid} ? "$pid ($self->{pidmap}{$pid})" : $pid);

        my $extra = '';
        my $pcount = keys %{ $notice{$name}{pid} };
        $pcount--; ## Not the firstpid please
        if ($pcount > 1) {
                $extra = sprintf ' (and %d other %s)',
                $pcount, 1 == $pcount ? 'PID' : 'PIDs';
        }

        my $times = '';
        $count = $notice{$name}{count};
        if ($count > 1) {
            $times = " $count times";
        }

        my $msg = sprintf 'Got NOTICE %s%s from %s%s (line %d)',
                $name, $times, $prettypid, $extra, $line;
        $self->glog($msg, LOG_DEBUG);
    }

    return \%notice;

} ## end of db_get_notices


sub send_signal_to_PID {

    ## Send a USR1 to one or more PIDs
    ## Arguments: one
    ## 1. Hashref of info, including:
    ##    sync => name of a sync to filter PID files with
    ## Returns: number of signals sucessfully sent

    my ($self, $arg) = @_;

    my $total = 0;

    ## Slurp in all the files from the PID directory
    my $piddir = $config{piddir};
    opendir my $dh, $piddir or die qq{Could not opendir "$piddir" $!\n};
    my @pidfiles = grep { /^bucardo.*\.pid$/ } readdir $dh;
    closedir $dh or warn qq{Could not closedir "$piddir": $!\n};

    ## Send a signal to the ones we care about
    for my $pidfile (sort @pidfiles) {

        next if $arg->{sync} and $pidfile !~ /\bsync\.$arg->{sync}\b/;

        my $pfile = File::Spec->catfile( $piddir => $pidfile );
        if (open my $fh, '<', $pfile) {
            my $pid = <$fh>;
            close $fh or warn qq{Could not close "$pfile": $!\n};
            if (! defined $pid or $pid !~ /^\d+$/) {
                $self->glog("Warning: No PID found in file, so removing $pfile", LOG_TERSE);
                unlink $pfile;
            }
            elsif ($pid == $$) {
            }
            else {
                $total += kill $signumber{'USR1'} => $pid;
                $self->glog("Sent USR1 signal to process $pid", LOG_VERBOSE);
            }
        }
        else {
            $self->glog("Warning: Could not open file, so removing $pfile", LOG_TERSE);
            unlink $pfile;
        }
    }

    return $total;

} ## end of send_signal_to_PID


sub validate_sync {

    ## Check each database a sync needs to use, and validate all tables and columns
    ## This also populates the all important $self->{sdb} hash
    ## We use sdb to prevent later accidental mixing with $sync->{db}
    ## Arguments: one
    ## 1. Hashref of sync information
    ## Returns: boolean success/failure

    my ($self,$s) = @_;

    my $syncname = $s->{name};
    my $SQL;

    $self->glog(qq{Running validate_sync on "$s->{name}"}, LOG_NORMAL);

    ## Populate $s->{db} with all databases in this sync
    $SQL = 'SELECT db.*, m.role, m.priority FROM dbmap m JOIN db ON (db.name = m.db) WHERE m.dbgroup = ?';
    $sth = $self->{masterdbh}->prepare($SQL);
    $count = $sth->execute($s->{dbs});
    $s->{db} = $sth->fetchall_hashref('name');

    ## Figure out what role each database will play in this sync
    my %role = ( source => 0, target => 0, fullcopy => 0);

    ## Establish a connection to each database used
    ## We also populate the "source" database as the first source we come across
    my ($sourcename,$srcdbh);

    ## How many database were restored from a stalled state
    my $restored_dbs = 0;

    for my $dbname (sort keys %{ $s->{db} }) {

        ## Helper var so we don't have to type this out all the time
        my $d = $s->{db}{$dbname};

        ## Check for inactive databases
        if ($d->{status} eq 'inactive') {
            ## Source databases are never allowed to be inactive
            if ($d->{role} eq 'source') {
                $self->glog("Source database $dbname is not active, cannot run this sync", LOG_WARN);
                ## Normally, we won't get here as the sync should not be active
                ## Mark the syncs as stalled and move on
                $s->{status} = 'stalled';
                $SQL = 'UPDATE bucardo.sync SET status = ? WHERE name = ?';
                eval {
                    my $sth = $self->{masterdbh}->prepare($SQL);
                    $sth->execute('stalled',$syncname);
                    $self->{masterdbh}->commit();
                };
                if ($@) {
                    $self->glog("Failed to set sync $syncname as stalled: $@", LOG_WARN);
                    $self->{masterdbh}->rollback();
                }
                return 0;
            }
            ## Warn about non-source ones, but allow the sync to proceed
            $self->glog("Database $dbname is not active, so it will not be used", LOG_WARN);

            ## No sense in connecting to it
            next;
        }

        ## If we've not already populated sdb, do so now
        if (! exists $self->{sdb}{$dbname}) {

            $self->{sdb}{$dbname} = $d;

            my $role = $d->{role};
            if ($d->{dbtype} =~ /flat/o) {
                $self->glog(qq{Skipping flatfile database "$dbname"}, LOG_NORMAL);
                next;
            }
            $self->glog(qq{Connecting to database "$dbname" ($role)}, LOG_TERSE);
            eval {
                ## We do not want the CTL handler here
                local $SIG{__DIE__} = undef;
                ($d->{backend}, $d->{dbh}) = $self->connect_database($dbname);
            };
            if (!defined $d->{backend}) {
                $self->glog("Connection failed: $@", LOG_TERSE);
                ## If this was already stalled, we can simply reject the validation
                if ($d->{status} eq 'stalled') {
                    $self->glog("Stalled db $dbname failed again: $@", LOG_VERBOSE);
                    return 0;
                }
                ## Wasn't stalled before, but is now!
                ## This is a temporary setting: we don't modify masterdbh
                $d->{status} = 'stalled';
                return 0;
            }

            $self->show_db_version_and_time($d->{dbh}, $d->{backend}, qq{Database "$dbname" });

            ## If this db was previously stalled, restore it
            if ($d->{status} eq 'stalled') {
                $self->glog("Restoring stalled db $dbname", LOG_NORMAL);
                $SQL = 'UPDATE bucardo.db SET status = ? WHERE name = ?';
                my $sth = $self->{masterdbh}->prepare($SQL);
                eval {
                    $sth->execute('active',$dbname);
                    $self->{masterdbh}->commit();
                    $restored_dbs++;
                    $d->{status} = 'active';
                };
                if ($@) {
                    $self->glog("Failed to set db $dbname as active: $@", LOG_WARN);
                    $self->{masterdbh}->rollback();
                    ## If this fails, we don't want the sync restored
                    $restored_dbs = 0;
                }
            }

        }

        ## If the whole sync was stalled but we retored its dbs above,
        ## restore the sync as well
        if ($restored_dbs) {
            $self->glog("Restoring stalled sync $syncname", LOG_NORMAL);
            $SQL = 'UPDATE bucardo.sync SET status = ? WHERE name = ?';
            eval {
                my $sth = $self->{masterdbh}->prepare($SQL);
                $sth->execute('active',$syncname);
                $s->{status} = 'active';
            };
            if ($@) {
                $self->glog("Failed to set sync $syncname as active: $@", LOG_WARN);
                $self->{masterdbh}->rollback();
            }
        }

        ## Help figure out source vs target later on
        $role{$d->{role}}++;

        ## We want to grab the first source we find and populate $sourcename and $srcdbh
        if (! defined $sourcename and $s->{db}{$dbname}{role} eq 'source') {
            $sourcename = $dbname;
            $srcdbh = $self->{sdb}{$dbname}{dbh};
        }

    } ## end each database

    ## If we have more than one source, then everyone is a target
    ## Otherwise, only non-source databases are
    for my $dbname (keys %{ $s->{db} }) {

        my $d = $s->{db}{$dbname};

        $d->{istarget} = ($d->{role} ne 'source' or $role{source} > 1) ? 1 : 0;
        $d->{issource} = $d->{role} eq 'source' ? 1 : 0;
    }

    ## Grab the authoritative list of goats in this herd
    $s->{goatlist} = $self->find_goats($s->{herd});

    ## Call validate_sync: checks tables, columns, sets up supporting
    ## schemas, tables, functions, and indexes as needed

    eval {
        local $SIG{__DIE__} = undef;
        $self->glog(qq{Calling validate_sync on sync "$syncname"}, LOG_VERBOSE);
        $self->{masterdbh}->do("SELECT validate_sync('$syncname')");
    };
    if ($@) {
        $self->glog("Error from validate_sync: $@", LOG_NORMAL);
        $self->{masterdbh}->rollback;
        return 0;
    }

    ## Prepare some SQL statements for immediate and future use
    my %SQL;

    ## Given a schema and table name, return safely quoted names
    $SQL{checktable} = q{
            SELECT c.oid, quote_ident(n.nspname), quote_ident(c.relname), quote_literal(n.nspname), quote_literal(c.relname)
            FROM   pg_class c, pg_namespace n
            WHERE  c.relnamespace = n.oid
            AND    c.oid = ?::regclass
        };
    $sth{checktable} = $srcdbh->prepare($SQL{checktable});

    ## Given a table, return detailed column information
    $SQL{checkcols} = q{
            SELECT   attname, quote_ident(attname) AS qattname, atttypid, format_type(atttypid, atttypmod) AS ftype,
                     attnotnull, atthasdef, attnum,
                     (SELECT pg_get_expr(adbin, adrelid) FROM pg_attrdef WHERE adrelid=attrelid
                      AND adnum=attnum AND atthasdef) AS def
            FROM     pg_attribute
            WHERE    attrelid = ?::regclass AND attnum > 0 AND NOT attisdropped
            ORDER BY attnum
        };
    $sth{checkcols} = $srcdbh->prepare($SQL{checkcols});

    ## Reset custom code related counters for this sync
    $s->{need_rows} = $s->{need_safe_dbh} = $s->{need_safe_dbh_strict} = 0;

    ## Empty out any existing lists of code types
    for my $key (grep { /^code_/ } sort keys %$s) {
        $s->{$key} = [];
    }

    ## Validate all (active) custom codes for this sync
    my $goatlistcodes = join ',' => map { $_->{id} } @{$s->{goatlist}};
    my $goatclause = length $goatlistcodes ? "OR m.goat IN ($goatlistcodes)" : '';

    $SQL = qq{
            SELECT c.src_code, c.id, c.whenrun, c.getdbh, c.name, COALESCE(c.about,'?') AS about,
                   c.status, m.active, m.priority, COALESCE(m.goat,0) AS goat
            FROM customcode c, customcode_map m
            WHERE c.id=m.code AND m.active IS TRUE
            AND (m.sync = ? $goatclause)
            ORDER BY m.priority ASC, m.goat IS NULL, c.name ASC
        };
    $sth = $self->{masterdbh}->prepare($SQL);
    $sth->execute($syncname);

    ## Loop through all customcodes for this sync
    for my $c (@{$sth->fetchall_arrayref({})}) {
        if ($c->{status} ne 'active') {
            $self->glog(qq{ Skipping custom code $c->{id} ($c->{name}): not active }. LOG_NORMAL);
            next;
        }
        $self->glog(qq{  Validating custom code $c->{id} ($c->{whenrun}) (goat=$c->{goat}): $c->{name}}, LOG_WARN);

        ## Carefully compile the code and catch complications
        TRY: {
            local $@;
            local $_;
            $c->{coderef} = eval qq{
                package Bucardo::CustomCode;
                sub { $c->{src_code} }
            }; ## no critic (ProhibitStringyEval)
            if ($@) {
                $self->glog(qq{Warning! Custom code $c->{id} ($c->{name}) for sync "$syncname" did not compile: $@}, LOG_WARN);
                return 0;
            };
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
            ## Every goat gets this code
            for my $g ( @{$s->{goatlist}} ) {
                push @{$g->{"code_$c->{whenrun}"}}, $c;
                $g->{has_exception_code}++ if $c->{whenrun} eq 'exception';
            }
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

    } ## end checking each custom code

    ## Go through each goat in this sync, adjusting items and possibly bubbling up info to sync
    for my $g (@{$s->{goatlist}}) {
        ## None of this applies to non-tables
        next if $g->{reltype} ne 'table';

        ## If we didn't find exception custom code above, set it to 0 for this goat
        $g->{has_exception_code} ||= 0;

        if (!defined $g->{rebuild_index}) {
            $g->{rebuild_index} = $s->{rebuild_index};
        }

    } ## end each goat

    ## There are things that a fullcopy sync does not do
    if ($s->{fullcopy}) {
        $s->{track_rates} = 0;
    }

    ## Build our customname hash for use below when checking remote database tables
    my %customname;
    $SQL = q{SELECT goat,newname,db,COALESCE(db,'') AS db, COALESCE(sync,'') AS sync FROM bucardo.customname};
    my $maindbh = $self->{masterdbh};
    $sth = $maindbh->prepare($SQL);
    $sth->execute();
    for my $row (@{$sth->fetchall_arrayref({})}) {
        ## Ignore if this is for some other sync
        next if length $row->{sync} and $row->{sync} ne $syncname;

        $customname{$row->{goat}}{$row->{db}} = $row->{newname};
    }

	# Table cache
    $SQL{checktableonce} = q{
            SELECT n.nspname, c.relname, c.oid, quote_ident(n.nspname) as safeschema, quote_ident(c.relname) as safetable, quote_literal(n.nspname) as safeschemaliteral, quote_literal(c.relname) as safetableliteral
            FROM   pg_class c, pg_namespace n
            WHERE  c.relnamespace = n.oid
        };
    $sth = $srcdbh->prepare($SQL{checktableonce});
	$sth->execute();
    my %tablescache;
	for my $row (@{$sth->fetchall_arrayref({})}) {
        $tablescache{"$row->{nspname}.$row->{relname}"} = {
            map { $_ => $row->{$_} } qw(oid safeschema safetable safeschemaliteral safetableliteral)
        };
    }
	$sth->finish();

    GOAT: for my $g (@{$s->{goatlist}}) {

        ## TODO: refactor with work in validate_sync()

        my $t = "$g->{schemaname}.$g->{tablename}";
        $self->glog(qq{  Inspecting source $g->{reltype} "$t" on database "$sourcename"}, LOG_NORMAL);
        ## Check the source table, save escaped versions of the names

        if (!exists ($tablescache{$t})) {
            my $msg = qq{Could not find $g->{reltype} "$t"\n};
            $self->glog($msg, LOG_WARN);
            warn $msg;
            return 0;
        }

        for my $key (keys %{ $tablescache{ $t } }) {
            $g->{$key} = $tablescache{$t}{$key};
        }

        my ($S,$T) = ($g->{safeschema},$g->{safetable});

        ## Plunk the oid into a hash for easy lookup below when saving FK information
        $s->{tableoid}{$g->{oid}}{name} = "$S.$T";

        ## Makedelta for this table starts empty
        $g->{makedelta} ||= '';

        ## Determine the conflict method for each goat
        ## Use the syncs if it has one, otherwise the default
        $g->{conflict_strategy} = $s->{conflict_strategy} || $config{default_conflict_strategy};
        $self->glog(qq{  Set conflict strategy for $S.$T to "$g->{conflict_strategy}"}, LOG_DEBUG);
        ## We do this even if g->{code_conflict} exists so it can fall through

        my $colinfo;
        if ($g->{reltype} eq 'table') {

            ## Save information about each column in the primary key
            if (!defined $g->{pkey} or !defined $g->{qpkey}) {
                die "Table $g->{safetable} has no pkey or qpkey - do you need to run validate_goat() on it?\n";
            }

            ## Much of this is used later on, for speed of performing the sync
            $g->{pkey}           = [split /\|/o => $g->{pkey}];
            $g->{qpkey}          = [split /\|/o => $g->{qpkey}];
            $g->{pkeytype}       = [split /\|/o => $g->{pkeytype}];
            $g->{numpkcols}      = @{$g->{pkey}};
            $g->{hasbinarypk}    = 0; ## Not used anywhere?
            $i = 0;
            for (@{$g->{pkey}}) {
                $g->{binarypkey}{$i++} = 0;
            }

            ## All pks together for the main delta query
            ## We change bytea to base64 so we don't have to declare binary args anywhere
            $g->{pklist} = '';
            for ($i = 0; defined $g->{pkey}[$i]; $i++) {
                $g->{pklist} .= sprintf '%s,',
                    $g->{pkeytype}[$i] eq 'bytea'
                        ? qq{ENCODE("$g->{pkey}[$i]", 'base64')}
                            : qq{"$g->{pkey}[$i]"};
            }
            ## Remove the final comma:
            chop $g->{pklist};

            ## The name of the delta and track tables for this table
            $SQL = 'SELECT bucardo.bucardo_tablename_maker(?)';
            $sth = $self->{masterdbh}->prepare($SQL);
            $sth->execute($S.'_'.$T);
            $g->{makername} = $sth->fetchall_arrayref()->[0][0];
            if ($g->{makername} =~ s/"//g) {
                $g->{deltatable} = qq{"delta_$g->{makername}"};
                $g->{tracktable} = qq{"track_$g->{makername}"};
                $g->{stagetable} = qq{"stage_$g->{makername}"};
            }
            else {
                $g->{deltatable} = "delta_$g->{makername}";
                $g->{tracktable} = "track_$g->{makername}";
                $g->{stagetable} = "stage_$g->{makername}";
            }

            ## Turn off the search path, to help the checks below match up
            $srcdbh->do('SET LOCAL search_path = pg_catalog');

            ## Check the source columns, and save them
            $sth = $sth{checkcols};
            $sth->execute(qq{"$g->{schemaname}"."$g->{tablename}"});
            $colinfo = $sth->fetchall_hashref('attname');
            ## Allow for 'dead' columns in the attnum ordering
            $i = 1;
            for (sort { $colinfo->{$a}{attnum} <=> $colinfo->{$b}{attnum} } keys %$colinfo) {
                $colinfo->{$_}{realattnum} = $i++;
            }
            $g->{columnhash} = $colinfo;

            ## Build lists of columns
            $i = 1;
            $g->{cols} = [];
            $g->{safecols} = [];
          COL: for my $colname (sort { $colinfo->{$a}{attnum} <=> $colinfo->{$b}{attnum} } keys %$colinfo) {
                ## Skip if this column is part of the primary key
                for my $pk (@{$g->{pkey}}) {
                    next COL if $pk eq $colname;
                }
                push @{$g->{cols}}, $colname;
                push @{$g->{safecols}}, $colinfo->{$colname}{qattname};
                $colinfo->{$colname}{order} = $i++;
            }

            ## Stringified versions of the above lists, for ease later on
            $g->{columnlist} = join ',' => @{$g->{cols}};
            $g->{safecolumnlist} = join ',' => @{$g->{safecols}};

            ## Note which columns are bytea
          BCOL: for my $colname (keys %$colinfo) {
                my $c = $colinfo->{$colname};
                next if $c->{atttypid} != 17; ## Yes, it's hardcoded, no sweat
                $i = 0;
                for my $pk (@{$g->{pkey}}) {
                    if ($colname eq $pk) {
                        $g->{binarypkey}{$i} = 1;
                        $g->{hasbinarypk} = 1;
                        next BCOL;
                    }
                    $i++;
                }
                ## This is used to bind_param these as binary during inserts and updates
                push @{$g->{binarycols}}, $colinfo->{$colname}{order};
            }

            $srcdbh->do('RESET search_path');

        } ## end if reltype is table

        my $sourceseq = 1;
        #$g->{reltype} eq 'sequence'
        #    ? $self->get_sequence_info($srcdbh, $S, $T)
        #    : {};

        next if $g->{reltype} ne 'table';

        ## Verify sequences or tables+columns on remote databases
        for my $dbname (sort keys %{ $self->{sdb} }) {

            ## Only ones for this sync, please
            next if ! exists $s->{db}{$dbname};

            my $d = $self->{sdb}{$dbname};

            next if $d->{role} eq 'source';

            ## Flat files are obviously skipped as we create them de novo
            next if $d->{dbtype} =~ /flat/o;

            ## Mongo is skipped because it can create schemas on the fly
            next if $d->{dbtype} =~ /mongo/o;

            ## Redis is skipped because we can create keys on the fly
            next if $d->{dbtype} =~ /redis/o;

            ## MySQL/MariaDB/Drizzle/Oracle/SQLite is skipped for now, but should be added later
            next if $d->{dbtype} =~ /mysql|mariadb|drizzle|oracle|sqlite/o;

            if ($self->{quickstart}) {
                $self->glog("  quickstart: Skipping table check for $dbname.$S.$T", LOG_VERBOSE);
                next;
            }

            ## Respond to ping here and now for very impatient watchdog programs
            $maindbh->commit();

            my $nlist = $self->db_get_notices($maindbh);
            for my $name (keys %{ $nlist }) {
                my $npid = $nlist->{$name}{firstpid};
                if ($name eq 'mcp_fullstop') {
                    $self->glog("Received full stop notice from PID $npid, leaving", LOG_WARN);
                    $self->cleanup_mcp("Received stop NOTICE from PID $npid");
                    exit 0;
                }
                if ($name eq 'mcp_ping') {
                    $self->glog("Got a ping from PID $npid, issuing pong", LOG_DEBUG);
                    $self->db_notify($maindbh, 'mcp_pong');
                }
            }

            ## Get a handle for the remote database
            my $dbh = $d->{dbh};

            ## If a sequence, verify the information and move on
            if ($g->{reltype} eq 'sequenceSKIP') {
                my $targetseq = $self->get_sequence_info($dbh, $S, $T);
                for my $key (sort keys %$targetseq) {
                    if (! exists $sourceseq->{$key}) {
                        $self->glog(qq{Warning! Sequence on target has item $key, but source does not!}, LOG_WARN);
                        next;
                    }
                    if ($targetseq->{$key} ne $sourceseq->{$key}) {
                        $self->glog("Warning! Sequence mismatch. Source $key=$sourceseq->{$key}, target is $targetseq->{$key}", LOG_WARN);
                        next;
                    }
                }

                next;

            } ## end if sequence

            ## Turn off the search path, to help the checks below match up
            $dbh->do('SET LOCAL search_path = pg_catalog');

            ## Grab column information about this table
            $sth = $dbh->prepare($SQL{checkcols});

            ## Change to the customname if needed
            my ($RS,$RT) = ($S,$T);

            ## We don't need to check if this is a source: this is already targets only
            my $using_customname = 0;
            if (exists $customname{$g->{id}}) {
                ## If there is an entry for this particular database, use that
                ## Otherwise, use the default one
                if (exists $customname{$g->{id}}{$dbname} or exists $customname{$g->{id}}{''}) {
                    $RT = $customname{$g->{id}}{$dbname} || $customname{$g->{id}}{''};
                    $using_customname = 1;

                    ## If this has a dot, change the schema as well
                    ## Otherwise, we simply use the existing schema
                    if ($RT =~ s/(.+)\.//) {
                        $RS = $1;
                    }
                }
            }

            $self->glog(qq{   Inspecting target $g->{reltype} "$RS.$RT" on database "$dbname"}, LOG_NORMAL);

            $sth->execute("$RS.$RT");
            my $targetcolinfo = $sth->fetchall_hashref('attname');
            ## Allow for 'dead' columns in the attnum ordering
            $i = 1;
            for (sort { $targetcolinfo->{$a}{attnum} <=> $targetcolinfo->{$b}{attnum} } keys %$targetcolinfo) {
                $targetcolinfo->{$_}{realattnum} = $i++;
            }

            $dbh->do('RESET search_path');
            $dbh->rollback();

            ## We'll state no problems until we are proved wrong
            my $column_problems = 0;

            ## Check each column in alphabetic order
            for my $colname (sort keys %$colinfo) {

                ## Simple var mapping to make the following code sane
                my $fcol = $targetcolinfo->{$colname};
                my $scol = $colinfo->{$colname};

                $self->glog(qq{    Column on target database "$dbname": "$colname" ($scol->{ftype})}, LOG_DEBUG);
                ## Always fatal: column on source but not target
                if (! exists $targetcolinfo->{$colname}) {
                    $column_problems = 2;
                    my $msg = qq{Source database for sync "$syncname" has column "$colname" of table "$t", but target database "$dbname" does not};
                    $self->glog("Warning: $msg", LOG_WARN);
                    warn $msg;
                    next;
                }

                ## Almost always fatal: types do not match up
                if ($scol->{ftype} ne $fcol->{ftype}) {
                    ## Carve out some known exceptions (but still warn about them)
                    ## Allowed: varchar == text
                    ## Allowed: timestamp* == timestamp*
                    ## Allowed: int == bigint
                    if (
                        ($scol->{ftype} eq 'character varying' and $fcol->{ftype} eq 'text')
                        or
                        ($scol->{ftype} eq 'text' and $fcol->{ftype} eq 'character varying')
                        or
                        ($scol->{ftype} eq 'integer' and $fcol->{ftype} eq 'bigint')
                        or
                        ($scol->{ftype} =~ /^timestamp/ and $fcol->{ftype} =~ /^timestamp/)
                ) {
                        my $msg = qq{Source database for sync "$syncname" has column "$colname" of table "$t" as type "$scol->{ftype}", but target database "$dbname" has a type of "$fcol->{ftype}". You should really fix that.};
                        $self->glog("Warning: $msg", LOG_WARN);
                    }
                    else {
                        $column_problems = 2;
                        my $msg = qq{Source database for sync "$syncname" has column "$colname" of table "$t" as type "$scol->{ftype}", but target database "$dbname" has a type of "$fcol->{ftype}"};
                        $self->glog("Warning: $msg", LOG_WARN);
                        next;
                    }
                }

                ## Fatal in strict mode: NOT NULL mismatch
                if ($scol->{attnotnull} != $fcol->{attnotnull}) {
                    $column_problems ||= 1; ## Don't want to override a setting of "2"
                    my $msg = sprintf q{Source database for sync "%s" has column "%s" of table "%s" set as %s, but target database "%s" has column set as %s},
                        $syncname,
                            $colname,
                                $t,
                                    $scol->{attnotnull} ? 'NOT NULL' : 'NULL',
                                        $dbname,
                                            $scol->{attnotnull} ? 'NULL'     : 'NOT NULL';
                    $self->glog("Warning: $msg", LOG_WARN);
                    warn $msg;
                }

                ## Fatal in strict mode: DEFAULT existence mismatch
                if ($scol->{atthasdef} != $fcol->{atthasdef}) {
                    $column_problems ||= 1; ## Don't want to override a setting of "2"
                    my $msg = sprintf q{Source database for sync "%s" has column "%s" of table "%s" %s, but target database "%s" %s},
                        $syncname,
                            $colname,
                                $t,
                                    $scol->{atthasdef} ? 'with a DEFAULT value' : 'has no DEFAULT value',
                                        $dbname,
                                            $scol->{atthasdef} ? 'has none'             : 'does';
                    $self->glog("Warning: $msg", LOG_WARN);
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
                        s/\)::/::/;

                        ## Also make exceptions for DEFAULT casting text to integers/numerics
                        s/^'(-?\d+(?:\.\d+)?)'\s*::\s*(?:integer|numeric).*$/\$1/i;
                    }
                    my $msg;
                    if ($scol_def eq $fcol_def) {
                        $msg = q{Postgres version mismatch leads to this difference, which is being tolerated: };
                    }
                    else {
                        $column_problems ||= 1; ## Don't want to override a setting of "2"
                        $msg = '';
                    }
                    $msg .= qq{Source database for sync "$syncname" has column "$colname" of table "$t" with a DEFAULT of "$scol->{def}", but target database "$dbname" has a DEFAULT of "$fcol->{def}"};
                    $self->glog("Warning: $msg", LOG_WARN);
                    warn $msg;
                }

                ## Fatal in strict mode: order of columns does not match up
                if ($scol->{realattnum} != $fcol->{realattnum}) {
                    $column_problems ||= 1; ## Don't want to override a setting of "2"
                    my $msg = qq{Source database for sync "$syncname" has column "$colname" of table "$t" at position $scol->{realattnum} ($scol->{attnum}), but target database "$dbname" has it in position $fcol->{realattnum} ($fcol->{attnum})};
                    $self->glog("Warning: $msg", LOG_WARN);
                    warn $msg;
                }

            } ## end each column to be checked

            ## Fatal in strict mode: extra columns on the target side
            for my $colname (sort keys %$targetcolinfo) {
                next if exists $colinfo->{$colname};
                $column_problems ||= 1; ## Don't want to override a setting of "2"
                my $msg = qq{Target database has column "$colname" on table "$t", but source database does not};
                $self->glog("Warning: $msg", LOG_WARN);
                warn $msg;
            }

            ## Real serious problems always bail out
            return 0 if $column_problems >= 2;

            ## If this is a minor problem, and we are using a customname,
            ## allow it to pass
            $column_problems = 0 if $using_customname;

            ## If other problems, only bail if strict checking is on both sync and goat
            ## This allows us to make a sync strict, but carve out exceptions for goats
            return 0 if $column_problems and $s->{strict_checking} and $g->{strict_checking};

        } ## end each target database

    } ## end each goat

    ## Generate mapping of foreign keys
    ## This helps us with conflict resolution later on
    my $oidlist = join ',' => map { $_->{oid} } @{ $s->{goatlist} };
    if ($oidlist) {

        ## Postgres added the array_agg function in 8.4, so if this is older than that,
        ## we add our own copy
        my $arrayagg = 'array_agg';
        if ($srcdbh->{pg_server_version} < 80400) {

            ## We reset the search_path below, so we need to force the query below to use the public namespace
            $arrayagg = 'public.array_agg';

            ## Searching for the proname rather than the aggregate should be good enough
            $SQL = 'SELECT proname FROM pg_proc WHERE proname ~ ?';
            $sth = $srcdbh->prepare($SQL);
            $count = $sth->execute('array_agg');
            $sth->finish();
            if ($count < 1) {
                $SQL = q{CREATE AGGREGATE array_agg(anyelement) ( SFUNC=array_append, STYPE=anyarray, INITCOND='{}')};
                $srcdbh->do($SQL);
            }
        }

        $SQL = qq{SELECT conname,
                    conrelid, conrelid::regclass,
                    confrelid, confrelid::regclass,
                    $arrayagg(a.attname), $arrayagg(z.attname)
             FROM pg_constraint c
             JOIN pg_attribute a ON (a.attrelid = conrelid AND a.attnum = ANY(conkey))
             JOIN pg_attribute z ON (z.attrelid = confrelid AND z.attnum = ANY (confkey))
             WHERE contype = 'f'
             AND (conrelid IN ($oidlist) OR confrelid IN ($oidlist))
             GROUP BY 1,2,3,4,5
        };

        ## We turn off search_path to get fully-qualified relation names
        $srcdbh->do('SET LOCAL search_path = pg_catalog');

        for my $row (@{ $srcdbh->selectall_arrayref($SQL) }) {

            my ($conname, $oid1,$t1, $oid2,$t2, $c1,$c2) = @$row;

            ## The referenced table is not being tracked in this sync
            if (! exists $s->{tableoid}{$oid2}) {
                ## Nothing to do except report this problem and move on
                $self->glog("Table $t1 references $t2($conname), which is not part of this sync!", LOG_NORMAL);
                next;
            }

            ## A table referencing us is not being tracked in this sync
            if (! exists $s->{tableoid}{$oid1}) {
                ## Nothing to do except report this problem and move on
                $self->glog("Table $t2 is referenced by $t1($conname), which is not part of this sync!", LOG_NORMAL);
                next;
            }

            ## Both exist, so tie them together
            $s->{tableoid}{$oid1}{references}{$oid2} = [$conname,$c1,$c2];
            $s->{tableoid}{$oid2}{referencedby}{$oid1} = [$conname,$c1,$c2];

        }

        $srcdbh->do('RESET search_path');
        $srcdbh->commit();

    }

    ## If autokick, listen for a triggerkick on all source databases
    if ($s->{autokick}) {
        my $l = "kick_sync_$syncname";
        for my $dbname (sort keys %{ $s->{db} }) {

            my $d = $s->{db}{$dbname};

            next if $d->{status} ne 'active';
            $self->glog("Listen for $l on $dbname ($d->{role})", LOG_DEBUG);
            next if $d->{role} ne 'source';
            my $dbh = $self->{sdb}{$dbname}{dbh};
            $self->db_listen($dbh, $l, $dbname, 0);
            $dbh->commit;
        }
    }

    ## Success!
    return 1;

} ## end of validate_sync


sub activate_sync {

    ## We've got a new sync to be activated (but not started)
    ## Arguments: one
    ## 1. Hashref of sync information
    ## Returns: boolean success/failure

    my ($self,$s) = @_;

    my $maindbh = $self->{masterdbh};
    my $syncname = $s->{name};

    ## Connect to each database used by this sync and validate tables
    if (! $self->validate_sync($s)) {
        $self->glog("Validation of sync $s->{name} FAILED", LOG_WARN);
        $s->{mcp_active} = 0;
        return 0;
    }

    ## If the kids stay alive, the controller must too
    if ($s->{kidsalive} and !$s->{stayalive}) {
        $s->{stayalive} = 1;
        $self->glog('Warning! Setting stayalive to true because kidsalive is true', LOG_WARN);
    }

    ## Mark this sync as active: used in sync kicks/reloads later on
    $self->{sync}{$syncname}{mcp_active} = 1;

    ## Let any listeners know we are done
    $self->db_notify($maindbh, "activated_sync_$syncname", 1);
    ## We don't need to listen for activation requests anymore
    $self->db_unlisten($maindbh, "activate_sync_$syncname", '', 1);
    ## But we do need to listen for deactivate and kick requests
    $self->db_listen($maindbh, "deactivate_sync_$syncname", '', 1);
    $self->db_listen($maindbh, "kick_sync_$syncname", '', 1);
    $self->db_listen($maindbh, "pause_sync_$syncname", '', 1);
    $self->db_listen($maindbh, "resume_sync_$syncname", '', 1);
    $maindbh->commit();

    ## Redo our process name to include an updated list of active syncs
    my @activesyncs;
    for my $syncname (sort keys %{ $self->{sync} }) {
        next if ! $self->{sync}{$syncname}{mcp_active};
        push @activesyncs, $syncname;
    }

    ## Change our process name to show all active syncs
    $0 = "Bucardo Master Control Program v$VERSION.$self->{extraname} Active syncs: ";
    $0 .= join ',' => @activesyncs;

    return 1;

} ## end of activate_sync


sub deactivate_sync {

    ## We need to turn off a running sync
    ## Arguments: one
    ## 1. Hashref of sync information
    ## Returns: boolean success/failure

    my ($self,$s) = @_;

    my $maindbh = $self->{masterdbh};
    my $syncname = $s->{name};

    ## Kill the controller
    my $ctl = $s->{controller};
    if (!$ctl) {
        $self->glog('Warning! Controller not found', LOG_WARN);
    }
    else {
        $count = kill $signumber{USR1} => $ctl;
        $self->glog("Sent kill USR1 to CTL process $ctl. Result: $count", LOG_NORMAL);
    }
    $s->{controller} = 0;

    $self->{sync}{$syncname}{mcp_active} = 0;

    ## Let any listeners know we are done
    $self->db_notify($maindbh, "deactivated_sync_$syncname");
    ## We don't need to listen for deactivation or kick/pause/resume requests
    $self->db_unlisten($maindbh, "deactivate_sync_$syncname", '', 1);
    $self->db_unlisten($maindbh, "kick_sync_$syncname", '', 1);
    $self->db_unlisten($maindbh, "pause_sync_$syncname", '', 1);
    $self->db_unlisten($maindbh, "resume_sync_$syncname", '', 1);
    ## But we do need to listen for an activation request
    $self->db_listen($maindbh, "activate_sync_$syncname", '', 1);
    $maindbh->commit();

    ## If we are listening for kicks on the source, stop doing so
    for my $dbname (sort keys %{ $self->{sdb} }) {

        my $d = $self->{sdb}{$dbname};

        next if $d->{dbtype} ne 'postgres';

        next if $d->{role} ne 'source';

        $d->{dbh} ||= $self->connect_database($dbname);
        $d->{dbh}->commit();
        if ($s->{autokick}) {
            my $l = "kick_sync_$syncname";
            $self->db_unlisten($d->{dbh}, $l, $dbname, 0);
            $d->{dbh}->commit();
        }
    }

    ## Redo our process name to include an updated list of active syncs
    my @activesyncs;
    for my $syncname (keys %{ $self->{sync} }) {
        push @activesyncs, $syncname;
    }

    $0 = "Bucardo Master Control Program v$VERSION.$self->{extraname} Active syncs: ";
    $0 .= join ',' => @activesyncs;

    return 1;

} ## end of deactivate_sync


sub fork_controller {

    ## Fork off a controller process
    ## Arguments: two
    ## 1. Hashref of sync information
    ## 2. The name of the sync
    ## Returns: undef

    my ($self, $s, $syncname) = @_;

    my $newpid = $self->fork_and_inactivate('CTL');

    if ($newpid) { ## We are the parent
        $self->glog(qq{Created controller $newpid for sync "$syncname". Kick is $s->{kick_on_startup}}, LOG_NORMAL);
        $s->{controller} = $newpid;
        $self->{pidmap}{$newpid} = 'CTL';

        ## Reset counters for ctl restart via maxkicks and lifetime settings
        $s->{ctl_kick_counts} = 0;
        $s->{start_time} = time();

        return;
    }

    ## We are the kid, aka the new CTL process

    ## Sleep a hair so the MCP can finish the items above first
    sleep 0.05;

    ## No need to keep information about other syncs around
    $self->{sync} = $s;

    $self->start_controller($s);

    exit 0;

} ## end of fork_controller


sub fork_and_inactivate {

    ## Call fork, and immediately inactivate open database handles
    ## Arguments: one
    ## 1. Type of thing we are forking (VAC, CTL, KID)
    ## Returns: nothing

    my $self = shift;
    my $type = shift || '???';

    my $newpid = fork;
    if (!defined $newpid) {
        die qq{Warning: Fork for $type failed!\n};
    }

    if ($newpid) { ## Parent
        ## Very slight sleep to increase the chance of something happening to the kid
        ## before InactiveDestroy is set
        sleep 0.1;
    }
    else { ## Kid
        ## Walk through the list of all known DBI databases
        ## Inactivate each one, then undef it

        ## Change to a better prefix, so 'MCP' does not appear in the logs
        $self->{logprefix} = $type;

        ## It is probably still referenced elsewhere, so handle that - how?
        for my $iname (keys %{ $self->{dbhlist} }) {
            my $ldbh = $self->{dbhlist}{$iname};
            $self->glog("Inactivating dbh $iname post-fork", LOG_DEBUG2);
            $ldbh->{InactiveDestroy} = 1;
            delete $self->{dbhlist}{$iname};
        }
        ## Now go through common shared database handle locations, and delete them
        $self->{masterdbh}->{InactiveDestroy} = 1
            if $self->{masterdbh};
        delete $self->{masterdbh};

        ## Clear the 'sdb' structure of any existing database handles
        if (exists $self->{sdb}) {
            for my $dbname (keys %{ $self->{sdb} }) {
                if (exists $self->{sdb}{$dbname}{dbh}) {
                    if (ref $self->{sdb}{$dbname}{dbh}) {
                        $self->glog("Removing sdb reference to database $dbname", LOG_DEBUG);
                        $self->{sdb}{$dbname}{dbh}->{InactiveDestroy} = 1;
                    }
                    delete $self->{sdb}{$dbname}{dbh};
                }
            }
        }

        ## Clear any sync-specific database handles
        if (exists $self->{sync}) {
            if (exists $self->{sync}{name}) { ## This is a controller/kid with a single sync
                for my $dbname (sort keys %{ $self->{sync}{db} }) {
                    if (exists $self->{sync}{db}{$dbname}{dbh}) {
                        if (ref $self->{sync}{db}{$dbname}{dbh}) {
                            $self->glog("Removing reference to database $dbname", LOG_DEBUG2);
                            $self->{sync}{db}{$dbname}{dbh}->{InactiveDestroy} = 1;
                        }
                        delete $self->{sync}{db}{$dbname}{dbh};
                    }
                }
            }
            else {
                for my $syncname (keys %{ $self->{sync} }) {
                    for my $dbname (sort keys %{ $self->{sync}{$syncname}{db} }) {
                        if (exists $self->{sync}{$syncname}{db}{$dbname}{dbh}) {
                            if (ref $self->{sync}{$syncname}{db}{$dbname}{dbh}) {
                                $self->glog("Removing reference to database $dbname in sync $syncname", LOG_DEBUG2);
                                $self->{sync}{$syncname}{db}{$dbname}{dbh}->{InactiveDestroy} = 1;
                            }
                            delete $self->{sync}{$syncname}{db}{$dbname}{dbh};
                        }
                    }
                }
            }
        }
    }

    return $newpid;

} ## end of fork_and_inactivate


sub fork_vac {

    ## Fork off a VAC process
    ## Arguments: none
    ## Returns: undef

    my $self = shift;
    my $SQL;

    ## Fork it off
    my $newpid = $self->fork_and_inactivate('VAC');

    ## Parent MCP just makes a note in the logs and returns
    if ($newpid) { ## We are the parent
        $self->glog(qq{Created VAC $newpid}, LOG_NORMAL);
        $self->{vacpid} = $newpid;
        return;
    }

    ## Prefix all log lines with this TLA (was MCP)
    $self->{logprefix} = 'VAC';

    ## Set our process name
    $0 = qq{Bucardo VAC.$self->{extraname}};

    ## Store our PID into a file
    ## Save the complete returned name for later cleanup
    $self->{vacpidfile} = $self->store_pid( 'bucardo.vac.pid' );

    ## Start normal log output for this controller: basic facts
    my $msg = qq{New VAC daemon. PID=$$};
    $self->glog($msg, LOG_NORMAL);

    ## Allow the MCP to signal us (request to exit)
    local $SIG{USR1} = sub {
        ## Do not change this message: looked for in the controller DIE sub
        die "MCP request\n";
    };

    ## From this point forward, we want to die gracefully
    local $SIG{__DIE__} = sub {

        ## Arguments: one
        ## 1. Error message
        ## Returns: never (exit 0)

        my ($diemsg) = @_;

        ## Store the line that did the actual exception
        my $line = (caller)[2];

        ## Don't issue a warning if this was simply a MCP request
        my $warn = ($diemsg =~ /MCP request|Not needed/ ? '' : 'Warning! ');
        $self->glog(qq{${warn}VAC was killed at line $line: $diemsg}, $warn ? LOG_WARN :LOG_VERBOSE);

        ## Not a whole lot of cleanup to do on this one: just shut database connections and leave
        $self->{masterdbh}->disconnect() if exists $self->{masterdbhvac};

        for my $dbname (keys %{ $self->{sdb} }) {
            my $d = $self->{sdb}{$dbname};
            if (defined $d->{dbh} and $d->{dbh}) {
                $d->{dbh}->disconnect();
            }
        }


        ## Remove our pid file
        unlink $self->{vacpidfile} or $self->glog("Warning! Failed to unlink $self->{vacpidfile}", LOG_WARN);

        exit 0;

    }; ## end SIG{__DIE__} handler sub

    ## Connect to the master database
    ($self->{master_backend}, $self->{masterdbh}) = $self->connect_database();
    $self->{masterdbhvac} = 1;
    my $maindbh = $self->{masterdbh};
    $self->glog("Bucardo database backend PID: $self->{master_backend}", LOG_VERBOSE);

    ## Map the PIDs to common names for better log output
    $self->{pidmap}{$$} = 'VAC';
    $self->{pidmap}{$self->{master_backend}} = 'Bucardo DB';

    ## Listen for an exit request from the MCP
    my $exitrequest = 'stop_vac';
    $self->db_listen($maindbh, $exitrequest, '', 1); ## No payloads please

    ## Commit so we start listening right away
    $maindbh->commit();

    ## Reconnect to all databases we care about
    for my $dbname (keys %{ $self->{sdb} }) {

        my $d = $self->{sdb}{$dbname};

        ## We looped through all the syncs earlier to determine which databases
        ## really need to be vacuumed. The criteria:
        ## not a fullcopy sync, dbtype is postgres, role is source
        next if ! $d->{needsvac};

        ## Establish a new database handle
        ($d->{backend}, $d->{dbh}) = $self->connect_database($dbname);
        $self->glog(qq{Connected to database "$dbname" with backend PID of $d->{backend}}, LOG_NORMAL);
        $self->{pidmap}{$d->{backend}} = "DB $dbname";
        ## We don't want details about the purging
        $d->{dbh}->do(q{SET client_min_messages = 'warning'});
    }

    ## Track how long since we last came to life for vacuuming
    my $lastvacrun = 0;

    ## The main loop
  VAC: {

        ## Bail if the stopfile exists
        if (-e $self->{stop_file}) {
            $self->glog(qq{Found stopfile "$self->{stop_file}": exiting}, LOG_TERSE);
            ## Do not change this message: looked for in the controller DIE sub
            my $stopmsg = 'Found stopfile';

            ## Grab the reason, if it exists, so we can propagate it onward
            my $vacreason = get_reason(0);
            if ($vacreason) {
                $stopmsg .= ": $vacreason";
            }

            ## This exception is caught by the controller's __DIE__ sub above
            die "$stopmsg\n";
        }

        ## Process any notifications from the main database
        ## Ignore things we may have sent ourselves
        my $nlist = $self->db_get_notices($maindbh, $self->{master_backend});

      NOTICE: for my $name (sort keys %{ $nlist }) {

            my $npid = $nlist->{$name}{firstpid};

            ## Strip prefix so we can easily use both pre and post 9.0 versions
            $name =~ s/^vac_//o;

            ## Exit request from the MCP?
            if ($name eq $exitrequest) {
                die "Process $npid requested we exit\n";
            }

            ## Just ignore everything else

        } ## end of each notification

        ## To ensure we can receive new notifications next time:
        $maindbh->commit();

        ## Should we attempt a vacuum?
        if (time() - $lastvacrun >= $config{vac_run}) {

            $lastvacrun = time();

            ## If there are no valid backends, we want to stop running entirely
            my $valid_backends = 0;

            ## Kick each one off async
            for my $dbname (sort keys %{ $self->{sdb}} ) {

                my $d = $self->{sdb}{$dbname};

                next if ! $d->{needsvac};

                my $dbh = $d->{dbh};

                ## Safety check: if the bucardo schema is not there, we don't want to vacuum
                if (! exists $d->{hasschema}) {
                    $SQL = q{SELECT count(*) FROM pg_namespace WHERE nspname = 'bucardo'};
                    $d->{hasschema} = $dbh->selectall_arrayref($SQL)->[0][0];
                    if (! $d->{hasschema} ) {
                        $self->glog("Warning! Cannot vacuum db $dbname unless we have a bucardo schema", LOG_WARN);
                    }
                }

                ## No schema? We've already complained, so skip it silently
                next if ! $d->{hasschema};

                $valid_backends++;

                ## Async please
                $self->glog(qq{Running bucardo_purge_delta on database "$dbname"}, LOG_VERBOSE);
                $SQL = q{SELECT bucardo.bucardo_purge_delta('45 seconds')};
                $sth{"vac_$dbname"} = $dbh->prepare($SQL, { pg_async => PG_ASYNC } );
                $sth{"vac_$dbname"}->execute();
                $d->{async_active} = time;

            } ## end each source database

            ## If we found no backends, we can leave right away, and not run again
            if (! $valid_backends) {

                $self->glog('No valid backends, so disabling the VAC daemon', LOG_VERBOSE);

                $config{bucardo_vac} = 0;

                ## Caught by handler above
                die 'Not needed';

            }

            ## Finish each one up
            for my $dbname (sort keys %{ $self->{sdb}} ) {

                my $d = $self->{sdb}{$dbname};

                ## As above, skip if not a source or no schema available
                next if ! $d->{needsvac};

                next if ! $d->{hasschema};

                my $dbh = $d->{dbh};

                $self->glog(qq{Finish and fetch bucardo_purge_delta on database "$dbname"}, LOG_DEBUG);
                $count = $sth{"vac_$dbname"}->pg_result();
                $d->{async_active} = 0;

                my $info = $sth{"vac_$dbname"}->fetchall_arrayref()->[0][0];
                $dbh->commit();

                $self->glog(qq{Purge on db "$dbname" gave: $info}, LOG_VERBOSE);

            } ## end each source database

        } ## end of attempting to vacuum

        sleep $config{vac_sleep};

        redo VAC;

    } ## end of main VAC loop

    exit 0;

} ## end of fork_vac


sub reset_mcp_listeners {

    ## Unlisten everything, the relisten to specific entries
    ## Used by reload_mcp()
    ## Arguments: none
    ## Returns: undef

    my $self = shift;

    my $maindbh = $self->{masterdbh};

    ## Unlisten everything
    $self->db_unlisten_all($maindbh);
    ## Need to commit here to work around Postgres bug!
    $maindbh->commit();

    ## Listen for MCP specific items
    for my $l
        (
            'mcp_fullstop',
            'mcp_reload',
            'reload_config',
            'log_message',
            'mcp_ping',
            'kid_pid_start',
            'kid_pid_stop',
    ) {
        $self->db_listen($maindbh, $l, '', 1);
    }

    ## Listen for sync specific items
    for my $syncname (keys %{ $self->{sync} }) {
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

            my $listen = "${l}_$syncname";
            $self->db_listen($maindbh, $listen, '', 1);
        }

        ## Listen for controller telling us the sync is done
        $self->db_listen($maindbh, "syncdone_$syncname");

    }

    $maindbh->commit();

    return;

} ## end of reset_mcp_listeners


sub reload_mcp {

    ## Reset listeners, kill kids, load and activate syncs
    ## Arguments: none
    ## Returns: number of syncs we activated

    my $self = shift;

    my $SQL;

    ## Grab a list of all the current syncs from the database and store as objects
    $self->{sync} = $self->get_syncs();

    ## Try and restore any stalled syncs
    $self->restore_syncs();

    ## This unlistens any old syncs
    $self->reset_mcp_listeners();

    ## Stop any kids that currently exist

    ## First, we loop through the PID directory and signal all CTL processes
    ## These should in turn remove their kids
    $self->signal_pid_files('ctl');

    ## Next, we signal any KID processes that are still around
    $self->signal_pid_files('kid');

    ## Next we use dbrun to see if any database connections are still active
    ## First, a brief sleep to allow things to catch up
    sleep 0.5;

    $self->terminate_old_goats();

    my $maindbh = $self->{masterdbh};

    ## At this point, we are authoritative, so we can safely clean out the syncrun table
    $SQL = q{
          UPDATE bucardo.syncrun
          SET status=?, ended=now()
          WHERE ended IS NULL
        };
    $sth = $maindbh->prepare($SQL);
    my $cleanmsg = "Old entry ended (MCP $$)";
    $count = $sth->execute($cleanmsg);
    $maindbh->commit();
    if ($count >= 1) {
        $self->glog("Entries cleaned from the syncrun table: $count", LOG_NORMAL);
    }

    $SQL = q{DELETE FROM bucardo.dbrun};
    $maindbh->do($SQL);

    $self->glog(('Loading sync table. Rows=' . (scalar (keys %{ $self->{sync} }))), LOG_VERBOSE);

    ## Load each sync in alphabetical order
    my @activesyncs;
    for (sort keys %{ $self->{sync} }) {
        my $s = $self->{sync}{$_};
        my $syncname = $s->{name};

        ## Note that the mcp has changed this sync
        $s->{mcp_changed} = 1;

        ## Reset some boolean flags for this sync
        $s->{mcp_active} = $s->{kick_on_startup} = $s->{controller} = 0;

        ## If this sync is not active or stalled, don't bother going any further
        if ($s->{status} ne 'active' and $s->{status} ne 'stalled') {
            $self->glog(qq{Skipping sync "$syncname": status is "$s->{status}"}, LOG_TERSE);
            next;
        }

        ## If we are doing specific syncs, check the name
        if (exists $self->{dosyncs}) {
            if (! exists $self->{dosyncs}{$syncname}) {
                $self->glog(qq{Skipping sync "$syncname": not explicitly named}, LOG_VERBOSE);
                next;
            }
            $self->glog(qq{Activating sync "$syncname": explicitly named}, LOG_VERBOSE);
        }
        else {
            $self->glog(qq{Activating sync "$syncname"}, LOG_NORMAL);
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


sub cleanup_mcp {

    ## MCP is shutting down, so we:
    ## - disconnect from the database
    ## - attempt to kill any controller kids
    ## - send a final NOTIFY
    ## - remove our own PID file
    ## Arguments: one
    ## 1. String with a reason for exiting
    ## Returns: undef

    my ($self,$exitreason) = @_;

    ## Rollback and disconnect from the master database if needed
    if ($self->{masterdbh}) {
        $self->{masterdbh}->rollback();
        $self->{masterdbh}->disconnect();
    }

    ## Reconnect to the master database for some final cleanups
    my ($finalbackend,$finaldbh) = $self->connect_database();
    $self->glog("Final database backend PID: $finalbackend", LOG_VERBOSE);

    ## Sleep a bit to let the processes clean up their own pid files
    sleep 1.5;

    ## We know we are authoritative for all pid files in the piddir
    ## Use those to kill any open processes that we think are still bucardo related
    my $piddir = $config{piddir};
    opendir my $dh, $piddir or die qq{Could not opendir "$piddir" $!\n};

    ## As before, we only worry about certain files,
    ## even though nothing else should be in there
    my @pidfiles2 = grep { /^bucardo.*\.pid$/ } readdir $dh;
    closedir $dh or warn qq{Could not closedir "$piddir": $!\n};

    ## For each file, attempt to kill the process it refers to
    for my $pidfile (sort @pidfiles2) {
        next if $pidfile eq 'bucardo.mcp.pid'; ## That's us!
        my $pfile = File::Spec->catfile( $piddir => $pidfile );
        if (-e $pfile) {
            $self->glog("Trying to kill stale PID file $pidfile", LOG_DEBUG);
            my $result = $self->kill_bucardo_pidfile($pfile);
            if ($result == -4) { ## kill 0 indicates that PID is no more
                $self->glog("PID from $pidfile is gone, removing file", LOG_NORMAL);
                unlink $pfile;
            }
        }
    }

    ## Gather system and database timestamps, output them to the logs
    my $end_systemtime = scalar localtime;
    my $end_dbtime = eval { $finaldbh->selectcol_arrayref('SELECT now()')->[0] } || 'unknown';
    $self->glog(qq{End of cleanup_mcp. Sys time: $end_systemtime. Database time: $end_dbtime}, LOG_TERSE);

    ## Let anyone listening know we have stopped
    $self->db_notify($finaldbh, 'stopped', 1) if $end_dbtime ne 'unknown';
    $finaldbh->disconnect();

    ## For the very last thing, remove our own PID file
    if (unlink $self->{pid_file}) {
        $self->glog(qq{Removed pid file "$self->{pid_file}"}, LOG_DEBUG);
    }
    else {
        $self->glog("Warning! Failed to remove pid file $self->{pid_file}", LOG_WARN);
    }

    return;

} ## end of cleanup_mcp



sub terminate_old_goats {

    ## Uses the dbrun table to see if any existing connections are still active
    ## This can happen if a KID is killed but a large COPY is still going on
    ## Arguments: one
    ## 1. Optional sync name to limit the reaping to
    ## Returns: number of backends successfully terminated

    my $self = shift;
    my $sync = shift || '';

    my $maindbh = $self->{masterdbh};

    my $SQL;

    ## Grab all backends in the table
    $SQL = 'SELECT * FROM bucardo.dbrun WHERE pgpid IS NOT NULL';

    ## Just for one sync if that was passed in
    if ($sync) {
        $SQL .= ' AND sync = ' . $maindbh->quote($sync);
    }

    $sth = $maindbh->prepare($SQL);
    $sth->execute();

    ## Create a hash with the names of the databases as the first-level keys,
    ## and the process ids as the second-level keys.
    my %dbpid;
    for my $row (@{ $sth->fetchall_arrayref({}) }) {
        $dbpid{$row->{dbname}}{$row->{pgpid}} = $row->{started};
    }

    ## Use pg_stat_activity to find a match, then terminate it
    my $pidcol = $maindbh->{pg_server_version} >= 90200 ? 'pid' : 'procpid';
    $SQL = "SELECT 1 FROM pg_stat_activity WHERE $pidcol = ? AND query_start = ?";
    my $SQLC = 'SELECT pg_cancel_backend(?)';
    my $total = 0;
    for my $dbname (sort keys %{ $self->{sdb} }) {

        my $d = $self->{sdb}{$dbname};

        ## All of this is very Postgres specific
        next if $d->{dbtype} ne 'postgres';

        ## Loop through each backend PID found for this database
      EPID: for my $pid (sort keys %{ $dbpid{$dbname} }) {
            my $time = $dbpid{$dbname}{$pid};

            if (! defined $d->{dbh}) {
                $self->glog("Existing database connection gone: reconnecting to $dbname", LOG_VERBOSE);
                eval {
                    ($d->{backend}, $d->{dbh}) = $self->connect_database($dbname);
                };
                if (! defined $d->{dbh}) {
                    $self->glog("Database $dbname unreachable, skipping cleanup of pid $pid", LOG_NORMAL);
                    next EPID;
                }
            }

            $sth = $d->{dbh}->prepare($SQL);

            ## See if the process is still around by matching PID and query_start time
            $count = $sth->execute($pid, $time);
            $sth->finish();

            ## If no match, silently move on
            next if $count < 1;

            ## If we got a match, try and kill it
            $sth = $d->{dbh}->prepare($SQLC);
            $count = $sth->execute($pid);
            my $res = $count < 1 ? 'failed' : 'ok';
            $self->glog("Attempted to kill backend $pid on db $dbname, started $time. Result: $res", LOG_NORMAL);

            ## We are going to count both failed and ok as the same for the return number
            $total += $count;
        }
    }

    return $total;

} ## end of terminate_old_goats


sub kill_bucardo_pidfile {

    ## Given a file, extract the PID and kill it
    ## Arguments: 2
    ## 1. File to be checked
    ## 2. String either 'strict' or not. Strict does TERM and KILL in addition to USR1
    ## Returns: same as kill_bucardo_pid, plus:
    ## -100: File not found
    ## -101: Could not open the file
    ## -102: No PID found in the file

    my ($self,$file,$strength) = @_;

    ## Make sure the file supplied exists!
    if (! -e $file) {
        $self->glog(qq{Failed to find PID file "$file"}, LOG_VERBOSE);
        return -100;
    }

    ## Try and open the supplied file
    my $fh;
    if (! open $fh, '<', $file) {
        $self->glog(qq{Failed to open PID file "$file": $!}, LOG_VERBOSE);
        return -101;
    }

    ## Try and extract the numeric PID from inside of it
    ## Should be the only thing on the first line
    if (<$fh> !~ /(\d+)/) {
        $self->glog(qq{Failed to find a PID in the file PID "$file"}, LOG_TERSE);
        close $fh or warn qq{Could not close "$file": $!};
        return -102;
    }

    ## Close the file and call another method to do the dirty work

    close $fh or warn qq{Could not close "$file": $!};

    return $self->kill_bucardo_pid($1 => $strength);

} ## end of kill_bucardo_pidfile


sub kill_bucardo_pid {

    ## Send a kill signal to a specific process
    ## Arguments: two
    ## 1. PID to be killed
    ## 2. String either 'strict' or not. Strict does KILL and TERM in addition to USR1
    ## Returns: 1 on successful kill, < 0 otherwise
    ## 0: no such PID or not a 'bucardo' PID
    ## +1 : successful TERM
    ## -1: Failed to signal with USR1
    ## +2: Successful KILL
    ## -2: Failed to signal with TERM and KILL
    ## -3: Invalid PID (non-numeric)
    ## -4: PID does not exist

    my ($self,$pid,$nice) = @_;

    $self->glog("Attempting to kill PID $pid", LOG_VERBOSE);

    ## We want to confirm this is still a Bucardo process
    ## The most portable way at the moment is a plain ps -p
    ## Windows users are on their own

    ## If the PID is not numeric, throw a warning and return
    if ($pid !~ /^\d+$/o) {
        $self->glog("Warning: invalid PID supplied to kill_bucardo_pid: $pid", LOG_WARN);
        return -3;
    }

    ## Make sure the process is still around
    ## If not, log it and return
    if (! kill(0 => $pid) ) {
        $self->glog("Process $pid did not respond to a kill 0", LOG_NORMAL);
        return -4;
    }

    ## It's nice to do some basic checks when possible that these are Bucardo processes
    ## For non Win32 boxes, we can try a basic ps
    ## If no header line, drive on
    ## If command is not perl, skip it!
    ## If args is not perl or bucardo, skip it
    if ($^O !~ /Win/) {
        my $COM = "ps -p $pid -o comm,args";
        my $info = qx{$COM};
        if ($info !~ /^COMMAND/) {
            $self->glog(qq{Could not determine ps information for pid $pid}, LOG_VERBOSE);
        }
        elsif ($info !~ /\bbucardo\s+/oi) {
            $self->glog(qq{Will not kill process $pid: ps args is not 'Bucardo', got: $info}, LOG_TERSE);
            return 0;
        }
    } ## end of trying ps because not Windows

    ## At this point, we've done due diligence and can start killing this pid
    ## Start with a USR1 signal
    $self->glog("Sending signal $signumber{USR1} to pid $pid", LOG_DEBUG);
    $count = kill $signumber{USR1} => $pid;

    if ($count >= 1) {
        $self->glog("Successfully signalled pid $pid with kill USR1", LOG_DEBUG);
        return 1;
    }

    ## If we are not strict, we are done
    if ($nice ne 'strict') {
        $self->glog("Failed to USR1 signal pid $pid", LOG_TERSE);
        return -1;
    }

    $self->glog("Sending signal $signumber{TERM} to pid $pid", LOG_DEBUG);
    $count = kill $signumber{TERM} => $pid;

    if ($count >= 1) {
        $self->glog("Successfully signalled pid $pid with kill TERM", LOG_DEBUG);
        return 1;
    }

    $self->glog("Failed to TERM signal pid $pid", LOG_TERSE);

    ## Raise the stakes and issue a KILL signal
    $self->glog("Sending signal $signumber{KILL} to pid $pid", LOG_DEBUG);
    $count = kill $signumber{KILL} => $pid;

    if ($count >= 1) {
        $self->glog("Successfully signalled pid $pid with kill KILL", LOG_DEBUG);
        return 2;
    }

    $self->glog("Failed to KILL signal pid $pid", LOG_TERSE);
    return -2;

} ## end of kill_bucardo_pid


sub signal_pid_files {

    ## Finds the pid in all matching pid files, and signals with USR1
    ## Arguments: 1
    ## 1. String to match the file inside the PID directory with
    ## Returns: number successfully signalled

    my ($self,$string) = @_;

    my $signalled = 0;

    ## Open the directory that contains our PID files
    my $piddir = $config{piddir};
    opendir my $dh, $piddir or die qq{Could not opendir "$piddir": $!\n};
    my ($name, $fh);
    while (defined ($name = readdir($dh))) {

        ## Skip unless it's a matched file
        next if index($name, $string) < 0;

        $self->glog(qq{Attempting to signal PID from file "$name"}, LOG_TERSE);

        ## File must be readable
        my $cfile = File::Spec->catfile( $piddir => $name );
        if (! open $fh, '<', $cfile) {
            $self->glog(qq{Could not open $cfile: $!}, LOG_WARN);
            next;
        }

        ## File must contain a number (the PID)
        if (<$fh> !~ /(\d+)/) {
            $self->glog(qq{Warning! File "$cfile" did not contain a PID!}, LOG_WARN);
            next;
        }

        my $pid = $1; ## no critic (ProhibitCaptureWithoutTest)
        close $fh or warn qq{Could not close "$cfile": $!\n};

        ## No sense in doing deeper checks that this is still a Bucardo process,
        ## as a USR1 should be a pretty harmless signal
        $count = kill $signumber{USR1} => $pid;
        if ($count != 1) {
            $self->glog(qq{Failed to signal $pid with USR1}, LOG_WARN);
        }
        else {
            $signalled++;
        }

    } ## end each file in the pid directory

    closedir $dh or warn qq{Warning! Could not closedir "$piddir": $!\n};

    return $signalled;

} ## end of signal_pid_files






sub cleanup_controller {

    ## Controller is shutting down
    ## Disconnect from the database
    ## Attempt to kill any kids
    ## Remove our PID file
    ## Arguments: two
    ## 1. Exited normally? (0 or 1)
    ## 2. Reason for leaving
    ## Return: undef

    my ($self,$normalexit,$reason) = @_;

    if (exists $self->{cleanexit}) {
        $reason = 'Normal exit';
    }

    ## Disconnect from the master database
    if ($self->{masterdbh}) {
        ## Ask all kids to exit as well
        my $exitname = "kid_stopsync_$self->{syncname}";
        $self->{masterdbh}->rollback();
        $self->db_notify($self->{masterdbh}, $exitname);

        # Quick debug to find active statement handles
        # for my $s (@{$self->{masterdbh}{ChildHandles}}) {
        #    next if ! ref $s or ! $s->{Active};
        #    $self->glog(Dumper $s->{Statement}, LOG_NORMAL);
        #}
        $self->{masterdbh}->rollback();
        $self->{masterdbh}->disconnect();
    }

    ## Sleep a bit to let the processes clean up their own pid files
    sleep 0.5;

    ## Kill any kids who have a pid file for this sync
    ## By kill, we mean "send a friendly USR1 signal"

    my $piddir = $config{piddir};
    opendir my $dh, $piddir or die qq{Could not opendir "$piddir" $!\n};
    my @pidfiles = readdir $dh;
    closedir $dh or warn qq{Could not closedir "$piddir": $!\n};

    for my $pidfile (sort @pidfiles) {
        my $sname = $self->{syncname};
        next unless $pidfile =~ /^bucardo\.kid\.sync\.$sname\.?.*\.pid$/;
        my $pfile = File::Spec->catfile( $piddir => $pidfile );
        if (open my $fh, '<', $pfile) {
            my $pid = <$fh>;
            close $fh or warn qq{Could not close "$pfile": $!\n};
            if (! defined $pid or $pid !~ /^\d+$/) {
                $self->glog("Warning: no PID found in file, so removing $pfile", LOG_TERSE);
                unlink $pfile;
            }
            else {
                kill $signumber{USR1} => $pid;
                $self->glog("Sent USR1 signal to kid process $pid", LOG_VERBOSE);
            }
        }
        else {
            $self->glog("Warning: could not open file, so removing $pfile", LOG_TERSE);
            unlink $pfile;
        }
    }

    $self->glog("Controller $$ exiting at cleanup_controller. Reason: $reason", LOG_TERSE);

    ## Remove the pid file
    if (unlink $self->{ctlpidfile}) {
        $self->glog(qq{Removed pid file "$self->{ctlpidfile}"}, LOG_DEBUG);
    }
    else {
        $self->glog("Warning! Failed to remove pid file $self->{ctlpidfile}", LOG_WARN);
    }

    ## Reconnect and clean up the syncrun table
    my ($finalbackend, $finaldbh) = $self->connect_database();
    $self->glog("Final database backend PID: $finalbackend", LOG_VERBOSE);

    ## Need to make this one either lastgood or lastbad
    ## In theory, this will never set lastgood
    $self->end_syncrun($finaldbh, $normalexit ? 'good' : 'bad',
                       $self->{syncname}, "Ended (CTL $$)");
    $finaldbh->commit();
    $finaldbh->disconnect();
    $self->glog('Made final adjustment to the syncrun table', LOG_DEBUG);

    return;

} ## end of cleanup_controller


sub end_syncrun {

    ## End the current syncrun entry, and adjust lastgood/lastbad/lastempty as needed
    ## If there is no null ended for this sync, does nothing
    ## Does NOT commit
    ## Arguments: four
    ## 1. The database handle to use
    ## 2. How did we exit ('good', 'bad', or 'empty')
    ## 3. The name of the sync
    ## 4. The new status to put
    ## Returns: undef

    my ($self, $ldbh, $exitmode, $syncname, $status) = @_;

    my $SQL;

    ## Which column are we changing?
    my $lastcol =
        $exitmode eq 'good'  ? 'lastgood' :
        $exitmode eq 'bad'   ? 'lastbad'  :
        $exitmode eq 'empty' ? 'lastempty' :
        die qq{Invalid exitmode "$exitmode"};

    ## Make sure we have something to update
    $SQL = q{
        SELECT ctid
        FROM   bucardo.syncrun
        WHERE  sync = ?
        AND    ended IS NULL};
    $sth = $ldbh->prepare($SQL);
    $count = $sth->execute($syncname);
    if ($count < 1) {
        $sth->finish();
        return;
    }
    if ($count > 1) {
        $self->glog("Expected one row from end_syncrun, but got $count", LOG_NORMAL);
    }
    my $ctid = $sth->fetchall_arrayref()->[0][0];

    ## Remove the previous 'last' entry, if any
    $SQL = qq{
        UPDATE bucardo.syncrun
        SET    $lastcol = 'false'
        WHERE  $lastcol IS TRUE
        AND    sync = ?
        };
    $sth = $ldbh->prepare($SQL);
    $sth->execute($syncname);

    ## End the current row, and elevate it to a 'last' position
    $SQL = qq{
        UPDATE bucardo.syncrun
        SET    $lastcol = 'true', ended=now(), status=?
        WHERE  ctid = ?
        };
    $sth = $ldbh->prepare($SQL);
    $sth->execute($status, $ctid);

    return;

} ## end of end_syncrun


sub run_ctl_custom_code {

    ## Arguments: four
    ## 1. Sync object
    ## 2. Input object
    ## 2. Hashref of customcode information
    ## 3. Strictness boolean, defaults to false
    ## 4. Number of attempts, defaults to 0
    ## Returns: string indicating what to do, one of:
    ## 'next'
    ## 'redo'
    ## 'normal'

    my $self = shift;
    my $sync = shift;
    my $input = shift;
    my $c = shift;
    my $strictness = shift || '';
    my $attempts = shift || 0;

    $self->glog("Running $c->{whenrun} controller custom code $c->{id}: $c->{name}", LOG_NORMAL);

    my $cc_sourcedbh;
    if (!defined $sync->{safe_sourcedbh}) {
        $cc_sourcedbh = $self->connect_database($sync->{sourcedb});
        my $darg;
        for my $arg (sort keys %{ $dbix{source}{notstrict} }) {
            next if ! length $dbix{source}{notstrict}{$arg};
            $darg->{$arg} = $dbix{source}{notstrict}{$arg};
        }
        $darg->{dbh} = $cc_sourcedbh;
        $sync->{safe_sourcedbh} = DBIx::Safe->new($darg);
    }

    $input = {
        sourcedbh  => $sync->{safe_sourcedbh},
        syncname   => $sync->{name},
        goatlist   => $sync->{goatlist},
        rellist    => $sync->{goatlist},
        sourcename => $sync->{sourcedb},
        targetname => '',
        message    => '',
        warning    => '',
        error      => '',
        nextcode   => '',
        endsync    => '',
    };

    $self->{masterdbh}->{InactiveDestroy} = 1;
    $cc_sourcedbh->{InactiveDestroy} = 1;
    local $_ = $input;
    $c->{coderef}->($input);
    $self->{masterdbh}->{InactiveDestroy} = 0;
    $cc_sourcedbh->{InactiveDestroy} = 0;
    $self->glog("Finished custom code $c->{name}", LOG_VERBOSE);
    if (length $input->{message}) {
        $self->glog("Message from $c->{whenrun} code $c->{name}: $input->{message}", LOG_TERSE);
    }
    if (length $input->{warning}) {
        $self->glog("Warning! Code $c->{whenrun} $c->{name}: $input->{warning}", LOG_WARN);
    }
    if (length $input->{error}) {
        $self->glog("Warning! Code $c->{whenrun} $c->{name}: $input->{error}", LOG_WARN);
        die "Code $c->{whenrun} $c->{name} error: $input->{error}";
    }
    if (length $input->{nextcode}) { ## Mostly for conflict handlers
        return 'next';
    }
    if (length $input->{endsync}) {
        $self->glog("Code $c->{whenrun} requests a cancellation of the rest of the sync", LOG_TERSE);
        ## before_txn and after_txn only should commit themselves
        $cc_sourcedbh->rollback();
        $self->{masterdbh}->commit();
        sleep $config{endsync_sleep};
        return 'redo';
    }

    return 'normal';

} ## end of run_ctl_custom_code


sub create_newkid {

    ## Fork and create a KID process
    ## Arguments: one
    ## 1. Hashref of sync information ($self->{sync}{$syncname})
    ## Returns: PID of new process

    my ($self, $kidsync) = @_;

    ## Just in case, ask any existing kid processes to exit
    $self->db_notify($self->{masterdbh}, "kid_stopsync_$self->{syncname}");

    ## Sleep a hair so we don't have the newly created kid get the message above
#    sleep 1;

    ## Fork off a new process which will become the KID
    my $newkid = $self->fork_and_inactivate('KID');

    if ($newkid) { ## We are the parent
        my $msg = sprintf q{Created new kid %s for sync "%s"},
            $newkid, $self->{syncname};
        $self->glog($msg, LOG_VERBOSE);

        ## Map this PID to a name for CTL use elsewhere
        $self->{pidmap}{$newkid} = 'KID';

        sleep $config{ctl_createkid_time};

        return $newkid;
    }

    ## At this point, this is the kid. Make sure we do not inherit the CTL error handler:
    $SIG{__DIE__} = undef;

    ## Create the kid process
    $self->start_kid($kidsync);

    exit 0;

} ## end of create_newkid


sub get_deadlock_details {

    ## Given a database handle, extract deadlock details from it
    ## Arguments: two
    ## 1. Database handle
    ## 2. Database error string
    ## Returns: detailed string, or an empty one

    my ($self, $dldbh, $dlerr) = @_;
    return '' unless $dlerr =~ /Process \d+ waits for /;
    return '' unless defined $dldbh and $dldbh;

    $dldbh->rollback();
    my $pid = $dldbh->{pg_pid};
    while ($dlerr =~ /Process (\d+) waits for (.+) on relation (\d+) of database (\d+); blocked by process (\d+)/g) {
        next if $1 == $pid;
        my ($process,$locktype,$relation) = ($1,$2,$3);
        ## Fetch the relation name
        my $getname = $dldbh->prepare(q{SELECT nspname||'.'||relname FROM pg_class c, pg_namespace n ON (n.oid=c.relnamespace) WHERE c.oid = ?});
        $getname->execute($relation);
        my $relname = $getname->fetchall_arrayref()->[0][0];

        my $clock_timestamp = $dldbh->{pg_server_version} >= 80200
            ? 'clock_timestamp()' : 'timeofday()::timestamptz';

        ## Fetch information about the conflicting process
        my $pidcol = $dldbh->{pg_server_version} >= 90200 ? 'pid' : 'procpid';
        my $queryinfo =$dldbh->prepare(qq{
SELECT
  current_query AS query,
  datname AS database,
  TO_CHAR($clock_timestamp, 'HH24:MI:SS (YYYY-MM-DD)') AS current_time,
  TO_CHAR(backend_start, 'HH24:MI:SS (YYYY-MM-DD)') AS backend_started,
  TO_CHAR($clock_timestamp - backend_start, 'HH24:MI:SS') AS backend_age,
  CASE WHEN query_start IS NULL THEN '?' ELSE
    TO_CHAR(query_start, 'HH24:MI:SS (YYYY-MM-DD)') END AS query_started,
  CASE WHEN query_start IS NULL THEN '?' ELSE
    TO_CHAR($clock_timestamp - query_start, 'HH24:MI:SS') END AS query_age,
  COALESCE(host(client_addr)::text,''::text) AS ip,
  CASE WHEN client_port <= 0 THEN 0 ELSE client_port END AS port,
  usename AS user
FROM pg_stat_activity
WHERE $pidcol = ?
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


sub cleanup_kid {

    ## Kid is shutting down
    ## Remove our PID file
    ## Arguments: two
    ## 1. Reason for leaving
    ## 2. Extra information
    ## Returns: undef

    my ($self,$reason,$extrainfo) = @_;

    $self->glog("Kid $$ exiting at cleanup_kid. $extrainfo Reason: $reason", LOG_TERSE);

    ## Remove the pid file, but only if it has our PID in it!
    my $file = $self->{kidpidfile};
    my $fh;
    if (! open my $fh, '<', $file) {
        $self->glog("Warning! Could not find pid file $file", LOG_WARN);
    }
    elsif (<$fh> !~ /(\d+)/) {
        $self->glog("Warning! File $file did not contain a PID", LOG_WARN);
    }
    else {
        my $oldpid = $1;
        if ($$ !~ $oldpid) {
            $self->glog("File $file contained foreign PID $oldpid, so will not remove", LOG_WARN);
        }
        elsif (unlink $file) {
            $self->glog(qq{Removed pid file $file}, LOG_DEBUG);
        }
        else {
            $self->glog("Warning! Failed to remove pid file $file", LOG_WARN);
        }
    }
    return;

} ## end of cleanup_kid


sub store_pid {

    ## Store the PID of the current process somewhere (e.g. local disk)
    ## Arguments: one
    ## 1. Name of the file
    ## Returns: complete name of the file, with directory

    my $self = shift;
    my $file = shift or die;

    ## Put this file into our pid directory
    my $pidfile = File::Spec->catfile( $config{piddir} => $file );

    ## Check for and remove old processes
    my $oldpid = '?';
    if (-e $pidfile) {
        ## Send the PID in the file a USR1. If we did so, sleep a little bit
        ## to allow that process to clean itself up
        $self->signal_pid_files($pidfile) and sleep 1;
        if (-e $pidfile) {
            $self->glog("Overwriting $pidfile: old process was $oldpid", LOG_NORMAL);
        }
    }

    ## Overwrite anything that is already there
    open my $pidfh, '>', $pidfile or die qq{Cannot write to $pidfile: $!\n};
    print {$pidfh} "$$\n";
    close $pidfh or warn qq{Could not close "$pidfile": $!\n};
    $self->glog("Created $pidfile", LOG_DEBUG);

    return $pidfile;

} ## end of store_pid


sub table_has_rows {

    ## See if the given table has any rows or not
    ## Arguments: two
    ## 1. Target database object (contains dbtype and possibly dbh)
    ## 2. Name of the table
    ## Returns: true or false

    my ($self,$d,$tname) = @_;

    my $SQL;

    ## Some types do not have a count
    return 0 if $d->{does_append_only};

    if ($d->{does_limit}) {
        $SQL = "SELECT 1 FROM $tname LIMIT 1";
        $sth = $d->{dbh}->prepare($SQL);
        $sth->execute();
        $count = $sth->rows();
        $sth->finish();
        return $count >= 1 ? 1 : 0;
    }
    elsif ('mongo' eq $d->{dbtype}) {
        my $collection = $d->{dbh}->get_collection($tname);
        $count = $collection->count({});
        return $count >= 1 ? 1 : 0;
    }
    elsif ('oracle' eq $d->{dbtype}) {
        $SQL = "SELECT 1 FROM $tname WHERE rownum > 1";
        $sth = $d->{dbh}->prepare($SQL);
        $sth->execute();
        $count = $sth->rows();
        $sth->finish();
        return $count >= 1 ? 1 : 0;
    }
    elsif ('redis' eq $d->{dbtype}) {
        ## No sense in returning anything here
        return 0;
    }
    else {
        die "Cannot handle database type $d->{dbtype} yet!";
    }

    return 0;

} ## end of table_has_rows


sub get_sequence_info {

    ## Get sequence information
    ## Not technically MVCC but good enough for our purposes
    ## Arguments: five
    ## 1. Database handle
    ## 2. Schema name
    ## 3. Sequence name
    ## 4. (optional) Name of the sync
    ## 5. (optional) Target database name
    ## Returns: hashref of information

    ## If five arguments are given, look up the "old" information in bucardo_sequences
    ## With only three arguments, pull directly from the sequence

    return; ## XXX sequence work

    my ($self,$ldbh,$schemaname,$seqname,$syncname,$targetname) = @_;

    my $SQL;

    if (defined $syncname) {
        ## Pull "old" sequence information. May be empty.
        $SQL = "SELECT $sequence_columns FROM bucardo.bucardo_sequences "
            . ' WHERE schemaname=? AND seqname = ? AND syncname=? AND targetname=?';
        $sth = $ldbh->prepare($SQL);
        $sth->execute($schemaname,$seqname, $syncname, $targetname);
    }
    else {
        ## Pull directly from a named sequence
        $SQL = "SELECT $sequence_columns FROM $schemaname.$seqname";
        $sth = $ldbh->prepare($SQL);
        $sth->execute();
    }

    return $sth->fetchall_arrayref({})->[0];

} ## end of get_sequence_info


sub adjust_sequence {

    ## Adjusts all sequences as needed using a "winning" source database sequence
    ## If changed, update the bucardo_sequences table
    ## Arguments: four
    ## 1. goat object (which contains 'winning_db' and 'sequenceinfo')
    ## 2. sync object
    ## 2. Schema name
    ## 3. Sequence name
    ## 4. Name of the current sync
    ## Returns: number of changes made for this sequence

    my ($self,$g,$sync,$S,$T,$syncname) = @_;

    my $SQL;

    ## Total changes made across all databases
    my $changes = 0;

    my $winner = $g->{winning_db};

    my $sourceinfo = $g->{sequenceinfo}{$winner};

    ## Walk through all Postgres databases and set the sequence
    for my $dbname (sort keys %{ $sync->{db} }) {

        next if $dbname eq $winner; ## Natch

        my $d = $sync->{db}{$dbname};

        next if $d->{dbtype} ne 'postgres';

        next if ! $d->{adjustsequence};

        ## Reset the flag in case this sub is called more than once
        $d->{adjustsequence} = 0;

        my $targetinfo = $g->{sequenceinfo}{$dbname} || {};

        ## First, change things up via SETVAL if needed
        if (! exists $targetinfo->{last_value}
            or
            $sourceinfo->{last_value} != $targetinfo->{last_value}
            or
            $sourceinfo->{is_called} != $targetinfo->{is_called}) {
            $self->glog("Set sequence $dbname.$S.$T to $sourceinfo->{last_value} (is_called to $sourceinfo->{is_called})",
                        LOG_DEBUG);
            $SQL = qq{SELECT setval('$S.$T', $sourceinfo->{last_value}, '$sourceinfo->{is_called}')};
            $d->{dbh}->do($SQL);
            $changes++;
        }

        ## Then, change things up via ALTER SEQUENCE if needed
        my @alter;
        for my $col (@sequence_columns) {
            my ($name,$syntax) = @$col;

            ## Skip things not set by ALTER SEQUENCE
            next if ! $syntax;

            ## Older versions may not have all the fields!
            next if ! exists $sourceinfo->{$name} or ! exists $targetinfo->{$name};

            ## Skip if these items are the exact same
            next if $sourceinfo->{$name} eq $targetinfo->{$name};

            ## Fullcopy will not have this, and we won't report it
            if (exists $targetinfo->{$name}) {
                $self->glog("Sequence $S.$T has a different $name value: was $targetinfo->{$name}, now $sourceinfo->{$name}", LOG_VERBOSE);
            }

            ## If this is a boolean setting, we want to simply prepend a 'NO' for false
            if ($syntax =~ s/BOOL //) {
                push @alter => sprintf '%s%s',
                    $sourceinfo->{$name} ? '' : 'NO ',
                    $syntax;
            }
            else {
                push @alter => "$syntax $sourceinfo->{$name}";
            }
            $changes++;

        } ## end each sequence column

        if (@alter) {
            $SQL = "ALTER SEQUENCE $S.$T ";
            $SQL .= join ' ' => @alter;
            $self->glog("Running on target $dbname: $SQL", LOG_DEBUG);
            $d->{dbh}->do($SQL);
        }

    } ## end each database

    return $changes;

} ## end of adjust_sequence


sub run_kid_custom_code {

    ## Prepare and then run the custom code subroutine
    ## Arguments: two
    ## 1. Sync information
    ## 2. This code information
    ## Returns: status code, one of 'redo', 'last', 'retry', or 'normal'
    ## May also throw an exception if the calling code requests it

    my $self = shift;
    my $sync = shift;
    my $c    = shift;

    $self->glog("Running $c->{whenrun} custom code $c->{id}: $c->{name}", LOG_NORMAL);

    ## Allow the caller to maintain some state by providing a hash
    if (! exists $self->{kid_customcode_shared}) {
        $self->{kid_customcode_shared} = {};
    }

    ## Create a hash of information common to all customcodes
    my $info = {
        rows     => $sync->{deltarows},
        syncname => $sync->{name},
        version  => $self->{version}, ## Version of Bucardo

        message  => '',  ## Allows the code to send a message to the logs
        warning  => '',  ## Allows a warning to be thrown by the code
        error    => '',  ## Allows an exception to be thrown by the code
        skip     => '',  ## Tells the caller to skip this code
        lastcode => '',  ## Tells the caller to skip any other codes of this type
        endsync  => '',  ## Tells the caller to cancel the whole sync
        sendmail => sub { $self->send_mail(@_) },
        shared   => $self->{kid_customcode_shared},
    };

    ## Add in any items custom to this code
    if (exists $c->{info}) {
        for my $key (keys %{ $c->{info} }) {
            $info->{$key} = $c->{info}{$key};
        }
        delete $c->{info};
    }

    ## Make a copy of what we send them, so we can safely pull back info later
    my $infocopy = {};
    for (keys %$info) {
        $infocopy->{$_} = $info->{$_};
    }

    ## If they need database handles, provide them
    if ($c->{getdbh}) {
        my $strict = ($c->{whenrun} eq 'before_txn' or $c->{whenrun} eq 'after_txn') ? 1 : 0;
        for my $dbname (keys %{ $sync->{db} }) {
            $info->{dbh}{$dbname} = $strict ? $self->{safe_dbh}{$dbname}
                : $self->{safe_dbh_strict}{$dbname};
        }
    }

    ## Set all databases' InactiveDestroy to on, so the customcode doesn't mess things up
    for my $dbname (keys %{ $sync->{db} }) {
        $sync->{db}{$dbname}{dbh}->{InactiveDestroy} = 1;
    }

    ## Run the actual code!
    local $_ = $info;
    $c->{coderef}->($info);

    $self->glog("Finished custom code $c->{name}", LOG_VERBOSE);

    for my $dbname (keys %{ $sync->{db} }) {
        $sync->{db}{$dbname}{dbh}->{InactiveDestroy} = 0;
    }

    ## Check for any messages set by the custom code
    if (length $info->{message}) {
        $self->glog("Message from $c->{whenrun} code $c->{name}: $info->{message}", LOG_TERSE);
    }

    ## Check for any warnings set by the custom code
    if (length $info->{warning}) {
        $self->glog("Warning! Code $c->{whenrun} $c->{name}: $info->{warning}", LOG_WARN);
    }

    ## Check for any errors set by the custom code. Throw an exception if found.
    if (length $info->{error}) {
        $self->glog("Warning! Code $c->{whenrun} $c->{name}: $info->{error}", LOG_WARN);
        die "Code $c->{whenrun} $c->{name} error: $info->{error}";
    }

    ## Check for a request to end the sync.
    ## If found, rollback, adjust the Q, and redo the kid
    if (length $info->{endsync}) {
        $self->glog("Code $c->{whenrun} requests a cancellation of the rest of the sync", LOG_TERSE);
        ## before_txn and after_txn should commit themselves
        for my $dbname (keys %{ $sync->{db} }) {
            $sync->{db}{$dbname}{dbh}->rollback();
        }
        my $syncname = $infocopy->{syncname};
        my $targetname = $infocopy->{targetname};
        $sth{qend}->execute(0,0,0,$syncname,$targetname,$$);
        my $notify = "bucardo_syncdone_${syncname}_$targetname";
        my $maindbh = $self->{masterdbh};
        $self->db_notify($maindbh, $notify);
        sleep $config{endsync_sleep};
        return 'redo';
    }

    ## The custom code has requested we retry this sync (exception code only)
    if (exists $info->{retry} and $info->{retry}) {
        return 'retry';
    }

    ## The custom code has requested we don't call any other codes of the same type
    if (length $info->{lastcode}) {
        return 'last';
    }

    ## The custom code has requested we skip this code (and let any others try)
    if (length $info->{skip}) {
        return 'skip';
    }

    ## Four cases for handling conflicts:
    ## The customcode has told us how to handle this table
    ## The customcode has told us how to handle this table until a sync restart
    ## The customcode has told us how to handle all tables in the sync
    ## The customcode has told us how to handle all tables in the sync until a sync restart
    for my $case (qw/ tablewinner tablewinner_always syncwinner syncwinner_always /) {
        if (exists $info->{$case}) {
            return "$case: $info->{$case}";
        }
    }

    ## Default action, which usually means the next code in the list, if any
    return 'normal';

} ## end of run_kid_custom_code


sub truncate_table {

    ## Given a table, attempt to truncate it
    ## Arguments: three
    ## 1. Database object
    ## 2. Table object
    ## 3. Boolean if we should CASCADE the truncate or not
    ## Returns: true if the truncate succeeded without error, false otherwise

    my ($self, $Database, $Table, $does_cascade) = @_;

    my $SQL;

    ## Override any existing handlers so we can cleanly catch the eval
    local $SIG{__DIE__} = sub {};

    my $tablename = exists $Table->{tablename} ? $Table->{tablename} : "$Table->{safeschema}.$Table->{safetable}";

    if ($Database->{does_sql}) {
        if ($Database->{does_savepoints}) {
            $Database->{dbh}->do('SAVEPOINT truncate_attempt');
        }
        $SQL = sprintf 'TRUNCATE TABLE %s%s',
        $tablename,
        ($does_cascade and $Database->{does_cascade}) ? ' CASCADE' : '';
        my $truncate_ok = 0;

        eval {
            $Database->{dbh}->do($SQL);
            $truncate_ok = 1;
        };
        if (! $truncate_ok) {
            $Database->{does_savepoints} and $Database->{dbh}->do('ROLLBACK TO truncate_attempt');
            $self->glog("Truncate error for db $Database->{name}.$Database->{dbname}.$tablename: $@", LOG_NORMAL);
            return 0;
        }
        else {
            $Database->{does_savepoints} and $Database->{dbh}->do('RELEASE truncate_attempt');
            return 1;
        }
    }

    if ('mongo' eq $Database->{dbtype}) {
        my $collection = $Database->{dbh}->get_collection($tablename);
        $self->{oldmongo} ? $collection->remove({}, { safe => 1} ): $collection->delete_many({}, { safe => 1} );
        return 1;
    }

    elsif ('redis' eq $Database->{dbtype}) {
        ## No real equivalent here, as we do not map tables 1:1 to redis keys
        ## In theory, we could walk through all keys and delete ones that match the table
        ## We will hold off until someone actually needs that, however :)
        return 1;
    }

    return undef;

} ## end of truncate_table


sub delete_table {

    ## Given a table, attempt to unconditionally delete rows from it
    ## Arguments: two
    ## 1. Database object
    ## 2. Table object
    ## Returns: number of rows deleted

    my ($self, $d, $Table) = @_;

    my $tablename = exists $Table->{tablename} ? $Table->{tablename} : "$Table->{safeschema}.$Table->{safetable}";

    my $count = 0;

    if ($d->{does_sql}) {
        ($count = $d->{dbh}->do("DELETE FROM $tablename")) =~ s/0E0/0/o;
    }
    elsif ('mongo' eq $d->{dbtype}) {
        ## Same as truncate, really, except we return the number of rows
        my $collection = $d->{dbh}->get_collection($tablename);
        if ($self->{oldmongo}) {
            my $res = $collection->remove({}, { safe => 1} );
            $count = $res->{n};
        }
        else {
            my $res = $collection->delete_many({}, { safe => 1} );
            $count = $res->{deleted_count};
        }
    }
    elsif ('redis' eq $d->{dbtype}) {
        ## Nothing relevant here, as the table is only part of the key name
    }
    else {
        die "Do not know how to delete a dbtype of $d->{dbtype}";
    }

    return $count;

} ## end of delete_table


sub delete_rows {

    ## Given a list of rows, delete them from a table in one or more databases
    ## Arguments: four
    ## 1. Hashref of rows to delete, where the keys are the primary keys (\0 joined if multi).
    ## 2. Table object
    ## 3. Sync object
    ## 4. Target database object (or an arrayref of the same)
    ## Returns: number of rows deleted

    my ($self,$rows,$Table,$Sync,$TargetDB) = @_;

    ## Have we already truncated this table? If yes, skip and reset the flag
    if (exists $Table->{truncatewinner}) {
        return 0;
    }

    my ($S,$T) = ($Table->{safeschema},$Table->{safetable});

    my $syncname = $Sync->{name};
       my $pkcols = $Table->{pkeycols};
       my $pkcolsraw = $Table->{pkeycolsraw};

    ## Ensure the target database argument is always an array
    if (ref $TargetDB ne 'ARRAY') {
        $TargetDB = [$TargetDB];
    }

    ## We may be going from one table to another - this is the mapping hash
    my $customname = $Table->{newname}{$syncname} || {};

    ## Are we truncating?
    if (exists $self->{truncateinfo} and exists $self->{truncateinfo}{$S}{$T}) {

        ## Try and truncate each target
        for my $Target (@$TargetDB) {

            my $target_tablename = $customname->{$Target->{name}};

            my $type = $Target->{dbtype};

            ## Postgres is a plain and simple TRUNCATE, with an async flag
            ## TRUNCATE CASCADE is not needed as everything should be in one
            ## sync (herd), and we have turned all FKs off
            if ('postgres' eq $type) {
                $Target->{dbh}->do("$self->{sqlprefix}TRUNCATE table $target_tablename", { pg_async => PG_ASYNC });
                $Target->{async_active} = time;
            }
            ## For all other SQL databases, we simply truncate
            elsif ($Target->{does_sql}) {
                $Target->{dbh}->do("$self->{sqlprefix}TRUNCATE TABLE $target_tablename");
            }
            ## For MongoDB, we simply remove everything from the collection
            ## This keeps the indexes around (which is why we don't "drop")
            elsif ('mongo' eq $type) {
                my $collection = $Target->{dbh}->get_collection($target_tablename);
                $collection->remove({}, { safe => 1 } );
            }
            ## For flatfiles, write out a basic truncate statement
            elsif ($type =~ /flat/o) {
                printf {$Target->{filehandle}} qq{TRUNCATE TABLE $target_tablename;\n\n};
                $self->glog(qq{Appended truncate command to flatfile "$Target->{filename}"}, LOG_VERBOSE);
            }
            elsif ('redis' eq $type) {
                ## For Redis, do nothing
            }
            ## Safety valve:
            else {
                die qq{Do not know how to do truncate for type $type!\n};
            }

        } ## end each target to be truncated

        ## Final cleanup for each target
        for my $Target (@$TargetDB) {
            if ('postgres' eq $Target->{dbtype}) {
                ## Wait for the async truncate call to finish
                $Target->{dbh}->pg_result();
                $Target->{async_active} = 0;
            }
        }

        ## We do not know how many rows were actually truncated
        return 0;

    } ## end truncation

    ## We may want to break the SQL into separate statements if there are lots of keys
    my $chunksize = $config{statement_chunk_size} || $default_statement_chunk_size;

    ## The number of primary keys this table has affects our SQL
    my $numpks = $Table->{numpkcols};

    ## Setup our deletion SQL as needed
    my %SQL;
    for my $Target (@$TargetDB) {

        my $type = $Target->{dbtype};

        ## Track the number of rows actually deleted from this target
        $Target->{deleted_rows} = 0;

        ## Set to true when all rounds completed
        $Target->{delete_complete} = 0;

        ## No special preparation for mongo or redis
        next if $type =~ /mongo|redis/;

        ## The actual target table name: may differ from the source!
        my $target_tablename = $customname->{$Target->{name}};

        if ('firebird' eq $type) {
            $Table->{pklist} =~ s/\"//g; ## not ideal: fix someday
            $Table->{pklist} = uc $Table->{pklist};
            $target_tablename = qq{"$target_tablename"} if $target_tablename !~ /"/;
        }

        ## Set the type of SQL we are using: IN vs ANY. Default is IN
        ## Use of ANY is greatly preferred, but can only use if the
        ## underlying database supports it, and if we have a single column pk
        my $sqltype = ($Target->{does_ANY_clause} and 1==$numpks) ? 'ANY' : 'IN';

        ## Internal counters to help us break queries into chunks if needed
        my ($round, $roundtotal) = (0,0);

        ## Array to store each chunk of SQL
        my @chunk;
        ## Optimization for a single primary key using ANY(?)
        if ('ANY' eq $sqltype and ! exists $SQL{ANY}{$target_tablename}) {
            $SQL{ANY}{$target_tablename} = "$self->{sqlprefix}DELETE FROM $target_tablename WHERE $pkcols = ANY(?)";
            for my $key (keys %$rows) {
                push @{$chunk[$round]} => length $key ? ([split '\0', $key, -1]) : [''];
                if (++$roundtotal >= $chunksize) {
                    $roundtotal = 0;
                    $round++;
                }
            }
            $SQL{ANYargs} = \@chunk;
        }
        ## Normal DELETE call with IN() clause
        elsif ('IN' eq $sqltype and ! exists $SQL{IN}{$target_tablename}) {
            $SQL{IN}{$target_tablename} = sprintf '%sDELETE FROM %s WHERE (%s) IN (',
                $self->{sqlprefix},
                $target_tablename,
                $Table->{pklist};
            my $inner;
            if ($Target->{has_mysql_timestamp_issue}) {
                for my $key (keys %$rows) {
                    $inner = length $key
                        ? (join ',' => map { s/\'/''/go; s{\\}{\\\\}; s/\+\d\d$//; qq{'$_'}; } split '\0', $key, -1)
                        : q{''};
                    $chunk[$round] .= "($inner),";
                    if (++$roundtotal >= $chunksize) {
                        $roundtotal = 0;
                        $round++;
                    }
                }
            }
            else {
                for my $key (keys %$rows) {
                    $inner = length $key
                        ? (join ',' => map { s/\'/''/go; s{\\}{\\\\}; qq{'$_'}; } split '\0', $key, -1)
                        : q{''};
                    $chunk[$round] .= "($inner),";
                    if (++$roundtotal >= $chunksize) {
                        $roundtotal = 0;
                        $round++;
                    }
                }
            }
            ## Cleanup
            for (@chunk) {
                chop;
                $_ = "$SQL{IN}{$target_tablename} $_)";
            }
            $SQL{IN}{$target_tablename} = \@chunk;
        }

        $Target->{delete_rounds} = @chunk;

        ## If we bypassed because of a cached version, use the cached delete_rounds too
        if ('ANY' eq $sqltype) {
            if (exists $SQL{ANYrounds}{$target_tablename}) {
                $Target->{delete_rounds} = $SQL{ANYrounds}{$target_tablename};
            }
            else {
                $SQL{ANYrounds}{$target_tablename} = $Target->{delete_rounds};
            }
        }
        elsif ('IN' eq $sqltype) {
            if (exists $SQL{INrounds}{$target_tablename}) {
                $Target->{delete_rounds} = $SQL{INrounds}{$target_tablename};
            }
            else {
                $SQL{INrounds}{$target_tablename} = $Target->{delete_rounds};
            }
        }

        ## Empty our internal tracking items that may have been set previously
        $Target->{delete_round} = 0;
        delete $Target->{delete_sth};

    } ## end each Target

    ## Start the main deletion loop
    ## The idea is to be efficient as possible by always having as many
    ## async targets running as possible. We run one non-async at a time
    ## before heading back to check on the asyncs.

    my $done = 0;
    my $did_something;
    while (!$done) {

        $did_something = 0;

        ## Wrap up any async targets that have finished
        for my $Target (@$TargetDB) {
            next if ! $Target->{async_active} or $Target->{delete_complete};
            if ('postgres' eq $Target->{dbtype}) {
                if ($Target->{dbh}->pg_ready) {
                    ## If this was a do(), we already have the number of rows
                    if (1 == $numpks) {
                        $Target->{deleted_rows} += $Target->{dbh}->pg_result();
                    }
                    else {
                        $Target->{dbh}->pg_result();
                    }
                    $Target->{async_active} = 0;
                }
            }
            ## Don't need to check for invalid types: happens on the kick off below
        }

        ## Kick off all dormant async targets
        for my $Target (@$TargetDB) {

            ## Skip if this target does not support async, or is in the middle of a query
            next if ! $Target->{does_async} or $Target->{async_active} or $Target->{delete_complete};

            ## The actual target name
            my $target_tablename = $customname->{$Target->{name}};

            if ('postgres' eq $Target->{dbtype}) {

                ## Which chunk we are processing.
                $Target->{delete_round}++;
                if ($Target->{delete_round} > $Target->{delete_rounds}) {
                    $Target->{delete_complete} = 1;
                    next;
                }
                my $dbname = $Target->{name};
                $self->glog("Deleting from target $dbname.$target_tablename (round $Target->{delete_round} of $Target->{delete_rounds})", LOG_DEBUG);

                $did_something++;

                ## Single primary key, so delete using the ANY(?) format
                if (1 == $numpks) {
                    ## Use the or-equal so we only prepare this once
                    $Target->{delete_sth} ||= $Target->{dbh}->prepare("$SQL{ANY}{$target_tablename}", { pg_async => PG_ASYNC });
                    $Target->{delete_sth}->execute($SQL{ANYargs}->[$Target->{delete_round}-1]);
                }
                ## Multiple primary keys, so delete old school via IN ((x,y),(a,b))
                else {
                    my $pre = $Target->{delete_rounds} > 1 ? "/* $Target->{delete_round} of $Target->{delete_rounds} */ " : '';
                    ## The pg_direct tells DBD::Pg there are no placeholders, and to use PQexec directly
                    $Target->{deleted_rows} += $Target->{dbh}->
                        do($pre.$SQL{IN}{$target_tablename}->[$Target->{delete_round}-1], { pg_async => PG_ASYNC, pg_direct => 1 });
                }

                $Target->{async_active} = time;
            } ## end postgres
            else {
                die qq{Do not know how to do async for type $Target->{dbtype}!\n};
            }

        } ## end all async targets

        ## Kick off a single non-async target
        for my $Target (@$TargetDB) {

            ## Skip if this target is async, or has no more rounds
            next if $Target->{does_async} or $Target->{delete_complete};

            $did_something++;

            my $type = $Target->{dbtype};

            ## The actual target name
            my $target_tablename = $customname->{$Target->{name}};

            $self->glog("Deleting from target $target_tablename (type=$type)", LOG_DEBUG);

            if ('firebird' eq $type) {
                $target_tablename = qq{"$target_tablename"} if $target_tablename !~ /"/;
            }

            if ('mongo' eq $type) {

                ## Set the collection
                $Target->{collection} = $Target->{dbh}->get_collection($target_tablename);

                ## Because we may have multi-column primary keys, and each key may need modifying,
                ## we have to put everything into an array of arrays.
                ## The first level is the primary key number, the next is the actual values
                my @delkeys = [];

                ## The pkcolsraw variable is a simple comma-separated list of PK column names
                ## The rows variable is a hash with the PK values as keys (the values can be ignored)

                ## Binary PKs are easy: all we have to do is decode
                ## We can assume that binary PK means not a multi-column PK
                if ($Table->{hasbinarypkey}) {
                    @{ $delkeys[0] } = map { decode_base64($_) } keys %$rows;
                }
                else {

                    ## Break apart the primary keys into an array of arrays
                    my @fullrow = map { length($_) ? [split '\0', $_, -1] : [''] } keys %$rows;

                    ## Which primary key column we are currently using
                    my $pknum = 0;

                    ## Walk through each column making up the primary key
                    for my $realpkname (split /,/, $pkcolsraw, -1) {

                        ## Grab what type this column is
                        ## We need to map non-strings to correct types as best we can
                        my $ctype = $Table->{columnhash}{$realpkname}{ftype};

                        ## For integers, we simply force to a Perlish int
                        if ($ctype =~ /smallint|integer|bigint/o) {
                            @{ $delkeys[$pknum] } = map { int $_->[$pknum] } @fullrow;
                        }
                        ## Non-integer numbers get set via the strtod command from the 'POSIX' module
                        elsif ($ctype =~ /real|double|numeric/o) {
                            @{ $delkeys[$pknum] } = map { strtod $_->[$pknum] } @fullrow;
                        }
                        ## Boolean becomes true Perlish booleans via the 'boolean' module
                        elsif ($ctype eq 'boolean') {
                            @{ $delkeys[$pknum] } = map { $_->[$pknum] eq 't' ? boolean->true : boolean->false } @fullrow;
                        }
                        ## Everything else gets a direct mapping
                        else {
                            @{ $delkeys[$pknum] } = map { $_->[$pknum] } @fullrow;
                        }
                        $pknum++;
                    }
                } ## end of multi-column PKs

                ## We may need to batch these to keep the total message size reasonable
                my $max = keys %$rows;
                $max--;

                ## The bottom of our current array slice
                my $bottom = 0;

                ## This loop limits the size of our delete requests to mongodb
              MONGODEL: {
                    ## Calculate the current top of the array slice
                    my $top = $bottom + $chunksize;

                    ## Stop at the total number of rows
                    $top = $max if $top > $max;

                    ## If we have a single key, we can use the '$in' syntax
                    if ($numpks <= 1) {
                        my @newarray = @{ $delkeys[0] }[$bottom..$top];
                        if ($self->{oldmongo}) {
                            my $res = $Target->{collection}->remove( {$pkcolsraw => { '$in' => \@newarray }}, { safe => 1 });
                            $Target->{deleted_rows} += $res->{n};
                        }
                        else {
                            my $res = $Target->{collection}->delete_many( {$pkcolsraw => { '$in' => \@newarray }}, { safe => 1 });
                            $Target->{deleted_rows} += $res->{deleted_count};
                        }
                    }
                    else {
                        ## For multi-column primary keys, we cannot use '$in', sadly.
                        ## Thus, we will just call delete once per row

                        ## Put the names into an easy to access array
                        my @realpknames = split /,/, $pkcolsraw, -1;

                        my @find;

                        ## Which row we are currently processing
                        my $numrows = scalar keys %$rows;
                        for my $rownumber (0..$numrows-1) {
                            for my $pknum (0..$numpks-1) {
                                push @find => $realpknames[$pknum], $delkeys[$pknum][$rownumber];
                            }
                        }

                        if ($self->{oldmongo}) {
                            my $res = $Target->{collection}->remove( { '$and' => \@find }, { safe => 1 });
                            $Target->{deleted_rows} += $res->{n};
                        }
                        else {
                            my $res = $Target->{collection}->delete_many( { '$and' => \@find }, { safe => 1 });
                            $Target->{deleted_rows} += $res->{deleted_count};
                        }

                        ## We do not need to loop, as we just went 1 by 1 through the whole list
                        last MONGODEL;

                    }

                    ## Bail out of the loop if we've hit the max
                    last MONGODEL if $top >= $max;

                    ## Assign the bottom of our array slice to be above the current top
                    $bottom = $top + 1;

                    redo MONGODEL;
                }

                $self->glog("Mongo objects removed from $target_tablename: $Target->{deleted_rows}", LOG_VERBOSE);
            }
            elsif ('mysql' eq $type or 'drizzle' eq $type or 'mariadb' eq $type
                       or 'oracle' eq $type or 'sqlite' eq $type or 'firebird' eq $type) {
                my $tdbh = $Target->{dbh};
                for (@{ $SQL{IN}{$target_tablename} }) {
                    $Target->{deleted_rows} += $tdbh->do($_);
                }
            }
            elsif ('redis' eq $type) {
                ## We need to remove the entire tablename:pkey:column for each column we know about
                my $cols = $Table->{cols};
                for my $pk (keys %$rows) {
                    ## If this is a multi-column primary key, change our null delimiter to a colon
                    if ($Table->{numpkcols} > 1) {
                        $pk =~ s{\0}{:}go;
                    }
                    $Target->{deleted_rows} += $Target->{dbh}->del("$target_tablename:$pk");
                }
            }
            elsif ($type =~ /flat/o) { ## same as flatpg for now
                for (@{ $SQL{IN}{$target_tablename} }) {
                    print {$Target->{filehandle}} qq{$_;\n\n};
                }
                $self->glog(qq{Appended to flatfile "$Target->{filename}"}, LOG_VERBOSE);
            }
            else {
                die qq{No support for database type "$type" yet!};
            }

            $Target->{delete_complete} = 1;

            ## Only one target at a time, please: we need to check on the asyncs
            last;

        } ## end async target

        ## If we did nothing this round, and there are no asyncs running, we are done.
        ## Otherwise, we will wait for the oldest async to finish
        if (!$did_something) {
            if (! grep { $_->{async_active} } @$TargetDB) {
                $done = 1;
            }
            else {
                ## Since nothing else is going on, let's wait for the oldest async to finish
                my $Target = ( sort { $a->{async_active} > $b->{async_active} } grep { $_->{async_active} } @$TargetDB)[0];
                if (1 == $numpks) {
                    $Target->{deleted_rows} += $Target->{dbh}->pg_result();
                }
                else {
                    $Target->{dbh}->pg_result();
                }
                $Target->{async_active} = 0;
            }
        }

    } ## end of main deletion loop

    ## Generate our final deletion counts
    my $rows_deleted = 0;

    for my $Target (@$TargetDB) {

        ## We do not delete from certain types of targets
        next if $Target->{dbtype} =~ /mongo|flat|redis/o;

        my $target_tablename = $customname->{$Target->{name}};

        $rows_deleted += $Target->{deleted_rows};
        $self->glog(qq{Rows deleted from $Target->{name}.$target_tablename: $Target->{deleted_rows}}, LOG_VERBOSE);
    }

    return $rows_deleted;

} ## end of delete_rows


sub push_rows {

    ## Copy rows from one table to others
    ## Typically called after delete_rows()
    ## Arguments: six
    ## 1. Hashref of rows to copy, where the keys are the primary keys (\0 joined if multi). Can be empty.
    ## 2. Table object
    ## 3. Sync object (may be empty if we are not associated with a sync)
    ## 4. Source database object
    ## 5. Target database object (or an arrayref of the same)
    ## 6. Action mode - currently only 'copy' and 'fullcopy'
    ## Returns: number of rows copied (to each target, not the total)

    my ($self,$rows,$Table,$Sync,$SourceDB,$TargetDB,$mode) = @_;

    my $SQL;

    ## This will be zero for fullcopy of course
    my $total_rows = keys %$rows;

    if (!$total_rows and $mode ne 'fullcopy') {
        return 0; ## Can happen on a truncation
    }

    my $numpks = $Table->{numpkcols};

    ## If there are a large number of rows (and we are not using ANY) break the statement up
    my $chunksize = $config{statement_chunk_size} || $default_statement_chunk_size;

    ## Build a list of all PK values to feed to IN clauses
    ## This is an array in case we go over $chunksize
    my @pkvals = [];

    ## If there is only one primary key, and a sane number of rows, we can use '= ANY(?)'
    if ($mode ne 'fullcopy') {
        if ($numpks == 1 and $total_rows <= $chunksize) {
            $mode = 'anyclause';
        }
        ## Otherwise, we split up the primary key values into bins
        else {
            my $pk_array_number = 0;
            my $current_row = 1;

            ## Loop through each row and create the needed SQL fragment
            for my $key (keys %$rows) {

                push @{ $pkvals[$pk_array_number] ||= [] } => split '\0', $key, -1;

                ## Make sure our SQL statement doesn't grow too large
                if (++$current_row > $chunksize) {
                    $current_row = 1;
                    $pk_array_number++;
                }
            }
        }
    }

    my $syncname = $Sync->{name} || '';

    ## Make sure TargetDB is an arrayref (may come as a single TargetDB object)
    if (ref $TargetDB ne 'ARRAY') {
        $TargetDB = [$TargetDB];
    }

    ## Figure out the different SELECT clauses, and assign targets to them
    my %srccmd;
    for my $Target (@$TargetDB ) {

        ## The SELECT clause we use (usually an empty string unless customcols is being used)
        my $select_clause = $Table->{newcols}{$syncname}{$Target->{name}} || '';

        ## Associate this target with this clause
        push @{$srccmd{$select_clause}} => $Target;
    }

    ## We may want to change the target table based on the customname table
    ## It is up to the caller to populate these, even if the syncname is ''
    my $customname = $Table->{newname}{$syncname} || {};

     ## Name of the table to copy. Only Postgres can be used as a source
    my $source_tablename = "$Table->{safeschema}.$Table->{safetable}";
    my $sourcedbh = $SourceDB->{dbh};

    ## Actual number of source rows read and copied. May be less than $total_rows
    my $source_rows_read = 0;

    ## Loop through each select command and push it out to all targets that are associated with it
    for my $select_clause (sort keys %srccmd) {

        ## Build the clause (cache) and kick it off
        my $SELECT = $select_clause || 'SELECT *';

        ## Prepare each target that is using this select clause
        for my $Target (@{ $srccmd{$select_clause} }) {

            ## Internal name of this target
            my $targetname = $Target->{name};

            ## The actual target table name. Depends on dbtype and customname table entries
            my $target_tablename = $customname->{$targetname};

            ## The columns we are pushing to, both as an arrayref and a CSV:
            my $cols = $Table->{tcolumns}{$SELECT};
            my $columnlist = $Target->{does_sql} ?
                ('(' . (join ',', map { $Target->{dbh}->quote_identifier($_) } @$cols) . ')')
              : ('(' . (join ',', map { $_ } @$cols) . ')');

            my $type = $Target->{dbtype};

            ## Using columnlist avoids worrying about the order of columns

            if ('postgres' eq $type) {
                my $tgtcmd = "$self->{sqlprefix}COPY $target_tablename$columnlist FROM STDIN";
                $Target->{dbh}->do($tgtcmd);
            }
            elsif ('firebird' eq $type) {
                $columnlist =~ s/\"//g;
                $target_tablename = qq{"$target_tablename"} if $target_tablename !~ /"/;
                my $tgtcmd = "INSERT INTO $target_tablename$columnlist VALUES (";
                $tgtcmd .= '?,' x @$cols;
                $tgtcmd =~ s/,$/)/o;
                $Target->{sth} = $Target->{dbh}->prepare($tgtcmd);
            }
            elsif ('flatpg' eq $type) {
                print {$Target->{filehandle}} "COPY $target_tablename$columnlist FROM STDIN;\n";
            }
            elsif ('flatsql' eq $type) {
                print {$Target->{filehandle}} "INSERT INTO $target_tablename$columnlist VALUES\n";
            }
            elsif ('mongo' eq $type) {
            }
            elsif ('redis' eq $type) {
                ## No setup needed
            }
            elsif ('sqlite' eq $type or 'oracle' eq $type or
                   'mysql' eq $type or 'mariadb' eq $type or 'drizzle' eq $type) {
                my $tgtcmd = "INSERT INTO $target_tablename$columnlist VALUES (";
                $tgtcmd .= '?,' x @$cols;
                $tgtcmd =~ s/,$/)/o;
                $Target->{sth} = $Target->{dbh}->prepare($tgtcmd);
            }
            else {
                die qq{No support for database type "$type" yet!};
            }

            if ($type =~ /flat/) {
                $self->glog(qq{Appended to flatfile "$Target->{filename}"}, LOG_VERBOSE);
            }

        } ## end preparing each target for this select clause

        my $loop = 1;
        my $number_chunks = @pkvals;

        ## Loop through each chunk of primary keys to copy over
        for my $pk_values (@pkvals) {

            ## Start streaming rows from the source
            my $pre = $number_chunks > 1 ? "/* $loop of $number_chunks */ " : '';
            $self->glog(qq{${pre}Copying from $SourceDB->{name}.$source_tablename}, LOG_VERBOSE);

            ## If we are doing a small batch of single primary keys, use ANY
            ## For a fullcopy mode, leave the WHERE clause out completely
            if ($mode eq 'fullcopy' or $mode eq 'anyclause') {
                my $srccmd = sprintf '%sCOPY (%s FROM ONLY %s %s) TO STDOUT%s',
                    $self->{sqlprefix},
                    $SELECT,
                    $source_tablename,
                    $mode eq 'fullcopy' ? '' : " WHERE $Table->{pklist} = ANY(?)",
                    $Sync->{copyextra} ? " $Sync->{copyextra}" : '';

                my $srcsth = $sourcedbh->prepare($srccmd);
                $mode eq 'fullcopy' ? $srcsth->execute() : $srcsth->execute( [ keys %$rows ]);
            }
            else {
                ## Create the proper number of placeholders
                my $baseq = '?';
                if ($numpks > 1) {
                    $baseq = '?,' x $numpks;
                    $baseq =~ s/(.+?).$/\($1\)/;
                }
                my $number_values = @$pk_values;
                my $placeholders = "$baseq," x ($number_values / $numpks);
                chop $placeholders;

                my $srccmd = sprintf '%s%sCOPY (%s FROM ONLY %s WHERE %s IN (%s)) TO STDOUT%s',
                    $pre,
                    $self->{sqlprefix},
                    $SELECT,
                    $source_tablename,
                    $Table->{pkeycols},
                    $placeholders,
                    $Sync->{copyextra} ? " $Sync->{copyextra}" : '';

                my $srcsth = $sourcedbh->prepare($srccmd);
                $srcsth->execute( @$pk_values );
            }

            ## Loop through each row output from the source, storing it in $buffer
            ## Future optimization: slurp in X rows at a time, then process them
            my $buffer = '';
            while ($sourcedbh->pg_getcopydata($buffer) >= 0) {

                $source_rows_read++;

                ## For each target using this particular SELECT clause
                for my $Target (@{ $srccmd{$select_clause} }) {

                    my $type = $Target->{dbtype};

                    ## For Postgres, we simply do COPY to COPY
                    if ('postgres' eq $type) {
                        $Target->{dbh}->pg_putcopydata($buffer);
                    }
                    ## For flat files destined for Postgres, just do a tab-delimited dump
                    elsif ('flatpg' eq $type) {
                        print {$Target->{filehandle}} $buffer;
                    }
                    ## For other flat files, make a standard VALUES list
                    elsif ('flatsql' eq $type) {
                        chomp $buffer;
                        if ($source_rows_read > 1) {
                            print {$Target->{filehandle}} ",\n";
                        }
                        print {$Target->{filehandle}} '(' .
                             (join ',' => map { $self->{masterdbh}->quote($_) } split /\t/, $buffer, -1) . ')';
                    }
                    ## For Mongo, do some mongomagic
                    elsif ('mongo' eq $type) {

                        ## The actual target name
                        my $target_tablename = $customname->{$Target->{name}};
                        $Target->{collection} = $Target->{dbh}->get_collection($target_tablename);

                        ## Have to map these values back to their names
                        chomp $buffer;
                        my @cols = map { $_ = undef if $_ eq '\\N'; $_; } split /\t/, $buffer, -1;

                        my $targetcols = $Table->{tcolumns}{$SELECT};

                        ## Our object consists of the primary keys, plus all other fields
                        my $object = {};
                        for my $cname (@{ $targetcols }) {
                            $object->{$cname} = shift @cols;
                        }
                        ## Coerce non-strings into different objects
                        for my $key (keys %$object) {
                            ## Since mongo is schemaless, don't set null columns in the mongo doc
                            if (!defined($object->{$key})) {
                                delete $object->{$key};
                            }
                            elsif ($Table->{columnhash}{$key}{ftype} =~ /smallint|integer|bigint/o) {
                                $object->{$key} = int $object->{$key};
                            }
                            elsif ($Table->{columnhash}{$key}{ftype} eq 'boolean') {
                                if (defined $object->{$key}) {
                                    $object->{$key} = $object->{$key} eq 't' ? boolean->true : boolean->false;
                                }
                            }
                            elsif ($Table->{columnhash}{$key}{ftype} =~ /real|double|numeric/o) {
                                $object->{$key} = strtod($object->{$key});
                            }
                            elsif ($Table->{columnhash}{$key}{ftype} =~ /timestamp with time zone|date|abstime/o) {
                                $object->{$key} = DateTime->from_epoch(epoch => str2time($object->{$key}));
                            }
                        }
                        $self->{oldmongo} ? 
                            $Target->{collection}->insert($object, { safe => 1 }) :
                                $Target->{collection}->insert_one($object, { safe => 1 });
                    }
                    elsif ('redis' eq $type) {

                        ## We are going to set a Redis hash, in which the key is "tablename:pkeyvalue"
                        chomp $buffer;
                        my @colvals = map { $_ = undef if $_ eq '\\N'; $_; } split /\t/, $buffer, -1;
                        my @pkey;
                        for (1 .. $Table->{numpkcols}) {
                            push @pkey => shift @colvals;
                        }
                        my $pkeyval = join ':' => @pkey;
                        ## Build a list of non-null key/value pairs to set in the hash
                        my @add;
                        $i = $Table->{numpkcols} - 1;
                        my $targetcols = $Table->{tcolumns}{$SELECT};
                        for my $val (@colvals) {
                            $i++;
                            next if ! defined $val;
                            push @add, $targetcols->[$i], $val;
                        }

                        my $target_tablename = $customname->{$Target->{name}};
                        $Target->{dbh}->hmset("$target_tablename:$pkeyval", @add);
                    }
                    ## For SQLite, MySQL, MariaDB, Firebird, Drizzle, and Oracle, do some basic INSERTs
                    elsif ('sqlite' eq $type
                            or 'oracle' eq $type
                            or 'mysql' eq $type
                            or 'mariadb' eq $type
                            or 'drizzle' eq $type
                            or 'firebird' eq $type) {

                        chomp $buffer;
                        my @cols = map { $_ = undef if $_ eq '\\N'; $_; } split /\t/, $buffer, -1;
                        my $targetcols = $Table->{tcolumns}{$SELECT};
                        for my $cindex (0..@cols) {
                            next unless defined $cols[$cindex];
                            if ($Table->{columnhash}{$targetcols->[$cindex]}{ftype} eq 'boolean') {
                                # BOOLEAN support is inconsistent, but almost everyone will coerce 1/0 to TRUE/FALSE
                                $cols[$cindex] = ( $cols[$cindex] =~ /^[1ty]/i )? 1 : 0;
                            }
                        }
                        $Target->{sth}->execute(@cols);
                    }
                    ## Safety valve:
                    else {
                        die qq{No support for database type "$type" yet!};
                    }

                } ## end each target

            } ## end each row pulled from the source

            $loop++;

        } ## end each chunk of primary keys

        ## Workaround for DBD::Pg bug
        ## Once we require a minimum version of 2.18.1 or better, we can remove this!
        if ($SourceDB->{dbtype} eq 'postgres' and $self->{dbdpgversion} < 21801) {
            $sourcedbh->do('SELECT 1');
        }

        ## Perform final cleanups for each target
        for my $Target (@{ $srccmd{$select_clause} }) {

            my $target_tablename = $customname->{$Target->{name}};

            my $type = $Target->{dbtype};

            my $tname = $Target->{name};

            $self->glog(qq{Rows copied to ($type) $tname.$target_tablename: $source_rows_read}, LOG_VERBOSE);

            if ('postgres' eq $type) {
                my $dbh = $Target->{dbh};
                $dbh->pg_putcopyend();
                ## Same bug as above
                if ($self->{dbdpgversion} < 21801) {
                    $dbh->do('SELECT 1');
                }
                ## If this table is set to makedelta, add rows to bucardo.delta to simulate the
                ##   normal action of a trigger and add a row to bucardo.track to indicate that
                ##   it has already been replicated here.
                my $d = $Sync->{db}{$tname};
                if ($mode ne 'fullcopy' and $d->{does_makedelta}{$source_tablename} ) {

                    $self->glog("Using makedelta to populate delta and track tables for $tname.$target_tablename", LOG_VERBOSE);

                    my $cols = join ',' => @{ $Table->{qpkey} };

                    ## We use the original list, not what may have actually got copied!
                    for my $pk_values (@pkvals) {

                        ## Generate the correct number of placeholders
                        my $baseq = '?';
                        if ($numpks > 1) {
                            $baseq = '?,' x $numpks;
                            chop $baseq;
                        }
                        my $number_values = $mode eq 'copy' ? @$pk_values : keys %$rows;
                        my $placeholders = "($baseq)," x ($number_values / $numpks);
                        chop $placeholders;

                        my $SQL = sprintf 'INSERT INTO bucardo.%s (%s) VALUES %s',
                            $Table->{deltatable},
                            $cols,
                            $placeholders;

                        my $sth = $dbh->prepare($SQL);
                        $sth->execute($mode eq 'copy' ? @$pk_values : (keys %$rows));
                    }

                    # Make sure we track it - but only if this sync already acts as a source!
                    if ($Target->{role} eq 'source') {
                        $dbh->do(qq{
                            INSERT INTO bucardo.$Table->{tracktable}
                            VALUES (NOW(), ?)
                        }, undef, $d->{DBGROUPNAME});
                    }

                    ## We want to send a kick signal to other syncs that are using this table
                    ## However, we do not want to kick unless they are set to autokick and active
                    ## This works even if we do not have a real syncs, as $syncname will be ''
                    $self->glog('Signalling other syncs that this table has changed', LOG_DEBUG);
                    if (! exists $self->{kick_othersyncs}{$syncname}{$tname}{$target_tablename}) {
                        $SQL = 'SELECT name FROM sync WHERE herd IN (SELECT herd FROM herdmap WHERE goat IN (SELECT id FROM goat WHERE schemaname=? AND tablename = ?)) AND name <> ? AND autokick AND status = ?';
                        $sth = $self->{masterdbh}->prepare($SQL);
                        $sth->execute($Table->{schemaname}, $Table->{tablename}, $syncname, 'active');
                        $self->{kick_othersyncs}{$syncname}{$tname}{$target_tablename} = $sth->fetchall_arrayref();
                    }
                    ## For each sync returned from the query above, send a kick request
                    for my $row (@{ $self->{kick_othersyncs}{$syncname}{$tname}{$target_tablename} }) {
                        my $othersync = $row->[0];
                        $self->db_notify($dbh, "kick_sync_$othersync", 0, '', 1);
                    }
                }
            }
            elsif ('flatpg' eq $type) {
                print {$Target->{filehandle}} "\\\.\n\n";
            }
            elsif ('flatsql' eq $type) {
                print {$Target->{filehandle}} ";\n\n";
            }
            else {
                ## Nothing to be done for mongo, mysql, mariadb, sqlite, oracle, firebird, redis
            }

        } ## end each Target

    } ## end of each clause in the source command list

    return $source_rows_read;

} ## end of push_rows


sub vacuum_table {

    ## Compact and/or optimize the table in the target database
    ## Argument: five
    ## 1. Starting time for the kid, so we can output cumulative times
    ## 2. Database type
    ## 3. Database handle
    ## 4. Database name
    ## 5. Table name (may be in schema.table format)
    ## Returns: undef

    my ($self, $start_time, $dbtype, $ldbh, $dbname, $tablename) = @_;

    ## XXX Return output from vacuum/optimize as a LOG_VERBOSE or LOG_DEBUG?

    if ('postgres' eq $dbtype) {
        ## Do a normal vacuum of the table
        $ldbh->commit();
        $ldbh->{AutoCommit} = 1;
        $self->glog("Vacuuming $dbname.$tablename", LOG_VERBOSE);
        $ldbh->do("VACUUM $tablename");
        $ldbh->{AutoCommit} = 0;

        my $total_time = sprintf '%.2f', tv_interval($start_time);
        $self->glog("Vacuum complete. Time: $total_time", LOG_VERBOSE);
    }
    elsif ('mysql' eq $dbtype or 'drizzle' eq $dbtype or 'mariadb' eq $dbtype) {
        ## Optimize the table
        $self->glog("Optimizing $tablename", LOG_VERBOSE);

        $ldbh->do("OPTIMIZE TABLE $tablename");
        $ldbh->commit();

        my $total_time = sprintf '%.2f', tv_interval($start_time);
        $self->glog("Optimization complete. Time: $total_time", LOG_VERBOSE);
    }
    elsif ('sqlite' eq $dbtype) {
        # Note the SQLite command vacuums the entire database.
        # Should probably avoid multi-vacuuming if several tables have changed.
        $self->glog('Vacuuming the database', LOG_VERBOSE);
        $ldbh->do('VACUUM');

        my $total_time = sprintf '%.2f', tv_interval($start_time);
        $self->glog("Vacuum complete. Time: $total_time", LOG_VERBOSE);
    }
    elsif ('redis' eq $dbtype) {
        # Nothing to do, really
    }
    elsif ('mongodb' eq $dbtype) {
        # Use db.repairDatabase() ?
    }
    else {
        ## Do nothing!
    }

    return;

} ## end of vacuum_table


sub analyze_table {

    ## Update table statistics in the target database
    ## Argument: five
    ## 1. Starting time for the kid, so we can output cumulative times
    ## 2. Database type
    ## 3. Database handle
    ## 4. Database name
    ## 5. Table name (may be in schema.table format)
    ## Returns: undef

    my ($self, $start_time, $dbtype, $ldbh, $dbname, $tablename) = @_;

    ## XXX Return output from analyze as a LOG_VERBOSE or LOG_DEBUG?

    if ('postgres' eq $dbtype) {
        $ldbh->do("ANALYZE $tablename");
        my $total_time = sprintf '%.2f', tv_interval($start_time);
        $self->glog("Analyze complete for $dbname.$tablename. Time: $total_time", LOG_VERBOSE);
        $ldbh->commit();
    }
    elsif ('sqlite' eq $dbtype) {
        $ldbh->do("ANALYZE $tablename");
        my $total_time = sprintf '%.2f', tv_interval($start_time);
        $self->glog("Analyze complete for $dbname.$tablename. Time: $total_time", LOG_VERBOSE);
        $ldbh->commit();
    }
    elsif ('mysql' eq $dbtype or 'drizzle' eq $dbtype or 'mariadb' eq $dbtype) {
        $ldbh->do("ANALYZE TABLE $tablename");
        my $total_time = sprintf '%.2f', tv_interval($start_time);
        $self->glog("Analyze complete for $tablename. Time: $total_time", LOG_VERBOSE);
        $ldbh->commit();
    }
    else {
        ## Nothing to do here
    }

    return undef;

} ## end of analyze_table


sub msg { ## no critic

    my $name = shift || '?';

    my $msg = '';

    if (exists $msg{$lang}{$name}) {
        $msg = $msg{$lang}{$name};
    }
    elsif (exists $msg{'en'}{$name}) {
        $msg = $msg{'en'}{$name};
    }
    else {
        my $line = (caller)[2];
        die qq{Invalid message "$name" from line $line\n};
    }

    $i = 1;
    {
        my $val = $_[$i-1];
        $val = '?' if ! defined $val;
        last unless $msg =~ s/\$$i/$val/g;
        $i++;
        redo;
    }
    return $msg;

} ## end of msg


sub pretty_time {

    ## Transform number of seconds to a more human-readable format
    ## First argument is number of seconds
    ## Second optional arg is highest transform: s,m,h,d,w
    ## If uppercase, it indicates to "round that one out"

    my $sec = shift;
    my $tweak = shift || '';

    ## Round to two decimal places, then trim the rest
    $sec = sprintf '%.2f', $sec;
    $sec =~ s/0+$//o;
    $sec =~ s/\.$//o;

    ## Just seconds (< 2:00)
    if ($sec < 120 or $tweak =~ /s/) {
        return sprintf "$sec %s", $sec==1 ? msg('time-second') : msg('time-seconds');
    }

    ## Minutes and seconds (< 60:00)
    if ($sec < 60*60 or $tweak =~ /m/) {
        my $min = int $sec / 60;
        $sec %= 60;
        $sec = int $sec;
        my $ret = sprintf "$min %s", $min==1 ? msg('time-minute') : msg('time-minutes');
        $sec and $tweak !~ /S/ and $ret .= sprintf " $sec %s", $sec==1 ? msg('time-second') : msg('time-seconds');
        return $ret;
    }

    ## Hours, minutes, and seconds (< 48:00:00)
    if ($sec < 60*60*24*2 or $tweak =~ /h/) {
        my $hour = int $sec / (60*60);
        $sec -= ($hour*60*60);
        my $min = int $sec / 60;
        $sec -= ($min*60);
        $sec = int $sec;
        my $ret = sprintf "$hour %s", $hour==1 ? msg('time-hour') : msg('time-hours');
        $min and $tweak !~ /M/ and $ret .= sprintf " $min %s", $min==1 ? msg('time-minute') : msg('time-minutes');
        $sec and $tweak !~ /[SM]/ and $ret .= sprintf " $sec %s", $sec==1 ? msg('time-second') : msg('time-seconds');
        return $ret;
    }

    ## Days, hours, minutes, and seconds (< 28 days)
    if ($sec < 60*60*24*28 or $tweak =~ /d/) {
        my $day = int $sec / (60*60*24);
        $sec -= ($day*60*60*24);
        my $our = int $sec / (60*60);
        $sec -= ($our*60*60);
        my $min = int $sec / 60;
        $sec -= ($min*60);
        $sec = int $sec;
        my $ret = sprintf "$day %s", $day==1 ? msg('time-day') : msg('time-days');
        $our and $tweak !~ /H/     and $ret .= sprintf " $our %s", $our==1 ? msg('time-hour')   : msg('time-hours');
        $min and $tweak !~ /[HM]/  and $ret .= sprintf " $min %s", $min==1 ? msg('time-minute') : msg('time-minutes');
        $sec and $tweak !~ /[HMS]/ and $ret .= sprintf " $sec %s", $sec==1 ? msg('time-second') : msg('time-seconds');
        return $ret;
    }

    ## Weeks, days, hours, minutes, and seconds (< 28 days)
    my $week = int $sec / (60*60*24*7);
    $sec -= ($week*60*60*24*7);
    my $day = int $sec / (60*60*24);
    $sec -= ($day*60*60*24);
    my $our = int $sec / (60*60);
    $sec -= ($our*60*60);
    my $min = int $sec / 60;
    $sec -= ($min*60);
    $sec = int $sec;
    my $ret = sprintf "$week %s", $week==1 ? msg('time-week') : msg('time-weeks');
    $day and $tweak !~ /D/      and $ret .= sprintf " $day %s", $day==1 ? msg('time-day')    : msg('time-days');
    $our and $tweak !~ /[DH]/   and $ret .= sprintf " $our %s", $our==1 ? msg('time-hour')   : msg('time-hours');
    $min and $tweak !~ /[DHM]/  and $ret .= sprintf " $min %s", $min==1 ? msg('time-minute') : msg('time-minutes');
    $sec and $tweak !~ /[DHMS]/ and $ret .= sprintf " $sec %s", $sec==1 ? msg('time-second') : msg('time-seconds');

    return $ret;

} ## end of pretty_time


sub send_mail {

    ## Send out an email message
    ## Arguments: one
    ## 1. Hashref with mandatory args 'body' and 'subject'. Optional 'to'
    ## Returns: undef

    my $self = shift;

    ## Return right away if sendmail and sendmail_file are false
    return if ! $self->{sendmail} and ! $self->{sendmail_file};

    ## Hashref of args
    my $arg = shift;

    ## If 'default_email_from' is not set, we default to currentuser@currenthost
    my $from = $config{default_email_from} || (getpwuid($>) . '@' . $hostname);

    ## Who is the email going to? We usually use the default.
    $arg->{to} ||= $config{default_email_to};

    ## We should always pass in a subject, but just in case:
    $arg->{subject} ||= 'Bucardo Mail!';

    ## Like any good murder mystery, a body is mandatory
    if (! $arg->{body}) {
        $self->glog('Warning: Cannot send mail, no body message', LOG_WARN);
        return;
    }

    ## Where do we connect to?
    my $smtphost = $config{default_email_host} || 'localhost';
    my $smtpport = $config{default_email_port} || 25;

    ## Send normal email
    ## Do not send it if the 'example.com' default value is still in place
    if ($self->{sendmail} and $arg->{to} ne 'nobody@example.com') {
        ## Wrap the whole call in an eval so we can report errors
        my $evalworked = 0;
        eval {
            my $smtp = Net::SMTP->new(
                Host    => $smtphost,
                Port    => $smtpport,
                Hello   => $hostname,
                Timeout => 15
                );

            if ($config{email_auth_user} and $config{email_auth_pass}) {
                ## Requires Authen::SASL
                my ($auser,$apass) = ($config{email_auth_user}, $config{email_auth_pass});
                $self->glog("Attempting Net::SMTP::auth with user $auser", LOG_DEBUG);
                $smtp->auth($auser, $apass);
            }

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
            $evalworked = 1;
        };
        if (! $evalworked) {
            my $error = $@ || '???';
            $self->glog("Warning: Error sending email to $arg->{to}: $error", LOG_WARN);
        }
        else {
            $self->glog("Sent an email to $arg->{to} from $from: $arg->{subject}", LOG_NORMAL);
        }
    }

    ## Write the mail to a file
    if ($self->{sendmail_file}) {
        my $fh;
        ## This happens rare enough to not worry about caching the file handle
        if (! open $fh, '>>', $self->{sendmail_file}) {
            $self->glog(qq{Warning: Could not open sendmail file "$self->{sendmail_file}": $!}, LOG_WARN);
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

This document describes version 5.6.0 of Bucardo

=head1 WEBSITE

The latest news and documentation can always be found at:

https://bucardo.org/

=head1 DESCRIPTION

Bucardo is a Perl module that replicates Postgres databases using a combination
of Perl, a custom database schema, Pl/Perlu, and Pl/Pgsql.

Bucardo is unapologetically extremely verbose in its logging.

Full documentation can be found on the website, or in the files that came with
this distribution. See also the documentation for the bucardo program.

=head1 DEPENDENCIES

=over

=item * DBI (1.51 or better)

=item * DBD::Pg (2.0.0 or better)

=item * Sys::Hostname

=item * Sys::Syslog

=item * DBIx::Safe ## Try 'yum install perl-DBIx-Safe' or visit bucardo.org

=item * boolean (only if using MongoDB)

=back

=head1 BUGS

Bugs should be reported to bucardo-general@bucardo.org. A list of bugs can be found at
https://bucardo.org/bugs.html

=head1 CREDITS

Bucardo was originally developed and funded by Backcountry.com, who have been using versions
of it in production since 2002. Jon Jensen <jon@endpoint.com> wrote the original version.

=head1 AUTHOR

Greg Sabino Mullane <greg@turnstep.com>

=head1 LICENSE AND COPYRIGHT

Copyright (c) 2005-2020 Greg Sabino Mullane <greg@turnstep.com>.

This software is free to use: see the LICENSE file for details.

=cut
