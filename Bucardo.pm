#!/usr/local/bin/perl -- -*-cperl-*-

## The main Bucardo program
##
## Copyright 2006-2008 Greg Sabino Mullane <greg@endpoint.com>

package Bucardo;
use 5.008003;
use strict;
use warnings;

our $VERSION = '3.1.0';

## Begin Moose classes
{
package BCdatabase; ## "db" table
use Moose;

my @req1 = (qw(name dbname dbuser));
my @req2 = (qw());
my @opt1 = (qw(dbhost dbpass dbconn pgpass status));
my @opt2 = (qw(dbport synclimit));
for my $req (@req1) { has $req => ( is => 'rw', isa => 'Str', required => 1 ); }
for my $req (@req2) { has $req => ( is => 'rw', isa => 'Int', required => 1 ); }
for my $opt (@opt1) { has $opt => ( is => 'rw', isa => 'Str', required => 0 ); }
for my $opt (@opt2) { has $opt => ( is => 'rw', isa => 'Int', required => 0 ); }
has 'id' => ( is => 'ro', isa => 'Int' );
has 'cols' => ( is => 'ro', isa => 'ArrayRef', default => sub { [@req1,@req2,@opt1,@opt2] },);
}

{
package BCdbgroup;
use Moose;

my @req1 = (qw(name db));
my @req2 = (qw());
for my $req (@req1) { has $req => ( is => 'rw', isa => 'Str', required => 1 ); }
for my $req (@req2) { has $req => ( is => 'rw', isa => 'Int', required => 1 ); }
has 'id' => ( is => 'ro', isa => 'Int' );
has 'cols' => ( is => 'ro', isa => 'ArrayRef', default => sub { [@req1,@req2] },);
}

{
package BCgoat;
use Moose;

my @req1 =  (qw(tablename db));
my @req2 =  (qw());
my @opt1 =  (qw(schemaname has_delta pkey ghost customselect));
push @opt1, (qw(standard_conflict pkeytype ping analyze_after_copy makedelta rebuild_index));
my @opt2 =  (qw());
for my $req (@req1) { has $req => ( is => 'rw', isa => 'Str', required => 1 ); }
for my $req (@req2) { has $req => ( is => 'rw', isa => 'Int', required => 1 ); }
for my $opt (@opt1) { has $opt => ( is => 'rw', isa => 'Str', required => 0 ); }
for my $opt (@opt2) { has $opt => ( is => 'rw', isa => 'Int', required => 0 ); }
has 'id' => ( is => 'ro', isa => 'Int' );
has 'cols' => ( is => 'ro', isa => 'ArrayRef', default => sub { [@req1,@req2,@opt1,@opt2] },);
}

{
package BCherd;
use Moose;

has 'goat'     => ( is => 'rw', isa => 'Any', required => 1 );
has 'name'     => ( is => 'rw', isa => 'Str', required => 1 );
has 'priority' => ( is => 'rw', isa => 'Int', required => 0 );

}

{
package BCsync;
use Moose;

my @req1 =  (qw(name source));
my @req2 =  (qw());
my @opt1 =  (qw(synctype copytype deletemethod copyrows copyextra stayalive              ));
push @opt1, (qw(checktime status limitdbs precommand postcommand txnmode ping            ));
push @opt1, (qw(targetdb targetgroup kidtime kidsalive disable_triggers do_listen        ));
push @opt1, (qw(usecustomselect makedelta rebuild_index disable_rules analyze_after_copy ));
my @opt2 =  (qw());
for my $req (@req1) { has $req => ( is => 'rw', isa => 'Str', required => 1 ); }
for my $req (@req2) { has $req => ( is => 'rw', isa => 'Int', required => 1 ); }
for my $opt (@opt1) { has $opt => ( is => 'rw', isa => 'Str', required => 0 ); }
for my $opt (@opt2) { has $opt => ( is => 'rw', isa => 'Int', required => 0 ); }
has 'cols' => ( is => 'ro', isa => 'ArrayRef', default => sub { [@req1,@req2,@opt1,@opt2] },);
## These should not be set by us:
for my $opt (qw(id priority)) {
  has $opt => ( is => 'ro', isa => 'Str', required => 0 );
}
}

{
package BCcustomcode;
use Moose;

my @req1 =  (qw(name whenrun src_code));
my @req2 =  (qw());
my @opt1 =  (qw(about getdbh getrows ));
push @opt1, (qw(goat sync active priority ));
my @opt2 =  (qw());
for my $req (@req1) { has $req => ( is => 'rw', isa => 'Str', required => 1 ); }
for my $req (@req2) { has $req => ( is => 'rw', isa => 'Int', required => 1 ); }
for my $opt (@opt1) { has $opt => ( is => 'rw', isa => 'Str', required => 0 ); }
for my $opt (@opt2) { has $opt => ( is => 'rw', isa => 'Int', required => 0 ); }
has 'cols' => ( is => 'ro', isa => 'ArrayRef', default => sub { [@req1,@req2,@opt1,@opt2] },);
## These should not be set by us:
for my $opt (qw(id priority)) {
  has $opt => ( is => 'ro', isa => 'Str', required => 0 );
}

}

## Moose is a little messy, so we need this for now.
## Known problem: future versions of Moose will fix this.
$SIG{__DIE__} = sub {
	my $line = (caller)[2];
	(my $err = shift) =~ s{(.+?) at /\S+/Meta/Attribute.+}{$1}so;
	##Temporary extra:
	$err =~ s/(.+?)\n.+/$1/so if $err =~ /^\w/ and $err !~ /\$VAR1/o;
	die "Line $line: $err\n";
};



## Begin main Bucardo object

package Bucardo;

use sigtrap qw( die normal-signals );
use Moose;
use Moose::Util::TypeConstraints;
use Time::HiRes 'sleep';
use DBI;
use DBD::Pg ':pg_types';
my $DEFAULT = $DBD::Pg::DBDPG_DEFAULT;
use Mail::Sendmail;
use Sys::Hostname;
use IO::Handle;
use Data::Dumper;
$Data::Dumper::Varname = 'BUCARDO';
$Data::Dumper::Indent = 1;
use Getopt::Long;
use vars qw($SQL %SQL $sth %sth $bdb $count $info);

use Sys::Syslog;

use DBIx::Safe '1.2.4';

*STDOUT->autoflush(1);
*STDERR->autoflush(1);

my $DEBUG     = 0;
my $VERBOSE   = 1;
my $QUIET     = 0;
my $HELP      = 0;

## Where our debug files live
my $DEBUGDIR = './tmp';

## Specify exactly what database handles are allowed to do within custom code

## Strict: inside the main txn
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

## This needs to be fixed in DBD::Pg!!
my $MAXCOPYBUF = 100_000;

## Optional cleanup for the pidfiles
## The string PIDFILE will be replaced with the actual name
my $PIDCLEANUP = "/bin/chgrp bucardo PIDFILE";
$PIDCLEANUP = '';

## Save a copy of emails to a file? (override with $ENV{BUCARDO_SENDMAIL_FILE})
my $SENDMAIL_FILE = ""; ## "./bucardo.sendmail.log";

## How long to sleep when adding back an aborted sync?
my $KIDABORTSLEEP = 1.0;

my $hostname = hostname;
my $shorthost = $hostname;
$shorthost =~ s/^(.+?)\..*/$1/;

our %config;
our %config_about;

##
## BEGIN MOOSENESS
##

has 'created'      => ( is => 'ro', isa => 'Str', default => scalar localtime );
has 'ppid'         => ( is => 'ro', isa => 'Int', default => $$ );
has 'verbose'      => ( is => 'rw', isa => 'Int', default => 0 );

has 'debugsyslog'  => ( is => 'rw', isa => 'Int', default => 1 );
has 'debugdir'     => ( is => 'rw', isa => 'Str', default => './tmp' );
has 'debugfile'    => ( is => 'rw', isa => 'Int', default => 0 );
has 'debugfilesep' => ( is => 'rw', isa => 'Int', default => 0 );
has 'debugname'    => ( is => 'rw', isa => 'Str', default => '' );
has 'cleandebugs'  => ( is => 'rw', isa => 'Int', default => 0 );
has 'debugstderr'  => ( is => 'rw', isa => 'Int', default => 0 );
has 'debugstdout'  => ( is => 'rw', isa => 'Int', default => 0 );
has 'dryrun'       => ( is => 'rw', isa => 'Int', default => 0 );
has 'bcquiet'      => ( is => 'rw', isa => 'Int', default => 0 );
has 'sendmail'     => ( is => 'rw', isa => 'Int', default => 1 );
has 'extraname'    => ( is => 'rw', isa => 'Str', default => '' );

has 'masterdbh'    => ( is => 'ro' );


sub BUILD {
	my ($self, $params) = @_;

	if ($self->{debugdir}) {
		$DEBUGDIR = $self->{debugdir};
	}

	if ($self->{cleandebugs}) {
		system(qq{/bin/rm -f $DEBUGDIR/log.bucardo.*});
	}
	$self->{logprefix} = "BC! ";
	$SIG{CHLD} = 'IGNORE'; ## Zombie stopper

	if (exists $ENV{BUCARDO_DRYRUN}) {
		$self->{dryrun} = 1;
		print STDERR "** DRYRUN - Syncs will not be commited! **\n";
	}

	if ($self->{extraname}) {
		$self->{extraname} = " ($self->{extraname})";
	}

	$self->{masterdbh} = $self->connect_database();

	## Load in the configuration information
	$self->reload_config_database();

	if ($self->{debugsyslog}) {
		openlog 'Bucardo', "pid nowait", $config{syslog_facility};
	}

	$self->{pidfile} = "$config{piddir}/$config{pidfile}";
	$self->{stopfile} = "$config{piddir}/$config{stopfile}";

	return;
}


has 'dbname' => ( is => 'rw', isa => 'Str', required => 1 );
has 'dbuser' => ( is => 'rw', isa => 'Str', required => 1 );

has 'dbpass' => ( is => 'rw', isa => 'Str', required => 0, default => '' );
has 'dbhost' => ( is => 'rw', isa => 'Str', required => 0, default => '' );
has 'dbport' => ( is => 'rw', isa => 'Str', required => 0, default => '' );
has 'dbconn' => ( is => 'rw', isa => 'Str', required => 0, default => '' );

has 'database' => ( is => 'rw', isa => 'HashRef' );
after 'database' => sub {
	my ($self,$arg) = @_;
	return if ! defined $arg;
	my $db = BCdatabase->new($arg);
	$self->{database}{id} = $self->addtable($db, "db");
	## We may want to assign it immediately to a group
	if (exists $arg->{dbgroup}) {
		$self->dbgroup({db => $arg->{name}, name => $arg->{dbgroup}, priority => $arg->{priority}||0 });
	}
	return;
};

has 'dbgroup' => ( is => 'rw', isa => 'HashRef' );
after 'dbgroup' => sub {
	my ($self,$arg) = @_;
	return if ! defined $arg;
	my $group = BCdbgroup->new($arg);
	$self->make_dbgroup($arg->{name});
	## If we also got a database, add it to the group
	if (defined $arg->{db}) {
		## Does it exist?
		my $db = $self->get_db($arg->{db});
		my $maindbh = $self->{masterdbh};
		my $pri = $arg->{priority} || 0;
		$arg->{db} =~ s/\'/''/go;
		$arg->{name} =~ s/\'/''/go;
		$SQL = qq{INSERT INTO bucardo.dbmap (db, dbgroup, priority)}.
			qq{ VALUES ('$arg->{db}','$arg->{name}', $pri)};
		$maindbh->do($SQL);
		$maindbh->commit();
	}
	return;
};

has 'goat' => ( is => 'rw', isa => 'HashRef' );
after 'goat' => sub {
	my ($self,$arg) = @_;
	return if ! defined $arg;
	my $goat = BCgoat->new($arg);
	my $goatid = $self->addtable($goat, "goat")
		or die "No id returned from addtable inside 'goat'\n";
	return unless $arg->{herd};

	## Add this goat to a herd
	$arg->{name} = $arg->{herd};
	$arg->{goat} = $goatid;
	$self->herd($arg);
	return;
};



sub addtable {
	my ($self,$arg,$type) = @_;
	return if ! defined $arg or ! ref $arg;
	my @vals = grep { defined $arg->{$_} } @{$arg->cols};
	my $flatargs = join "," => @vals;
	my $qs = join "," => map { '?' } @vals;
	my $maindbh = $self->{masterdbh};
	$SQL = "INSERT INTO bucardo.$type($flatargs) VALUES ($qs)";
	$sth = $maindbh->prepare($SQL) or die "Could not add a table: $!";
	my $args = [map { $arg->{$_} } @vals];
	$sth->execute(@$args);
	$maindbh->commit();
	return if $type ne 'goat';
	return $self->{$type}{id} = $maindbh->selectall_arrayref("SELECT pg_catalog.lastval()")->[0][0];
}

has 'herd' => ( is => 'rw', isa => 'HashRef' );
after 'herd' => sub {
	my ($self,$arg) = @_;
	return if ! defined $arg;
	my $herd = BCherd->new($arg);
	my $goatid = $self->find_goat($herd->goat,$arg->{db});
	$self->make_herd($herd->name);
	my $maindbh = $self->{masterdbh};
	my $pri = $arg->{priority} || 0;
	my $SQL = "INSERT INTO bucardo.herdmap(goat,herd,priority) VALUES ($goatid,?,$pri)";
	$sth = $maindbh->prepare($SQL);
	$sth->execute($herd->name);
	$maindbh->commit();
	return;
};

has 'sync' => ( is => 'rw', isa => 'HashRef' );
after 'sync' => sub {
	my ($self,$arg) = @_;
	return if ! defined $arg;
	my $sync = BCsync->new($arg);
	## Make sure these are valid targets
	$self->make_dbgroup($sync->targetgroup);
	$self->make_herd($sync->source);
	$self->{sync}{id} = $self->addtable($sync, "sync");
};

has 'customcode' => ( is => 'rw', isa => 'HashRef' );
after 'customcode' => sub {

	## Add an entry to the customcode table
	## To add a new entry to customcode:
	##   mandatory args: name, whenrun, src_code
	##   optional args: about, getdbh, getrows

	my ($self,$arg) = @_;

	defined $arg and ref $arg eq 'HASH' or die qq{First argument must be a hashref\n};

	my $dbh = $self->{masterdbh};
	$dbh->rollback();

	## Check if we are doing a mapping
	if (defined $arg->{id}) {
		my $id = $arg->{id};
		$id =~ /^\d+$/ or die qq{Argument "id" must be a number\n};
		my $sync = (defined $arg->{sync} and length $arg->{sync}) ? $arg->{sync} : '';
		my $goat = (defined $arg->{goat} and length $arg->{goat}) ? $arg->{goat} : '';
		if (!length $sync and !length $goat) {
			die qq{Must pass in either a sync or goat argument\n};
		}
		## Is this a valid code?
		$SQL = "SELECT 1 FROM customcode WHERE id = $id";
		$count = $dbh->selectall_arrayref($SQL);
		defined $count->[0] or die qq{Code number $id does not exist\n};

		## Is this a valid sync?
		if (length $sync) {
			$SQL = "SELECT 1 FROM sync WHERE name = ?";
			$sth = $dbh->prepare($SQL);
			$count = $sth->execute($sync);
			$sth->finish();
			$count==1 or die qq{That sync does not exist\n};
		}
		else {
			## Is this a valid goat?
			$goat =~ /^\d+$/ or die qq{Invalid goat\n};
			$SQL = "SELECT 1 FROM goat WHERE id = ?";
			$sth = $dbh->prepare($SQL);
			$count = $sth->execute($goat);
			$sth->finish();
			$count==1 or die qq{That goat does not exist\n};
		}

		$SQL = "INSERT INTO customcode_map(code,sync,goat,active,priority) VALUES (?,?,?,?,?)";
		$sth = $dbh->prepare($SQL);
		$sth->execute(map { (defined $_ and length $_) ? $_ : $DEFAULT }
					  @$arg{qw/ id sync goat active priority /});

		$dbh->commit();
		return 1;
	}

	my $code = BCcustomcode->new($arg);

	## Add this to the database
	$SQL = "INSERT INTO customcode(name,about,whenrun,getdbh,getrows,src_code) VALUES (?,?,?,?,?,?)";
	$sth = $dbh->prepare($SQL);
	$sth->execute(map { defined $_ ? $_ : $DEFAULT } @$arg{qw/ name about whenrun getdbh getrows src_code /});
	$arg->{id} = $dbh->selectall_arrayref("SELECT currval('customcode_id_seq')")->[0][0];
	## Did they specify a sync or a goat?
	if (defined $arg->{goat} or defined $arg->{sync}) {
		$SQL = "INSERT INTO customcode_map(code,sync,goat,active,priority) VALUES (?,?,?,?,?)";
		$sth = $dbh->prepare($SQL);
		$sth->execute(map { defined $_ ? $_ : $DEFAULT } @$arg{qw / id sync goat active priority /});
	}

	$dbh->commit();
	return $arg->{id};

}; ## end 'after' customcode

sub remove_customcode {
	my ($self,$arg) = @_;

	defined $arg and ref $arg eq 'HASH' or die qq{Argument must be a hashref\n};

	my $id = (exists $arg->{id} and length $arg->{id}) ? $arg->{id} : '';
	my $name = (exists $arg->{name} and length $arg->{name}) ? $arg->{name} : '';
	my $code = (exists $arg->{code} and length $arg->{code}) ? $arg->{code} : '';

	if (!length $id and !length $name and !length $code) {
		die qq{Did not find required argument\n};
	}

	my $dbh = $self->{masterdbh};

	if (length $code) {
		$code =~ /^\d+$/ or die qq{Argument 'code' is not numeric\n};
		my $sync = (exists $arg->{sync} and length $arg->{sync}) ? $arg->{sync} : '';
		my $goat = (exists $arg->{goat} and length $arg->{goat}) ? $arg->{goat} : '';
		if (!length $sync and !length $goat) {
			die qq{Did not find required argument sync or goat\n};
		}
		if (length $goat) {
			$goat =~ /^\d+$/ or die qq{Argument goat is not numeric\n};
			$SQL = "DELETE FROM customcode_map WHERE code=? AND goat=?";
			$sth = $dbh->prepare($SQL);
			$count = $sth->execute($code,$goat);
		}
		else {
			$SQL = "DELETE FROM customcode_map WHERE code=? AND sync=?";
			$sth = $dbh->prepare($SQL);
			$count = $sth->execute($code,$sync);
		}
	}
	elsif (length $id) {
		$id =~ /^\d+$/ or die qq{Argument 'id' is not numeric\n};
		$SQL = "DELETE FROM customcode WHERE id = ?";
		$sth = $dbh->prepare($SQL);
		$count = $sth->execute($id);
	}
	else {
		$SQL = "DELETE FROM customcode WHERE name = ?";
		$sth = $dbh->prepare($SQL);
		$count = $sth->execute($name);
	}
	$dbh->commit();
	return $count eq '0E0' ? 0 : $count;

} ## end of remove_customcode


## Plural returns the latest list
my %thing =
	(
	goats    => \&get_goats,
	herds    => \&get_herds,
	syncs    => \&get_syncs,
	dbs      => \&get_dbs,
	dbgroups => \&get_dbgroups,
	 );
while (($a,$b) = each %thing) {
	has $a => ( is => 'ro', 'isa' => 'HashRef', lazy => 1, default=> $b, );
}




sub make_dbgroup {
	## Create a named dbgroup if it does not already exist
	my ($self,$dbgname) = @_;
	return if ! defined $dbgname;
	my $dbgroups = $self->get_dbgroups();
	return if exists $dbgroups->{$dbgname};
	$self->glog(qq{Creating new dbgroup named "$dbgname"});
	my $maindbh = $self->{masterdbh};
	$SQL = "INSERT INTO bucardo.dbgroup(name) VALUES (?)";
	my $sth = $maindbh->prepare($SQL);
	$sth->execute($dbgname);
	$maindbh->commit();
	return;
} ## end of make_dbgroup


sub make_herd {
	## Create a named herd if it does not exist
	my ($self,$herdname) = @_;
	return if ! defined $herdname;
	my $herds = $self->get_herds();
	return if exists $herds->{$herdname};
	$self->glog(qq{Creating new herd named "$herdname"});
	my $maindbh = $self->{masterdbh};
	$SQL = "INSERT INTO bucardo.herd(name) VALUES (?)";
	my $sth = $maindbh->prepare($SQL);
	$sth->execute($herdname);
	$maindbh->commit();
	return;
} ## end of make_herd


sub find_goat {
	## Return a goat id, given an id, a tablename, or a goat object
	my ($self,$beast,$dbid) = @_;
	my $goats = $self->get_goats();
	my $goatid;
	if (!defined $beast) {
		die "Invalid argument passed to find_goat\n";
	}

	if (ref $beast eq 'BCgoat') {
		$goatid = $beast->id;
	}
	elsif ($beast =~ /^\d+$/o) { ## TODO: explicitly disallow numeric table names
		exists $goats->{$beast} or die qq{Unknown goat id: $beast\n};
		$goatid = $beast;
	}
	elsif (! defined $dbid) {
		my @ids = map { $goats->{$_}{id} } grep { $goats->{$_}->{tablename} eq $beast } keys %$goats;
		if ($#ids >= 1) {
			die qq{More than one table named "$beast" found: please specify a database\n};
		}
		$goatid = $ids[0];
	}
	else {
		($goatid) =
			map { $goats->{$_}{id} }
			grep { $goats->{$_}->{tablename} eq $beast and $goats->{$_}{db} eq $dbid }
			keys %$goats;
	}
	if (!defined $goatid) {
		die qq{Invalid goat given: "$beast"\n};
	}
	return $goatid;
} ## end of find_goat



sub reload_config {

	my ($self) = @_;

	my $masterdbh = $self->{masterdbh};
	my $done = 'bucardo_reload_config_finished';
	$masterdbh->do("LISTEN $done");
	$masterdbh->commit();
	$masterdbh->do("NOTIFY bucardo_reload_config");
	$masterdbh->commit();

	my $BAIL = 200;
  CONFIG_WAIT: {
		while (my $notify = $masterdbh->func('pg_notifies')) {
			my ($name, $pid) = @$notify;
			last CONFIG_WAIT if $name eq $done;
		}
		$masterdbh->commit();
		sleep(0.1);
		die "Waited too long for reload_config to return\n" if --$BAIL < 1;
		redo;
	}

	return;

} ## end of reload_config



sub kick_sync {

	my ($self, $arg) = @_;
	my $msg;

	if (ref $arg ne 'HASH') {
		$msg = "Missing or invalid options given to method kick_sync!\n";
	}
	elsif (!defined $arg->{name} or !length $arg->{name}) {
		$msg = qq{A name argument is required for method kick_sync.\n};
	}
	elsif ($arg->{name} !~ /^[[:alpha:]]\w*$/) {
		$msg = qq{A valid name argument (1 letter followed by letters, numbers, or underscores) is required for method kick_sync.\n};
	}

	if ($msg) {
		warn $msg;
		$self->glog($msg);
		return;
	}

	my $masterdbh = $self->{masterdbh};

	for my $l
		(
		 "bucardo_syncdone_$arg->{name}",
		 "bucardo_syncerror_$arg->{name}",
	 ) {
		if (! $masterdbh->do("LISTEN $l")) {
			$msg = "Error from kick_sync LISTEN $l\n";
			warn $msg;
			$self->glog($msg);
			return;
		}
	}

	unless ($masterdbh->do("NOTIFY bucardo_kick_sync_$arg->{name}")) {
		$msg = "Error sending kick_sync NOTIFY bucardo_sync_$arg->{name}\n";
		warn $msg;
		$self->glog($msg);
		return;
	}

	$masterdbh->commit();
	$self->glog("Called kick_sync with args: " . join ' | ' => map { "$_=$arg->{$_}" } keys %$arg);

	return unless $arg->{wait};

	my $timeout = $arg->{timeout};
	my $timeout_error = 'timeout';

	{
		local $SIG{ALRM} = sub { die $timeout_error };

		eval {
			$timeout and alarm $timeout;
		  KICK_SYNC_WAIT: while (1) {
			  KICK_SYNC_NOTIFY: while (my $notify = $masterdbh->func('pg_notifies')) {
					my ($name, $pid) = @$notify;
					last KICK_SYNC_WAIT if $name =~ /syncdone/;
					if ($name =~ /syncerror/o) {
						$msg = "Received an error when trying to kick: perhaps it is not active?\n";
						warn $msg;
						$self->glog($msg);
						return;
				}
				}
				sleep $config{kick_sleep};
				$masterdbh->commit();
			}
			alarm 0 if $timeout;
		};
		alarm 0 if $timeout;
		if ($@) {
			if ($@ =~ /\Q$timeout_error/o) {
				$msg = qq{Timed out waiting $timeout seconds for sync "$arg->{name}" to complete\n};
			}
			else {
				$msg = qq{Error waiting for sync "$arg->{name}" to complete\n};
			}
			warn $msg;
			$self->glog($msg);
			return;
		}
	}

	return 1;

} ## end of kick_sync


sub glog {

	return if ! $_[0]->{verbose};
	my ($self,$msg,@extra) = @_;
	chomp $msg;

	if (@extra) {
		$msg = sprintf $msg, @extra;
	}

	my $prefix = $self->{logprefix} || '';
	$msg = "$prefix$msg";

	my $header = sprintf "%s%s%s",
		$config{log_showpid}  ? "($$) " : '',
		$config{log_showtime}==1 ? ('['.time.'] ')
			: $config{log_showtime}==2 ? ('['.scalar gmtime(time).'] ')
				: $config{log_showtime}==3 ? ('['.scalar localtime(time).'] ')
					: '',
		$config{log_showline} ? (sprintf '#%04d ', (caller)[2]) : '';

	## Route/tee serious errors to another file
	if ($msg =~ /Warning/o) {
		## TODO
	}

	if ($self->{debugsyslog}) {
		syslog "info", $msg;
	}
	if ($self->{debugfile}) {
		my $file = "$DEBUGDIR/log.bucardo";
		if ($self->{debugname}) {
			$file .= ".$self->{debugname}";
		}
		if ($self->{debugfilesep}) {
			$file .= ".$prefix.$$";
		}
		$file =~ s/ //g;
		open my $log, '>>', $file or die qq{Could not create "$file": $!\n};
		if (!$self->{debugfilesep}) {
			print $log "($$) ";
		}
		printf $log "%s%s%s\n",
			$config{log_showtime}==1 ? ('['.time.'] ')
				: $config{log_showtime}==2 ? ('['.scalar gmtime(time).'] ')
					: $config{log_showtime}==3 ? ('['.scalar localtime(time).'] ')
						: '',
			$config{log_showline} ? (sprintf '#%04d ', (caller)[2]) : '',
			$msg;
		close $log or warn qq{Could not close "$file": $!\n};
	}
	$self->{debugstderr} and print STDERR "$header $msg\n";
	$self->{debugstdout} and print STDOUT "$header $msg\n";
	return;
}

sub get_config {

	## Return current value of a configuration setting
	my ($self,$name) = @_;
	$name = lc $name;
	return $config{$name};
}

sub set_config {

	## Set value of a configuration setting
	## Returns old value
	my ($self,$name,$value) = @_;
	$name = lc $name;
	my $oldval = $self->get_config($name);
	$config{$name} = $value;
	return $oldval;
}

sub get_config_about {

	## Return current description of a configuration setting
	my ($self,$name) = @_;
	return $config_about{lc $name};
}

sub set_config_about {

	## Set description of a configuration setting
	## Returns old value
	my ($self,$name,$about) = @_;
	my $oldabout = $self->get_config(lc $name);
	$config_about{lc $name} = $about;
	return $oldabout;
}

sub store_config {

	## Put a configuration setting's value and description into the database
	my ($self,$name) = @_;

	$name = lc $name;
	my $maindbh = $self->{masterdbh};
	my ($value,$about) = ($config{$name},$config_about{$name});
	$SQL = "SELECT count(*) FROM bucardo_config WHERE type=NULL AND setting = ".$maindbh->quote($name);
	$count = $maindbh->selectall_arrayref($SQL)->[0][0];
	if ($count == 1) {
		$SQL = "UPDATE bucardo_config SET value=?, about=? WHERE type=NULL AND setting = ?";
		$sth = $maindbh->prepare($SQL);
		$sth->execute($value,$about,$name);
	}
	else {
		$SQL = "INSERT INTO bucardo_config(value,about,setting) VALUES (?,?,?)";
		$sth = $maindbh->prepare($SQL);
		$sth->execute($value,$about,$name);
	}
	$maindbh->commit();
	return;
}

sub get_db {
	## Return a BCdatabase object
	my ($self,$dbname) = @_;
	my $dbs = $self->get_dbs;
	if (!exists $dbs->{$dbname}) {
		die "No such database: $dbname\n";
	}
	my $db = BCdatabase->new($dbs->{$dbname});
	return $db;
}


sub get_dbs {

	my $self = shift;
	## Return a hashref of everything in the db table
	$SQL = "SELECT * FROM bucardo.db";
	$sth = $self->{masterdbh}->prepare($SQL);
	$sth->execute();
	my $info = $sth->fetchall_hashref('name');
	$self->{masterdbh}->commit();
	return $info;

} # end of get_dbs


sub get_dbgroups {

	my $self = shift;
	## Return a hashref of dbgroups
	$SQL = qq{
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
} # end of get_dbgroups


sub get_goats {

	my $self = shift;
	## Return a hashref of everything in the goat table
	$SQL = "SELECT * FROM bucardo.goat";
	$sth = $self->{masterdbh}->prepare($SQL);
	$sth->execute();
	my $info = $sth->fetchall_hashref('id');
	$self->{masterdbh}->commit();
	return $info;

} # end of get_goats


sub get_herds {

	my $self = shift;
	## Return a hashref of everything in the herd table
	$SQL = "SELECT * FROM bucardo.herd";
	$sth = $self->{masterdbh}->prepare($SQL);
	$sth->execute();
	my $info = $sth->fetchall_hashref('name');
	$self->{masterdbh}->commit();
	return $info;

} # end of get_herds


sub get_herd {

	## Return a hashref for a single herd, with table information

	my ($self,$herd) = @_;

	$herd or die qq{Must provide a herd\n};

	$SQL = qq{
        SELECT    h.name, g.db, g.tablename, g.schemaname, g.has_delta, g.ghost,
                  g.standard_conflict, g.pkey, g.qpkey, g.pkeytype
        FROM      bucardo.herd h
        LEFT JOIN bucardo.herdmap m ON (m.herd=h.name)
        LEFT JOIN bucardo.goat g ON (m.goat=g.id)
        WHERE     h.name = ?
    };
	my $maindbh = $self->{masterdbh};
	$sth = $maindbh->prepare($SQL);
	$sth->execute($herd);
	my $tree;
	for my $h (@{$sth->fetchall_arrayref({})}) {
		if (! defined $tree) {
			$tree = { name => $h->{name}, db => $h->{db} };
		}
		push @{$tree->{members}},
				   {
					schema             => $h->{schemaname},
					table              => $h->{tablename},
					has_delta          => $h->{has_delta},
					ghost              => $h->{ghost},
					analyze_after_copy => $h->{analyze_after_copy},
					standard_conflict  => $h->{standard_conflict},
					};
	}
	$maindbh->commit();
	return $tree

} # end of get_herd


sub get_syncs {

	my $self = shift;
	## Return an arrayref of everything in the sync table
	$SQL = qq{
        SELECT *,
            COALESCE(EXTRACT(epoch FROM checktime),0) AS checksecs
        FROM     bucardo.sync
        ORDER BY priority DESC, name DESC
    };
	$sth = $self->{masterdbh}->prepare($SQL);
	$sth->execute();
	my $info = $sth->fetchall_hashref("name");
	$self->{masterdbh}->commit();
	return $info;
} ## end of get_syncs



sub find_goats {
	## Given a herd, return an arrayref of goats
	my ($self,$herd) = @_;
	my $goats = $self->get_goats();
	my $maindbh = $self->{masterdbh};
	$SQL = qq{
        SELECT   goat
        FROM     bucardo.herdmap q
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


sub get_reason {
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
}


sub start_mcp {

	my ($self,$arg) = @_;
	my $old0 = $0;
	$0 = "Bucardo Master Control Program v$VERSION.$self->{extraname}";
	$self->{logprefix} = "MCP ";

	my $oldpass = $self->{dbpass};
	$self->{dbpass} = '<not shown>';
	my $dump = Dumper $self;
	$self->{dbpass} = $oldpass;
	my $reason = get_reason(1);
	my $body = qq{
		Master Control Program $$ was started on $hostname
		Args: $old0
		Version: $VERSION
	};
	my $subject = qq{Bucardo started on $shorthost};
	if ($reason) {
		$body .= "Reason: $reason\n";
		$subject .= " ($reason)";
	}
	$body =~ s/^\s+//gsm;
	$self->send_mail({ body => "$body\n\n$dump", subject => $subject });

	## If the pid file already exists, cowardly refuse to run
	if (-e $self->{pidfile}) {
		my $extra = '';
		my $fh;
		if (open ($fh, '<', $self->{pidfile}) and <$fh> =~ /(\d+)/) {
			$extra = " (PID=$1)";
		}
		my $msg = qq{File "$self->{pidfile}" already exists$extra: cannot run until it is removed};
		$self->glog($msg);
		warn $msg;
		exit;
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
		}
		exit;
	}

	## Create a new pid file
	open my $pid, '>', $self->{pidfile} or die qq{Cannot write to $self->{pidfile}: $!\n};
	my $now = scalar localtime;
	print $pid "$$\n$old0\n$now\n";
	close $pid or warn qq{Could not close "$self->{pidfile}": $!\n};
	if ($PIDCLEANUP) {
		(my $COM = $PIDCLEANUP) =~ s/PIDFILE/$self->{pidfile}/g;
		system($COM);
	}

	## Drop the existing database connection, fork, and get a new one
	$self->{masterdbh}->disconnect();
	my $seeya = fork;
	if (! defined $seeya) {
		die qq{Could not fork mcp!};
	}
	if ($seeya) {
		exit;
	}
	$self->{masterdbh} = $self->connect_database();

	$self->glog("Starting Bucardo version $VERSION");
	my $systemtime = time;
	$SQL = "SELECT extract(epoch FROM now()), now(), current_setting('timezone')";
	my $dbtime = $self->{masterdbh}->selectall_arrayref($SQL)->[0];
	$self->glog("Local system epoch: $systemtime. DB epoch: $dbtime->[0]");
	$systemtime = scalar localtime ($systemtime);
	$self->glog("Local system time: $systemtime. DB time: $dbtime->[1]");
	$systemtime = qx{/bin/date +"%Z"} || '?';
	chomp $systemtime;
	$self->glog("Local system timezone: $systemtime. DB timezone: $dbtime->[2]");
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
		$self->glog("Only doing these syncs: " . join ' ' => sort keys %{$self->{dosyncs}});
		$0 .= " Requested syncs: " . join ' ' => sort keys %{$self->{dosyncs}};
	}

	## Get all syncs, and check if each can be activated
	my $mcp;

	## We want to die gracefully
	$SIG{__DIE__} = sub {
		my $msg = shift;
		my $line = (caller)[2];
		$self->glog("Killed (line $line): $msg");
		my $body = "MCP $$ was killed: $msg";
		my $subject = "Bucardo MCP $$ was killed";

		my $respawn = (
					   $msg =~  /DBI connect/
					   or $msg =~ /Ping failed/
					   or $msg =~ /Restart Bucardo/
					   ) ? 1 : 0;

		if ($respawn) {
			$self->glog("Database problem, will respawn after a short sleep: $config{mcp_dbproblem_sleep}");
			$body .= " (will attempt respawn in $config{mcp_dbproblem_sleep} seconds)";
			$subject .= " (respawning)";
		}

		if (! $self->{clean_exit}) {
			$self->send_mail({ body => $body, subject => $subject });
		}

		## TODO: This reconnects, so can we be more careful about it dying?
		$self->cleanup_mcp("Killed (line $line): $msg");

		if ($respawn) {
			sleep($config{mcp_dbproblem_sleep});

			## We assume this is bucardo_ctl, and that we are in same directory as when called
			my $RUNME = $old0;
			$RUNME = "./$RUNME" if index($RUNME,'.')!=0;
			$RUNME .= qq{ forcestart "Attempting automatic respawn after MCP death"};
			$self->glog("Respawn attempt: $RUNME");
			exec $RUNME;
		}

		exit;
	}; ## end SIG{__DIE__}

	$self->reload_mcp();

	## We want to reload everything if someone HUPs us
	$SIG{HUP} = sub {
		$self->reload_mcp();
	};

	## Enter ourself into the audit_pid file
	my $maindbh = $self->{masterdbh};
	my $synclist;
	for (sort keys %{$self->{sync}}) {
		$synclist .= "$_:$self->{sync}{$_}{mcp_active} | ";
	}
	if (! defined $synclist) {
		die qq{The sync table appears to be empty!\n};
	}
	$synclist =~ s/\| $//;
	$self->{cdate} = scalar localtime;
	$SQL = qq{INSERT INTO bucardo.audit_pid (type,sync,ppid,pid,birthdate) }.
		qq{VALUES ('MCP',?,$self->{ppid},$$,?)};
	$sth = $maindbh->prepare($SQL);
	$sth->execute($synclist,$self->{cdate});
	$maindbh->do("NOTIFY bucardo_started");
	$maindbh->commit();

	## Start the main loop
	$self->mcp_main();

	sub mcp_main {

	my $self = shift;

	$self->glog("Entering main loop");

	my $maindbh = $self->{masterdbh};
	my $sync = $self->{sync};

	my ($n,@notice);

	my $lastpingcheck = 0;

  MCP: {

		## Bail if the stopfile exists
		if (-e $self->{stopfile}) {
			$self->glog(qq{Found stopfile "$self->{stopfile}": exiting\n});
			my $msg = "Found stopfile";
			my $reason = get_reason(0);
			if ($reason) {
				$msg .= ": $reason";
			}
			$self->cleanup_mcp("$msg\n");
			$self->glog("Exiting");
			exit;
		}

		## Every once in a while, make sure our db connection is still there
		if (time() - $lastpingcheck >= $config{mcp_pingtime}) {
			$maindbh->ping or die qq{Ping failed for main database!\n}; ## keep message in sync with above
			for my $db (keys %{$self->{pingdbh}}) {
				$self->{pingdbh}{$db}->ping
					or die qq{Ping failed for remote database $db\n};
			}
			$lastpingcheck = time();
		}

		## Gather up and handle any received notices
		undef @notice;
		while ($n = $maindbh->func('pg_notifies')) {
			push @notice, [$n->[0],$n->[1],'main'];
		}
		for my $pdb (keys %{$self->{pingdbh}}) {
			my $pingdbh = $self->{pingdbh}{$pdb};
			while ($n = $pingdbh->func('pg_notifies')) {
				push @notice, [$n->[0],$n->[1],"db $pdb"];
			}
		}
		for (@notice) {
			my ($name,$pid,$db) = @$_;
			$self->glog(qq{Got notice "$name" from $pid on $db});
			if ($name eq 'bucardo_mcp_fullstop') {
				$self->glog("Received full stop notice, leaving");
				$self->cleanup_mcp("Received stop NOTICE");
				exit;
			}

## These two are not active for now

			elsif ($name eq 'bucardo_activate_all_syncs') {
				for my $syncname (keys %$sync) {
					my $s = $sync->{$syncname};
					if (! $s->{mcp_active}) {
						if ($self->_activate_sync($s)) {
							$s->{mcp_active} = 1;
						}
					}
				}
			}
			elsif ($name eq 'bucardo_deactivate_all_syncs') {
				for my $syncname (keys %$sync) {
					my $s = $sync->{$syncname};
					if ($s->{mcp_active}) {
						if ($self->_deactivate_sync($s)) {
							$s->{mcp_active} = 0;
						}
					}
				}
			}

			elsif ($name eq 'bucardo_reload_config') {
				$self->glog("Reloading configuration table");
				$self->reload_config_database();
				$self->reload_mcp();

				## Let anyone listening know we are done
				$self->glog("Sent notice bucardo_reload_config_finished");
				$maindbh->do("NOTIFY bucardo_reload_config_finished");
				$maindbh->commit();
			}
			elsif ($name eq 'bucardo_mcp_reload') {
				$self->glog("Reloading MCP");
				$self->reload_mcp();

				## Let anyone listening know the sync is now ready
				$self->glog("Sent notice bucardo_reloaded_mcp");
				$maindbh->do("NOTIFY bucardo_reloaded_mcp");
				$maindbh->commit();
			}
			elsif ($name eq 'bucardo_mcp_ping') {
				$self->glog("Got a ping, issuing pong");
				$maindbh->do("NOTIFY bucardo_mcp_pong");
				$maindbh->commit();
			}
			elsif ($name =~ /^bucardo_reload_sync_(.+)/o) {
				my $syncname = $1;
				if (! exists $sync->{$syncname}) {
					$self->glog(qq{Invalid sync reload: "$syncname"});
				}
				elsif (!$sync->{$syncname}{mcp_active}) {
					$self->glog(qq{Sync "$syncname" is not active});
				}
				else {
					$self->glog("Deactivating sync $syncname");
					$self->_deactivate_sync($sync->{$syncname});

					## Reread from the database
					$SQL = qq{
                        SELECT *, COALESCE(EXTRACT(epoch FROM checktime),0) AS checksecs
                        FROM bucardo.sync
                        WHERE name = ?
                    };
					$sth = $maindbh->prepare($SQL);
					$count = $sth->execute($syncname);
					if ($count eq '0E0') {
						$sth->finish();
						$self->glog(qq{Warning! Cannot reload sync "$syncname" : no longer in the database!\n});
						$maindbh->commit();
						next; ## Handle the next notice
					}

					## TODO: Actually do a full disconnect and redo all the items in here

					my $info = $sth->fetchall_arrayref({})->[0];
					$maindbh->commit();

					## For now, just allow a few things to be changed "on the fly"
					for my $val (qw/checksecs stayalive limitdbs do_listen txnmode deletemethod status 
									analyze_after_copy disable_triggers/) {
						$sync->{$syncname}{$val} = $self->{sync}{$syncname}{$val} = $info->{$val};
					}
					## TODO: Fix those double assignments

					## Empty all of our custom code arrays
					for my $key (grep { /^code_/ } sort keys %{$self->{sync}{$syncname}}) {
						$sync->{$syncname}{$key} = $self->{sync}{$syncname}{$key} = [];
					}

					sleep 2; ## TODO: Actually wait somehow, perhaps fork

					$self->glog("Reactivating sync $syncname");
					$sync->{$syncname}{mcp_active} = 0;
					if (!$self->_activate_sync($sync->{$syncname})) {
						$self->glog(qq{Warning! Reactivation of sync "$syncname" failed});
					}
					else {
						## Let anyone listening know the sync is now ready
						$self->glog("Sent notice bucardo_reloaded_sync_$syncname");
						$maindbh->do("NOTIFY bucardo_reloaded_sync_$syncname");
					}
					$maindbh->commit();
				}
			}
			elsif ($name =~ /^bucardo_activate_sync_(.+)/o) {
				my $syncname = $1;
				if (! exists $sync->{$syncname}) {
					$self->glog(qq{Invalid sync activation: "$syncname"});
				}
				elsif ($sync->{$syncname}{mcp_active}) {
					$self->glog(qq{Sync "$syncname" is already activated});
					$maindbh->do("NOTIFY bucardo_activated_sync_$syncname");
					$maindbh->commit();
				}
				else {
					if ($self->_activate_sync($sync->{$syncname})) {
						$sync->{$syncname}{mcp_active} = 1;
					}
				}
			}
			elsif ($name =~ /^bucardo_deactivate_sync_(.+)/o) {
				my $syncname = $1;
				if (! exists $sync->{$syncname}) {
					$self->glog(qq{Invalid sync "$syncname"});
				}
				elsif (! $sync->{$syncname}{mcp_active}) {
					$self->glog(qq{Sync "$syncname" is already deactivated});
					$maindbh->do("NOTIFY bucardo_deactivated_sync_$syncname");
					$maindbh->commit();
				}
				else {
					if ($self->_deactivate_sync($sync->{$syncname})) {
						$sync->{$syncname}{mcp_active} = 0;
					}
				}
			}
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
					## Don't want people to wait around for a syncdone...
					$maindbh->do("NOTIFY bucardo_syncerror_$syncname");
					$maindbh->commit();
				}

			}

		} ## end each notice

		$maindbh->commit();

		## Just in case:
		$sync = $self->{sync};

		## Startup controllers for eligible syncs
	  SYNC: for my $syncname (keys %$sync) {
			## Skip if not activated
			next unless $sync->{$syncname}{mcp_active};

			my $s = $sync->{$syncname};

			## If this is not a stayalive, AND is not being kicked, skip it
			next if ! $s->{stayalive} and ! $s->{mcp_kicked};

			## If this is a previous stayalive, see if it is active, kick if needed
			if ($s->{stayalive} and $s->{controller}) {
				##$self->glog("Checking on previously started controller $s->{controller}");
				$count = kill 0, $s->{controller};
				if (! $count) {
					$self->glog("Could not find controller $s->{controller}, will create a new one. Kicked is $s->{mcp_kicked}");
					$s->{controller} = 0;
				}
				else { ## Presume it is alive and listening to us, kick if needed
					if ($s->{mcp_kicked}) {
						$self->glog(qq{Sent a kick request to controller $s->{controller} for sync "$syncname"});
						my $notify = "bucardo_ctl_kick_$syncname";
						$maindbh->do("NOTIFY $notify") or die "NOTIFY $notify failed";
						$maindbh->commit();
						$s->{mcp_kicked} = 0;
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
			my $pidfile = "$config{piddir}/bucardo_sync_$syncname.pid";
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
				$count = kill 0, $oldpid;
				if ($count==1) {
					if ($s->{mcp_changed}) {
						$self->glog(qq{Skipping sync "$syncname", seems to be already handled by $oldpid});
						## Make sure this kid is still running
						$count = kill 0, $oldpid;
						if (!$count) {
							$self->glog(qq{Warning! PID $oldpid was not found. Removing PID file});
							unlink $pidfile;
							$s->{mcp_problemchild} = 3;
							next SYNC;
						}
						$s->{mcp_changed} = 0;
					}
					if (! $s->{stayalive}) {
						$self->glog(qq{Non stayalive sync "$syncname" still active - sending it a notify});
					}
					my $notify = "bucardo_ctl_kick_$syncname";
					$maindbh->do("NOTIFY $notify") or die "NOTIFY $notify failed";
					$maindbh->commit();
					$s->{mcp_kicked} = 0;
					next SYNC;
				}
				$self->glog("No active pid $oldpid found. Killing just in case, and removing file");
				kill 15, $oldpid;
				unlink $pidfile;
				$s->{mcp_changed} = 1;
			} ## end if pidfile found for this sync

			## We may have found an error in the pid file detection the first time through
			$s->{mcp_problemchild} = 0;

			## Fork off the controller, then clean up the $s hash
			$self->{masterdbh}->commit();
			my $controller = fork;
			if (!defined $controller) {
				die qq{ERROR: Fork for controller failed!\n};
			}
			if (! $controller) {
				$self->{masterdbh}->{InactiveDestroy} = 1;
				$self->{masterdbh} = 0;
				for my $db (values %{$self->{pingdbh}}) {
					$db->{InactiveDestroy} = 1;
				}
				$self->start_controller($s);
				exit;
			}

			$self->glog(qq{Created controller $controller for sync "$syncname". Kick is $s->{mcp_kicked}});
			$s->{controller} = $controller;
			$s->{mcp_kicked} = 0;
			$s->{mcp_changed} = 1;

		} ## end each sync

		sleep $config{mcp_loop_sleep};
		redo MCP;
	}

	return;
	} ## end of MCP loop	

	sub reload_config_database {

		my $self = shift;

		undef %config;
		undef %config_about;

		$SQL = "SELECT setting,value,about,type,name FROM bucardo_config";
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

	sub reload_mcp {

		my $self = shift;

		$self->{sync} = $self->get_syncs();

		## This unlistens any old syncs
		$self->reset_mcp_listeners();

		## Kill any existing children
		opendir my $dh, $config{piddir} or die qq{Could not opendir "$config{piddir}": $!\n};
		while (defined ($_ = readdir($dh))) {
			next unless /bucardo_sync_(.+)\.pid/;
			my $syncname = $1; ## no critic
			$self->glog(qq{Attempting to kill controller process for "$syncname"});
			next unless open my $fh, '<', "$config{piddir}/$_";
			if (<$fh> !~ /(\d+)/) {
				$self->glog(qq{Warning! File "$config{piddir}/$_" did not contain a PID!\n});
				next;
			}
			my $pid = $1; ## no critic
			$self->glog(qq{Asked process $pid to terminate});
			kill 15, $pid;
			close $fh or warn qq{Could not close "$config{piddir}/$_": $!\n};
		}
		closedir $dh or warn qq{Could not closedir "$config{piddir}": $!\n};

		my @activesyncs;

		$self->glog("LOADING TABLE sync. Rows=%d", scalar (keys %{$self->{sync}}));
		for (sort keys %{$self->{sync}}) {
			my $s = $self->{sync}{$_};
			my $syncname = $s->{name};

			$self->{sync}{$_}{mcp_changed} = 1234;
			$s->{mcp_changed} = 234;

			## Has a status field already (e.g. active)
			$s->{mcp_active} = $s->{mcp_kicked} = $s->{controller} = 0;
			if ($s->{status} ne 'active') {
				$self->glog(qq{Skipping sync "$syncname": status is "$s->{status}"});
				next;
			}
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
			## Activate this sync
			$s->{mcp_active} = 1;
			if (! $self->_activate_sync($s)) {
				$s->{mcp_active} = 0;
			}
			push @activesyncs, $syncname if $s->{mcp_active};
		}

		$0 = "Bucardo Master Control Program v$VERSION.$self->{extraname} Active sync:";
		$0 .= join "," => @activesyncs;

		return;

	} ## end of reload_mcp


	sub reset_mcp_listeners {
		my $self = shift;
		my $maindbh = $self->{masterdbh};

		$maindbh->do("UNLISTEN *") or warn "UNLISTEN failed";
		for my $l
			(
			 "mcp_fullstop",
			 "mcp_reload",
			 "reload_config",
			 "mcp_ping",
		 ) {
			$self->glog(qq{Listening for "bucardo_$l"});
			$maindbh->do("LISTEN bucardo_$l") or die "LISTEN bucardo_$l failed";
		}
		for my $syncname (keys %{$self->{sync}}) {
			for my $l
				(
				 "activate_sync",
				 "deactivate_sync",
				 "reload_sync",
				 "kick_sync",
			 ) {
				next if $self->{sync}{$syncname}{status} ne 'active' and $l ne 'activate_sync';
				$self->glog(qq{Listening for "bucardo_${l}_$syncname"});
				my $listen = "bucardo_${l}_$syncname";
				$maindbh->do("LISTEN $listen") or die "LISTEN $listen failed";
			}
		}
		return $maindbh->commit();

	} ## end of reset_mcp_listeners

	sub _activate_sync {

		## We've got a new sync to be activated (but not started)
		my ($self,$s) = @_;

		my $maindbh = $self->{masterdbh};
		my $syncname = $s->{name};

		## Connect to each database used by this sync and validate tables
		if (! $self->validate_sync($s)) {
			$self->glog("Validation of sync FAILED");
			$s->{mcp_active} = 0;
			return 0;
		}

		## If the kids stay alive, the controller must too
		if ($s->{kidsalive} and !$s->{stayalive}) {
			$s->{stayalive} = 1;
			$self->glog("Warning! Setting stayalive to true because kidsalive is true");
		}

		$self->{sync}{$syncname}{mcp_active} = 1;

		## Redo our command line
		my @activesyncs;
		for my $syncname (keys %{$self->{sync}}) {
			my $s = $self->{sync}{$syncname};
			push @activesyncs, $syncname if $s->{mcp_active};
		}

		## Let any listeners know we are done
		$maindbh->do("NOTIFY bucardo_activated_sync_$syncname");
		$maindbh->commit();

		$0 = "Bucardo Master Control Program v$VERSION.$self->{extraname} Active sync:";
		$0 .= join "," => @activesyncs;

		return 1;

	} ## end of _activate_sync


	sub validate_sync {

		## Check each database a sync needs to use, and (optionally) validate all tables and columns

		my ($self,$s) = @_;

		my $syncname = $s->{name};

		## Get a list of all dbgroups in case targetgroups is set
		my $dbgroups = $self->get_dbgroups;

		## Grab the authoritative list of goats from the source herd
		$s->{goatlist} = $self->find_goats($s->{source});

		## Get the sourcedb from the first goat (should all be the same)
		$s->{sourcedb} = $s->{goatlist}[0]{db};

		## Connect to the source database and prepare to check tables and columns
		$self->{pingdbh}{$s->{sourcedb}} ||= $self->connect_database($s->{sourcedb});
		my $srcdbh = $self->{pingdbh}{$s->{sourcedb}};
 		if ($srcdbh eq 'inactive') {
			$self->glog("Source database is inactive, cannot proceed. Consider making the sync inactive instead");
			die "Source database is not active";
 		}

		my %SQL;
		$SQL{checktable} = qq{
            SELECT c.oid, pg_catalog.quote_ident(?),
                pg_catalog.quote_ident(n.nspname), pg_catalog.quote_ident(c.relname)
            FROM   pg_catalog.pg_class c, pg_catalog.pg_namespace n
            WHERE  c.relnamespace = n.oid
            AND    nspname = ?
            AND    relname = ?
        };
		$sth{checktable} = $srcdbh->prepare($SQL{checktable});

		$SQL{checkcols} = qq{
            SELECT   attname, pg_catalog.quote_ident(attname), atttypid
            FROM     pg_catalog.pg_attribute
            WHERE    attrelid = ? AND attnum > 0 AND NOT attisdropped
            ORDER BY attnum
        };
		$sth{checkcols} = $srcdbh->prepare($SQL{checkcols});

		## Connect to each target database used
		my %targetdbh;
		my $pdbh = $self->{pingdbh};
		if (defined $s->{targetdb}) {
 		  my $tdb = $s->{targetdb};
		  $self->glog(qq{Connecting to target database "$tdb"});
 		  $pdbh->{$tdb} ||= $self->connect_database($tdb);
 		  if ($pdbh->{$tdb} eq 'inactive') {
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
				$pdbh->{$tdb} ||= $self->connect_database($tdb);
				if ($pdbh->{$_} eq 'inactive') {
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

		## Validate all (active) custom code for this sync
		my $goatlist = join "," => map { $_->{id} } @{$s->{goatlist}};

		$SQL = qq{
            SELECT c.src_code, c.id, c.whenrun, c.getdbh, c.name, c.getrows, COALESCE(c.about,'?') AS about,
                   m.active, m.priority, COALESCE(m.goat,0) AS goat
            FROM customcode c, customcode_map m
            WHERE c.id=m.code AND m.active IS TRUE
            AND (m.sync = ? OR m.goat IN ($goatlist))
            ORDER BY priority ASC
        };
		$sth = $self->{masterdbh}->prepare($SQL);
		$sth->execute($syncname);
		$s->{need_rows} = $s->{need_safe_dbh} = $s->{need_safe_dbh_strict} = 0;

		for my $key (grep { /^code_/ } sort keys %$s) {
			$s->{$key} = [];
		}

		for my $c (@{$sth->fetchall_arrayref({})}) {
			$self->glog(qq{  Validating custom code $c->{id} ($c->{whenrun}) (goat=$c->{goat}): $c->{name}});
			my $dummy = q{->{dummy}};
			if ($c->{src_code} !~ /$dummy/) {
				$self->glog(qq{Warning! Code $c->{id} ("$c->{name}") does not contain the string $dummy\n});
				return 0;
			}
			else {
				$self->glog(qq{    OK: code contains a dummy string});
			}
			$c->{coderef} = sub { local $SIG{__DIE__} = sub {}; eval $c->{src_code}; }; ## no critic
			&{$c->{coderef}}({ dummy => 1 });
			if ($@) {
				$self->glog(qq{Warning! Custom code $c->{id} for sync "$syncname" did not compile: $@});
				return 0;
			}
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
			if ($c->{getrows}) {
				$s->{need_rows} = 1;
			}
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
		}

		## Consolidate some things that are set at both sync and goat levels
		$s->{does_makedelta} = $s->{makedelta};
		my $makedeltagoats = 0;

		for my $g (@{$s->{goatlist}}) {
			if ($g->{makedelta}) {
				$s->{does_makedelta} = 1;
				$g->{does_makedelta} = 1;
			}
			elsif (! defined $g->{makedelta}) {
				$g->{does_makedelta} = $s->{does_makedelta};
			}
			else {
				$g->{does_makedelta} = 0;
			}
			if ($g->{does_makedelta}) {
				$makedeltagoats++;
			}
			$g->{has_exception_code} ||= 0;
			if (!defined $g->{rebuild_index}) {
				$g->{rebuild_index} = $s->{rebuild_index};
			}
		}
		if ($s->{does_makedelta} and !$makedeltagoats) {
			$self->glog("Although sync set as makedelta, none of the goats within it are");
			$s->{does_makedelta} = 0;
		}

		## Go through each table and make sure it exists and matches everywhere
		for my $g (@{$s->{goatlist}}) {
			$self->glog(qq{  Validating source table "$g->{schemaname}.$g->{tablename}" on $s->{sourcedb}});

			## Check the source table, save escaped versions of the names
			$sth = $sth{checktable};
			$count = $sth->execute($g->{pkey},$g->{schemaname},$g->{tablename});
			if ($count != 1) {
				my $msg = qq{Could not find table $g->{schemaname}.$g->{tablename}\n};
				$self->glog($msg);
				warn $msg;
				return 0;
			}
			($g->{oid},$g->{safepkey},$g->{safeschema},$g->{safetable}) = @{$sth->fetchall_arrayref()->[0]};

			## Check the source columns, and save them
			$sth = $sth{checkcols};
			$sth->execute($g->{oid});
			my $colinfo = $sth->fetchall_arrayref();
			my $pkey = grep { $_->[0] eq $g->{pkey} } @$colinfo;
			my @cols = map { $_->[0] } grep { $_->[0] ne $g->{pkey} } @$colinfo;
			$g->{cols} = \@cols;
			my @cols2 = map { $_->[1] } grep { $_->[0] ne $g->{pkey} } @$colinfo;
			$g->{safecols} = \@cols2;
			$g->{columnlist} = join ',' => @cols;
			$g->{safecolumnlist} = join ',' => @cols2;

			my $x = 1;
			for (@$colinfo) {
				if (17 == $_->[2]) {
					$self->glog("Setting column $x as binary");
					if ($_->[0] eq $g->{pkey}) {
						$g->{binarypkey} = 1;
					}
					else {
						push @{$g->{binarycols}}, $x;
					}
				}
				$x++ if $_->[0] ne $g->{pkey};
			}

			## Verify tables and columns on remote databases
			for my $db (sort keys %targetdbh) {
				my $dbh = $pdbh->{$db};
				$self->glog(qq{    Comparing tables and columns on $db});
				$sth = $dbh->prepare($SQL{checktable});
				$count = $sth->execute('N/A',$g->{schemaname},$g->{tablename});
				if ($count != 1) {
					my $msg = qq{Could not find remote table $g->{schemaname}.$g->{tablename} on $db\n};
					$self->glog($msg);
					warn $msg;
					return 0;
				}
				my $oid = $sth->fetchall_arrayref()->[0][0];
				## Store away our oid, as we may need it later to access bucardo_delta
				$g->{targetoid}{$db} = $oid;

				$sth = $dbh->prepare($SQL{checkcols});
				$sth->execute($oid);
				my @cols = map { $_->[0] } grep { $_->[0] ne $g->{pkey} } @{$sth->fetchall_arrayref()};
				my $x;
				my $t = "$g->{schemaname}.$g->{tablename}";
				for ($x=0; defined $cols[$x]; $x++) {
					if (!defined $g->{cols}[$x]) {
						my $msg = qq{Source database "$s->{name}", table $t does not have column "$cols[$x]" as seen on target "$db"};
						$self->glog("FATAL: $msg");
						warn $msg;
						return 0;
					}
					if ($g->{cols}[$x] ne $cols[$x]) {
						my $msg = qq{Source database "$s->{name}" has a column mismatch on table $t with target "$db" ($g->{cols}[$x] <> $cols[$x])};
						$self->glog("FATAL: $msg");
						warn $msg;
						return 0;
					}
				}
				if (defined $g->{cols}[$x]) {
					my $msg = qq{Source database "$s->{name}", table $t has more columns than target "$db"};
					$self->glog("FATAL: $msg");
					warn $msg;
					return 0;
				}

			} ## end each target database

			## If we got a custom query, figure out which columns to transfer

			## TODO: Allow remote databases to have only a subset of columns
			my $customselect = $g->{customselect} || '';
			if ($customselect and $s->{usecustomselect}) {
				if ($s->{synctype} ne 'fullcopy') {
					my $msg = qq{ERROR: Custom select can only be used for fullcopy\n};
					warn $msg;
					$self->glog($msg);
					return 0;
				}
				my $msg;
				$self->glog(qq{Transforming custom select query "$customselect"});
				$sth = $srcdbh->prepare("SELECT * FROM ($customselect) AS foo LIMIT 0");
				$sth->execute();
				$info = $sth->{NAME};
				$sth->finish();
				$pkey = $g->{pkey};
				## It must contain the primary key
				if (! grep { $_ eq $pkey } @$info) {
					$msg = qq{ERROR: Custom SELECT does not contain the primary key "$pkey"\n};
					warn $msg;
					$self->glog($msg);
					return 0;
				}
				my $scols = $g->{cols};
				## It must all contain only columns already in the slave
				my $newcols = [];
				$SQL = "SELECT quote_ident(?)";
				$sth = $srcdbh->prepare($SQL);
				my $info2;
				for my $col (@$info) {
					if ($col ne $pkey and !grep { $_ eq $col } @$scols) {
						$msg = qq{ERROR: Custom SELECT returned unknown column "$col"\n};
						warn $msg;
						$self->glog($msg);
						return 0;
					}
					$sth->execute($col);
					push @$info2, $sth->fetchall_arrayref()->[0][0];
				}
				## Replace the actual set of columns with our subset
				my $collist = join ' | ' => @{$g->{cols}};
				$self->glog("Old columns: $collist");
				$collist = join ' | ' => @$info;
				$self->glog("New columns: $collist");
				$g->{cols} = $info;
				$g->{safecols} = $info2;
				## Replace the column lists
				$g->{columnlist} = join ',' => @$info;
				$g->{safecolumnlist} = join ',' => @$info2;

			} ## end custom select

			## If swap, verify the standard_conflict
			if ($s->{synctype} eq 'swap' and $g->{standard_conflict}) {
				my $sc = $g->{standard_conflict};
				die qq{Unknown standard_conflict for $syncname $g->{schemaname}.$g->{tablename}: $sc\n}
					unless
					'source' eq $sc or
					'target' eq $sc or
					'skip'   eq $sc or
					'random' eq $sc or
					'latest' eq $sc or
					'abort'  eq $sc;
				$self->glog(qq{    Standard conflict method "$sc" chosen});
			} ## end standard conflict

			## Sync must have a way to handle conflicts
			if ($s->{synctype} eq 'swap' and !$g->{standard_conflict} and !exists $g->{code_conflict}) {
				$self->glog(qq{Warning! Tables used in swaps must specify a conflict handler. $g->{schemaname}.$g->{tablename} appears to have neither});
				return 0;
			}

		} ## end each goat

		## Listen to the source if pinging
		$srcdbh->commit();
		if ($s->{ping} or $s->{do_listen}) {
			my $l = "bucardo_kick_sync_$syncname";
			$self->glog(qq{Listening on source server $s->{sourcedb} for "$l"});
			$srcdbh->do("LISTEN $l") or die "LISTEN $l failed";
			$srcdbh->commit();
		}
		## Same for the targets, but only if synctype is also "swap"
		for my $db (sort keys %targetdbh) {
			my $dbh = $pdbh->{$db};

			## If using replica and connecting to a pre 8.3 server, switch to pg_class
			my $remoteversion = $dbh->{pg_server_version} || 80000;
			if ($s->{disable_triggers} eq 'replica' and $remoteversion < 80300) {
				$self->glog("Server version on $db does not support replica trigger disabling: using pg_class");
				$s->{disable_triggers} = $s->{disable_rules} = 'pg_class';
			}

			$dbh->commit();
			next if (! $s->{ping} and ! $s->{do_listen}) or $s->{synctype} ne 'swap';
			my $l = "bucardo_kick_sync_$syncname";
			$self->glog(qq{Listening on remote server $db for "$l"});
			$dbh->do("LISTEN $l") or die "LISTEN $l failed";
			$dbh->commit();
		}

		return 1;

	} ## end of validate_sync


	sub _deactivate_sync {

		my ($self,$s) = @_;

		my $maindbh = $self->{masterdbh};
		my $syncname = $s->{name};

		## Kill the controller
		my $ctl = $s->{controller};
		if (!$ctl) {
			$self->glog("Warning! Controller not found");
		}
		else {
			$count = kill 15, $ctl;
			$self->glog("Sent kill 15 to CTL process $ctl. Result: $count");
		}
		$s->{controller} = 0;

		$self->{sync}{$syncname}{mcp_active} = 0;

		## Redo our command line
		my @activesyncs;
		for my $syncname (keys %{$self->{sync}}) {
			my $s = $self->{sync}{$syncname};
			push @activesyncs, $syncname if $s->{mcp_active};
		}

		$0 = "Bucardo Master Control Program v$VERSION.$self->{extraname} Active sync:";
		$0 .= join "," => @activesyncs;

		## Let any listeners know we are done
		$maindbh->do("NOTIFY bucardo_deactivated_sync_$syncname");
		$maindbh->commit();

		return 1;

	} ## end of _deactivate_sync


	sub cleanup_mcp {

		## Kill children, remove pidfile, update tables, etc.
		my ($self,$reason) = @_;

		unlink $self->{pidfile};

		if (!ref $self) {
			print STDERR "Oops! cleanup_mcp was not called correctly. This is a Bad Thing\n";
			return;
		}
		$self->glog(qq{Removed file "$self->{pidfile}"});

		if ($self->{masterdbh}) {
			$self->{masterdbh}->rollback();
			$self->{masterdbh}->disconnect();
		}

		## Kill all children controllers belonging to us
		my $finaldbh = $self->connect_database();
		$SQL = qq{
            SELECT pid
            FROM   bucardo.audit_pid
            WHERE  ppid = $$
            AND    type = 'CTL'
            AND    killdate IS NULL
        };
		## TODO: Think about checking for Bucardo in ps string
		$sth = $finaldbh->prepare($SQL);
		$count = $sth->execute();
		## Another option is to simply let the controllers keep running...
		for (@{$sth->fetchall_arrayref()}) {
			my $kid = $_->[0];
			$self->glog("Found active controller $kid");
			if (kill 0, $kid) {
				$count = kill 15, $kid;
				$self->glog("Kill results: $count");
			}
			else {
				$self->glog("Controller $$ not found!");
			}
		}

		## Update the audit_pid table
		$SQL = qq{
            UPDATE bucardo.audit_pid
            SET    killdate = timeofday()::timestamp, death = ?
            WHERE  type='MCP'
            AND    birthdate=?
            AND    ppid=?
            AND    pid =?
            AND    killdate IS NULL
        };
		$sth = $finaldbh->prepare($SQL);
		$reason =~ s/\s+$//;
		$sth->execute($reason,$self->{cdate},$self->{ppid},$$);
		$finaldbh->commit();
		$finaldbh->rollback();
		my $systemtime = scalar localtime;
		my $dbtime = $finaldbh->selectall_arrayref("SELECT now()")->[0][0];
		$self->glog(qq{End of cleanup_mcp. Sys time: $systemtime. DB time: $dbtime});
		$finaldbh->disconnect();
		return;

	} ## end of cleanup_mcp

	return "We should never reach this point";

} ## end of start_mcp


sub start_controller {

	## For a particular sync, does all the listening and issuing of jobs

	our ($self,$sync) = @_;

	## For custom code:
	our $input = {};

	## Custom code may require a copy of the rows
	our $rows_for_custom_code;

	my ($syncname, $synctype, $kicked,  $source, $limitdbs) = @$sync{qw(
		   name     synctype mcp_kicked  source   limitdbs)};
	my ($sourcedb, $stayalive, $kidsalive, $checksecs) = @$sync{qw(
		 sourcedb   stayalive   kidsalive   checksecs)};

	$self->{syncname} = $syncname;
	$sync->{targetdb}    ||= 0;
	$sync->{targetgroup} ||= 0;

	$0 = qq{Bucardo Controller.$self->{extraname} Sync "$syncname" ($synctype) for source "$source"};
	$self->{logprefix} = "CTL ";

	## Upgrade any specific sync configs to real configs
	if (exists $config{sync}{$syncname}) {
		my ($setting,$value);
		while (my ($setting, $value) = each %{$config{sync}{$syncname}}) {
			$config{$setting} = $value;
			$self->glog("Set sync-level config setting $setting: $value");
		}
	}

	## Store the pid
	my $SYNCPIDFILE = "$config{piddir}/bucardo_sync_$syncname.pid";
	open my $pid, '>', $SYNCPIDFILE or die qq{Cannot write to $SYNCPIDFILE: $!\n};
	print $pid "$$\n";
	close $pid or warn qq{Could not close "$SYNCPIDFILE": $!\n};
	if ($PIDCLEANUP) {
		(my $COM = $PIDCLEANUP) =~ s/PIDFILE/$SYNCPIDFILE/g;
		system($COM);
	}
	$self->{SYNCPIDFILE} = $SYNCPIDFILE;

	my $showtarget = sprintf "%s: %s",
		$sync->{targetdb} ? "database" : "database group",
		$sync->{targetdb} ||= $sync->{targetgroup};

	my $msg = qq{Controller starting for sync "$syncname". Source herd is "$source"};
	$self->glog($msg);
	my $mailmsg = "$msg\n";
	$msg = qq{  $showtarget synctype:$synctype stayalive:$stayalive checksecs:$checksecs };
	$self->glog($msg);
	$mailmsg .= "$msg\n";
	my $disabletrig = $sync->{disable_triggers};
	$msg = qq{  limitdbs:$limitdbs kicked:$kicked kidsalive:$kidsalive triggers: $disabletrig};
	$self->glog($msg);
	$mailmsg .= "$msg\n";

	$SIG{__DIE__} = sub {
		my ($msg) = @_;
		my $line = (caller)[2];
		if (! $self->{clean_exit}) {
			$self->glog(qq{Warning! Controller for "$syncname" was killed at line $line: $msg});
			for (values %{$self->{dbs}}) {
				$_->{dbpass} = '???' if defined $_->{dbpass};
			}
			## Trim the bloated sync list to just our modified one:
			## TODO: Do this in the MCP before we fork and fork again
			$self->{sync} = $sync;
			my $oldpass = $self->{dbpass};
			$self->{dbpass} = '???';
			## TODO: Strip out large src_code sections
			my $dump = Dumper $self;
			$self->{dbpass} = $oldpass; ## For our final cleanup connection
			my $body = qq{
				Controller $$ has been killed at line $line
				Host: $hostname
				Sync name: $syncname
				Stats page: $config{stats_script_url}?host=$sourcedb&sync=$syncname
				Source herd: $source
				Target $showtarget
				Error: $msg
				Parent process: $self->{ppid}
				Version: $VERSION
			};
			$body =~ s/^\s+//gsm;
			my $moresub = '';
			if ($msg =~ /Found stopfile/) {
				$moresub = " (stopfile)";
			}
			elsif ($msg =~ /could not serialize access/) {
				$moresub = " (serialization)";
			}
			elsif ($msg =~ /deadlock/) {
				$moresub = " (deadlock)";
			}
			elsif ($msg =~ /could not connect/) {
				$moresub = " (no connection)";
			}
			my $subject = qq{Bucardo "$syncname" controller killed on $shorthost$moresub};
			$self->send_mail({ body => "$body\n\n$dump", subject => $subject });
		}
		$self->cleanup_controller("Killed (line $line): $msg");
		exit;
	};

	## Connect to the master database
	our $maindbh = $self->{masterdbh} = $self->connect_database();

	## Listen for kick requests from the MCP
	my $kicklisten = "bucardo_ctl_kick_$syncname";
	$self->glog(qq{Listening for "$kicklisten"});
	$maindbh->do("LISTEN $kicklisten") or die "LISTEN $kicklisten failed";

	## TODO: Think about readding bucardo_ctl_xx_fullstop

	## Add ourself to the audit table
	$self->{ccdate} = scalar localtime;
	$SQL = qq{INSERT INTO bucardo.audit_pid (type,sync,ppid,pid,birthdate)}.
		qq{ VALUES ('CTL',?,$self->{ppid},$$,?)};
	$sth = $maindbh->prepare($SQL);
	$sth->execute($syncname,$self->{ccdate});
	$maindbh->commit();

	## Prepare to see how busy this sync is
	$self->{SQL}{qfree} = $SQL = qq{
        SELECT targetdb
        FROM   bucardo.q
        WHERE  sync=?
        AND    ended IS NULL
        AND    aborted IS NULL
    };
	$sth{qfree} = $maindbh->prepare($SQL);
	## Prepare to see how busy everyone is
	$self->{SQL}{qfreeall} = $SQL = qq{
        SELECT sourcedb, targetdb
        FROM   bucardo.q
        WHERE  ended IS NULL
        AND    aborted IS NULL
    };
	$sth{qfreeall} = $maindbh->prepare($SQL);

	for my $m (@{$sync->{goatlist}}) {
		$msg = sprintf qq{  Herd member $m->{oid}: $m->{schemaname}.$m->{tablename}%s%s%s},
			$m->{ghost} ? ' [GHOST]' : '',
				$m->{has_delta} ? ' [DELTA]' : '',
					$m->{does_makedelta} ? ' [MAKEDELTA]' : '';
		$self->glog($msg);
		if (defined $m->{customselect}) {
			$self->glog("   customselect: $m->{customselect}");
		}
		$self->glog("    Target oids: " . join " " => map { "$_:$m->{targetoid}{$_}" } sort keys %{$m->{targetoid}});
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
		$maindbh->do("LISTEN $listen") or die "LISTEN $listen failed";
	}

	## Listen for a ping request
	$maindbh->do('LISTEN bucardo_ctl_'.$$.'_ping');
	$maindbh->commit();

	## Make sure we are checking the source database as well
	$limitperdb += $dbinfo->{$sourcedb}{sourcelimit};
	$dbinuse{source}{$sourcedb} = 0;

	## This is how we tell kids to go:
	$SQL = qq{INSERT INTO bucardo.q (sync, ppid, sourcedb, targetdb, synctype)}.
		qq{ VALUES (?,?,?,?,?) };
	$sth{qinsert} = $maindbh->prepare($SQL);

	## We are only responsible for making sure there is one nullable
	$SQL = qq{
        SELECT 1
        FROM   bucardo.q
        WHERE  sync=?
        AND    sourcedb=?
        AND    targetdb=?
        AND    started IS NULL
    };
	$sth{qcheck} = $maindbh->prepare($SQL);

	$SQL = qq{
        SELECT targetdb, pid, whydie
        FROM   bucardo.q
        WHERE  sync=?
        AND    started IS NOT NULL
        AND    ended IS NULL
        AND    aborted IS NOT NULL
    };
	$sth{qcheckaborted} = $maindbh->prepare($SQL);

	$SQL = qq{
        UPDATE bucardo.q
        SET    ended = timeofday()::timestamp
        WHERE  sync=?
        AND    targetdb = ?
        AND    pid = ?
        AND    started IS NOT NULL
        AND    ended IS NULL
        AND    aborted IS NOT NULL
    };
	$sth{qfixaborted} = $maindbh->prepare($SQL);

	$SQL = qq{
        UPDATE bucardo.q
        SET    ended = timeofday()::timestamp
        WHERE  sync=?
        AND    targetdb = ?
        AND    started IS NOT NULL
        AND    ended IS NULL
        AND    aborted IS NOT NULL
    };
	$sth{qclearaborted} = $maindbh->prepare($SQL);

	$SQL = qq{
        UPDATE bucardo.q
        SET    aborted=timeofday()::timestamp, whydie=?
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
		$SQL = "SELECT date(now() - checktime) FROM sync WHERE name = $safesyncname";
		my $cdate = $maindbh->selectall_arrayref($SQL)->[0][0];
		## World of hurt here if constraint_exclusion is not set!
		$maindbh->do("SET constraint_exclusion = 'true'");
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
	my %kidalive;
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
		for (@{$sth{cleanq}->fetchall_arrayref({})}) {
			$self->glog("Cleaning out old q entry. sync=$safesyncname pid=$_->{pid} ppid=$_->{ppid} targetdb=$_->{targetdb} started:$_->{was_started} ended:$_->{was_ended} aborted:$_->{was_aborted} cdate=$_->{cdate}");
			## Make sure we kick this off again
			if (exists $targetdb->{$_->{targetdb}}) {
				$targetdb->{$_->{targetdb}}{kicked} = 1;
				$kicked = 2;
			}
			else {
				$_->{targetdb} ||= 'NONE';
				$self->glog("Warning! Invalid targetdb found for $safesyncname: $_->{targetdb} pid=$_->{pid} cdate=$_->{cdate}");
				$self->glog("Warning! SQL was $SQL. Count was $count");
			}
		}
		$SQL = qq{
              UPDATE bucardo.q
              SET started=timeofday()::timestamp, ended=timeofday()::timestamp, aborted=timeofday()::timestamp, whydie='Controller cleaning out unstarted q entry'
              WHERE sync = $safesyncname
              AND started IS NULL
        };
		$maindbh->do($SQL);

		## Clear out any aborted kids (the kids don't end so we can populate targetdb->{kicked} above)
		## The whydie has already been set by the kid
		$SQL = qq{
              UPDATE bucardo.q
              SET ended=timeofday()::timestamp
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
	}

	## If running an after_sync customcode, we need a timestamp
	if (exists $sync->{code_after_sync}) {
		$SQL = "SELECT now()";
		$sync->{starttime} = $maindbh->selectall_arrayref($SQL)->[0][0];
		$maindbh->rollback();
	}

	## If these are perpetual children, kick them off right away
	if ($kidsalive) {
		for my $dbname (sort keys %$targetdb) {
			my $kid = $targetdb->{$dbname};
			if ($kid->{pid}) { ## Can this happen?
				my $pid = $kid->{pid};
				$count = kill 0, $pid;
				if ($count) {
					$self->glog(qq{A kid is already handling database "$dbname": not starting});
					next;
				}
			}
			$kid->{dbname} = $dbname;
			$self->{kidcheckq} = 1;
			$self->create_newkid($sync,$kid);
		}
	}

	my ($n);

	my $lastpingcheck = 0;

	## A kid will control a specific sync for a specific targetdb
	## We tell all targetdbs for this sync by setting $kicked to 1
	## For individual ones only, we set $targetdb->{$dbname}{kicked} to true
	## and $kicked to 2

  CONTROLLER: {

		if (-e $self->{stopfile}) {
			$self->glog(qq{Found stopfile "$self->{stopfile}": exiting\n});
			my $msg = "Found stopfile";
			my $reason = get_reason(0);
			if ($reason) {
				$msg .= ": $reason";
			}
			die "$msg\n";
		}

		## Every once in a while, make sure we can still talk to the database
		if (time() - $lastpingcheck >= $config{ctl_pingtime}) {
			## If this fails, simply have the MCP restart it
			$maindbh->ping or die qq{Ping failed for main database!\n};
			$lastpingcheck = time();
		}

		if (!$kicked) {

			## See if we got any notices - unless we've already been kicked
			my (%notice,@notice);
			while ($n = $maindbh->func('pg_notifies')) {
				push @notice, [$n->[0],$n->[1]];
			}
			$maindbh->commit();
			for (@notice) {
				my ($name, $pid) = @$_;
				$self->glog(qq{Got notice "$name" from $pid});
				## Kick request from the MCP?
				if ($name eq $kicklisten) {
					$kicked = 1;
					## TODO: Reset the abort count for all targets?
				}
				## Got a ping?
				elsif ($name eq 'bucardo_ctl_'.$$.'_ping') {
					$self->glog("Got a ping, issuing pong");
					$maindbh->do('NOTIFY bucardo_ctl_'.$$.'_pong');
					$maindbh->commit();
				}
				## A kid has finished?
				elsif ($name =~ /^bucardo_syncdone_${syncname}_(.+)$/o) {
					my $dbname = $1;
					## If they are all finished, possibly exit
					$targetdb->{$dbname}{finished} = 1;
					## Reset the abort count for this database
					$self->{aborted}{$dbname} = 0;
					## If everyone is finished, tell the MCP (overlaps?)
					if (! grep { ! $_->{finished} } values %$targetdb) {
						my $notify = "bucardo_syncdone_$syncname";
						$maindbh->do("NOTIFY $notify") or die "NOTIFY $notify failed";
						$self->glog(qq{Sent notice "bucardo_syncdone_$syncname"});
						$maindbh->commit();

						## Run all after_sync codes
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
									## XXX Do a deltacount for fullcopy?

									my ($S,$T,$namepk,$qnamepk) = ($g->{safeschema},$g->{safetable},$g->{pkey},$g->{safepkey});
									my $safepkeytype = $g->{pkeytype} =~ /timestamp|date/o ? 'text' : $g->{pkeytype};
									my $x=0;
									my $aliaslist = join ',' => map { "$_ AS $g->{cols}[$x++]" } @{$g->{safecols}};
									if (length $aliaslist) {
										$aliaslist = ", $aliaslist";
									}

									$SQL{trix} = qq{
                                      SELECT    DISTINCT d.rowid AS "BUCARDO_ID", t.$qnamepk $aliaslist
                                      FROM      bucardo.bucardo_delta d
                                      LEFT JOIN $S.$T t ON (t.${qnamepk}::$safepkeytype = d.rowid::$safepkeytype)
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
									$sth = $srcdbh->prepare($SQL);
									$sth->execute();
									$rows_for_custom_code->{source}{$S}{$T} = $sth->fetchall_hashref('BUCARDO_ID');

									if ($synctype eq 'swap') {
										## XXX Separate getrows into swap and targets in case we don't need both?
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
						}

						## If we are not a stayalive, this is a good time to leave
						if (! $stayalive and ! $kidsalive) {
							$self->glog("Children are done, so leaving");
							exit;
						}

						## If we ran an after_sync and grabbed rows, reset some things
						if (exists $rows_for_custom_code->{source}) {
							$rows_for_custom_code = {};
							$SQL = "SELECT timeofday()::timestamp";
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
				$self->glog(qq{Timed out - force a sync for "$syncname"});
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
										"Will not create this until a kick.");
						}
						next;
					}
					$self->glog(qq{Cleaning up aborted sync from q table for "$atarget". PID was $apid});
					## Recreate this entry, unless it is already there
					$count = $sth{qcheck}->execute($syncname,$sourcedb,$atarget);
					$sth{qcheck}->finish();
					if ($count < 1) {
						$self->glog(qq{Re-adding sync to q table for db "$atarget"});
						$count = $sth{qinsert}->execute($syncname,$self->{ppid},$sourcedb,$atarget,$synctype);
						$maindbh->commit();
						sleep $KIDABORTSLEEP;
						$self->glog("Creating kid to handle resurrected q row");
						my $kid = $targetdb->{$atarget};
						$kid->{dbname} = $atarget;
						$self->{kidcheckq} = 1;
						$self->create_newkid($sync,$kid);
					}
					else {
						$self->glog("Already an empty slot, so not re-adding");
					}
				}
				$kid_check_abort = time();
			}

		} ## end !checked

		## Check that our children are alive and healthy
		if (time() - $kidchecktime >= $config{ctl_checkonkids_time}) {
			for my $dbname (sort keys %$targetdb) {
				my $kid = $targetdb->{$dbname};
				next if ! $kid->{pid};
				my $pid = $kid->{pid};
				$count = kill 0, $pid;
				if ($count != 1) {
					## Make sure this kid has cleaned up after themselves in the q table
					$count = $sth{qupdateabortedpid}->execute('?',$syncname,$pid,$self->{ppid},$dbname);
					if ($count >= 1) {
						$self->glog("Rows updated child $pid to aborted in q: $count");
					}
					## If they are finished, and kidsalive is false, then all is good.
					$kid->{pid} = 0; ## No need to check it again
					if ($kid->{finished} and !$kidsalive) {
						$self->glog(qq{Kid $pid has died a natural death. Removing from list});
						next;
					}
					$self->glog(qq{Warning! Kid $pid seems to have died. Sync "$syncname"});
				}
			} ## end each database / kid

			$kidchecktime = time();
		} # end of time to check on our kids

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
			};
			if ($c->{getrows}) {
				$input->{rows} = $rows_for_custom_code;
			}

			## TODO: Think about wrapping in an eval?
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
			elsif ($limitperdb and $dbinuse{target}{$dbname} >= $dbinfo->{$dbname}{targetlimit}) {
				$self->glog(qq{No room in queue for another target db "$dbname" Limit: $dbinfo->{$dbname}{targetlimit} Used: $dbinuse{target}{$dbname}});
				shift @q for (1..$offset);
				$queueclear = 0;
				next Q;
			}
			## Got any more slots for this source db?
			elsif ($limitperdb and $dbinuse{source}{$sourcedb} >= $dbinfo->{$sourcedb}{sourcelimit}) {
				$self->glog(qq{No room in queue for another source db "$dbname" Limit: $dbinfo->{$dbname}{sourcelimit} Used: $dbinuse{source}{$dbname}});
				shift @q for (1..$offset);
				$queueclear = 0;
				next Q;
			}
			else {
				$ok2add = 1;
				$activecount++;
				$self->glog(qq{Added "$dbname" to queue, because we had free slots});
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
					$self->glog("Could not add to q sync=$syncname,source=$sourcedb,target=$dbname,count=$count. Sending manual notification");
					my $notify = "bucardo_q_${syncname}_$dbname";
					$maindbh->do("NOTIFY $notify") or die "NOTIFY $notify failed";
				}
				$maindbh->commit();

				## Check if there is a kid alive for this database: spawn if needed
				if (! $kid->{pid} or ! (kill 0, $kid->{pid})) {
					$kid->{dbname} = $dbname;
					$self->glog("Creating a kid");
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

		my ($self,$sync,$kid) = @_;
		$self->{parent} = $$;

		## Clear out any aborted kid entries, so the controller does not resurrect them.
		## It's fairly sane to do this here, as we can assume a kid will be immediately created,
		## and that kid will create a new aborted entry if it fails.
		## We want to do it pre-fork, so we don't clear out a kid that aborts quickly.

		$sth{qclearaborted}->execute($self->{syncname},$kid->{dbname});
		$self->{masterdbh}->commit();

		my $newkid = fork;
		if (! defined $newkid) {
			die qq{Fork failed for new kid in start_controller};
		}
		if ($newkid) {
			$kid->{pid} = $newkid;
			$kid->{cdate} = time;
			$kid->{life}++;
			$kid->{finished} = 0;
			$self->glog(qq{Created new kid $newkid for sync "$self->{syncname}"});
			sleep $config{ctl_createkid_time};
			return;
		}

		$self->{masterdbh}->{InactiveDestroy} = 1;
		$self->{life} = ++$kid->{life};
		$self->start_kid($sync,$kid->{dbname});
		$self->{clean_exit} = 1;
		exit;

	} ## end of create_newkid

	die "How did we reach outside of the main controller loop?";

} ## end of start_controller



sub cleanup_controller {

	my ($self,$reason) = @_;

	if (exists $self->{cleanexit}) {
		$reason = "Normal exit";
	}

	## Disconnect from the database
	$self->{masterdbh}->rollback();
	$self->{masterdbh}->disconnect();

	## Remove the pid file
	unlink $self->{SYNCPIDFILE};

	## Kill all Bucardo children mentioned in the audit table for this sync
	my $finaldbh = $self->connect_database();
	$SQL = qq{
        SELECT pid
        FROM   bucardo.audit_pid
        WHERE  sync=?
        AND    type = 'KID'
        AND    killdate IS NULL
        AND    death IS NULL
    };
	$sth = $finaldbh->prepare($SQL);
	$sth->execute($self->{syncname});
	for (@{$sth->fetchall_arrayref()}) {
		my $kidpid = $_->[0];
		## TODO: Make sure these are Bucardo processes! - check for "Bucardo" string?
		$self->glog("Asking kid process $kidpid to terminate");
		kill 15, $kidpid;
	}
	## Asking them more than once is not going to do any good
	$SQL = qq{
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
	$SQL = qq{
        UPDATE bucardo.audit_pid
        SET    killdate = timeofday()::timestamp, death = ?
        WHERE  type='CTL'
        AND    birthdate=?
        AND    ppid=?
        AND    pid =?
        AND    killdate IS NULL
    };
	$sth = $finaldbh->prepare($SQL);
	$reason =~ s/\s+$//;
	$sth->execute($reason,$self->{ccdate},$self->{ppid},$$);
	$finaldbh->commit();

	$self->glog("Controller exiting at cleanup_controller. Reason: $reason");

	return;

} ## end of cleanup_controller

sub get_deadlock_details {

	## Given a database handle, extract deadlock details from it
	my ($self,$dldbh, $dlerr) = @_;
	return '' unless $dlerr =~ /Process \d+ waits for /;
	return '' unless defined $dldbh and $dldbh;

	$dldbh->rollback();
	my $pid = $dldbh->{pg_pid};
	while ($dlerr =~ /Process (\d+) waits for (.+) on relation (\d+) of database (\d+); blocked by process (\d+)/g) {
		next if $1 == $pid;
		my ($process,$locktype,$relation) = ($1,$2,$3);
		## Fetch the relation name
		my $getname = $dldbh->prepare("SELECT relname FROM pg_catalog.pg_class WHERE oid = ?");
		$getname->execute($relation);
		my $relname = $getname->fetchall_arrayref()->[0][0];

		## Fetch informatin about the conflicting process
		my $queryinfo =$dldbh->prepare(qq{
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

	## A single kid, in charge of doing a sync from one db to another

	our ($self,$sync,$targetdb) = @_;

	our ($syncname, $synctype, $sourcedb, $goatlist, $txnmode, $kidsalive ) = @$sync{qw(
		   name     synctype   sourcedb   goatlist   txnmode   kidsalive )};

	$0 = qq{Bucardo Kid.$self->{extraname} Sync "$syncname": ($synctype) "$sourcedb" -> "$targetdb"};
	$self->{logprefix} = "KID ";

	$self->glog(qq{New kid, syncs "$sourcedb" to "$targetdb" for sync "$syncname" alive=$kidsalive Parent=$self->{parent}});

	if ($syncname eq $targetdb) {
		die qq{Cannot sync to the same database: $targetdb\n};
	}

	## Set these early so the DIE block can use them
	our ($maindbh,$sourcedbh,$targetdbh);
	our ($S,$T,$pkval,$namepk,$qnamepk) = ('?','?','?','?','?'); ## no critic

	## Keep track of how many times this kid has done work
	our $kidloop = 0;

	$SIG{__DIE__} = sub {
		my ($msg) = @_;
		$msg =~ s/\s+$//g;
		my $line = (caller)[2];
		my ($merr,$serr,$terr);
		if ($msg =~ /DBD::Pg/) {
			$merr = $maindbh->err || 'none';
			$serr = $sourcedbh->err || 'none';
			$terr = $targetdbh->err || 'none';
			$msg .= "\n main error: $merr source error: $serr target error: $terr\n";
		}
		my $gotosleep = 0;
		if ($msg =~ /could not serialize/) {
			$self->glog("Could not serialize, sleeping for $config{kid_serial_sleep} seconds");
			$gotosleep = $config{kid_serial_sleep};
		}

		## TODO: Develop better rules for this
		if ($msg =~ /TODOcould not serialize/) {
		  $self->glog("Could not serialize, requesting next run to have EXCLUSIVE locking.");
		  my $forcename = "/tmp/bucardo-force-lock-$syncname";
		  if (-e $forcename) {
			$self->glog(qq{File "$forcename" already exists, will not create});
		  }
		  elsif (open my $fh, '>', $forcename) {
			print $fh "EXCLUSIVE\nCreate by kid $$ due to previous serilization error\n";
			close $fh or warn qq{Could not close "$forcename": $!\n};
		  }
		  else {
			$self->glog(qq{Warning! Could not create "$forcename": $!\n});
		  }
		}

		## If deadlocks, try and gather more information
		if ($msg =~ /deadlock/o) {
			if ($terr ne 'none') {
				$msg .= $self->get_deadlock_details($targetdbh, $msg);
			}
			elsif ($serr ne 'none') {
				$msg .= $self->get_deadlock_details($sourcedbh, $msg);
			}
			elsif ($merr ne 'none') {
				$msg .= $self->get_deadlock_details($maindbh, $msg);
			}
		}

		## Drop all open connections, reconnect to main for cleanup
		defined $maindbh   and $maindbh   and ($maindbh->rollback,   $maindbh->disconnect  );
		defined $sourcedbh and $sourcedbh and ($sourcedbh->rollback, $sourcedbh->disconnect);
		defined $targetdbh and $targetdbh and ($targetdbh->rollback, $targetdbh->disconnect);
		sleep $gotosleep if $gotosleep;
		my $finaldbh = $self->connect_database();

		## Let anyone listening know that this target and sync aborted
		$finaldbh->do("NOTIFY bucardo_synckill_${syncname}_$targetdb");
		$finaldbh->do("NOTIFY bucardo_synckill_$syncname");
		$finaldbh->commit();

		## Mark ourself as aborted if we've started but not completed a job
		## The controller is responsible for marking aborted entries as ended
		$SQL = qq{
            UPDATE bucardo.q
            SET    aborted=timeofday()::timestamp, whydie=?
            WHERE  sync=?
            AND    pid=?
            AND    ended IS NULL
            AND    aborted IS NULL
        };
		## Note: we don't check for non-null started because it is never set without a pid
		## TODO: Is the above unique enough for all circumstances?
		$sth = $finaldbh->prepare($SQL);
		$count = $sth->execute($msg,$syncname,$$);
		$count = 0 if $count < 1;
		if ($count >= 1) {
			$self->glog("Warning! Rows set to aborted in the q table for this child: $count");
		}
		## Clean up the audit_pid table
		$SQL = qq{
            UPDATE bucardo.audit_pid
            SET    killdate=timeofday()::timestamp, death=?
            WHERE  type='KID'
            AND    sync=?
            AND    ppid=?
            AND    pid=?
        };
		$sth = $finaldbh->prepare($SQL);
		$sth->execute($msg,$syncname,$self->{ppid},$$);
		$finaldbh->commit();
		$finaldbh->disconnect();
		if (! $self->{clean_exit}) {
			$self->glog(qq{Warning! Child for sync "$syncname" ("$sourcedb" -> "$targetdb") was killed at line $line: $msg});
			for (values %{$self->{dbs}}) {
				$_->{dbpass} = '???';
			}
			$self->{dbpass} = '???';
			## Trim the bloated sync list to just our modified one:
			$self->{sync} = $sync;
			my $dump = Dumper $self;
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
			$body =~ s/^\s+//gsm;
			my $moresub = '';
			if ($msg =~ /Found stopfile/) {
				$moresub = " (stopfile)";
			}
			elsif ($msg =~ /could not serialize access/) {
				$moresub = " (serialization)";
			}
			elsif ($msg =~ /deadlock/) {
				$moresub = " (deadlock)";
			}
			elsif ($msg =~ /could not connect/) {
				$moresub = " (no connection)";
			}
			my $subject = qq{Bucardo kid for "$syncname" killed on $shorthost$moresub};
			$self->send_mail({ body => "$body\n\n$dump", subject => $subject });
		}
		exit;
	}; ## end $SIG{__DIE__}

	## Connect to the main database
	$maindbh = $self->{masterdbh} = $self->connect_database();
	$maindbh->do("SET statement_timeout = 0");

	## Add ourself to the audit table
	$SQL = qq{INSERT INTO bucardo.audit_pid (type,sync,ppid,pid,birth)}.
		qq{ VALUES ('KID',?,$self->{ppid},$$,'Life: $self->{life}')};
	$sth = $maindbh->prepare($SQL);
	$sth->execute($syncname);

	## Listen for important changes to the q table, if we are persistent
	my $listenq = "bucardo_q_${syncname}_$targetdb";
	if ($kidsalive) {
		$maindbh->do("LISTEN $listenq") or die "LISTEN $listenq failed";
	}
	## Listen for a ping, even if not persistent
	$maindbh->do('LISTEN bucardo_kid_'.$$.'_ping');
	$maindbh->commit();

	## Prepare to update the q table when we start...
	$SQL = qq{
        UPDATE bucardo.q
        SET    started=timeofday()::timestamptz, pid = ?
        WHERE  sync=?
        AND    targetdb=?
        AND    started IS NULL
    };
	$sth{qsetstart} = $maindbh->prepare($SQL);

	## .. and when we finish.
	$SQL = qq{
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

	## Connect to the source database
	$sourcedbh = $self->connect_database($sourcedb);

	## Connect to the target database
	$targetdbh = $self->connect_database($targetdb);

	## If we are using delta tables, prepare all relevant SQL
	if ($synctype eq 'pushdelta' or $synctype eq 'swap') {

		if ($sync->{does_makedelta}) {
			$SQL = qq{INSERT INTO bucardo.bucardo_track(txntime,tablename,targetdb) VALUES (now(),?,?)};
			$sth{source}{inserttrack} = $sourcedbh->prepare($SQL) if $synctype eq 'swap';
			$sth{target}{inserttrack} = $targetdbh->prepare($SQL);
		}

		for my $g (@$goatlist) {
			($S,$T,$namepk,$qnamepk) = ($g->{safeschema},$g->{safetable},$g->{pkey},$g->{safepkey});

			if ($g->{does_makedelta}) {
				$SQL = qq{INSERT INTO bucardo.bucardo_delta(tablename,rowid) VALUES (?,?)};
				$sth{source}{$g}{insertdelta} = $sourcedbh->prepare($SQL) if $synctype eq 'swap';
				$sth{target}{$g}{insertdelta} = $targetdbh->prepare($SQL);
			}

			if (length $g->{safecolumnlist}) {
				$SQL = "INSERT INTO $S.$T ($qnamepk, $g->{safecolumnlist}) VALUES (?,";
				$SQL .= join ',' => map {'?'} @{$g->{cols}};
				$SQL .= ")";
			}
			else {
				$SQL = "INSERT INTO $S.$T ($qnamepk) VALUES (?)";
			}
			if ($g->{binarypkey}) {
				$SQL =~ s/\?/DECODE(?,'base64')/;
			}

			$sth{target}{$g}{insertrow} = $targetdbh->prepare($SQL);
			if ($synctype eq 'swap') {
				$sth{source}{$g}{insertrow} = $sourcedbh->prepare($SQL);
			}

			if (length $g->{safecolumnlist}) {
				$SQL = "UPDATE $S.$T SET ";
				$SQL .= join ',' => map { "$_=?" } @{$g->{safecols}};
				$SQL .= " WHERE $qnamepk = ?";
			}
			else {
				$SQL = "UPDATE $S.$T SET $qnamepk=$qnamepk WHERE $qnamepk = ?";
			}
			if ($g->{binarypkey}) {
				$SQL =~ s/WHERE $qnamepk/WHERE ENCODE($qnamepk,'base64')/;
			}
			$sth{target}{$g}{updaterow} = $targetdbh->prepare($SQL);
			$synctype eq 'swap' and $sth{source}{$g}{updaterow} = $sourcedbh->prepare($SQL);

			if (exists $g->{binarycols}) {
				for (@{$g->{binarycols}}) {
					$sth{target}{$g}{insertrow}->bind_param($_+1, undef, {pg_type => PG_BYTEA});
					$sth{target}{$g}{updaterow}->bind_param($_, undef, {pg_type => PG_BYTEA});
					if ($synctype eq 'swap') {
						$sth{source}{$g}{insertrow}->bind_param($_+1, undef, {pg_type => PG_BYTEA});
						$sth{source}{$g}{updaterow}->bind_param($_, undef, {pg_type => PG_BYTEA});
					}
				}
			}

			## This casting is very important for index usage!
			my $safepkeytype = $g->{pkeytype} =~ /date|bytea/o ? 'text' : $g->{pkeytype};
			my $x=0;
			my $aliaslist = join ',' => map { "$_ AS $g->{cols}[$x++]" } @{$g->{safecols}};
			if (length $aliaslist) {
				$aliaslist = ", $aliaslist";
			}
			my $safesourcedb;
			## Note: column order important for splice and defined calls later
			$SQL{delta} = qq{
                SELECT    DISTINCT d.rowid AS "BUCARDO_ID",
                              t.$qnamepk $aliaslist
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
			if ($g->{binarypkey}) {
				$SQL{delta} =~ s/BUCARDO_JOIN/(ENCODE(t.${qnamepk},'base64')::text = d.rowid::text)/;
			}
			else {
				$SQL{delta} =~ s/BUCARDO_JOIN/(t.${qnamepk}::$safepkeytype = d.rowid::$safepkeytype)/;
			}
			($SQL = $SQL{delta}) =~ s/\$1/$g->{oid}/go;
			(my $safedbname = $targetdb) =~ s/\'/''/go;
			$SQL =~ s/\$2/$safedbname/o;
			$sth{source}{$g}{getdelta} = $sourcedbh->prepare($SQL);

			if ($synctype eq 'swap') {
				($safesourcedb = $sourcedb) =~ s/\'/''/go;
				($SQL = $SQL{delta}) =~ s/\$1/$g->{targetoid}{$targetdb}/g;
				$SQL =~ s/\$2/$safesourcedb/o;
				$sth{target}{$g}{getdelta} = $targetdbh->prepare($SQL);
			}

			## Mark all unclaimed visible delta rows as done in the track table
			## This must be called within the same transaction as the delta select
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
			($SQL = $SQL{track}) =~ s/\$1/$safedbname/go;
			$SQL =~ s/\$2/$g->{oid}/go;
			$sth{source}{$g}{track} = $sourcedbh->prepare($SQL);
			if ($synctype eq 'swap') {
				($SQL = $SQL{track}) =~ s/\$1/$safesourcedb/go;
				$SQL =~ s/\$2/$g->{targetoid}{$targetdb}/go;
				$sth{target}{$g}{track} = $targetdbh->prepare($SQL);
			}
		} ## end each goat

	} ## end pushdelta/swap

	## Setup the disable and enable triggers/rules one of three ways
	## Disabling triggers is a tradeoff: ALTER TABLE does icky locking, while pg_class 
	## modification is slightly dangerous due to non-MVCC system catalogs
	##
	## The only way to disable rules <= 8.2 is pg_class, or manually rebuilding each one
	## 8.3 and up allows the use of replica magic
	## TODO: allow no trigger drop for fullcopy?
	$SQL{disable_trigrules} = $SQL{enable_trigrules} = '';
	if ($sync->{disable_triggers} eq 'SQL') {
		$SQL{disable_trigrules} = join ";\n"
			=> map {
				"ALTER TABLE $_->{safeschema}.$_->{safetable} DISABLE TRIGGER ALL"
			}
				@$goatlist;
		$SQL{enable_trigrules} = join ";\n"
			=> map {
				"ALTER TABLE $_->{safeschema}.$_->{safetable} ENABLE TRIGGER ALL"
			}
				@$goatlist;
	}
	if ($sync->{disable_triggers} eq 'pg_class' or $sync->{disable_rules} eq 'pg_class') {
		my $dotrig = $sync->{disable_triggers} eq 'pg_class' ? 1 : 0;
		my $dorule = $sync->{disable_rules} eq 'pg_class' ? 1 : 0;
		my $setclause = '';
		$setclause .= 'reltriggers = 0' if $dotrig;
		$setclause .= ', ' if $dotrig and $dorule;
		$setclause .= 'relhasrules = false' if $dorule;
		$SQL = qq{
            UPDATE pg_catalog.pg_class
            SET    $setclause
            FROM   pg_catalog.pg_namespace
            WHERE  pg_catalog.pg_namespace.oid = relnamespace
            AND    (
        };
		$SQL .= join "OR\n"
			=> map { "(nspname='$_->{safeschema}' AND relname='$_->{safetable}')" }
			@$goatlist;
		$SQL .= ')';
		$SQL{disable_trigrules} .= ";\n" if $SQL{disable_trigrules};
		$SQL{disable_trigrules} .= $SQL;

		$setclause = '';
		if ($dotrig) {
			$setclause = qq{reltriggers = 
                (SELECT count(*) FROM pg_catalog.pg_trigger WHERE tgrelid = pg_catalog.pg_class.oid)
            };
		}
		$setclause .= ', ' if $dotrig and $dorule;
		if ($dorule) {
			$setclause .= qq{relhasrules =
                        CASE WHEN (
                            SELECT COUNT(*)
                            FROM   pg_catalog.pg_rules
                            WHERE  schemaname = \$1
                            AND    tablename = \$2
                        ) > 0
                        THEN true
                        ELSE false
                        END
        };
		}

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
					 $sql =~ s/\$1/'$_->{safeschema}'/g;
					 $sql =~ s/\$2/'$_->{safetable}'/g;
					 $sql;
				 }
				@$goatlist;

		$SQL{enable_trigrules} .= ";\n" if $SQL{enable_trigrules};
		$SQL{enable_trigrules} .= $SQL;
	}

	## Common settings for the database handles. Set before passing to DBIx::Safe below
	$sourcedbh->do("SET statement_timeout = 0");
	$targetdbh->do("SET statement_timeout = 0");
	if ($sync->{disable_triggers} eq 'replica') {
		$targetdbh->do("SET session_replication_role = 'replica'");
		if ($synctype eq 'swap') {
			$sourcedbh->do("SET session_replication_role = 'replica'");
		}
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

	## Everything with "our" is used in custom code

	## Summary information about our actions.
	## Perhaps store in the main database someday
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

	sub run_custom_code {
		my $c = shift;
		my $strictness = shift || '';

		$self->glog("Running $c->{whenrun} custom code $c->{id}: $c->{name}");

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
		};
		if ($c->{getrows}) {
			$input->{rows} = $rows_for_custom_code;
		}
		if ($c->{getdbh}) {
			$input->{sourcedbh} = $strictness eq 'nostrict' ? $safe_sourcedbh : $safe_sourcedbh_strict;
			$input->{targetdbh} = $strictness eq 'nostrict' ? $safe_targetdbh : $safe_targetdbh_strict;
		}
		## TODO: Use eval?
		$maindbh->{InactiveDestroy} = 1;
		$sourcedbh->{InactiveDestroy} = 1;
		$targetdbh->{InactiveDestroy} = 1;
		&{$c->{coderef}}($input);
		$maindbh->{InactiveDestroy} = 0;
		$sourcedbh->{InactiveDestroy} = 0;
		$targetdbh->{InactiveDestroy} = 0;
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
			$maindbh->do("NOTIFY $notify") or warn "NOTIFY $notify failed";
			$maindbh->commit();
			sleep $config{endsync_sleep};
			return 'redo'; ## redo this entire sync
		}
		return 'normal';

	} ## end of run_custom_code

	## Have we found a reason to check the queue yet?
	my $checkq;

  KID: {

		$checkq = 0;

		if (-e $self->{stopfile}) {
			$self->glog(qq{Found stopfile "$self->{stopfile}": exiting\n});
			last KID;
		}

		## If persistent, do an occasional ping. Listen for our only possible message.
		if ($kidsalive) {
			while (my $notify = $maindbh->func('pg_notifies')) {
				my ($name, $pid) = @$notify;
				if ($name eq $listenq) {
					$self->glog("Got a notice for $syncname: $sourcedb -> $targetdb");
					$checkq = 1;
				}
				## Got a ping?
				elsif ($name eq 'bucardo_kid_'.$$.'_ping') {
					$self->glog("Got a ping, issuing pong");
					$maindbh->do('NOTIFY bucardo_kid_'.$$.'_pong');
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
			$self->glog("Nothing to do: no entry found in the q table for this sync");
			$maindbh->rollback();
			redo KID if $kidsalive;
			last KID;
		}
		## Stake our claim
		$maindbh->commit();

		$kidloop++;

		my $start_time = time();

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
			my $result = run_custom_code($code, 'nostrict');
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
		## Note that all database handles are currently not in a txn (commit or rollback called)
		if (defined $txnmode) {
			$targetdbh->do("SET TRANSACTION ISOLATION LEVEL $txnmode");
		}
		if ($synctype eq 'swap' or $synctype eq 'pushdelta') {
			if (defined $txnmode) {
				$sourcedbh->do("SET TRANSACTION ISOLATION LEVEL $txnmode");
			}
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
				my $com = "$g->{safeschema}.$g->{safetable} IN $lock_table_mode MODE";
				$self->glog("$sourcedb: Locking table $com");
				$sourcedbh->do("LOCK TABLE $com");
				$self->glog("$targetdb: Locking table $com");
				$targetdbh->do("LOCK TABLE $com");
			}
		}

		## Run all 'before_check_rows' code
		for my $code (@{$sync->{code_before_check_rows}}) {
			my $result = run_custom_code($code, 'strict');
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

			## For each table in this herd, grab a count of changes
			$deltacount{allsource} = $deltacount{alltarget} = 0;
			for my $g (@$goatlist) {
				($S,$T) = ($g->{safeschema},$g->{safetable});
				$deltacount{allsource} += $deltacount{source}{$S}{$T} = $sth{source}{$g}{getdelta}->execute();
				$sth{source}{$g}{getdelta}->finish() if $deltacount{source}{$S}{$T} =~ s/0E0/0/o;
				$self->glog(qq{Source delta count for $S.$T: $deltacount{source}{$S}{$T}});

				if ($synctype eq 'swap') {
					$deltacount{alltarget} += $deltacount{target}{$S}{$T} = $sth{target}{$g}{getdelta}->execute();
					$sth{target}{$g}{getdelta}->finish() if $deltacount{target}{$S}{$T} =~ s/0E0/0/o;
					$self->glog(qq{Target delta count for $S.$T: $deltacount{target}{$S}{$T}});
				}
			}
			if ($synctype eq 'swap') {
				$self->glog("Total source delta count: $deltacount{allsource}");
				$self->glog("Total target delta count: $deltacount{alltarget}");
			}
			$deltacount{all} = $deltacount{allsource} + $deltacount{alltarget};
			$self->glog("Total delta count: $deltacount{all}");

			## If no changes, rollback dbs, close out q, notify listeners, and leave or reloop
			if (! $deltacount{all}) {
				$targetdbh->rollback();
				$sourcedbh->rollback();
				$sth{qend}->execute(0,0,0,$syncname,$targetdb,$$);
				$maindbh->do("NOTIFY bucardo_syncdone_${syncname}_$targetdb")
					or die qq{NOTIFY failed: bucardo_syncdone_${syncname}_$targetdb};
				$maindbh->commit();
				sleep $config{kid_nodeltarows_sleep};
				redo KID if $kidsalive;
				last KID;
			}
		} ## end count delta rows

		## Run all 'before_trigger_drop' code
		for my $code (@{$sync->{code_before_trigger_drop}}) {
			my $result = run_custom_code($code, 'strict');
			if ($result eq 'redo') { ## redo rollsback source and target
				redo KID if $kidsalive;
				last KID;
			}
		}

		if ($SQL{disable_trigrules}) {
			$self->glog(qq{Disabling triggers and rules on $targetdb});
			$targetdbh->do($SQL{disable_trigrules});
			if ($synctype eq 'swap') {
				$self->glog(qq{Disabling triggers and rules on $sourcedb});
				$sourcedbh->do($SQL{disable_trigrules});
			}
		}

		if ($synctype eq 'fullcopy') {

			for my $g (@$goatlist) {

				($S,$T) = ($g->{safeschema},$g->{safetable});

				if ($g->{ghost}) {
					$self->glog("Skipping ghost table $S.$T");
					next;
				}

				$self->glog("Emptying out target table $S.$T using $sync->{deletemethod}");
				if ($sync->{deletemethod} eq 'truncate') {
					$targetdbh->do("TRUNCATE TABLE $S.$T");
				}
				else {
					($dmlcount{D}{target}{$S}{$T} = $targetdbh->do("DELETE FROM $S.$T")) =~ s/0E0/0/o;
					$dmlcount{alldeletes}{target} += $dmlcount{D}{target}{$S}{$T};
					$self->glog("Rows deleted from $S.$T: $dmlcount{D}{target}{$S}{$T}");
				}

				my ($srccmd,$tgtcmd);
				if ($sync->{usecustomselect} and $g->{customselect}) {
					my $temptable = "bucardo_temp_$g->{tablename}_$$"; ## Raw version, not "safetable"
					$self->glog("Creating temp table $temptable for custom select on $S.$T");
					$sourcedbh->do("CREATE TEMP TABLE $temptable AS $g->{customselect}");
					$srccmd = "COPY $temptable TO STDOUT $sync->{copyextra}";
					$tgtcmd = "COPY $S.$T($g->{safecolumnlist}) FROM STDIN $sync->{copyextra}";
				}
				else {
					$srccmd = "COPY $S.$T TO STDOUT $sync->{copyextra}";
					$tgtcmd = "COPY $S.$T FROM STDIN $sync->{copyextra}";
				}

				$self->glog("Running on $sourcedb: $srccmd");
				$sourcedbh->do($srccmd);

				my $hasindex = 0;
				if ($g->{rebuild_index}) {
					$SQL = "SELECT relhasindex FROM pg_class WHERE oid = $g->{targetoid}{$targetdb}";
					$hasindex = $targetdbh->selectall_arrayref($SQL)->[0][0];
					if ($hasindex) {
						$self->glog("Turning off indexes for $S.$T on $targetdb");
						$SQL = "UPDATE pg_class SET relhasindex = 'f' WHERE oid = $g->{targetoid}{$targetdb}";
						$targetdbh->do($SQL);
					}
				}

				$self->glog("Running on $targetdb: $tgtcmd");
				$targetdbh->do($tgtcmd);
				my $buffer='';
				$dmlcount{I}{target}{$S}{$T} = 0;
				while ($sourcedbh->pg_getline($buffer, $MAXCOPYBUF)) {
					$targetdbh->pg_putline($buffer);
					$dmlcount{I}{target}{$S}{$T}++;
				}
				$targetdbh->pg_endcopy();
				$self->glog(qq{End COPY of "$S.$T". Rows inserted: $dmlcount{I}{target}{$S}{$T}});
				$dmlcount{allinserts}{target} += $dmlcount{I}{target}{$S}{$T};

				if ($hasindex) {
					$SQL = "UPDATE pg_class SET relhasindex = 't' WHERE oid = $g->{targetoid}{$targetdb}";
					$targetdbh->do($SQL);
					$self->glog("Reindexing table $S.$T on $targetdb");
					$targetdbh->do("REINDEX TABLE $S.$T");
				}

				if ($sync->{analyze_after_copy} and $g->{analyze_after_copy}) {
					$self->glog("Analyzing $S.$T on $targetdb");
					$targetdbh->do("ANALYZE $S.$T");
				}

			} ## end each goat

			if ($sync->{deletemethod} ne 'truncate') {
				$self->glog("Total target rows deleted: $dmlcount{alldeletes}{target}");
			}
			$self->glog("Total target rows copied: $dmlcount{allinserts}{target}");

		} ## end of synctype fullcopy

		elsif ($synctype eq 'pushdelta') {

			for my $g (@$goatlist) {

				($S,$T,$namepk,$qnamepk) = ($g->{safeschema},$g->{safetable},$g->{pkey},$g->{safepkey});

				## Skip this table if no rows have changed on the source
				next unless $deltacount{source}{$S}{$T};

				my $toid = $g->{targetoid}{$targetdb};

				my $hasindex = 0;
				if ($g->{rebuild_index}) {
					$SQL = "SELECT relhasindex FROM pg_class WHERE oid = $toid";
					$hasindex = $targetdbh->selectall_arrayref($SQL)->[0][0];
					if ($hasindex) {
						$self->glog("Turning off indexes for $S.$T on $targetdb");
						$SQL = "UPDATE pg_class SET relhasindex = 'f' WHERE oid = $toid";
						$targetdbh->do($SQL);
					}
				}

				my $info = $sth{source}{$g}{getdelta}->fetchall_arrayref();

				if ($sync->{need_rows}) {
					$rows_for_custom_code->{$S}{$T} =
						{
						 source   => $info,
						 pkeyname => $g->{pkey},
						 pkeytype => $g->{pkeytype},
						 };
				}

				if ($g->{does_makedelta}) {
					for (@$info) {
						$sth{target}{$g}{insertdelta}->execute($toid,$_->[0]);
						$self->glog("Inserted makedelta on $T record $g->{oid}, $_->[0] on $targetdb");
					}
					$sth{target}{inserttrack}->execute($toid,$targetdb);
					$count = @$info;
					$self->glog("Total makedelta rows added for $S.$T on $targetdb: $count");
				}

				## First, delete any rows that no longer exist on the target:
				my @tgtdelete = map { ($a=$_->[0]) =~ s/\'/''/g; qq{'$a'} } grep { !defined $_->[1] } @$info; ## no critic
				$count = @tgtdelete;
				$SQL = "DELETE FROM $S.$T WHERE $qnamepk IN ";
				if ($count) {
					while (@tgtdelete) {
						no warnings;
						my $list = '';
						$list .= (shift @tgtdelete) . ',' for 1..$config{max_delete_clause};
						$list =~ s/,+$//o;
						$self->glog("Delete from $S.$T: $list");
						$dmlcount{D}{target}{$S}{$T} += $targetdbh->do("$SQL ($list)");
					}
					$dmlcount{D}{target}{$S}{$T} = 0 if $dmlcount{D}{target}{$S}{$T} eq '0E0';
					$self->glog(qq{Rows deleted from target $S.$T: $dmlcount{D}{target}{$S}{$T}});
					$dmlcount{alldeletes}{target} += $dmlcount{D}{target}{$S}{$T};
					$count = $deltacount{source}{$S}{$T} - $count;
				}
				else {
					$count = $deltacount{source}{$S}{$T};
				}

				$self->glog("Rows to be upserted for $S.$T: $count");

				$dmlcount{U}{target}{$S}{$T} = 0;
				$dmlcount{I}{target}{$S}{$T} = 0;

			  ROW: for my $row (grep { defined $_->[1] } @$info) {
					## Grab the first column from the join above as the primary key.
					## Discard the second one (pkey)
					$pkval = splice(@$row,0,2);

					my $upsert = 0; ## How many times we've tried to process this rows
				  UPSERT: {
						$count = $sth{target}{$g}{updaterow}->execute(@$row,$pkval);
						if ($count ne '0E0') {
							$self->glog("Updated $S.$T.$qnamepk: $pkval");
							$dmlcount{U}{target}{$S}{$T} += $count;
							next ROW;
						}

						## Row does not exist, that we know of. Try an insert
						$targetdbh->pg_savepoint("bucardo_insert");
						eval {
							$count = $sth{target}{$g}{insertrow}->execute($pkval,@$row);
						};
						if ($@) {
							$self->glog("Rolling back because of $@");
							$targetdbh->pg_rollback_to("bucardo_insert");
							## This may or may not be due to a primary key race condition.
							## We give it one unconditional chance, then check the message
							## A few more tries after that and nobody can go on
							if ($upsert > $config{upsert_attempts}
								or
								($upsert >= 1 and $@ !~ /duplicate key violates unique constraint/)
								) {
								$self->glog("Warning! Could not insert pk $pkval to $S.$T on $targetdb, aborting");
								die qq{Upsert insert failed for $S.$T.$pkval on $targetdb: $@};
							}
							$upsert++;
							redo UPSERT;
						}
						else { ## The insert worked
							$dmlcount{I}{target}{$S}{$T} += $count;
							$self->glog("Inserted $S.$T.$qnamepk: $pkval");
							$targetdbh->pg_release("bucardo_insert");
						}
					} ## end UPSERT
				} ## end ROW

				$self->glog("Upsert results: updates=$dmlcount{U}{target}{$S}{$T} inserts=$dmlcount{I}{target}{$S}{$T}");
				$dmlcount{allupdates}{target} += $dmlcount{U}{target}{$S}{$T};
				$dmlcount{allinserts}{target} += $dmlcount{I}{target}{$S}{$T};

				if ($hasindex) {
					$SQL = "UPDATE pg_class SET relhasindex = 't' WHERE oid = $toid";
					$targetdbh->do($SQL);
					$self->glog("Reindexing table $S.$T on $targetdb");
					$targetdbh->do("REINDEX TABLE $S.$T");
				}

				## Update the source bucardo_tracker for this table
				$self->glog("Updating bucardo_track for $S.$T on $sourcedb");
				$sth{source}{$g}{track}->execute();

			} ## end each goat

			## TODO: Exception catching?

			$self->glog("Pushdelta counts: updates=$dmlcount{allupdates}{target} inserts=$dmlcount{allinserts}{target}");

		} ## end pushdelta

		elsif ($synctype eq 'swap') {

			for my $g (@$goatlist) {

				($S,$T,$namepk,$qnamepk) = ($g->{safeschema},$g->{safetable},$g->{pkey},$g->{safepkey});

				## Skip if neither side has changes for this table
				next unless $deltacount{source}{$S}{$T} or $deltacount{target}{$S}{$T};

				## Use copies as rollback/redo may change the originals
				$deltacount{src2}{$S}{$T} = $deltacount{source}{$S}{$T};
				$deltacount{tgt2}{$S}{$T} = $deltacount{target}{$S}{$T};

				$dmlcount{I}{source}{$S}{$T} = $dmlcount{U}{source}{$S}{$T} = $dmlcount{D}{source}{$S}{$T} =
				$dmlcount{I}{target}{$S}{$T} = $dmlcount{U}{target}{$S}{$T} = $dmlcount{D}{target}{$S}{$T} = 0;

				my $toid = $g->{targetoid}{$targetdb};

				my ($hasindex_src,$hasindex_tgt) = (0,0);

				if ($g->{rebuild_index}) { ## Usually not a good idea for swap sync
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

				$g->{exceptions} = 0; ## Total number of times we've failed for this table
			  SAVEPOINT: {

				my $info1 = $deltacount{src2}{$S}{$T}<1 ? {} : $sth{source}{$g}{getdelta}->fetchall_hashref('BUCARDO_ID');
				my $info2 = $deltacount{tgt2}{$S}{$T}<1 ? {} : $sth{target}{$g}{getdelta}->fetchall_hashref('BUCARDO_ID');
				if ($sync->{need_rows}) {
					$rows_for_custom_code->{$S}{$T} =
						{
						 source   => $info1,
						 target   => $info2,
						 pkeyname => $g->{pkey},
						 pkeytype => $g->{pkeytype},
						 };
				}

				## Go through all keys and resolve any conflicts. Bitmap action:
				## 1 = Add source row to the target db
				## 2 = Add target row to the source db
				## 4 = Add source row to the source db
				## 8 = Add target row to the target db
				for my $temp_pkval (sort keys %$info1) {
					$pkval = $temp_pkval;
					## No problem if it only exists on the source
					if (! exists $info2->{$pkval}) {
						$self->glog("No conflict, source only for $S.$T.$qnamepk: $pkval");
						$info1->{$pkval}{BUCARDO_ACTION} = 1; ## source to target
					}
					else {
						## Standard conflict handlers don't need info to make a decision
						if (!exists $g->{code_conflict}) {
							my $sc = $g->{standard_conflict};
							$self->glog(qq{Conflict detected for $S.$T:$pkval. Using standard conflict "$sc"});
							if ('source' eq $sc) {
								$info1->{$pkval}{BUCARDO_ACTION} = 1; ## source to target
							}
							elsif ('target' eq $sc) {
								$info1->{$pkval}{BUCARDO_ACTION} = 2; ## target to source
							}
							elsif ('skip' eq $sc) { ## XXX Too dangerous? Not allow 0 in general?
								$info1->{$pkval}{BUCARDO_ACTION} = 0;
							}
							elsif ('random' eq $sc) {
								$info1->{$pkval}{BUCARDO_ACTION} = rand 2 > 1 ? 1 : 2;
							}
							elsif ('abort' eq $sc) {
								die qq{Warning! Aborting sync $syncname due to conflict for $S:$T:$pkval\n};
							}
							elsif ('latest' eq $sc) {
								$SQL{sc_latest} ||=
									qq{SELECT extract(epoch FROM max(txntime)) FROM bucardo.bucardo_delta WHERE tablename=? AND rowid=?};
								$sth{sc_latest_src} ||= $sourcedbh->prepare($SQL{sc_latest});
								$sth{sc_latest_src}->execute($g->{oid},$pkval);
								my $srctime = $sth{sc_latest_src}->fetchall_arrayref()->[0][0];
								$sth{sc_latest_tgt} ||= $targetdbh->prepare($SQL{sc_latest});
								$sth{sc_latest_tgt}->execute($toid,$pkval);
								my $tgttime = $sth{sc_latest_tgt}->fetchall_arrayref()->[0][0];
								$self->glog(qq{Delta source time: $srctime Target time: $tgttime});
								$info1->{$pkval}{BUCARDO_ACTION} = $srctime >= $tgttime ? 1 : 2;
							}
							else {
								die qq{Unknown standard conflict for sync $syncname on $T.$S: $sc\n};
							}
						}
						else { ## Custom conflict handler. Gather up info to pass to it.
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

							## Run the conflict handler(s)
							for my $code (@{$g->{code_conflict}}) {
								my $result = run_custom_code($code, 'strict');
								if ($result eq 'next') {
									$self->glog("Going to next available conflict code");
									next;
								}
								if ($result eq 'redo') { ## ## redo rollsback source and target
									$self->glog("Custom conflict handler has requested we redo this sync");
									redo KID if $kidsalive;
									last KID;
								}

								$self->glog("Conflict handler action: $rowinfo{action}");

								## Check for conflicting actions
								if ($rowinfo{action} & 2 and $rowinfo{action} & 4) {
									$self->glog("Warning! Conflict handler cannot return 2 and 4. Ignoring 4");
									$rowinfo{action} -= 4;
								}
								if ($rowinfo{action} & 1 and $rowinfo{action} & 8) {
									$self->glog("Warning! Conflict handler cannot return 1 and 8. Ignoring 8");
									$rowinfo{action} -= 8;
								}

								$info1->{$pkval}{BUCARDO_ACTION} = $rowinfo{action};

								last;
							}
						} ## end custom code handler
					} ## end conflict
				} ## end each key in source delta list

				## Since we've already handled conflicts, simply mark "target only" rows
				for my $tpkval (keys %$info2) {
					next if exists $info1->{$tpkval};
					$self->glog("No conflict, target only for $S.$T.$qnamepk: $pkval");
					$info1->{$tpkval}{BUCARDO_ACTION} = 2; ## target to source
				}

				## Give some summary statistics
				my %actionstat;
				for (values %$info1) {
					$actionstat{$_->{BUCARDO_ACTION}}++ if exists $_->{BUCARDO_ACTION};
				}
				$self->glog("Action summary: " . join ' ' => map { "$_:$actionstat{$_}" } sort keys %actionstat);

				## For each key, either mark as deleted, or mark as needing to be checked
				my (@srcdelete,@tgtdelete,@srccheck,@tgtcheck);

				## Used for makedelta:
				my (@srcdelete2,@tgtdelete2);

				## How many rows are we upserting?
				my $changecount = 0;

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
					my $safepk;
					if ($g->{pkeytype} =~ /int$/o) { ## smallint, int, bigint, bytea(base64text)
						$safepk = $pkval;
					}
					else {
						($safepk = $pkval) =~ s/\'/''/go;
						$safepk = qq{'$safepk'};
					}

					## Delete from source if going to source and has been deleted
					if (($action & 2 and ! defined $info2->{$pkval}{$namepk}) ## target to source
					 or ($action & 4 and ! defined $info1->{$pkval}{$namepk})) { ## source to source
						push @srcdelete, $safepk;
						push @srcdelete2, $pkval if $g->{does_makedelta};
						## Strip out this action as done (2 and 4 are mutually exclusive)
						$info1->{$pkval}{BUCARDO_ACTION} -= ($action & 2) ? 2 : 4;
						$action = $info1->{$pkval}{BUCARDO_ACTION};
					}

					## Delete from target if going to target and has been deleted
					if (($action & 1 and ! defined $info1->{$pkval}{$namepk}) ## source to target
					 or ($action & 8 and ! defined $info2->{$pkval}{$namepk})) { ## target to target
						push @tgtdelete, $safepk;
						push @tgtdelete2, $pkval if $g->{does_makedelta};
						## Strip out this action as done (1 and 8 are mutually exclusive)
						$info1->{$pkval}{BUCARDO_ACTION} -= ($action & 1) ? 1 : 8;
						$action = $info1->{$pkval}{BUCARDO_ACTION};
					}

					next if ! $action; ## Delete only

					$changecount++;

					## If going from target to source, verify if it exists on source or not
					if (($action & 2) and !defined $info1->{$pkval}{$namepk}) {
						push @srccheck, $safepk;
					}

					## If going from source to target, verify it it exists on target or not
					if (($action & 1) and !defined $info2->{$pkval}{$namepk}) {
						push @tgtcheck, $safepk;
					}

				}

				## Add in the makedelta rows as needed
				if ($g->{does_makedelta}) {
					for (@srcdelete2) {
						$sth{source}{$g}{insertdelta}->execute($g->{oid},$_);
						$self->glog("Adding in source bucardo_delta row (delete) for $g->{oid} and $_");
					}
					for (@tgtdelete2) {
						$sth{target}{$g}{insertdelta}->execute($toid,$_);
						$self->glog("Adding in target bucardo_delta row (delete) for $toid and $_");
					}
				}

				## If we have exception handling code, create a savepoint to rollback to
				if ($g->{has_exception_code}) {
					$self->glog("Creating savepoints on source and target for exception handler(s)");
					$sourcedbh->pg_savepoint("bucardo_$$") or die qq{Savepoint creation failed for bucardo_$$};
					$targetdbh->pg_savepoint("bucardo_$$") or die qq{Savepoint creation failed for bucardo_$$};
				}

				## Do deletions in chunks
				$SQL = $g->{binarypkey} ?
					"DELETE FROM $S.$T WHERE ENCODE($qnamepk,'base64') IN"
						: "DELETE FROM $S.$T WHERE $qnamepk IN";
				$count = @srcdelete;
				if ($count) {
					while (@srcdelete) {
						no warnings;
						my $list = '';
						$list .= (shift @srcdelete) . ',' for 1..$config{max_delete_clause};
						$list =~ s/,+$//o;
						$self->glog("Deleting from source: $list");
						$dmlcount{D}{source}{$S}{$T} += $sourcedbh->do("$SQL ($list)");
					}
					$self->glog(qq{Rows deleted from source "$S.$T": $dmlcount{D}{source}{$S}{$T}/$count});
				}
				$count = @tgtdelete;
				if ($count) {
					while (@tgtdelete) {
						no warnings;
						my $list = '';
						$list .= (shift @tgtdelete) . ',' for 1..$config{max_delete_clause};
						$list =~ s/,+$//o;
						$self->glog("Deleting from target: $list");
						$dmlcount{D}{target}{$S}{$T} += $targetdbh->do("$SQL ($list)");
					}
					$self->glog(qq{Rows deleted from target "$S.$T": $dmlcount{D}{target}{$S}{$T}/$count});
				}

				## Get authoritative existence information for all undefined keys
				## Before this point, the lack of a matching record from the left join
				## only tells us that the real row *might* exist.
				## And upserts are too expensive here :)
				$SQL = $g->{binarypkey} ? 
					"SELECT ENCODE($qnamepk,'base64') AS $qnamepk FROM $S.$T WHERE ENCODE($qnamepk,'base64') IN "
						: "SELECT $qnamepk FROM $S.$T WHERE $qnamepk IN ";

				while (@srccheck) {
					no warnings;
					my $list = '';
					$list .= (shift @srccheck) . ',' for 1..$config{max_select_clause};
					$list =~ s/,+$//o;
					for (@{$sourcedbh->selectall_arrayref("$SQL ($list)")}) {
						$info1->{$_->[0]}{$namepk} = 1;
					}
				}
				while (@tgtcheck) {
					no warnings;
					my $list = '';
					$list .= (shift @tgtcheck) . ',' for 1..$config{max_select_clause};
					$list =~ s/,+$//o;
					for (@{$targetdbh->selectall_arrayref("$SQL ($list)")}) {
						$info2->{$_->[0]}{$namepk} = 1;
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

					## TODO: Any way to easily remove eval if not doing savepoints?
				  GENX: {
						## Temporarily override our kid-level handler due to the eval
						local $SIG{__DIE__} = sub {}; ## TODO: WORKS?: if $g->{has_exception_code};

						## This eval block needed for potential error handling
						eval {

							if ($action & 1) { ## Source to target
								if (defined $info2->{$pkval}{$namepk}) {
									$self->glog("$prefix UPDATE source to target pk $pkval");
									$count = $sth{target}{$g}{updaterow}->execute(@srcrow,$pkval);
									$dmlcount{U}{target}{$S}{$T}++;
								}
								else {
									$self->glog("$prefix INSERT source to target pk $pkval");
									$count = $sth{target}{$g}{insertrow}->execute($pkval,@srcrow);
									$dmlcount{I}{target}{$S}{$T}++;
								}
							}
							if ($action & 2) { ## Target to source
								if (defined $info1->{$pkval}{$namepk}) {
									$self->glog("$prefix UPDATE target to source pk $pkval");
									$count = $sth{source}{$g}{updaterow}->execute(@tgtrow,$pkval);
									$dmlcount{U}{source}{$S}{$T}++;
								}
								else {
									$self->glog("$prefix INSERT target to source pk $pkval");
									$count = $sth{source}{$g}{insertrow}->execute($pkval,@tgtrow);
									$dmlcount{I}{source}{$S}{$T}++;
								}
							}
							if ($action & 4) { ## Source to source
								if (defined $info1->{$pkval}{$namepk}) {
									$self->glog("$prefix UPDATE source to source pk $pkval");
									$count = $sth{source}{$g}{updaterow}->execute(@srcrow,$pkval);
									$dmlcount{U}{source}{$S}{$T}++;
								}
								else {
									$self->glog("$prefix INSERT source to source pk $pkval");
									$count = $sth{source}{$g}{insertrow}->execute($pkval,@srcrow);
									$dmlcount{I}{source}{$S}{$T}++;
								}
							}
							if ($action & 8) { ## Target to target
								if (defined $info2->{$pkval}{$namepk}) {
									$self->glog("$prefix UPDATE target to target pk $pkval");
									$count = $sth{target}{$g}{updaterow}->execute(@tgtrow,$pkval);
									$dmlcount{U}{target}{$S}{$T}++;
								}
								else {
									$self->glog("$prefix INSERT target to target pk $pkval");
									$count = $sth{target}{$g}{insertrow}->execute($pkval,@tgtrow);
									$dmlcount{I}{target}{$S}{$T}++;
								}
							}
							## XXX Move this elsewhere?
							if ($g->{does_makedelta}) {
								if ($action & 2 or $action & 4) {
									$sth{source}{$g}{insertdelta}->execute($g->{oid},$pkval);
									$self->glog("Adding in source bucardo_delta row (upsert) for $g->{oid} and $pkval");
								}
								if ($action & 1 or $action & 8) {
									$sth{target}{$g}{insertdelta}->execute($toid,$pkval);
									$self->glog("Adding in target bucardo_delta row (upsert) for $toid and $pkval");
								}
							}
						}; ## end eval block
					} ## end GENX block

					if (!$g->{has_exception_code}) {
						if ($@) {
							$self->glog("Warning! Aborting due to exception for $S.$T.$qnamepk: $pkval Error was $@");
							die $@;
						}
					}
					elsif ($@) {

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
									pkeytype     => $g->{pkeytype},
									pkey         => $pkval,
									action       => 0,
									dbi_error    => $DBI::errstr,
									source_error => $sourcedbh->err ? 1 : 0,
									target_error => $targetdbh->err ? 1 : 0,
								);

						$self->glog("Rolling back to savepoints, due to database error: $DBI::errstr");
						$sourcedbh->pg_rollback_to("bucardo_$$");
						$targetdbh->pg_rollback_to("bucardo_$$");

						## Run the exception handler(s)
						my $runagain = 0;
						for my $code (@{$g->{code_exception}}) {
							$self->glog("Trying exception code $code->{id}: $code->{name}");
							my $result = run_custom_code($code, 'strict');
							if ($result eq 'next') {
								$self->glog("Going to next available exception code");
								next;
							}
							if ($result eq 'redo') { ## redo rollsback source and target
								$self->glog("Exception handler requested redoing the sync");
								redo KID;
							}
							if ($input->{runagain}) {
								$self->glog("Exception handler thinks we can try again");
								$runagain = 1;
								last;
							}
						}

						if (!$runagain) {
							$self->glog("No exception handlers were able to help, so we are bailing out");
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
						redo SAVEPOINT;

					} ## end exception and savepointing
				} ## end each PKEY

				if ($g->{has_exception_code}) {
					$sourcedbh->pg_release("bucardo_$$");
					$targetdbh->pg_release("bucardo_$$");
				}

				## Add in makedelta rows for bucardo_track as needed
				if ($g->{does_makedelta}) {
					if ($dmlcount{D}{source}{$S}{$T} or $dmlcount{U}{source}{$S}{$T} or $dmlcount{I}{source}{$S}{$T}) {
						$sth{source}{inserttrack}->execute($g->{oid},$targetdb);
						$self->glog("Added makedelta bucardo_track row for $S.$T on $sourcedb ($g->{oid},$targetdb)");
					}
					if ($dmlcount{D}{target}{$S}{$T} or $dmlcount{U}{target}{$S}{$T} or $dmlcount{I}{target}{$S}{$T}) {
						$sth{target}{inserttrack}->execute($toid,$sourcedb);
						$self->glog("Added makedelta bucardo_track row for $S.$T on $targetdb ($toid,$sourcedb)");
					}
				}

				## Update both bucardo trackers for this table
				$deltacount{allsource} and $sth{source}{$g}{track}->execute();
				$deltacount{alltarget} and $sth{target}{$g}{track}->execute();

				$dmlcount{allinserts}{source} += $dmlcount{I}{source}{$S}{$T};
				$dmlcount{allupdates}{source} += $dmlcount{U}{source}{$S}{$T};
				$dmlcount{alldeletes}{source} += $dmlcount{D}{source}{$S}{$T};
				$dmlcount{allinserts}{target} += $dmlcount{I}{target}{$S}{$T};
				$dmlcount{allupdates}{target} += $dmlcount{U}{target}{$S}{$T};
				$dmlcount{alldeletes}{target} += $dmlcount{D}{target}{$S}{$T};

			} ## end SAVEPOINT

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

		else {
			$self->glog("UNKNOWN synctype $synctype: bailing");
			die qq{Unknown sync type $synctype};
		}

		# Run all 'before_trigger_enable' code
		for my $code (@{$sync->{code_before_trigger_enable}}) {
			my $result = run_custom_code($code, 'strict');
			if ($result eq 'redo') { ## redo rollsback source and target
				redo KID if $kidsalive;
				last KID;
			}
		}

		if ($SQL{disable_trigrules} or $SQL{enable_trigrules}) {
			die "Invalid enable_trigrules!\n" if ! $SQL{enable_trigrules};
			$self->glog(qq{Enabling triggers and rules});
			$sourcedbh->do($SQL{enable_trigrules}) if $synctype eq 'swap';
			$targetdbh->do($SQL{enable_trigrules});
		}

		# Run all 'after_trigger_enable' code
		for my $code (@{$sync->{code_after_trigger_enable}}) {
			my $result = run_custom_code($code, 'strict');
			if ($result eq 'redo') { ## redo rollsback source and target
				redo KID if $kidsalive;
				last KID;
			}
		}

		if ($self->{dryrun}) {
			$self->glog("Dryrun, rolling back...");
			$targetdbh->rollback();
			$sourcedbh->rollback();
			$maindbh->rollback();
		}
		else {
			$self->glog("Issuing final commit for source and target");
			$sourcedbh->commit();
			$targetdbh->commit();
		}

		## Mark as done in the q table, and notify the parent directly
		$self->glog("Marking as done in the q table, notifying controller");
		$sth{qend}->execute($dmlcount{allupdates}{source}+$dmlcount{allupdates}{target},
							$dmlcount{allinserts}{source}+$dmlcount{allinserts}{target},
							$dmlcount{alldeletes}{source}+$dmlcount{alldeletes}{target},
							$syncname,$targetdb,$$);
		my $notify = "bucardo_syncdone_${syncname}_$targetdb";
		$maindbh->do("NOTIFY $notify") or die "NOTIFY $notify failed!";
		$maindbh->commit();

		my $total_time = time() - $start_time;
		$self->glog("Finished syncing. Time: $total_time. Updates: $dmlcount{allupdates}{source}+$dmlcount{allupdates}{target} Inserts: $dmlcount{allinserts}{source}+$dmlcount{allinserts}{target} Deletes: $dmlcount{alldeletes}{source}+$dmlcount{alldeletes}{target} Sync: $syncname. Keepalive: $kidsalive");

		## Remove lock file if we used it
		if ($lock_table_mode and -e $force_lock_file) {
		  $self->glog("Removing lock control file $force_lock_file");
		  unlink $force_lock_file;
		}

		# Run all 'after_txn' code
		for my $code (@{$sync->{code_after_txn}}) {
			my $result = run_custom_code($code, 'nostrict');
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
	$SQL = qq{
        UPDATE bucardo.audit_pid
        SET    killdate = timeofday()::timestamp
        WHERE  type='KID'
        AND    sync=?
        AND    ppid=?
        AND    pid =?
        AND    killdate IS NULL
    };
	$sth = $maindbh->prepare($SQL);
	$sth->execute($syncname,$self->{ppid},$$);
	$maindbh->commit();
	$maindbh->disconnect();

	$sourcedbh->rollback();
	$sourcedbh->disconnect();
	$targetdbh->rollback();
	$targetdbh->disconnect();

	$self->glog("Kid exiting");
	$self->{clean_exit} = 1;
	exit;

} ## end of start_kid


sub connect_database {

	## Given a database id, return a database handle for it
	## Returns 'inactive' if the database is inactive according to the db table

	my $self = shift;

	my $id = shift || 0;

	my ($dsn,$dbh,$user,$pass);

	## If id is 0, connect to the main database
	if (!$id) {
		$dsn = "dbi:Pg:dbname=$self->{dbname}";
		defined $self->{dbport} and length $self->{dbport} and $dsn .= ";port=$self->{dbport}";
		defined $self->{dbhost} and length $self->{dbhost} and $dsn .= ";host=$self->{dbhost}";
		defined $self->{dbconn} and length $self->{dbconn} and $dsn .= ";$self->{dbconn}";
		$user = $self->{dbuser};
		$pass = $self->{dbpass};
	}
	else {
		my $db = $self->dbs;
		exists $db->{$id} or die qq{Invalid database id!: $id\n};

		my $d = $db->{$id};
		if ($d->{status} ne 'active') {
			return 'inactive';
		}

		$dsn = "dbi:Pg:dbname=$d->{dbname}";
		defined $d->{dbport} and length $d->{dbport} and $dsn .= ";port=$d->{dbport}";
		defined $d->{dbhost} and length $d->{dbhost} and $dsn .= ";host=$d->{dbhost}";
		length $d->{dbconn} and $dsn .= ";$d->{dbconn}";
		$user = $d->{dbuser};
		$pass = $d->{dbpass} || '';
	}

	$dbh = DBI->connect
		(
		 $dsn,
		 $user,
		 $pass,
		 {AutoCommit=>0, RaiseError=>1, PrintError=>0}
	);

	if (!$id) {
		## Prepend bucardo to the search path
		$dbh->do("SELECT pg_catalog.set_config('search_path', 'bucardo,' || current_setting('search_path'), false)");
		$dbh->commit();
	}

	return $dbh;

} ## end of connect_database


sub deactivate_sync {

	## Request a named sync be deactivated
	my ($self,$syncname) = @_;

	if (!defined $syncname or ! length $syncname) {
		die qq{Must provide a syncname\n};
	}

	my $sync = $self->get_syncs;

	if (! exists $sync->{$syncname}) {
		die qq{Could not find a sync named "$syncname"};
	}

	my $dbh = $self->{masterdbh} or die qq{No database connection!\n};

	my $msg = "bucardo_deactivate_sync_$syncname";

	$dbh->do("NOTIFY $msg");
	$dbh->commit();

	return;

} ## end of deactivate_sync


sub activate_sync {

	my ($self,$syncname) = @_;

	if (!defined $syncname or ! length $syncname) {
		die qq{Must provide a syncname\n};
	}

	my $sync = $self->get_syncs;

	if (! exists $sync->{$syncname}) {
		die qq{Could not find a sync named "$syncname"};
	}

	my $dbh = $self->{masterdbh} or die qq{No database connection!\n};

	my $msg = "bucardo_activate_sync_$syncname";

	$dbh->do("NOTIFY $msg");
	return $dbh->commit();

} ## end of activate_sync


sub reload_all_syncs {

	my ($self) = @_;

	my $dbh = $self->{masterdbh} or die qq{No database connection!\n};

	my $msg = "bucardo_mcp_reload";

	$dbh->do("NOTIFY $msg");
	return $dbh->commit();

} ## end of reload_all_syncs


sub send_mail {

	my ($self,$arg) = @_;

	my $from = getpwuid($>) . '@' . $hostname;

	if ($config{default_email_from}) {
		$from = $config{default_email_from};
	}

	$arg->{to} ||= $config{default_email_to};
	$arg->{subject} ||= 'Bucardo Mail!';
	if (! $arg->{body}) {
		$self->glog("ERROR: Cannot send mail, no body message");
		return;
	}

	if ($self->{sendmail} and $arg->{to} ne 'nobody@example.com') {
		my $ret = Mail::Sendmail::sendmail(
						   To      => $arg->{to},
						   From    => $from,
						   Message => $arg->{body},
						   Subject => $arg->{subject},
						   );
		if ($ret) {
			$self->glog("Sent an email to $arg->{to}: $arg->{subject}");
		}
		else {
			my $error = $Mail::Sendmail::error || '???';
			$self->glog("Warning: Error sending email to $arg->{to}: $error");
		}
	}

	if ($ENV{BUCARDO_SENDMAIL_FILE}) {
		$SENDMAIL_FILE = $ENV{BUCARDO_SENDMAIL_FILE};
	}
	if ($SENDMAIL_FILE) {
		my $fh;
		if (! open $fh, '>>', $SENDMAIL_FILE) {
			$self->glog(qq{Warning: Could not open sendmail file "$SENDMAIL_FILE": $!\n});
			return;
		}
		my $now = scalar localtime;
		print $fh qq{
==========================================
To: $arg->{to}
From: $from
Subject: $arg->{subject}
Date: $now
$arg->{body}

};
		close $fh or warn qq{Could not close "$SENDMAIL_FILE": $!\n};
	}

	return;

} ## end of send_mail

1;


__END__

=pod

=head1 NAME

Bucardo - Postgres multi-master replication system

=head1 VERSION

This documents describes Bucardo version 3.1.0

=head1 SYNOPSIS

  ## Import the schema into the main Bucardo database:
  $ psql -U bucardo bucardo -f bucardo.schema

  ## Populate the tables within

  ## Start Bucardo up
  $ ./bucardo_ctl start "Initial startup - Jean"

  ## Kick off a sync manually
  $ ./bucardo_ctl kick prices

  ## Check on the status of all syncs
  $ ./bucardo_ctl status

  ## Shut Bucardo down
  $ ./bucardo_ctl stop "Bringing new server online - Adele"

=head1 WEBSITE

The latest news and documentation can always be found at:

http://bucardo.org/

=head1 DESCRIPTION

Bucardo is a Perl module that replicates Postgres databases using a combination 
of Perl, a custom database schema, Pl/Perlu, and Pl/Pgsql.

Bucardo is unapologetically extremely verbose in its logging.

Full documentation can be found on the website, or in the files that came with 
this distribution.

=head1 DEPENDENCIES

* DBI
* DBD::Pg
* Moose
* IO::Handle
* Mail::Sendmail
* Sys::Hostname
* Sys::Syslog

=head1 BUGS

Bugs should be reported to bucardo-general@bucardo.org. A list of bugs can be found at 
http://bucardo.org/bugs.html

=head1 CREDITS

Bucardo was originally developed and funded by Backcountry.com, who have been using versions 
of it in production since 2002.

=head1 AUTHOR

Greg Sabino Mullane <greg@endpoint.com>

=head1 LICENSE AND COPYRIGHT

Copyright (c) 2005-2008 Greg Sabino Mullane <greg@endpoint.com>.

This software is free to use: see the LICENSE file for details.

=cut
