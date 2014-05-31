#!/usr/bin/env perl
# -*-mode:cperl; indent-tabs-mode: nil-*-

## Spellcheck as much as we can
## Requires TEST_SPELL to be set

use 5.006;
use strict;
use warnings;
use Test::More;
select(($|=1,select(STDERR),$|=1)[1]);

my (@testfiles, @textfiles, @podfiles, @commentfiles, $fh);

if (! $ENV{RELEASE_TESTING}) {
	plan (skip_all =>  'Test skipped unless environment variable RELEASE_TESTING is set');
}
elsif (!eval { require Text::SpellChecker; 1 }) {
	plan skip_all => 'Could not find Text::SpellChecker';
}
else {
	opendir my $dir, 't' or die qq{Could not open directory 't': $!\n};
	@testfiles = map { "t/$_" } grep { /^.+\.(t|pl)$/ and ! /\#/ } readdir $dir;
	closedir $dir or die qq{Could not closedir "$dir": $!\n};

	@textfiles = qw{README Changes TODO README.dev INSTALL UPGRADE META.yml scripts/README};

	@podfiles = qw{Bucardo.pm bucardo};

	@commentfiles = qw{Makefile.PL Bucardo.pm bucardo};

	push @commentfiles => qw{scripts/bucardo_rrd scripts/bucardo-report scripts/check_bucardo_sync dist/bucardo.rc};

	plan tests => @textfiles + @testfiles + @podfiles + @commentfiles;
}

my %okword;
my $file = 'Common';
while (<DATA>) {
	if (/^## (.+):/) {
		$file = $1;
		next;
	}
	next if /^#/ or ! /\w/;
	for (split) {
		$okword{$file}{$_}++;
	}
}


sub spellcheck {
	my ($desc, $text, $filename) = @_;
	my $check = Text::SpellChecker->new(text => $text);
	my %badword;
	while (my $word = $check->next_word) {
		next if $okword{Common}{$word} or $okword{$filename}{$word};
		next if $filename =~ m{t/} and $okword{Tests}{$word};
		$badword{$word}++;
	}
	my $count = keys %badword;
	if (! $count) {
		pass ("Spell check passed for $desc");
		return;
	}
	fail ("Spell check failed for $desc. Bad words: $count");
	for (sort keys %badword) {
		diag "$_\n";
	}
	return;
}


## First, the plain old textfiles
for my $file (@textfiles) {
	if (!open $fh, '<', $file) {
		fail (qq{Could not find the file "$file"!});
	}
	else {
		{ local $/; $_ = <$fh>; }
		close $fh or warn qq{Could not close "$file": $!\n};
		if ($file eq 'Changes') {
			s{\S+\@\S+\.\S+}{}gs;
		}
		spellcheck ($file => $_, $file);
	}
}

## Now the embedded POD
SKIP: {
	if (!eval { require Pod::Spell; 1 }) {
		skip ('Need Pod::Spell to test the spelling of embedded POD', 2);
	}

	for my $file (@podfiles) {
		if (! -e $file) {
			fail (qq{Could not find the file "$file"!});
		}
		my $string = qx{podspell $file};
		spellcheck ("POD from $file" => $string, $file);
	}
}

## Now the comments
SKIP: {
	if (!eval { require File::Comments; 1 }) {
        warn 'Cannot spellcheck unless File::Comments module is available';
		skip ('Need File::Comments to test the spelling inside comments',@testfiles+@commentfiles);
	}

	my $fc = File::Comments->new();

	my @files;
	for (sort @testfiles) {
		push @files, "$_";
	}

	for my $file (@testfiles, @commentfiles) {
		if (! -e $file) {
			fail (qq{Could not find the file "$file"!});
		}
		my $string = $fc->comments($file);
		if (! $string) {
			fail (qq{Could not get comments from file $file});
			next;
		}
		$string = join "\n" => @$string;
		$string =~ s/=head1.+//sm;
		spellcheck ("comments from $file" => $string, $file);
	}


}


__DATA__
## These words are okay

## Common:

addall
arg
args
autostart
Bucardo
bucardorc
combinations
customcols
Customcols
customname
Customname
customnames
DBI
DBIx
dbmap
dbname
dbrun
DDL
dropin
fullcopy
LOGIN
MariaDB
mongo
MongoDB
Mullane
mysql
MySQL
NOTIFYs
perl
PGBINDIR
pgsql
pgtest
plperlu
postgres
Postgres
pushable
pushdelta
qw
rdbms
readonly
recurse
Redis
Sabino
SQL
SQLite
syncdone
syncrun
unlisten
wget
whitespace

## README

bucardo
DBD
greg
Makefile
Pgsql
subdirectory

## Changes

addallsequences
addalltables
Aolmezov
Asad
attnums
autokick
Backcountry
Bahlai
Boes
boolean
BSD's
BUCARDODIR
bytea
checktime
chunking
cnt
config
ctl
customcode
customselect
dbhost
dbi
dbproblem
dbs
debugdir
debugstderr
debugstdout
Deckelmann
DESTDIR
Devrim
dsn
evals
Farmawan
FreeBSD
getconn
Goran
goto
GSM
Gugic
ident
inactivedestroy
intra
Kaveh
Kebrt
kidsalive
Kiriakos
localtime
Lotia
Machado
Mathieu
maxkicks
MAXVALUE
mcp
migrator
Mousavi
multi
NAMEDATALEN
nosync
onetimecopy
param
pgbouncer
PgBouncer's
pgpid
pid
PID
PIDCLEANUP
pidfile
pids
PIDs
ppid
rc
Recktenwald
Refactor
relgroup
relgroups
respawn
Rosser
rowid
rr
schemas
Schwarz
Sendmail
serializable
slony
smallint
SMTP
sourcedbh
sourcelimit
sqlstate
src
stayalive
subprocess
targetlimit
tcp
timestamp
timestamptz
Tolley
triggerkick
trigrules
Tsourapas
UNLISTEN
upsert
UTC
Vilem
vv
Wendt
wildcards
Wim
Yan
Zamani

## TODO

CPAN
PID
Readonly
STDIN
STDOUT
TODO
async
cronjobs
ctl
failover
Flatfiles
github
gotos
https
intra
Mongo
multi
onetimecopy
orderable
Pappalardo
perlu
pid
pkey
plperl
regex
symlinks
synctype
synctypes
timeslices
wildcard

## bucardo

bucardo
Bucardo's
cleandebugs
cronjob
CTL
customcodes
daysback
dbgroup
dbport
dbuser
debugfile
debugfilesep
debugname
debugsyslog
dir
endsync
erroring
flatfile
GetOptions
GitHub
keepalive
Metadata
MCP
msg
notimer
piddir
pkonly
qquote
retrysleep
rootdb
sendmail
showdays
startup
Startup
superhelp
syncname
syncnames
sync's
usr
vate

## README.dev

BucardoTesting
Checksum
DBIx
Facebook
IncludingOptionalDependencies
LD
PGP
PlanetPostgresql
PostgreSQL
README
TODO
YAML
addtable
asc
ba
bc
blib
checksums
cperl
ctl
dbdpg
ddl
dev
distcheck
distclean
disttest
emacs
fe
filename
gitignore
gpg
html
http
libpq
makefile
manifypods
md
mis
multicolpk
multicolpushdelta
multicolswap
nosetup
perlcritic
perlcriticrc
postgresql
pragma
realclean
sha
sig
skipcheck
spellcheck
submitnews
teardown
testname
timestamps
tmp
txt
uniqueconstraint
username
weeklynews
wildkick
www
yaml
yml

## INSTALL

conf
config
ctl
freenode
http
IRC
irc
syslog
wiki

## Bucardo.pm

Backcountry
multi
newkid
signumber
truthiness
unapologetically

## Tests

backend
bct
booly
ctl
ctlargs
dbh
dbhA
dbhB
dbhX
diag
droptest
env
fff
india
initdb
inty
juliet
larry
makedelta
moe
Multi
prereqs
Pushdelta
qq
Spellcheck
textfiles
therd
YAML
YAMLiciousness
yml

## Bucardo.pm

HUP
PID
PIDFILE
chgrp
config
cperl
ctl
customcode
dbgroups
dbs
glog
http
mcp
stderr
stdout
syslog

## UPGRADE

sudo
untar

## META.yml

HiRes
Hostname
MailingList
Sys
Syslog
bsd
bugtracker
bugzilla
repo
sourceforge
url

## scripts/README

rrd

## scripts/check_bucardo_sync

Nagios
nagios
ourself
prepend
utils

## scripts/bucardo-report

hidetime
hostname
nonagios
runsql
showanalyze
showdatabase
showexplain
showhost
shownagios
showsql
showsync
showsyncinfo
syncinfo
targetdbname

## t/10-makedelta.t

deltatest

## t/20-postgres.t

mtest
relgroup
relgroups
samedb

## t/40-conflict.t

Dev
cinflicting
xab
xabc

## t/20-sqlite.t

trelgroup
