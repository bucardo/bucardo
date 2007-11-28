#!/usr/bin/perl

## Update a Bucardo database to most recent version

use strict;
use warnings;
use DBI;
use Getopt::Long;
use Data::Dumper;

my %opt =
	(
	 commit     => 0,
	 schemafile => '../bucardo.schema',
	 quiet      => 0,
	 verbose    => 0,
	 );
GetOptions(\%opt,
		   'commit',
		   'quiet+',
		   'verbose+',
		   'schemafile=s',
		   'dbuser=s',
		   'dbname=s',
		   'dbport=i',
		   'dbhost=s',
);

my $quiet   = $opt{quiet} || 0;
my $verbose = $opt{verbose} || 0;

my $DBPORT = $opt{dbport} || '';
my $DBHOST = $opt{dbhost} || '';
my $DBUSER = $opt{dbuser} || 'bucardo';
my $DBPASS = '';
my $DBNAME = $opt{dbname} || 'bucardo';

my $DSN = "dbi:Pg:dbname=$DBNAME";
length $DBHOST and $DSN .= ";host=$DBHOST";
length $DBPORT and $DSN .= ";port=$DBPORT";

## Connect to the database holding your Bucardo information
my $dbh = DBI->connect($DSN,$DBUSER,$DBPASS,{AutoCommit=>0, RaiseError=>1});
my ($SQL,$sth,$count,$COM,$version);
$dbh->do("SET search_path = bucardo");

$version = '3.0.8';

## 3.0.8: pkeytypes are done automatically, no need to limit what types we support
drop_constraint('goat','goat_pkeytype_check');

## 3.0.8: The validate_goat function was completely rewritten
reload_function('validate_goat');

## 3.0.8: New column added to goat of 'qpkey'
add_column('goat.qpkey TEXT NULL');

## 3.0.8: No longer need pkey and pkeytype to be not null or have defaults
column_attribs({table => 'goat', attribs=>'nullok nodefault', cols=>'pkey pkeytype'});

finishup();

sub column_attribs {

	my $arg = shift;
	my $table = $arg->{table};
	for my $col (split /\s+/ => $arg->{cols}) {
		my $colinfo = column_info($table,$col);
		next if !defined $colinfo;
		for my $change (split /\s+/ => $arg->{attribs}) {
			if ('nullok' eq $change) {
				next if ! $colinfo->{attnotnull};
				$COM = "ALTER TABLE $table ALTER $col DROP NOT NULL";
				$verbose and print "Running: $COM\n";
				$dbh->do($COM);
			}
			elsif ('nodefault' eq $change) {
				next if ! $colinfo->{atthasdef};
				$COM = "ALTER TABLE $table ALTER $col DROP DEFAULT";
				$verbose and print "Running: $COM\n";
				$dbh->do($COM);
			}
			else {
				die "Unknown column attrib: $change\n";
			}
		}
 	}
	return;
}


sub column_info {

	my ($table,$col) = @_;
	$SQL = "SELECT a.* FROM pg_attribute a, pg_class c, pg_namespace n WHERE c.oid = a.attrelid AND n.nspname = 'bucardo' ";
	$SQL .= "AND n.oid = c.relnamespace AND c.relname=? AND a.attname=?";
	$sth = $dbh->prepare($SQL);
	$count = $sth->execute($table,$col);
	if ($count eq '0E0') {
		$sth->finish();
		$verbose and print qq{Could not find column '$col' of table '$table'\n};
		return;
	}
	return $sth->fetchall_arrayref({})->[0];
}


sub drop_constraint {

	my ($table,$name) = @_;
	if (got_constraint($name)) {
		$COM = "ALTER TABLE goat DROP CONSTRAINT goat_pkeytype_check";
		$verbose and print "Running: $COM\n";
		$dbh->do($COM);
		$quiet or print qq{Version $version: dropped constraint '$name' from table '$table'\n};
	}
	else {
		$verbose and print "Did not find constraint '$name' on table '$table'\n";
	}
	return;
}

sub add_column {

	my $string = shift;
	die "Invalid add_column call: $string\n"
		if $string !~ /(\w+)\.(\w+) (\w.+)/;
	my ($table,$col,$def) = ($1,$2,$3);

	my $colinfo = column_info($table,$col);
	if (defined $colinfo) {
		$verbose and print qq{Column '$col' of table '$table' already exists\n};
		return;
	}
	$COM = "ALTER TABLE $table ADD $col $def";
	$verbose and print qq{Running: $COM\n};
	$dbh->do($COM);
	$quiet or print qq{Version $version: added column '$col' to table '$table'\n};
	return;
}


sub reload_function {

	my $name = shift;

	my $file = $opt{schemafile};
	die "Bad schemafile: $file\n" unless $file =~ m{^[\w\.//]+$};

	open my $fh, '<', $file or die qq{Could not open "$file": $!\n};
	my $point = 0;
	my $slurp = '';
	while (<$fh>) {
		if (!$point) {
			next unless /^CREATE FUNCTION $name\b/;
			$verbose and print qq{Found function $name at line $. of $file\n};
			$point = 1;
		}
		$slurp .= $_;
		last if /^\$bc\$;\E/;
	}
	close $fh or die qq{Could not close "$file": $!\n};
	$slurp =~ s/CREATE FUNCTION/CREATE OR REPLACE FUNCTION/;
	my $lines = $slurp =~ y/\n/\n/;
	$dbh->do($slurp);
	$quiet or print "Version $version: replaced function $name: $lines lines\n";
	return;
}


sub got_constraint {

	my $name = shift;
	$SQL = "SELECT count(*) FROM pg_constraint c, pg_namespace n WHERE n.oid=c.connamespace AND nspname = 'bucardo' AND conname = ?";
	$sth = $dbh->prepare($SQL);
	$sth->execute($name);
	return $sth->fetchall_arrayref()->[0][0];
}

sub finishup {

	if ($opt{commit}) {
		print "Commit changes? [y/n]\n";
		if (<> =~ /y/i) {
			$dbh->commit();
			print "Changes committed.\n";
			$dbh->disconnect();
			exit;
		}
	}
	print "Rolling back changes.\n";
	$opt{commit} or print "Run with --commit argument to make permanent\n";
	$dbh->rollback();
	$dbh->disconnect();
	return;
}
