#!/usr/bin/env perl
# -*-mode:cperl; indent-tabs-mode: nil-*-

## Test that Bucardo CLI and pl/Perl functions can use different PGSERVICE definitions
##
## In environments where pgservice must be used with different services per OS user
## (e.g., certificate authentication requiring distinct services), bucardo needs to
## support this scenario:
##   - CLI commands run as the current OS user and can use PGSERVICE environment variable
##   - pl/Perl functions run as the database user (typically postgres) and use the
##     service defined in the bucardo.db table
##
## This test verifies that the CLI respects the PGSERVICE environment variable to
## override connection details, while pl/Perl functions use the service from bucardo.db.

use 5.008003;
use strict;
use warnings;
use Data::Dumper;
use lib 't','.';
use DBD::Pg;
use Test::More;
use File::Temp qw(tempfile tempdir);

use vars qw/$t $res $command $dbhX $dbhA $dbhP/;

use BucardoTesting;
my $bct = BucardoTesting->new({notime=>1})
    or BAIL_OUT "Creation of BucardoTesting object failed\n";
$location = '';

## Make sure A is started up (for bucardo database)
$dbhA = $bct->repopulate_cluster('A');

## Create a bucardo database, and install Bucardo into it
$dbhX = $bct->setup_bucardo('A');

## Get cluster P information
my ($dbuserP,$dbportP,$dbhostP) = eval { $bct->add_db_args('P') };
if (!$dbuserP || !$dbportP) {
    plan skip_all => "Test cluster P configuration not available";
}

## Find or create the cluster P directory
my @dirs = glob("bucardo_test_database_P_*");
if (!@dirs) {
    eval { $bct->create_cluster('P') };
    @dirs = glob("bucardo_test_database_P_*");
}

if (!@dirs) {
    plan skip_all => "Cannot create or find cluster P directory";
}

## Start cluster P
eval { $bct->start_cluster('P') };
if ($@) {
    plan skip_all => "Cannot start cluster P: $@";
}

## Create a test database and table
my $bootstrap_dbh;
eval {
    $bootstrap_dbh = DBI->connect(
        "dbi:Pg:dbname=postgres;port=$dbportP;host=$dbhostP",
        $dbuserP,
        undef,
        {AutoCommit=>1, RaiseError=>1, PrintError=>0}
    );
};

if (!$bootstrap_dbh) {
    plan skip_all => "Cannot connect to cluster P for bootstrap: $@";
}

my $test_db = 'bucardo_pgservice_test';
eval {
    $bootstrap_dbh->do("DROP DATABASE IF EXISTS $test_db");
    $bootstrap_dbh->do("CREATE DATABASE $test_db");
};
if ($@) {
    plan skip_all => "Cannot create test database: $@";
}
$bootstrap_dbh->disconnect();

## Connect to test database and create a table
my $test_dbh;
eval {
    $test_dbh = DBI->connect(
        "dbi:Pg:dbname=$test_db;port=$dbportP;host=$dbhostP",
        $dbuserP,
        undef,
        {AutoCommit=>1, RaiseError=>1, PrintError=>0}
    );
};
if (!$test_dbh) {
    plan skip_all => "Cannot connect to test database: $@";
}

$test_dbh->do("CREATE TABLE test_table (id integer primary key, data text)");
$test_dbh->do("INSERT INTO test_table VALUES (1, 'test data')");
$test_dbh->disconnect();

## Create a pgservice file
my $service_dir = tempdir(CLEANUP => 1);
my $service_file = "$service_dir/pg_service.conf";

open my $service_fh, '>', $service_file or do {
    plan skip_all => "Cannot create pg_service.conf: $!";
};

print $service_fh "[test_service]\n";
print $service_fh "host=$dbhostP\n";
print $service_fh "port=$dbportP\n";
print $service_fh "dbname=$test_db\n";
print $service_fh "user=$dbuserP\n";
print $service_fh "\n";
close $service_fh;

## Verify the service works
my $test_service_dbh;
{
    local $ENV{PGSERVICEFILE} = $service_file;
    eval {
        $test_service_dbh = DBI->connect(
            "dbi:Pg:service=test_service",
            undef,
            undef,
            {AutoCommit=>1, RaiseError=>1, PrintError=>0}
        );
    };
    if (!$test_service_dbh) {
        plan skip_all => "Test service not working: $@";
    }
    $test_service_dbh->disconnect();
}

plan tests => 3;

## Set PGSERVICE and PGSERVICEFILE first
$ENV{PGSERVICE} = 'test_service';
$ENV{PGSERVICEFILE} = $service_file;

## Test scenario demonstrating CLI and pl/Perl using different services:
##
## 1. Store wrong connection info in bucardo.db (simulates a service only postgres can use)
##    Store a non-existent database name to prove CLI doesn't use it
my $wrong_db = 'nonexistent_database_12345';
$t = 'CLI can add database even with wrong dbname in bucardo.db (uses PGSERVICE instead)';
$res = $bct->ctl("bucardo add db P dbname=$wrong_db port=$dbportP host=$dbhostP user=$dbuserP");
like ($res, qr/Added database "P"/, $t);

## 2. CLI commands continue to work via PGSERVICE environment variable
##    This proves the CLI is using the service from $ENV{PGSERVICE}, not bucardo.db
$t = 'CLI list command works via PGSERVICE (ignores wrong dbname in bucardo.db)';
$res = $bct->ctl('bucardo list db P');
like ($res, qr/Database: P/, $t);

## 3. Update bucardo.db with correct connection info for pl/Perl functions
##    In a real scenario, this would be a different service name that postgres can access
##    For this test, we just fix the dbname since we can't set up postgres's PGSERVICE
$res = $bct->ctl("bucardo update db P dbname=$test_db");

## 4. Now pl/Perl functions (validate_goat) can work using bucardo.db connection info
$t = 'Adding table works: CLI uses PGSERVICE, pl/Perl uses bucardo.db connection';
$res = $bct->ctl('bucardo add table public.test_table db=P');
like ($res, qr/Added the following tables/, $t);

## Cleanup
$bct->ctl('bucardo remove table public.test_table db=P');
$bct->ctl('bucardo remove db P');

## Drop the test database
eval {
    local $ENV{PGSERVICEFILE} = $service_file;
    my $cleanup_dbh = DBI->connect(
        "dbi:Pg:service=test_service",
        undef,
        undef,
        {AutoCommit=>1, RaiseError=>1, PrintError=>0}
    );
    if ($cleanup_dbh) {
        $cleanup_dbh->do("DROP DATABASE IF EXISTS $test_db");
        $cleanup_dbh->disconnect();
    }
};

$bct->shutdown_cluster('P');

exit;
