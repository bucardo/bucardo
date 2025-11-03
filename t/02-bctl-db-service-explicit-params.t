#!/usr/bin/env perl
# -*-mode:cperl; indent-tabs-mode: nil-*-

## Test that explicitly provided connection parameters are preserved when using dbservice
##
## This test verifies that when a user provides explicit connection parameters
## (dbhost, dbport, dbname, dbuser, etc.) along with dbservice, those parameters
## are stored in the bucardo.db table rather than being stripped out.
##
## This is important for scenarios where:
## - Users want to track which host/port a service points to
## - Users want the ability to override service settings
## - Different environments need different connection strategies

use 5.008003;
use strict;
use warnings;
use Data::Dumper;
use lib 't','.';
use DBD::Pg;
use Test::More;

use vars qw/$t $res $command $dbhX $dbhA/;

use BucardoTesting;
my $bct = BucardoTesting->new({notime=>1})
    or BAIL_OUT "Creation of BucardoTesting object failed\n";
$location = '';

## Make sure A is started up (for bucardo database)
$dbhA = $bct->repopulate_cluster('A');

## Create a bucardo database, and install Bucardo into it
$dbhX = $bct->setup_bucardo('A');

plan tests => 8;

## Test 1: Add database with dbservice and explicit parameters
## Use --force to skip connection test since the service doesn't actually exist
$t = 'Add database with dbservice and explicit connection parameters';
$res = $bct->ctl('bucardo add db testdb1 dbhost=example.com dbport=5431 dbname=mydb dbuser=myuser dbservice=myservice ssp=false --force');
like ($res, qr/Added database "testdb1"/, $t);

## Test 2: Verify dbhost is stored
$t = 'dbhost is preserved when explicitly provided with dbservice';
$res = $dbhX->selectrow_array("SELECT dbhost FROM bucardo.db WHERE name='testdb1'");
is ($res, 'example.com', $t);

## Test 3: Verify dbport is stored
$t = 'dbport is preserved when explicitly provided with dbservice';
$res = $dbhX->selectrow_array("SELECT dbport FROM bucardo.db WHERE name='testdb1'");
is ($res, 5431, $t);

## Test 4: Verify dbname is stored
$t = 'dbname is preserved when explicitly provided with dbservice';
$res = $dbhX->selectrow_array("SELECT dbname FROM bucardo.db WHERE name='testdb1'");
is ($res, 'mydb', $t);

## Test 5: Verify dbuser is stored
$t = 'dbuser is preserved when explicitly provided with dbservice';
$res = $dbhX->selectrow_array("SELECT dbuser FROM bucardo.db WHERE name='testdb1'");
is ($res, 'myuser', $t);

## Test 6: Verify dbservice is stored
$t = 'dbservice is stored correctly';
$res = $dbhX->selectrow_array("SELECT dbservice FROM bucardo.db WHERE name='testdb1'");
is ($res, 'myservice', $t);

## Test 7: Add database with only dbservice (no explicit params)
$t = 'Add database with only dbservice (no explicit connection params)';
$res = $bct->ctl('bucardo add db testdb2 dbservice=anotherservice --force');
like ($res, qr/Added database "testdb2"/, $t);

## Test 8: Verify that without explicit params, fields are empty or NULL
$t = 'dbhost/dbport/dbname are empty or NULL when not explicitly provided with dbservice';
my ($host, $port, $name) = $dbhX->selectrow_array(
    "SELECT dbhost, dbport, dbname FROM bucardo.db WHERE name='testdb2'"
);
ok ((!defined($host) || $host eq '') && (!defined($port) || $port eq '') && (!defined($name) || $name eq ''), $t);

## Cleanup
$bct->ctl('bucardo remove db testdb1');
$bct->ctl('bucardo remove db testdb2');

exit;
