#!/usr/bin/env perl
# -*-mode:cperl; indent-tabs-mode: nil-*-

## Test of customcode to handle exceptions

use 5.008003;
use strict;
use warnings;
use Data::Dumper;
use lib 't','.';
use DBD::Pg;
use Test::More;

use vars qw/ $dbhX $dbhA $dbhB $dbhC $res $command $t $SQL %pkey %sth %sql $sth $count $val /;

use BucardoTesting;
my $bct = BucardoTesting->new({location => 'postgres'})
    or BAIL_OUT "Creation of BucardoTesting object failed\n";

plan tests => 9999;

END {
    $bct and $bct->stop_bucardo($dbhX);
    $dbhX and $dbhX->disconnect();
    $dbhA and $dbhA->disconnect();
    $dbhB and $dbhB->disconnect();
    $dbhC and $dbhC->disconnect();
}

## Get A, B, and C created, emptied out, and repopulated with sample data
$dbhA = $bct->repopulate_cluster('A');
$dbhB = $bct->repopulate_cluster('B');
$dbhC = $bct->repopulate_cluster('C');

## Create a bucardo database, and install Bucardo into it
$dbhX = $bct->setup_bucardo('A');

## Teach Bucardo about three databases
for my $name (qw/ A B C /) {
    $t = "Adding database from cluster $name works";
    my ($dbuser,$dbport,$dbhost) = $bct->add_db_args($name);
    $command = "bucardo add db $name dbname=bucardo_test user=$dbuser port=$dbport host=$dbhost";
    $res = $bct->ctl($command);
    like ($res, qr/Added database "$name"/, $t);
}

## Create a table with a non-primary key unique constraint

## Create a new sync for it

## Add some rows and verify that basic replication is working

## Cause a unique index violation and confirm the sync dies

## Add in a customcode exception handler

## Start the sync and verify the exception handler allows the sync to continue

## Test disabling the customcode

exit;
