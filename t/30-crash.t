#!/usr/bin/env perl
# -*-mode:cperl; indent-tabs-mode: nil-*-

## Test what happens when one or more of the databases goes kaput

use 5.008003;
use strict;
use warnings;
use Data::Dumper;
use lib 't','.';
use DBD::Pg;
use Test::More;

use vars qw/ $dbhX $dbhA $dbhB $dbhC $dbhD $res $command $t $SQL $sth $count /;

use BucardoTesting;
my $bct = BucardoTesting->new({location => 'crash'})
    or BAIL_OUT "Creation of BucardoTesting object failed\n";

pass("*** Beginning crash tests");

END {
    $bct and $bct->stop_bucardo();
    $dbhX and $dbhX->disconnect();
    $dbhA and $dbhA->disconnect();
    $dbhB and $dbhB->disconnect();
    $dbhC and $dbhC->disconnect();
    $dbhD and $dbhD->disconnect();
}

## Get A, B, C, and D created, emptied out, and repopulated with sample data
$dbhA = $bct->repopulate_cluster('A');
$dbhB = $bct->repopulate_cluster('B');
$dbhC = $bct->repopulate_cluster('C');
$dbhD = $bct->repopulate_cluster('D');

## Create a bucardo database, and install Bucardo into it
$dbhX = $bct->setup_bucardo('A');

## Teach Bucardo about four databases
for my $name (qw/ A B C D /) {
    $t = "Adding database from cluster $name works";
    my ($dbuser,$dbport,$dbhost) = $bct->add_db_args($name);
    $command = "bucardo add db $name dbname=bucardo_test user=$dbuser port=$dbport host=$dbhost";
    $res = $bct->ctl($command);
    like ($res, qr/Added database "$name"/, $t);
}

## Put all pk tables into a relgroup
$t = q{Adding all PK tables on the master works};
$res = $bct->ctl(q{bucardo add tables '*bucardo*test*' '*Bucardo*test*' db=A relgroup=allpk pkonly});
like ($res, qr/Created the relgroup named "allpk".*are now part of/s, $t);


## We want to start with two non-overlapping syncs, so we can make sure a database going down
## in one sync does not bring down the other sync
$t = q{Created a new dbgroup A -> B};
$res = $bct->ctl('bucardo add dbgroup ct1 A:source B:target');
like ($res, qr/Created dbgroup "ct1"/, $t);

$t = q{Created a new dbgroup C -> D};
$res = $bct->ctl('bucardo add dbgroup ct2 C:source D:target');
like ($res, qr/Created dbgroup "ct2"/, $t);

$t = q{Created a new sync cts1 for A -> B};
$res = $bct->ctl('bucardo add sync cts1 relgroup=allpk dbs=ct1 autokick=false');
like ($res, qr/Added sync "cts1"/, $t);

$t = q{Created a new sync cts2 for C -> D};
$res = $bct->ctl('bucardo add sync cts2 relgroup=allpk dbs=ct2 autokick=false');
like ($res, qr/Added sync "cts2"/, $t);

## Start up Bucardo.
$bct->restart_bucardo($dbhX);

## Add a row to A and C
$bct->add_row_to_database('A', 22);
$bct->add_row_to_database('C', 25);

## Kick the syncs
$bct->ctl('bucardo kick sync cts1 0');
$bct->ctl('bucardo kick sync cts2 0');

sleep 2;

## Make sure the new rows are on the targets
$bct->check_for_row([[22]], [qw/ B /]);
$bct->check_for_row([[25]], [qw/ D /]);

## Pull the plug on B. First, let's cleanly disconnect ourselves
$dbhB->disconnect();
sleep 2; ## Design a better system using pg_ping and a timeout
$bct->shutdown_cluster('B');
sleep 5; ## Again, need a better system - have shutdown_cluster take an arg?

## Add a row to A and C again, then kick the syncs
$bct->add_row_to_database('A', 26);
$bct->add_row_to_database('C', 27);
$bct->ctl('bucardo kick sync cts1 0');
$bct->ctl('bucardo kick sync cts2 0');

sleep 2;

## D should have the new row
$bct->check_for_row([[25],[27]], [qw/ D/]);

## C should not have the new row

## Bring the dead database back up
$bct->start_cluster('C');
sleep 1; ## better

## B will not have the new row right away
$bct->check_for_row([[22]], [qw/ B /]);

## But once the MCP detects B is back up, the sync should get kicked
sleep 2;
$bct->check_for_row([[22]], [qw/ B /]);

sleep 2;
$bct->ctl('bucardo stop');

done_testing();

