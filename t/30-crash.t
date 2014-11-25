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

## SYNC a_b is A => B
$t = q{Created a new sync a_b for A -> B};
$res = $bct->ctl('bucardo add sync a_b relgroup=allpk dbs=A,B autokick=false');
like ($res, qr/Added sync "a_b"/, $t);

## SYNC c_d is C => D
$t = q{Created a new sync c_d for C -> D};
$res = $bct->ctl('bucardo add sync c_d relgroup=allpk dbs=C,D autokick=false');
like ($res, qr/Added sync "c_d"/, $t);

## Change our timeout so our testing doesn't take too long
$bct->ctl('bucardo set mcp_pingtime=10');

## Make sure nobody has any rows yet, then start Bucardo
$bct->check_for_row([], [qw/ A B C D /]);
$bct->restart_bucardo($dbhX);

## Add a row to A and make sure it gets to B
$bct->add_row_to_database('A', 22);
$bct->ctl('bucardo kick sync a_b 0');
$bct->check_for_row([[22]], [qw/ B /]);

## Add a row to C and make sure it gets to D
$bct->add_row_to_database('C', 25);
$bct->ctl('bucardo kick sync c_d 0');
$bct->check_for_row([[25]], [qw/ D /]);

## We are going to kill B and make sure the C->D sync still works
$dbhB->disconnect();
$bct->shutdown_cluster('B');

sleep 20;
## Add a row to A and C again, then kick the syncs
$bct->add_row_to_database('A', 26);
$bct->add_row_to_database('C', 27);
$bct->ctl('bucardo msg xxxx');
$bct->ctl('bucardo kick sync a_b 0');
$bct->ctl('bucardo kick sync c_d 0');

## D should have the new row
$bct->check_for_row([[25],[27]], [qw/ D/]);

## B should not, as it's dead! Listen for an announcement of its resurrection
$dbhX->do('LISTEN bucardo_syncstart_a_b');
$dbhX->commit();

## Bring the dead database back up
$bct->start_cluster('B');

## B will /not/ have the new row right away
$bct->check_for_row([[22]], [qw/ B /]);

diag Dumper $bct->ctl('bucardo status');
diag Dumper $bct->ctl('bucardo status c_d');
diag Dumper $bct->ctl('bucardo update sync c_d active');

$bct->wait_for_notice($dbhX, 'bucardo_syncstart_a_b', 90);
pass('Sync a_b was resurrected');

sleep 30;
$bct->ctl('bucardo kick sync a_b c_d 0');

$bct->check_for_row([[22],[26]], [qw/ B /]);

$bct->ctl('bucardo stop');

done_testing();

