#!/usr/bin/env perl
# -*-mode:cperl; indent-tabs-mode: nil-*-

## Test of conflicts

use 5.008003;
use strict;
use warnings;
use Data::Dumper;
use lib 't','.';
use DBD::Pg;
use Test::More;

use vars qw/ $dbhX $dbhA $dbhB $dbhC $dbhD $res $command $t $SQL %pkey %sth %sql $sth $count $val /;

use BucardoTesting;
my $bct = BucardoTesting->new({location => 'conflict'})
    or BAIL_OUT "Creation of BucardoTesting object failed\n";

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
$dbhD = $bct->repopulate_cluster('D');

## Store into hashes for convenience

my %dbh = (A=>$dbhA, B=>$dbhB, C=>$dbhC, D=>$dbhD);

## Create a bucardo database, and install Bucardo into it
$dbhX = $bct->setup_bucardo('A');

## Teach Bucardo about three databases
for my $name (qw/ A B C D/) {
    $t = "Adding database from cluster $name works";
    my ($dbuser,$dbport,$dbhost) = $bct->add_db_args($name);
    $command = "bucardo add db $name dbname=bucardo_test user=$dbuser port=$dbport host=$dbhost";
    $res = $bct->ctl($command);
    like ($res, qr/Added database "$name"/, $t);
}

$bct->ctl('bucardo remove dbgroup ctest');
$bct->ctl('bucardo remove relgroup ctest');

## Create a new dbgroup with three sources and one target
$t = q{Created a new dbgroup ctest for ( A <=> B <=> C ) -> D};
$res = $bct->ctl('bucardo add dbgroup ctest A:source B:source C:source D:target');
like ($res, qr/Created dbgroup "ctest"/, $t);

## Create a new relgroup with all tables
$t = q{Created a new relgroup ctest};
$res = $bct->ctl('bucardo add relgroup ctest all');
like ($res, qr/Created relgroup "ctest"/, $t);

## Create a new sync
$t = q{Created a new sync named ctest};
$res = $bct->ctl('bucardo add sync ctest dbgroup=ctest relgroup=ctest autokick=false');
like ($res, qr/Added sync "ctest"/, $t);

## Start listening for a syncdone message
$dbhX->do('LISTEN bucardo_syncdone_ctest');
$dbhX->commit();

## Start up Bucardo
$bct->restart_bucardo($dbhX);

## No conflict, just update some rows to make sure the sync is working
$bct->add_row_to_database('A', 1);
$bct->ctl('bucardo kick sync ctest 10');
$bct->check_for_row([[1]], [qw/ B C D/]);

## Create a conflict
$bct->add_row_to_database('A', 2);
$bct->add_row_to_database('B', 2);
$bct->add_row_to_database('C', 2);

$bct->ctl('bucardo kick sync ctest 10');

$bct->check_for_row([[1],[2]], [qw/ A B C D/]);

$t = q{Cannot set conflict handler to invalid database name};
$res = $bct->ctl('bucardo update sync ctest conflict="a b c"');
like($res, qr{is not a db for this sync}, $t);

## Create another conflict, but change our tactics
$t = q{Set conflict handler to valid database name list};
$res = $bct->ctl('bucardo update sync ctest conflict="C A B"');
like($res, qr{Set conflict strategy}, $t);

$bct->ctl('bucardo reload sync ctest');

$bct->update_row_in_database('A', 1, 111);
$bct->update_row_in_database('C', 1, 333);
$bct->update_row_in_database('B', 1, 222);

## Database C should be the winner
$bct->ctl('bucardo kick sync ctest 10');
$bct->check_for_row([[2],[333]], [qw/ A B C D/]);

## Same thing, but C is not changed, so A should win
$bct->update_row_in_database('A', 1, 1111);
$bct->update_row_in_database('B', 1, 2222);
$bct->ctl('bucardo kick sync ctest 10');
$bct->check_for_row([[2],[1111]], [qw/ A B C D/]);

## Test of the default conflict strategy
$bct->update_row_in_database('B', 1, 102);
$bct->update_row_in_database('A', 2, 201);
$bct->update_row_in_database('B', 2, 202);
$bct->add_row_to_database('A', 3);
$bct->update_row_in_database('A', 3, 301);
$bct->update_row_in_database('C', 2, 203);

$t = q{Set conflict strategy to 'bucardo_latest'};
$res = $bct->ctl('bucardo update sync ctest conflict=latest');
like($res, qr{Set conflict strategy to 'bucardo_latest'}, $t);

$t = q{Reloaded sync ctest};
$res = $bct->ctl('bucardo reload sync ctest');
like($res, qr{success}, $t);

## Database B should be the winner for 1, C for 2, and A for 3
$bct->ctl('bucardo kick sync ctest 10');
$bct->check_for_row([[102],[203],[301]], [qw/ A B C D /]);

## Test of conflict handler 'bucardo_latest_all_tables'
$t = q{Set conflict strategy to 'bucardo_latest_all_tables'};
$res = $bct->ctl('bucardo update sync ctest conflict=latest_all');
like($res, qr{Set conflict strategy to 'bucardo_latest_all_tables'}, $t);

$t = q{Reloaded sync ctest};
$res = $bct->ctl('bucardo reload sync ctest');
like($res, qr{success}, $t);

$bct->update_row_in_database('B', 1, 102);
$bct->update_row_in_database('A', 2, 201);
$bct->update_row_in_database('B', 2, 202);
$bct->update_row_in_database('A', 3, 301);
$bct->update_row_in_database('C', 2, 203);

## Database B should be the winner for 1, C for 2, and A for 3
$bct->ctl('bucardo kick sync ctest 10');
$bct->check_for_row([[102],[203],[301]], [qw/ A B C D /]);

## Use a custom conflict handler by adding a customcode to this sync
$t = q{Added new customcode ctest1 for conflict in the ctest sync};
$res = $bct->ctl('bucardo add customcode ctest1 whenrun=conflict sync=ctest src_code=t/ctest1.pl');
like($res, qr{Added customcode "ctest1"}, $t);

$t = q{Reloaded sync ctest};
$res = $bct->ctl('bucardo reload sync ctest');
like($res, qr{success}, $t);

$bct->update_row_in_database('A', 2, 211);
$bct->update_row_in_database('B', 2, 212);
$bct->update_row_in_database('C', 2, 213);
$bct->update_row_in_database('A', 1, 111);
$bct->update_row_in_database('C', 1, 113);
$bct->ctl('bucardo kick sync ctest 10');
$bct->check_for_row([[111],[212],[301]], [qw/ A B C D /]);

## Test customcode with specific winners per row
$t = q{Added new customcode ctest2 for conflict in the ctest sync};
$res = $bct->ctl('bucardo add customcode ctest2 whenrun=conflict relation=bucardo_test1 src_code=t/ctest2.pl');
like($res, qr{Added customcode "ctest2"}, $t);

$t = q{Reloaded sync ctest};
$res = $bct->ctl('bucardo reload sync ctest');
like($res, qr{success}, $t);

$bct->update_row_in_database('A', 1, 101);
$bct->update_row_in_database('B', 1, 102);
$bct->update_row_in_database('C', 1, 103);
$bct->update_row_in_database('A', 2, 221);
$bct->update_row_in_database('B', 2, 222);
$bct->update_row_in_database('C', 2, 223);
$bct->ctl('bucardo kick sync ctest 10');
## ctest2.pl sets B as winner inside the conflict hash
$bct->check_for_row([[102],[222],[301]], [qw/ A B C D /]);


## Test customcode on a goat only


done_testing();
exit;
