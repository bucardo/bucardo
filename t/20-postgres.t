#!/usr/bin/env perl
# -*-mode:cperl; indent-tabs-mode: nil-*-

## Test using Postgres as a database target

use 5.008003;
use strict;
use warnings;
use Data::Dumper;
use lib 't','.';
use DBD::Pg;
use Test::More;
use MIME::Base64;

use vars qw/ $dbhX $dbhA $dbhB $dbhC $dbhD $res $command $t $SQL %pkey %sth %sql $sth $count $val /;

use BucardoTesting;
my $bct = BucardoTesting->new({location => 'postgres'})
    or BAIL_OUT "Creation of BucardoTesting object failed\n";

## Some of the tests are called inside of BucardoTesting.pm
## e.g. $bct->check_for_row([[1]], [qw/ B C D/]);
## The above runs one test for each passed in database x the number of test tables
## 1 4
my $numtables = keys %tabletype;
my $numsequences = keys %sequences;
my $single_tests = 23;
my $check_for_row_3 = 1;
my $check_for_row_4 = 4;
my $check_sequences_same = 1;

plan tests => $single_tests +
    ( $check_sequences_same * $numsequences ) + ## Simple sequence testing
    ( $check_for_row_3 * $numtables * 3 ) + ## B C D
    ( $check_for_row_4 * $numtables * 4 ); ## A B C D

pass("*** Beginning postgres tests");

END {
    $bct and $bct->stop_bucardo($dbhX);
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

## Put all pk tables into a herd
$t = q{Adding all PK tables on the master works};
$res = $bct->ctl(q{bucardo add tables '*bucardo*test*' '*Bucardo*test*' db=A herd=allpk pkonly});
like ($res, qr/Created the herd named "allpk".*are now part of/s, $t);

## Add all sequences
$t = q{Adding all sequences to the main herd};
$res = $bct->ctl(q{bucardo add all sequences herd=allpk});
like ($res, qr/New sequences added/s, $t);

## Create a new database group going from A to B and C and D
$t = q{Created a new database group A -> B C D};
$res = $bct->ctl('bucardo add dbgroup pg1 A:source B:target C:target D:target');
like ($res, qr/Created database group "pg1"/, $t);

## Create a new database group going from A and B to C and D
$t = q{Created a new database group (A <=> B ) -> C D};
$res = $bct->ctl('bucardo add dbgroup pg2 A:source B:source C D');
like ($res, qr/Created database group "pg2"/, $t);

## Create a new database group going from A and B and C to D
$t = q{Created a new database group (A <=> B <=> C) -> D};
$res = $bct->ctl('bucardo add dbgroup pg3 A:source B:source C:source D');
like ($res, qr/Created database group "pg3"/, $t);

## Create a new database group going from A and B and C and D
$t = q{Created a new database group (A <=> B <=> C <=> D)};
$res = $bct->ctl('bucardo add dbgroup pg4 A:source B:source C:source D:source');
like ($res, qr/Created database group "pg4"/, $t);

## Create some new syncs. Only one should be active at a time!
$t = q{Created a new sync for dbgroup pg1};
$res = $bct->ctl('bucardo add sync pgtest1 herd=allpk dbs=pg1 status=inactive');
like ($res, qr/Added sync "pgtest1"/, $t);

$t = q{Created a new sync for dbgroup pg2};
$res = $bct->ctl('bucardo add sync pgtest2 herd=allpk dbs=pg2 status=inactive ping=false');
like ($res, qr/Added sync "pgtest2"/, $t);

$t = q{Created a new sync for dbgroup pg3};
$res = $bct->ctl('bucardo add sync pgtest3 herd=allpk dbs=pg3 status=inactive ping=false');
like ($res, qr/Added sync "pgtest3"/, $t);

$t = q{Created a new sync for dbgroup pg4};
$res = $bct->ctl('bucardo add sync pgtest4 herd=allpk dbs=pg4 status=inactive ping=false');
like ($res, qr/Added sync "pgtest4"/, $t);

## Add a row to A, to make sure it does not go anywhere with inactive syncs
$bct->add_row_to_database('A', 1);

## Start up Bucardo. All syncs are inactive, so nothing should happen,
## and Bucardo should exit
$bct->restart_bucardo($dbhX, 'bucardo_stopped');

## Activate the pg1 sync
$t = q{Activated sync pgtest1};
$bct->ctl('bucardo update sync pgtest1 status=active');

## Start listening for a syndone message
## Bucardo should fire the sync off right away without a kick
$dbhX->do('LISTEN bucardo_syncdone_pgtest1');
$dbhX->commit();

## Start up Bucardo again
$bct->restart_bucardo($dbhX);

## Wait for our sync to finish
$bct->wait_for_notice($dbhX, 'bucardo_syncdone_pgtest1');

## See if things are on the others databases
$bct->check_for_row([[1]], [qw/ B C D/]);

## Switch to a 2 source, 2 target sync
$bct->ctl('bucardo update sync pgtest1 status=inactive');
$bct->ctl('bucardo update sync pgtest2 status=active');
$bct->ctl('bucardo deactivate sync pgtest1');
$bct->ctl('bucardo activate sync pgtest2 0');

## Add some rows to both masters, make sure it goes everywhere
for my $num (2..4) {
    $bct->add_row_to_database('A', $num);
}
for my $num (5..10) {
    $bct->add_row_to_database('B', $num);
}

## Kick off B. Everything should go to A, C, and D
$bct->ctl('bucardo kick sync pgtest2 0');

## Kick off A. Should fail, as the sync is inactive
$t = q{Inactive sync times out when trying to kick};
$res = $bct->ctl('bucardo kick sync pgtest1 0');
like($res, qr/Cannot kick inactive sync/, $t);

## All rows should be on A, B, C, and D
my $expected = [];
push @$expected, [$_] for 1..10;
$bct->check_for_row($expected, [qw/A B C D/]);

## Deactivate pgtest2, bring up pgtest3
$bct->ctl('bucardo update sync pgtest2 status=inactive');
$bct->ctl('bucardo update sync pgtest3 status=active');
$bct->ctl('bucardo deactivate sync pgtest2');
$bct->ctl('bucardo activate sync pgtest3 0');

## Kick off the sync to pick up the deltas from the previous runs
$bct->ctl('bucardo kick sync pgtest3 0');

## This one has three sources: A, B, and C. Remove rows from each
$bct->remove_row_from_database('A', 10);
$bct->remove_row_from_database('A', 9);
$bct->remove_row_from_database('A', 8);
$bct->remove_row_from_database('B', 6);
$bct->remove_row_from_database('B', 5);
$bct->remove_row_from_database('B', 4);
$bct->remove_row_from_database('C', 2);
$bct->remove_row_from_database('C', 1);

## Kick it off
$bct->ctl('bucardo kick sync pgtest3 0');

## Only rows left everywhere should be 3 and 7
$bct->check_for_row([[3],[7]], [qw/A B C D/]);

## Cause a conflict: same row on A, B, and C.
$bct->add_row_to_database('A', 1);
$bct->add_row_to_database('B', 1);

$bct->add_row_to_database('A', 2);
$bct->add_row_to_database('B', 2);
$bct->add_row_to_database('C', 2);

## Kick and check everyone is the same
$bct->ctl('bucardo kick sync pgtest3 0');
$bct->check_for_row([[1],[2],[3],[7]], [qw/A B C D/]);

## Change sequence information, make sure it gets out to everyone
$dbhA->do('alter sequence bucardo_test_seq1 start 20 restart 25 minvalue 10 maxvalue 8675');
$dbhA->commit();
$dbhB->do('alter sequence bucardo_test_seq2 start 200 restart 250 minvalue 100 maxvalue 86753');
$dbhB->commit();
$dbhC->do(q{SELECT setval('bucardo_test_seq3', 12345)});
$dbhC->commit();

$bct->ctl('bucardo kick sync pgtest3 0');
$bct->check_sequences_same([qw/A B C D/]);

## Create a PK conflict and let B "win" due to the timestamp
$SQL = 'UPDATE bucardo_test1 SET data1 = ? WHERE id = ?';
$dbhB->do($SQL, {}, 'Bravo', 3);
$dbhC->do($SQL, undef, 'Charlie', 3);
$dbhA->do($SQL, undef, 'Alpha', 3);
## Order of commits should not matter: the timestamp comes from the start of the transaction
$dbhC->commit();
$dbhB->commit();
$dbhA->commit();

$bct->ctl('bucardo kick sync pgtest3 0');
$bct->check_for_row([[1],[2],[3],[7]], [qw/A B C D/]);


$SQL = 'SELECT data1 FROM bucardo_test1 WHERE id = ?';
$val = $dbhA->selectall_arrayref($SQL, undef, 3)->[0][0];
$t = 'Conflict resolution respects earliest transaction time for A';
is ($val, 'Charlie', $t);
$t = 'Conflict resolution respects earliest transaction time for B';
$val = $dbhB->selectall_arrayref($SQL, undef, 3)->[0][0];
is ($val, 'Charlie', $t);
$t = 'Conflict resolution respects earliest transaction time for C';
$val = $dbhC->selectall_arrayref($SQL, undef, 3)->[0][0];
is ($val, 'Charlie', $t);

exit;

