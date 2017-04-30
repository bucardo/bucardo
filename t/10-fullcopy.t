#!/usr/bin/env perl
# -*-mode:cperl; indent-tabs-mode: nil-*-

## Test fullcopy functionality

use 5.008003;
use strict;
use warnings;
use Data::Dumper;
use lib 't','.';
use DBD::Pg;
use Test::More;

use BucardoTesting;
my $bct = BucardoTesting->new({sync => 'fctest', location => 'fullcopy'})
    or BAIL_OUT "Creation of BucardoTesting object failed\n";

my $numtables = keys %tabletype;
my $numsequences = keys %sequences;
my $single_tests = 14;
my $table_tests = 2;
my $numdatabases = 3;
plan tests => $single_tests +
    ( $table_tests * $numtables * $numdatabases ) +
    ( 1 * $numsequences );

pass("*** Beginning 'fullcopy' tests");

use vars qw/ $dbhX $dbhA $dbhB $dbhC $dbhD $res $command $t %pkey $SQL %sth %sql/;

use vars qw/ $i $result /;

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

## Put all tables (including non-PK) into a herd
$t = q{Adding all tables on the master works};
$res = $bct->ctl(q{bucardo add tables '*bucardo*test*' '*Bucardo*test*' db=A herd=all});
like ($res, qr/Created the relgroup named "all".*are now part of/s, $t);

## Add all sequences as well
$t = q{Adding all tables on the master works};
$res = $bct->ctl(q{bucardo add sequences all herd=all});
like ($res, qr/New sequences added/s, $t);

## Add sequences to their own herd and sync
$t = q{Adding all sequences to a new sync works};
$res = $bct->ctl(q{bucardo add sequences all herd=seqonly});
like ($res, qr/Creating relgroup: seqonly/s, $t);

## Create a new dbgroup going from A to B and C and D
$t = q{Created a new fullcopy dbgroup A -> B C D};
$res = $bct->ctl('bucardo add dbgroup pg A:source B:fullcopy C:fullcopy D:fullcopy');
like ($res, qr/Created dbgroup "pg"/, $t);

## Create a new sync
$t = q{Created a new sync};
$res = $bct->ctl('bucardo add sync fctest herd=all dbs=pg');
like ($res, qr/Added sync "fctest"/, $t);

## Create a new sync for the sequences only
$t = q{Created a new sync};
$res = $bct->ctl('bucardo add sync seqtest herd=seqonly dbs=A:source,B:Source,C:target,D:target');
like ($res, qr/Added sync "seqtest"/, $t);

## Start up Bucardo with this new sync.
## No need to wait for the sync, as fullcopy syncs don't auto-run
$bct->restart_bucardo($dbhX);

## Add a row to each table in database A
$bct->add_row_to_database('A', 2);

## Kick off the sync and wait for it to return
$bct->ctl('bucardo kick fctest 0');

## Check targets for the new rows
$bct->check_for_row([[2]], [qw/ B C D/]);

## Do insert, update, and delete to targets
$bct->add_row_to_database('B',3);
$bct->remove_row_from_database('C', 2);

## Change the sequence on A
$dbhA->do('alter sequence bucardo_test_seq1 start 20 restart 25 minvalue 10 maxvalue 100');
$dbhA->commit();

## Kick off the sync, then check that everything was replaced
$bct->ctl('bucardo kick fctest seqtest 0');
sleep(3);
$bct->check_for_row([[2]], [qw/ B C D/]);

$bct->check_sequences_same([qw/A B C D/]);

## Test a sequence-only sync
$bct->ctl('bucardo kick seqtest 0');

pass("*** End 'fullcopy' tests");

exit;
