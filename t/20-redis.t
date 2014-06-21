#!/usr/bin/env perl
# -*-mode:cperl; indent-tabs-mode: nil-*-

## Test using Redis as a database target

use 5.008003;
use strict;
use warnings;
use Data::Dumper;
use lib 't','.';
use DBD::Pg;
use Test::More;
use MIME::Base64;

use vars qw/ $dbhX $dbhA $dbhB $res $command $t $SQL %pkey %sth %sql $sth $count $val /;

## Must have the Redis module
my $evalok = 0;
eval {
    require Redis;
    $evalok = 1;
};
if (!$evalok) {
    plan (skip_all =>  'Cannot test Redis unless the Perl module Redis is installed');
}

## Redis must be up and running
$evalok = 0;
my $dbhR;
eval {
    $dbhR = Redis->new();
    $evalok = 1;
};
if (!$evalok) {
    plan (skip_all =>  "Cannot test Redis as we cannot connect to a running Redis instance");
}

use BucardoTesting;

## For now, remove the bytea table type as we don't have full support yet
delete $tabletype{bucardo_test8};

my $bct = BucardoTesting->new({location => 'redis'})
    or BAIL_OUT "Creation of BucardoTesting object failed\n";

plan tests => 70;

pass("*** Beginning redis tests");

END {
    $dbhR and remove_test_tables();
    $bct and $bct->stop_bucardo($dbhX);
    $dbhX and $dbhX->disconnect();
    $dbhA and $dbhA->disconnect();
    $dbhB and $dbhB->disconnect();
}

## Remove any existing Bucardo test keys that may exist on the Redis server
remove_test_tables();

sub remove_test_tables {
    for my $table (sort keys %tabletype) {
        my @keylist = $dbhR->keys("$table:*");
        for my $key (@keylist) {
            $dbhR->del($key);
        }
    }
}

## Get A and B created, emptied out, and repopulated with sample data
$dbhA = $bct->repopulate_cluster('A');
$dbhB = $bct->repopulate_cluster('B');

## Create a bucardo database, and install Bucardo into it
$dbhX = $bct->setup_bucardo('A');

## Teach Bucardo about the Postgres databases
for my $name (qw/ A B /) {
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

## Add all sequences
$t = q{Adding all sequences to the main relgroup};
$res = $bct->ctl(q{bucardo add all sequences relgroup=allpk});
like ($res, qr/New sequences added/s, $t);

my $dbname = 'bucardo_test';

$t = 'Adding Redis database R works';
$command =
"bucardo add db R dbname=$dbname type=redis";
$res = $bct->ctl($command);
like ($res, qr/Added database "R"/, $t);

## Create a new dbgroup going from A to B and off to R
$t = q{Created a new dbgroup A <=> B -> R};
$res = $bct->ctl('bucardo add dbgroup pg1 A:source B:source R:target');
like ($res, qr/Created dbgroup "pg1"/, $t);

$t = q{Created a new sync for dbgroup pg1};
$res = $bct->ctl('bucardo add sync pgtest1 relgroup=allpk dbs=pg1 status=active');
like ($res, qr/Added sync "pgtest1"/, $t);

## Add a row to A, and one to B
$bct->add_row_to_database('A', 1);
$bct->add_row_to_database('B', 2);

## Start listening for a syncdone message
$dbhX->do('LISTEN bucardo_syncdone_pgtest1');
$dbhX->commit();

## Start up Bucardo
$bct->restart_bucardo($dbhX, 'bucardo_syncdone_pgtest1');

## See if things are on the other databases
$bct->check_for_row([[1],[2]], [qw/ A B /]);

## Check that both rows made it out to Redis
for my $rownum (1..2) {
    for my $table (sort keys %tabletype) {
        my $type = $tabletype{$table};
        my $val = $val{$type}{$rownum};
        my $expected = { inty => $rownum, booly => 't', data1 => 'foo' };
        if ($table eq 'bucardo_test2') {
            $val .= ':foo';
            delete $expected->{data1};
        }
        my $name = "$table:$val";
        my %hash = $dbhR->hgetall($name);
        $t = "Table $table, pkey $val is replicated to Redis as expected";
        if (! is_deeply(\%hash, $expected, $t)) {
            diag Dumper \%hash;
        }
    }
}

## Make sure null maps to the field being removed
for my $table (sort keys %tabletype) {
    $SQL = qq{UPDATE "$table" SET booly=NULL};
    $dbhA->do($SQL);
}
$dbhA->commit();

$bct->ctl('bucardo kick pgtest1 0');

## Check that both rows made it out to Redis
for my $rownum (1..2) {
    for my $table (sort keys %tabletype) {
        my $type = $tabletype{$table};
        my $val = $val{$type}{$rownum};
        my $expected = { inty => $rownum, data1 => 'foo' };
        if ($table eq 'bucardo_test2') {
            $val .= ':foo';
            delete $expected->{data1};
        }
        my $name = "$table:$val";
        my %hash = $dbhR->hgetall($name);
        $t = "Table $table, pkey $val is replicated to Redis as expected (booly gone)";
        if (! is_deeply(\%hash, $expected, $t)) {
            diag Dumper \%hash;
        }
    }
}

exit;
