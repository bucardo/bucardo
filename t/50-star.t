#!/usr/bin/env perl
# -*-mode:cperl; indent-tabs-mode: nil-*-

## Test Bucardo in a large star network
## We will use 'A' as the hub, and the three others B C D, each having multiple dbs

use 5.008003;
use strict;
use warnings;
use Data::Dumper;
use lib 't','.';
use DBD::Pg;
use Test::More;

use vars qw/ $dbhX $dbhA $dbhB $dbhC $dbhD $res $command $t $SQL $sth $count /;

use BucardoTesting;
my $bct = BucardoTesting->new({location => 'star'})
    or BAIL_OUT "Creation of BucardoTesting object failed\n";

pass("*** Beginning star tests");

END {
    $bct and $bct->stop_bucardo();
    $dbhX and $dbhX->disconnect();
    $dbhA and $dbhA->disconnect();
    $dbhB and $dbhB->disconnect();
    $dbhC and $dbhC->disconnect();
    $dbhD and $dbhD->disconnect();
}

## Get A, B, C, and D created, emptied out, and repopulated with sample data
my $extras = 5;
$dbhA = $bct->repopulate_cluster('A');
$dbhB = $bct->repopulate_cluster('B',$extras);
$dbhC = $bct->repopulate_cluster('C',$extras);
$dbhD = $bct->repopulate_cluster('D',$extras);

## Create a bucardo database, and install Bucardo into it
$dbhX = $bct->setup_bucardo('A');

## Teach Bucardo about all databases
my (@alldbs, @alldbhs);
for my $name (qw/ A B C D /) {
    my ($dbuser,$dbport,$dbhost) = $bct->add_db_args($name);
    for my $number (0..$extras) {
        my $dbname = 'bucardo_test';
        my $bname = $name;
        ## Always a single hub
        next if $number and $name eq 'A';
        if ($number) {
            $dbname .= $number;
            $bname .= $number;
        }
        $t = "Added database $bname for database $dbname";
        $command = "bucardo add db $bname dbname=$dbname user=$dbuser port=$dbport host=$dbhost";
        $command .= ' makedelta=1' if $name eq 'A';
        $res = $bct->ctl($command);
        like ($res, qr/Added database "$bname"/, $t);
        push @alldbs => $bname;
        my $dbh = $bct->connect_database($bname);
        push @alldbhs => $dbh;
    }
}

## Put all pk tables into a relgroup
$t = q{Added all PK tables to a relgroup named 'allpk'};
$res = $bct->ctl(q{bucardo add tables '*bucardo*test*' '*Bucardo*test*' db=A relgroup=allpk pkonly});
like ($res, qr/Created the relgroup named "allpk".*are now part of/s, $t);

## Make a simpler relgroup of just one table
$t = q{Created relgroup of just bucardo_test1 named 'rel1'};
$res = $bct->ctl(q{bucardo add relgroup rel1 bucardo_test1});
like ($res, qr/relgroup "rel1"/s, $t);

## Create a lot of syncs. Each simulates a multi-source from center to a distinct server leaf
my $number = 2;
for my $db (qw/ B C D /) {
    for my $num (0..$extras) {
        my $syncname = "star$number";
        my $leaf = sprintf '%s%s', $db, $num || '';
        $t = qq{Created a new sync $syncname going for A <=> $leaf};
        my $command = "bucardo add sync $syncname relgroup=rel1 dbs=A,$leaf:source autokick=true";
        $res = $bct->ctl($command);
        like ($res, qr/Added sync "$syncname"/, $t);
        $number++;
    }
}

## Start up the Bucardo daemon
$bct->restart_bucardo($dbhX);

## Add a row to A and make sure it gets to all leafs
$bct->add_row_to_database('A', 1);
$bct->ctl('bucardo kick sync star1 0');
sleep 5;
$bct->check_for_row([[1]], \@alldbs, '', 'bucardo_test1');

$number = 0;
my $maxnumber = 1;
$SQL = 'INSERT INTO bucardo_test1(id,inty) VALUES (?,?)';
for my $dbh (@alldbhs) {
    $number++;
    next if $number < 2; ## Do ont want to add anything to the "A" database
    $dbh->do($SQL, undef, $number, $number);
    $dbh->commit();
    $maxnumber = $number;
}
sleep 5;

my $result = [];
push @$result, [$_] for 1..$maxnumber;
$bct->check_for_row($result, [qw/ A B C D /], '', 'bucardo_test1');

$bct->ctl('bucardo stop');

done_testing();
