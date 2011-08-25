#!/usr/bin/env perl
# -*-mode:cperl; indent-tabs-mode: nil-*-

## Test bucardo_delta and bucardo_track table tasks

use 5.008003;
use strict;
use warnings;
use Data::Dumper;
use lib 't','.';
use DBD::Pg;
use Test::More;
use MIME::Base64;

use vars qw/ $bct $dbhX $dbhA $dbhB $dbhC $res $command $t $SQL %pkey %sth %sql $sth $count/;

use BucardoTesting;
$bct = BucardoTesting->new() or BAIL_OUT "Creation of BucardoTesting object failed\n";
$location = '';

my $numtabletypes = keys %tabletype;
my $numsequences = keys %sequences;
plan tests => 160;

pass("*** Beginning delta tests");

END {
    $bct and $bct->stop_bucardo($dbhX);
    $dbhX and  $dbhX->disconnect();
    $dbhA and $dbhA->disconnect();
    $dbhB and $dbhB->disconnect();
    $dbhC and $dbhC->disconnect();
}

## Get Postgres databases A, B, and C created
$dbhA = $bct->repopulate_cluster('A');
$dbhB = $bct->repopulate_cluster('B');
$dbhC = $bct->repopulate_cluster('C');

## Create a bucardo database, and install Bucardo into it
$dbhX = $bct->setup_bucardo('A');

## Tell Bucardo about these databases

## One source and two targets
for my $name (qw/ A B C /) {
    $t = "Adding database from cluster $name works";
    my ($dbuser,$dbport,$dbhost) = $bct->add_db_args($name);
    $command = "bucardo add db $name dbname=bucardo_test user=$dbuser port=$dbport host=$dbhost";
    $res = $bct->ctl($command);
    like ($res, qr/Added database "$name"/, $t);
}

## Teach Bucardo about all pushable tables, adding them to a new herd named "therd"
$t = q{Adding all tables on the master works};
$command =
"bucardo add tables all db=A herd=therd pkonly";
$res = $bct->ctl($command);
like ($res, qr/Creating herd: therd.*New tables added: \d/s, $t);

## Add all sequences, and add them to the newly created herd
$t = q{Adding all sequences on the master works};
$command =
"bucardo add sequences all db=A herd=therd";
$res = $bct->ctl($command);
like ($res, qr/New sequences added: \d/, $t);

## Create a new database group
$t = q{Created a new database group};
$command =
"bucardo add dbgroup pg A:source B:target C:target";
$res = $bct->ctl($command);
like ($res, qr/Created database group "pg"/, $t);

## Create a new sync
$t = q{Created a new sync};
$command =
"bucardo add sync dtest herd=therd dbs=pg ping=false";
$res = $bct->ctl($command);
like ($res, qr/Added sync "dtest"/, $t);

## Make sure the bucardo_delta and bucardo_track tables are empty
for my $table (sort keys %tabletype) {

    my $tracktable = "track_public_$table";
    my $deltatable = "delta_public_$table";

    $t = "The track table $tracktable is empty";
    $SQL = "SELECT 1 FROM bucardo.$tracktable";
    $count = $dbhA->do($SQL);
    is ($count, '0E0', $t);

    $t = "The delta table $deltatable is empty";
    $SQL = "SELECT 1 FROM bucardo.$deltatable";
    $count = $dbhA->do($SQL);
    is ($count, '0E0', $t);
}

## Start up Bucardo with this new sync
$bct->restart_bucardo($dbhX);

## Get the statement handles ready for each table type
for my $table (sort keys %tabletype) {

    $pkey{$table} = $table =~ /test5/ ? q{"id space"} : 'id';

    ## INSERT
    for my $x (1..6) {
        $SQL = $table =~ /X/
            ? "INSERT INTO $table($pkey{$table}) VALUES (?)"
                : "INSERT INTO $table($pkey{$table},data1,inty) VALUES (?,'foo',$x)";
        $sth{insert}{$x}{$table}{A} = $dbhA->prepare($SQL);
        if ('BYTEA' eq $tabletype{$table}) {
            $sth{insert}{$x}{$table}{A}->bind_param(1, undef, {pg_type => PG_BYTEA});
        }
    }

    ## SELECT
    $sql{select}{$table} = "SELECT inty FROM $table ORDER BY $pkey{$table}";
    $table =~ /X/ and $sql{select}{$table} =~ s/inty/$pkey{$table}/;

    ## DELETE ALL
    $SQL = "DELETE FROM $table";
    $sth{deleteall}{$table}{A} = $dbhA->prepare($SQL);

    ## DELETE ONE
    $SQL = "DELETE FROM $table WHERE inty = ?";
    $sth{deleteone}{$table}{A} = $dbhA->prepare($SQL);

    ## TRUNCATE
    $SQL = "TRUNCATE TABLE $table";
    $sth{truncate}{$table}{A} = $dbhA->prepare($SQL);
    ## UPDATE
    $SQL = "UPDATE $table SET inty = ?";
    $sth{update}{$table}{A} = $dbhA->prepare($SQL);
}

## Add one row per table type to A
for my $table (keys %tabletype) {
    my $type = $tabletype{$table};
    my $val1 = $val{$type}{1};
    $sth{insert}{1}{$table}{A}->execute($val1);
}

## Before the commit on A ... B and C should be empty
for my $table (sort keys %tabletype) {
    my $type = $tabletype{$table};

    $t = qq{B has not received rows for table $table before A commits};
    $res = [];
    bc_deeply($res, $dbhB, $sql{select}{$table}, $t);

    $t = qq{C has not received rows for table $table before A commits};
    bc_deeply($res, $dbhC, $sql{select}{$table}, $t);

}

## Commit
$dbhA->commit();

## Make sure that bucardo_track is empty and bucardo_delta has the expected value
for my $table (sort keys %tabletype) {

    my $tracktable = "track_public_$table";
    my $deltatable = "delta_public_$table";

    $t = "The track table $tracktable is empty";
    $SQL = "SELECT 1 FROM bucardo.$tracktable";
    $count = $dbhA->do($SQL);
    is ($count, '0E0', $t);

    $t = "The delta table $deltatable contains the correct id";
    $SQL = "SELECT $pkey{$table} FROM bucardo.$deltatable";
    $dbhA->do(q{SET TIME ZONE 'GMT'});
    $res = $dbhA->selectall_arrayref($SQL);
    my $type = $tabletype{$table};
    my $val1 = $val{$type}{1};
    is_deeply ($res, [[$val1]], $t);
}

## Kick it off
$bct->ctl('bucardo kick dtest 0');

## Check targets for the new rows
for my $table (sort keys %tabletype) {

    my $type = $tabletype{$table};
    $res = [[1]];

    $t = qq{Row with pkey of type $type gets copied to B};
    bc_deeply($res, $dbhB, $sql{select}{$table}, $t);

    $t = qq{Row with pkey of type $type gets copied to C};
    bc_deeply($res, $dbhC, $sql{select}{$table}, $t);

}

## Make sure that bucardo_track now has a row
for my $table (sort keys %tabletype) {

    my $tracktable = "track_public_$table";

    $t = "The track table $tracktable contains the proper entry";
    $SQL = "SELECT target FROM bucardo.$tracktable";
    $res = $dbhA->selectall_arrayref($SQL);
    is_deeply ($res, [['dbgroup pg']], $t);

}

## Run the purge program
$bct->ctl('bucardo purge');

for my $table (sort keys %tabletype) {

    my $tracktable = "track_public_$table";
    my $deltatable = "delta_public_$table";

    $t = "The track table $tracktable contains no entries post purge";
    $SQL = "SELECT 1 FROM bucardo.$tracktable";
    $count = $dbhA->do($SQL);
    is ($count, '0E0', $t);

    $t = "The delta table $deltatable contains no entries post purge";
    $SQL = "SELECT 1 FROM bucardo.$deltatable";
    $count = $dbhA->do($SQL);
    is ($count, '0E0', $t);

}

## Create a doubled up entry in the delta table (two with same timestamp and pk)
for my $table (keys %tabletype) {
    $sth{update}{$table}{A}->execute(42);
    $sth{update}{$table}{A}->execute(52);
}
$dbhA->commit();

## Check for two entries per table
for my $table (sort keys %tabletype) {

    my $tracktable = "track_public_$table";
    my $deltatable = "delta_public_$table";

    $t = "The track table $tracktable is empty";
    $SQL = "SELECT 1 FROM bucardo.$tracktable";
    $count = $dbhA->do($SQL);
    is ($count, '0E0', $t);

    $t = "The delta table $deltatable contains two entries";
    $SQL = "SELECT 1 FROM bucardo.$deltatable";
    $count = $dbhA->do($SQL);
    is ($count, 2, $t);

}

## Kick it off
$bct->ctl('bucardo kick dtest 0');

## Run the purge program
$bct->ctl('bucardo purge');

for my $table (sort keys %tabletype) {

    my $tracktable = "track_public_$table";
    my $deltatable = "delta_public_$table";

    $t = "The track table $tracktable contains no entries post purge";
    $SQL = "SELECT 1 FROM bucardo.$tracktable";
    $count = $dbhA->do($SQL);
    is ($count, '0E0', $t);

    $t = "The delta table $deltatable contains no entries post purge";
    $SQL = "SELECT 1 FROM bucardo.$deltatable";
    $count = $dbhA->do($SQL);
    is ($count, '0E0', $t);

}

exit;
