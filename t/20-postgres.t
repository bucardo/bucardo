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

use vars qw/ $bct $dbhX $dbhA $dbhB $dbhC $dbhD $res $command $t $SQL %pkey %sth %sql $sth $count/;

use BucardoTesting;
$bct = BucardoTesting->new() or BAIL_OUT "Creation of BucardoTesting object failed\n";
$location = '';

my $numtabletypes = keys %tabletype;
my $numsequences = keys %sequences;
plan tests => 226;

pass("*** Beginning postgres tests");

END {
    $bct and $bct->stop_bucardo($dbhX);
    $dbhX and  $dbhX->disconnect();
    $dbhA and $dbhA->disconnect();
    $dbhB and $dbhB->disconnect();
    $dbhC and $dbhC->disconnect();
    $dbhD and $dbhD->disconnect();
}

## Get Postgres databases A, B, C, and D created
$dbhA = $bct->repopulate_cluster('A');
$dbhB = $bct->repopulate_cluster('B');
$dbhC = $bct->repopulate_cluster('C');
$dbhD = $bct->repopulate_cluster('D');

## Create a bucardo database, and install Bucardo into it
$dbhX = $bct->setup_bucardo('A');

## Tell Bucardo about these databases

## Four Postgres databases will be source, source, target, and target
for my $name (qw/ A B C D /) {
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
"bucardo add dbgroup pg A:source B:source C:target D:slave";
$res = $bct->ctl($command);
like ($res, qr/Created database group "pg"/, $t);

## Create a new sync
$t = q{Created a new sync};
$command =
"bucardo add sync pgtest herd=therd dbs=pg ping=false";
$res = $bct->ctl($command);
like ($res, qr/Added sync "pgtest"/, $t);

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

## Before the commit on A ... B, C, and D should be empty
for my $table (sort keys %tabletype) {
    my $type = $tabletype{$table};

    $t = qq{B has not received rows for table $table before A commits};
    $res = [];
    bc_deeply($res, $dbhB, $sql{select}{$table}, $t);

    $t = qq{C has not received rows for table $table before A commits};
    bc_deeply($res, $dbhC, $sql{select}{$table}, $t);

    $t = qq{D has not received rows for table $table before A commits};
    bc_deeply($res, $dbhD, $sql{select}{$table}, $t);

}

## Commit, then kick off the sync
$dbhA->commit();
$bct->ctl('bucardo kick pgtest 0');

## Check targets for the new rows
for my $table (sort keys %tabletype) {

    my $type = $tabletype{$table};
    $res = [[1]];

    $t = qq{Row with pkey of type $type gets copied to B};
    bc_deeply($res, $dbhB, $sql{select}{$table}, $t);

    $t = qq{Row with pkey of type $type gets copied to C};
    bc_deeply($res, $dbhC, $sql{select}{$table}, $t);

    $t = qq{Row with pkey of type $type gets copied to D};
    bc_deeply($res, $dbhD, $sql{select}{$table}, $t);

}

## Update each row
for my $table (keys %tabletype) {
    $sth{update}{$table}{A}->execute(42);
}
$dbhA->commit();
$bct->ctl('bucardo kick pgtest 0');

for my $table (keys %tabletype) {

    my $type = $tabletype{$table};
    $res = [[42]];

    $t = qq{Row with pkey of type $type gets copied to B after update};
    bc_deeply($res, $dbhB, $sql{select}{$table}, $t);

    $t = qq{Row with pkey of type $type gets copied to C after update};
    bc_deeply($res, $dbhC, $sql{select}{$table}, $t);

    $t = qq{Row with pkey of type $type gets copied to D after update};
    bc_deeply($res, $dbhD, $sql{select}{$table}, $t);

}

## Delete each row
for my $table (keys %tabletype) {
    $sth{deleteall}{$table}{A}->execute();
}
$dbhA->commit();
$bct->ctl('bucardo kick pgtest 0');


for my $table (keys %tabletype) {

    my $type = $tabletype{$table};
    $res = [];

    $t = qq{Row with pkey of type $type gets copied to B after delete};
    bc_deeply($res, $dbhB, $sql{select}{$table}, $t);

    $t = qq{Row with pkey of type $type gets copied to C after delete};
    bc_deeply($res, $dbhC, $sql{select}{$table}, $t);

    $t = qq{Row with pkey of type $type gets copied to D after delete};
    bc_deeply($res, $dbhD, $sql{select}{$table}, $t);

}


## Insert two rows, then delete one of them
## Add one row per table type to A
for my $table (keys %tabletype) {
    my $type = $tabletype{$table};
    my $val1 = $val{$type}{1};
    $sth{insert}{1}{$table}{A}->execute($val1);
    my $val2 = $val{$type}{2};
    $sth{insert}{2}{$table}{A}->execute($val2);
}
$dbhA->commit();
$bct->ctl('bucardo kick pgtest 0');

for my $table (keys %tabletype) {

    my $type = $tabletype{$table};
    $res = [[1],[2]];

    $t = qq{Row with pkey of type $type gets copied to B after double insert};
    bc_deeply($res, $dbhB, $sql{select}{$table}, $t);

    $t = qq{Row with pkey of type $type gets copied to C after double insert};
    bc_deeply($res, $dbhC, $sql{select}{$table}, $t);

    $t = qq{Row with pkey of type $type gets copied to D after double insert};
    bc_deeply($res, $dbhD, $sql{select}{$table}, $t);

}

## Delete one of the rows
for my $table (keys %tabletype) {
    $sth{deleteone}{$table}{A}->execute(2); ## inty = 2
}
$dbhA->commit();
$bct->ctl('bucardo kick pgtest 0');

for my $table (keys %tabletype) {

    my $type = $tabletype{$table};
    $res = [[1]];

    $t = qq{Row with pkey of type $type gets copied to B after single delete};
    bc_deeply($res, $dbhB, $sql{select}{$table}, $t);

    $t = qq{Row with pkey of type $type gets copied to C after single delete};
    bc_deeply($res, $dbhC, $sql{select}{$table}, $t);

    $t = qq{Row with pkey of type $type gets copied to D after single delete};
    bc_deeply($res, $dbhD, $sql{select}{$table}, $t);

}

## Insert two more rows, then truncate
for my $table (keys %tabletype) {
    my $type = $tabletype{$table};
    my $val3 = $val{$type}{3};
    $sth{insert}{3}{$table}{A}->execute($val3);
    my $val4 = $val{$type}{4};
    $sth{insert}{4}{$table}{A}->execute($val4);
    $dbhA->do("TRUNCATE TABLE $table");
}
$dbhA->commit();
$bct->ctl('bucardo kick pgtest 0');

for my $table (keys %tabletype) {

    my $type = $tabletype{$table};
    $res = [];

    $t = qq{Row with pkey of type $type gets removed from B after truncate};
    bc_deeply($res, $dbhB, $sql{select}{$table}, $t);

    $t = qq{Row with pkey of type $type gets removed from C after truncate};
    bc_deeply($res, $dbhC, $sql{select}{$table}, $t);

    $t = qq{Row with pkey of type $type gets removed from D after truncate};
    bc_deeply($res, $dbhD, $sql{select}{$table}, $t);

}

## Tests of customcols
$t = q{add customcols returns expected message};
$res = $bct->ctl('bucardo add customcols bucardo_test1 "SELECT id, data1, inty*3 AS inty"');
like($res, qr/\QNew columns for public.bucardo_test1: "SELECT id, data1, inty*3 AS inty"/, $t);

## Restart the sync
$bct->restart_bucardo($dbhX);

## Add a new row to A
for my $table (sort keys %tabletype) {
    my $type = $tabletype{$table};
    my $val1 = $val{$type}{1};
    $count = $sth{insert}{1}{$table}{A}->execute($val1);
    last;
}

## Commit, then kick off the sync
$dbhA->commit();
$bct->ctl('bucardo kick pgtest 0');

## Check targets for the new rows
for my $table (sort keys %tabletype) {

    my $type = $tabletype{$table};
    $res = [[3]];

    $t = qq{Row with pkey of type $type gets copied to C with customcol};
    bc_deeply($res, $dbhC, $sql{select}{$table}, $t);

    $t = qq{Row with pkey of type $type gets copied to D with customcol};
    bc_deeply($res, $dbhD, $sql{select}{$table}, $t);

    last;

}




exit;
