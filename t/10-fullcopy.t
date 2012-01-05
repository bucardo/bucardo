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
my $bct = BucardoTesting->new() or BAIL_OUT "Creation of BucardoTesting object failed\n";
$location = 'fullcopy';

my $numtabletypes = keys %tabletype;
plan tests => 339;

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

## Tell Bucardo about these databases

## Teach Bucardo about four databases
for my $name (qw/ A B C D /) {
    $t = "Adding database from cluster $name works";
    my ($dbuser,$dbport,$dbhost) = $bct->add_db_args($name);
    $command = "bucardo add db $name dbname=bucardo_test user=$dbuser port=$dbport host=$dbhost";
    $res = $bct->ctl($command);
    like ($res, qr/Added database "$name"/, $t);
}

## Put all primary key tables into a herd
$t = q{Adding all PK tables on the master works};
$command =
"bucardo add tables all db=A herd=pk pkonly";
$res = $bct->ctl($command);
like ($res, qr/Creating herd: pk.*New tables added: \d/s, $t);

## Create a new database group going from A and B to C and D
$t = q{Created a new database group};
$command =
"bucardo add dbgroup pg A:source B:source C D";
$res = $bct->ctl($command);
like ($res, qr/Created database group "pg"/, $t);

## Create a new sync
$t = q{Created a new sync};
$command =
"bucardo add sync fctest herd=pk dbs=pg ping=false";
$res = $bct->ctl($command);
like ($res, qr/Added sync "fctest"/, $t);

## Start up Bucardo with this new sync
$bct->restart_bucardo($dbhX);
## Immediate kicks to catch any startup sync effects
$bct->ctl('bucardo kick fctest 0');
$bct->ctl('bucardo kick fctest 0');

## Get the statement handles ready for each table type
for my $table (sort keys %tabletype) {

    $pkey{$table} = $table =~ /test5/ ? q{"id space"} : 'id';

    ## INSERT
    my (@boolys) = qw( ? true false null false true true null );
    for my $x (1..7) {
        $SQL = $table =~ /X/
            ? "INSERT INTO $table($pkey{$table}) VALUES (?)"
                : "INSERT INTO $table($pkey{$table},data1,inty,booly) VALUES (?,'foo',$x,$boolys[$x])";
        $sth{insert}{$x}{$table}{A} = $dbhA->prepare($SQL);
        $sth{insert}{$x}{$table}{C} = $dbhC->prepare($SQL);

        if ('BYTEA' eq $tabletype{$table}) {
            $sth{insert}{$x}{$table}{A}->bind_param(1, undef, {pg_type => PG_BYTEA});
            $sth{insert}{$x}{$table}{C}->bind_param(1, undef, {pg_type => PG_BYTEA});
        }
    }

    ## SELECT
    $sql{select}{$table} = "SELECT inty FROM $table ORDER BY $pkey{$table}";
    $table =~ /X/ and $sql{select}{$table} =~ s/inty/$pkey{$table}/;

    ## DELETE ALL
    $SQL = "DELETE FROM $table";
    $sth{deleteall}{$table}{A} = $dbhA->prepare($SQL);
    $sth{deleteall}{$table}{D} = $dbhD->prepare($SQL);

    ## DELETE ONE
    $SQL = "DELETE FROM $table WHERE inty = ?";
    $sth{deleteone}{$table}{A} = $dbhA->prepare($SQL);

    ## TRUNCATE
    $SQL = "TRUNCATE TABLE $table";
    $sth{truncate}{$table}{A} = $dbhA->prepare($SQL);

    ## UPDATE
    $SQL = "UPDATE $table SET inty = ?";
    $sth{update}{$table}{A} = $dbhA->prepare($SQL);
    $sth{update}{$table}{B} = $dbhB->prepare($SQL);
    $sth{update}{$table}{C} = $dbhC->prepare($SQL);
    $sth{update}{$table}{D} = $dbhD->prepare($SQL);

    ## UPDATE2
    $SQL = "UPDATE $table SET inty = ? WHERE inty = ?";
    $sth{update2}{$table}{A} = $dbhA->prepare($SQL);
    $sth{update2}{$table}{B} = $dbhB->prepare($SQL);
    $sth{update2}{$table}{C} = $dbhC->prepare($SQL);
    $sth{update2}{$table}{D} = $dbhD->prepare($SQL);
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
$bct->ctl('bucardo kick fctest 0');

## Check targets for the new rows
for my $table (sort keys %tabletype) {

    my $type = $tabletype{$table};
    $res = [[1]];

    $t = qq{Row with pkey of type $type gets copied to B.$table};
    bc_deeply($res, $dbhB, $sql{select}{$table}, $t);

    $t = qq{Row with pkey of type $type gets copied to C.$table};
    bc_deeply($res, $dbhC, $sql{select}{$table}, $t);

    $t = qq{Row with pkey of type $type gets copied to D.$table};
    bc_deeply($res, $dbhD, $sql{select}{$table}, $t);

}

## Update each row
for my $table (keys %tabletype) {
    $sth{update}{$table}{A}->execute(42);
}
$dbhA->commit();
$bct->ctl('bucardo kick fctest 0');

for my $table (sort keys %tabletype) {

    my $type = $tabletype{$table};
    $res = [[42]];

    $t = qq{Row with pkey of type $type gets copied to B after update};
    bc_deeply($res, $dbhB, $sql{select}{$table}, $t);

    $t = qq{Row with pkey of type $type gets copied to C after update};
    bc_deeply($res, $dbhC, $sql{select}{$table}, $t);

    $t = qq{Row with pkey of type $type gets copied to D after update};
    bc_deeply($res, $dbhD, $sql{select}{$table}, $t);

}

## Add rows to one target, and remove from the other
## Set a onetimecopy to straighten it all out
## Other master should be unaffected

## Update the row on C, and add a new one:
for my $table (keys %tabletype) {
    my $type = $tabletype{$table};
    my $val3 = $val{$type}{3};
    $sth{update}{$table}{C}->execute(99);
    $sth{insert}{3}{$table}{C}->execute($val3);
}
$dbhC->commit();

## Remove all rows from D:
for my $table (keys %tabletype) {
    $sth{deleteall}{$table}{D}->execute();
}
$dbhD->commit();

## Update the row on B:
for my $table (keys %tabletype) {
    $sth{update}{$table}{B}->execute(86);
}
$dbhB->commit();

## Set the sync as unconditional onetimecopy
$bct->ctl('bucardo update sync fctest onetimecopy=1');

## Reload it (which kicks it off, then kicks again post-onetimecopy)
$bct->ctl('bucardo reload sync fctest');
## One more kick to clean out our messages
$bct->ctl('bucardo kick fctest 0');

## A, C, and D should have the same information now
for my $table (sort keys %tabletype) {

    my $type = $tabletype{$table};
    $res = [[42]];

    $t = qq{Database A has expected rows for $table after onetimecopy};
    bc_deeply($res, $dbhA, $sql{select}{$table}, $t);

    $t = qq{Database C has expected rows for $table after onetimecopy};
    bc_deeply($res, $dbhC, $sql{select}{$table}, $t);

    $t = qq{Database D has expected rows for $table after onetimecopy};
    bc_deeply($res, $dbhD, $sql{select}{$table}, $t);

}
$dbhA->commit();
$dbhB->commit();
$dbhC->commit();
$dbhD->commit();

for my $table (keys %tabletype) {
    $sth{update}{$table}{A}->execute(80);
}
$dbhA->commit();

## Update the same row to create a conflict with B
## B should win, as is it 'latest'
for my $table (keys %tabletype) {
    $sth{update}{$table}{B}->execute(81);
}
$dbhB->commit();

sleep 1;
## Kick it to get everything synced
$bct->ctl('bucardo kick fctest 0');

for my $table (sort keys %tabletype) {

    my $type = $tabletype{$table};
    $res = [[81]];

    $t = qq{Database A has expected rows for $table after onetimecopy};
    bc_deeply($res, $dbhA, $sql{select}{$table}, $t);

    $t = qq{Database B has expected rows for $table after onetimecopy};
    bc_deeply($res, $dbhB, $sql{select}{$table}, $t);

    $t = qq{Database C has expected rows for $table after onetimecopy};
    bc_deeply($res, $dbhC, $sql{select}{$table}, $t);

    $t = qq{Database D has expected rows for $table after onetimecopy};
    bc_deeply($res, $dbhD, $sql{select}{$table}, $t);

}

## Update the row on C, and add a new one:
for my $table (keys %tabletype) {
    my $type = $tabletype{$table};
    my $val3 = $val{$type}{3};
    $sth{update}{$table}{C}->execute(99);
    $sth{insert}{3}{$table}{C}->execute($val3);
}
$dbhC->commit();

## Remove all rows from A and D:
for my $table (keys %tabletype) {
    $sth{deleteall}{$table}{A}->execute();
    $sth{deleteall}{$table}{D}->execute();
}
$dbhA->commit();
$dbhD->commit();

## Set the sync as conditional onetimecopy
$bct->ctl('bucardo update sync fctest onetimecopy=2');

## Reload it (which kicks it off)
$bct->ctl('bucardo reload sync fctest');
sleep 2;

## A and D should still be empty, as onetimecopy=2 will not run if source is empty
for my $table (sort keys %tabletype) {

    my $type = $tabletype{$table};
    $res = [];

    $t = qq{Database A has expected rows for $table after onetimecopy=2};
    bc_deeply($res, $dbhA, $sql{select}{$table}, $t);

    $t = qq{Database D has expected rows for $table after onetimecopy=2};
    bc_deeply($res, $dbhD, $sql{select}{$table}, $t);

}

## C should have one row removed because of A leaving, and one remaining unchanged
for my $table (sort keys %tabletype) {

    my $type = $tabletype{$table};
    $res = [[3]];

    $t = qq{Database C has expected rows for $table after onetimecopy=2};
    bc_deeply($res, $dbhC, $sql{select}{$table}, $t);
}

## Stop Bucardo, disable that sync, create a new one that is pure fullcopy
$bct->stop_bucardo();
$res = $bct->ctl('bucardo update sync fctest status=inactive');

## Create a new database group going from A to B and C (fullcopy only)
$t = q{Created a new database group};
$command =
"bucardo add dbgroup pg2 A:source B:fullcopy C:fullcopy";
$res = $bct->ctl($command);
like ($res, qr/Created database group "pg2"/, $t);

## Create a new sync
$t = q{Created a new sync};
$command =
"bucardo add sync fctest2 herd=pk dbs=pg2 ping=false";
$res = $bct->ctl($command);
like ($res, qr/Added sync "fctest2"/, $t);

## Remove the delta trigger from A
for my $table (keys %tabletype) {

    $SQL = "DROP TRIGGER bucardo_delta ON $table";
    $dbhA->do($SQL);

    ## Add a few rows
    my $type = $tabletype{$table};
    for my $num (4..6) {
        $sth{insert}{$num}{$table}{A}->execute( $val{$type}{$num} );
    }
}
$dbhA->commit();

## Start up Bucardo with this new sync
$bct->restart_bucardo($dbhX);

## As fullcopy syncs should not autostart, nothing should have changed
## Give things a chance to spin up:
sleep 2;

for my $table (sort keys %tabletype) {

    my $type = $tabletype{$table};
    $res = [[3]];

    $t = qq{Database B has expected rows for $table prior to a kick};
    bc_deeply([], $dbhB, $sql{select}{$table}, $t);

    $t = qq{Database C has expected rows for $table prior to a kick};
    bc_deeply($res, $dbhC, $sql{select}{$table}, $t);

}

## Kick it
$bct->ctl('bucardo kick fctest2 0');

## C and D should have the rows from A
for my $table (sort keys %tabletype) {

    my $type = $tabletype{$table};
    $res = [[4],[5],[6]];

    $t = qq{Database B has expected rows for $table after fullcopy kick};
    bc_deeply($res, $dbhB, $sql{select}{$table}, $t);

    $t = qq{Database C has expected rows for $table after fullcopy kick};
    bc_deeply($res, $dbhC, $sql{select}{$table}, $t);
}

## Mix fullcopy and delta targets with a new sync
$bct->stop_bucardo();
$res = $bct->ctl('bucardo update sync fctest2 status=inactive');

## Create a new database group going from A to B (swap), C (fullcopy) and D (delta)
$t = q{Created a new database group};
$command =
"bucardo add dbgroup pg3 A:source B:source C:fullcopy D:target";
$res = $bct->ctl($command);
like ($res, qr/Created database group "pg3"/, $t);

## Create a new sync
$t = q{Created a new sync};
$command =
"bucardo add sync fctest3 herd=pk dbs=pg3 ping=false";
$res = $bct->ctl($command);
like ($res, qr/Added sync "fctest3"/, $t);

## Start up Bucardo with this new sync
## Sync should start right away, as it is not all fullcopy
$bct->restart_bucardo($dbhX);
sleep 2;

for my $table (sort keys %tabletype) {

    my $type = $tabletype{$table};
    $res = [[4],[5],[6]];

    $t = qq{Database A has expected rows for $table};
    bc_deeply($res, $dbhA, $sql{select}{$table}, $t);

    $t = qq{Database B has expected rows for $table};
    bc_deeply($res, $dbhB, $sql{select}{$table}, $t);

    $t = qq{Database C has expected rows for $table};
    bc_deeply($res, $dbhC, $sql{select}{$table}, $t);

    $t = qq{Database D has now rows yet, as no deltas have built up};
    bc_deeply([], $dbhD, $sql{select}{$table}, $t);
}

## Touch two rows in A, and one in B
## They should cross-replicate, as well as go out to D

for my $table (sort keys %tabletype) {

    my $type = $tabletype{$table};
    $sth{update2}{$table}{A}->execute(44,4);
    $sth{update2}{$table}{A}->execute(55,5);
    $sth{update2}{$table}{B}->execute(66,6);

}
$dbhA->commit();
$dbhB->commit();

## Kick twice, just in case
$bct->ctl('bucardo kick fctest3 0');
$bct->ctl('bucardo kick fctest3 0');

for my $table (sort keys %tabletype) {

    my $type = $tabletype{$table};
    $res = [[44],[55],[66]];

    $t = qq{Database A has expected rows for $table};
    bc_deeply($res, $dbhA, $sql{select}{$table}, $t);

    $t = qq{Database B has expected rows for $table};
    bc_deeply($res, $dbhB, $sql{select}{$table}, $t);

    $t = qq{Database C has expected rows for $table};
    bc_deeply($res, $dbhC, $sql{select}{$table}, $t);

    $t = qq{Database D has expected rows for $table};
    bc_deeply($res, $dbhD, $sql{select}{$table}, $t);

}

pass("*** End 'fullcopy' tests");

exit;
