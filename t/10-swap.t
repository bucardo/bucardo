#!/usr/bin/env perl
# -*-mode:cperl; indent-tabs-mode: nil-*-

## Test swap functionality

use 5.008003;
use strict;
use warnings;
use Data::Dumper;
use lib 't','.';
use DBD::Pg;
use Test::More;

use BucardoTesting;
my $bct = BucardoTesting->new() or BAIL_OUT "Creation of BucardoTesting object failed\n";
$location = 'swap';

my $numtabletypes = keys %tabletype;
my $numsequences = keys %sequences;
plan tests => 12 + ($numtabletypes * 24) + ($numsequences + 1);

pass("*** Beginning swap tests");

use vars qw/ $dbhX $dbhA $dbhB $dbhC $dbhD $res $command $t %pkey $SQL %sth %sql/;

END {
    $bct->stop_bucardo($dbhX);
    $dbhX->disconnect();
    $dbhA->disconnect();
    $dbhB->disconnect();
}

## Get A and B emptied out, and repopulated with sample data
$dbhA = $bct->repopulate_cluster('A');
$dbhB = $bct->repopulate_cluster('B');

## Create a bucardo database, and install Bucardo into it
$dbhX = $bct->setup_bucardo('A');

## Tell Bucardo about these databases

$t = 'Adding database from cluster A works';
my ($dbuser,$dbport,$dbhost) = $bct->add_db_args('A');
$command =
"bucardo_ctl add db bucardo_test name=A user=$dbuser port=$dbport host=$dbhost";
$res = $bct->ctl($command);
like ($res, qr/Added database "A"/, $t);

$t = 'Adding database from cluster B works';
($dbuser,$dbport,$dbhost) = $bct->add_db_args('B');
$command =
"bucardo_ctl add db bucardo_test name=B user=$dbuser port=$dbport host=$dbhost";
$res = $bct->ctl($command);
like ($res, qr/Added database "B"/, $t);

## Teach Bucardo about all pushable tables, adding them to a new herd named "therd"
$t = q{Adding all tables on the master works};
$command =
"bucardo_ctl add tables all db=A herd=therd pkonly";
$res = $bct->ctl($command);
like ($res, qr/Creating herd: therd.*New tables added: \d/s, $t);
if ($res =~ /New tables added: (\d+)/ and $1 < 1) {
    BAIL_OUT 'Tables were not added to herd?!';
}

## Add all sequences, and add them to the newly created herd
$t = q{Adding all sequences on the master works};
$command =
"bucardo_ctl add sequences all db=A herd=therd";
$res = $bct->ctl($command);
like ($res, qr/New sequences added: \d/, $t);

## Tell it how to handle conflicts
$command =
"bucardo_ctl update all tables standard_conflict=source";
$res = $bct->ctl($command);
like ($res, qr/"all tables"/, $t);

$command =
"bucardo_ctl update all sequences standard_conflict=source";
$res = $bct->ctl($command);
like ($res, qr/"all sequences"/, $t);

## Add a new swap sync that goes from A to B
$t = q{Adding a new swap sync works};
$command =
"bucardo_ctl add sync swaptest type=swap source=therd targetdb=B";
$res = $bct->ctl($command);
like ($res, qr/Added sync "swaptest/, $t);

## We want to know when the sync has finished
$dbhX->do(q{LISTEN "bucardo_syncdone_swaptest"});
$dbhX->commit();

## Time to startup Bucardo
$bct->restart_bucardo($dbhX);

## Get the statement handles ready for each table type
for my $table (sort keys %tabletype) {

    $pkey{$table} = $table =~ /test5/ ? q{"id space"} : 'id';

    ## INSERT
    for my $x (1..8) {
        $SQL = $table =~ /X/
            ? "INSERT INTO $table($pkey{$table}) VALUES (?)"
                : "INSERT INTO $table($pkey{$table},data1,inty) VALUES (?,'foo',$x)";
        $sth{insert}{$x}{$table}{A} = $dbhA->prepare($SQL);
        $sth{insert}{$x}{$table}{B} = $dbhB->prepare($SQL);
        if ('BYTEA' eq $tabletype{$table}) {
            $sth{insert}{$x}{$table}{A}->bind_param(1, undef, {pg_type => PG_BYTEA});
            $sth{insert}{$x}{$table}{B}->bind_param(1, undef, {pg_type => PG_BYTEA});
        }
    }

    ## SELECT
    $sql{select}{$table} = "SELECT inty FROM $table ORDER BY $pkey{$table}";
    $table =~ /X/ and $sql{select}{$table} =~ s/inty/$pkey{$table}/;

    ## DELETE
    $SQL = "DELETE FROM $table";
    $sth{deleteall}{$table}{A} = $dbhA->prepare($SQL);

}

## Add one row per table type to A
for my $table (keys %tabletype) {
    my $type = $tabletype{$table};
    my $val1 = $val{$type}{1};
    $sth{insert}{1}{$table}{A}->execute($val1);
}

## Before the commit on A, B should be empty
for my $table (sort keys %tabletype) {
    my $type = $tabletype{$table};
    $t = qq{B has not received rows for table $table before A commits};
    $res = [];
    bc_deeply($res, $dbhB, $sql{select}{$table}, $t);
}

## Commit, then wait for the automatic sync to finish up
$dbhA->commit();
wait_for_notice($dbhX, 'bucardo_syncdone_swaptest', 5);

## Check the second database for the new rows
for my $table (sort keys %tabletype) {

    my $type = $tabletype{$table};
    $t = qq{Row with pkey of type $type gets copied to B};

    $res = [[1]];
    bc_deeply($res, $dbhB, $sql{select}{$table}, $t);
}

## The droptest table should be populated for A, but not for B
for my $table (sort keys %tabletype) {

    $t = qq{Triggers and rules fired on A};
    $SQL = qq{SELECT type FROM droptest WHERE name = '$table' ORDER BY 1};

    $res = [['rule'],['trigger']];
    bc_deeply($res, $dbhA, $SQL, $t);

    $t = qq{Triggers and rules did not fire on B};
    $res = [];
    bc_deeply($res, $dbhB, $SQL, $t);
}

## Turn off the automatic syncing
$command =
'bucardo_ctl update sync swaptest ping=0';
$res = $bct->ctl($command);

$command =
"bucardo_ctl reload sync swaptest";
$res = $bct->ctl($command);

## Delete rows from A
for my $table (keys %tabletype) {
    $sth{deleteall}{$table}{A}->execute();
}
$dbhA->commit();

## B should still have the rows
for my $table (sort keys %tabletype) {

    my $type = $tabletype{$table};
    $t = qq{Row with pkey of type $type is not deleted from B before kick};

    $res = [[1]];
    bc_deeply($res, $dbhB, $sql{select}{$table}, $t);
}

## Kick the sync and wait for it to finish
$bct->ctl('kick sync swaptest 0');

for my $seq (sort keys %sequences) {

    $t = qq{Sequence $seq is copied to database B};

    $SQL = "SELECT sequence_name, last_value, increment_by, max_value, min_value, is_cycled FROM $seq";
    my $seqA = $dbhA->selectall_arrayref($SQL)->[0];
    my $seqB = $dbhB->selectall_arrayref($SQL)->[0];
    is_deeply($seqA, $seqB, $t);

}

## Rows should be gone from B now
for my $table (sort keys %tabletype) {

    my $type = $tabletype{$table};
    $t = qq{Row with pkey of type $type is deleted from B};

    $res = [];
    bc_deeply($res, $dbhB, $sql{select}{$table}, $t);
}

## Now add two rows at once
for my $table (keys %tabletype) {
    my $type = $tabletype{$table};
    my $val2 = $val{$type}{2};
    my $val3 = $val{$type}{3};
    $sth{insert}{2}{$table}{A}->execute($val2);
    $sth{insert}{3}{$table}{A}->execute($val3);
}
$dbhA->commit();

## Kick the sync and wait for it to finish
$bct->ctl('kick sync swaptest 0');

## B should have the two new rows
for my $table (sort keys %tabletype) {

    my $type = $tabletype{$table};
    $t = qq{Two rows with pkey of type $type are copied to B};

    $res = [[2],[3]];
    bc_deeply($res, $dbhB, $sql{select}{$table}, $t);
}

## Test out an update
for my $table (keys %tabletype) {
    my $type = $tabletype{$table};
    $SQL = "UPDATE $table SET inty=inty+10";
    $dbhA->do($SQL);
}
$dbhA->commit();
$bct->ctl('kick sync swaptest 0');

## B should have the updated rows
for my $table (sort keys %tabletype) {

    my $type = $tabletype{$table};
    $t = qq{Updates of two rows with pkey of type $type are copied to B};

    $res = [[12],[13]];
    bc_deeply($res, $dbhB, $sql{select}{$table}, $t);
}

## Test insert, update, and delete all at once, across multiple transactions
for my $table (keys %tabletype) {
    my $type = $tabletype{$table};
    $SQL = "UPDATE $table SET inty=inty-3";
    $dbhA->do($SQL);
    $dbhA->commit();

    my $val4 = $val{$type}{4};
    $sth{insert}{4}{$table}{A}->execute($val4);
    $dbhA->commit();

    $SQL = "DELETE FROM $table WHERE inty = 10";
    $dbhA->do($SQL);
    $dbhA->commit();
}
$bct->ctl('kick sync swaptest 0');

## B should have the updated rows
for my $table (sort keys %tabletype) {

    my $type = $tabletype{$table};
    $t = qq{Updates of two rows with pkey of type $type are copied to B};

    $res = [[9],[4]];
    bc_deeply($res, $dbhB, $sql{select}{$table}, $t);
}

for my $table (sort keys %tabletype) {
    my $type = $tabletype{$table};
    $dbhA->do("COPY $table($pkey{$table},inty,data1) FROM STDIN");
    my $val5 = $val{$type}{5};
    $val5 =~ s/\0//;
    $dbhA->pg_putcopydata("$val5\t5\tfive");
    $dbhA->pg_putcopyend();
    $dbhA->commit();
}
$bct->ctl('kick sync swaptest 0');

## B should have the new rows
for my $table (sort keys %tabletype) {

    my $type = $tabletype{$table};
    $t = qq{COPY to A with pkey type $type makes it way to B};

    $res = [[9],[4],[5]];
    bc_deeply($res, $dbhB, $sql{select}{$table}, $t);
}

## Same row inserted on both sides
$bct->ctl(q{message "Begin insert to swap"});
for my $table (sort keys %tabletype) {
    $dbhA->do("TRUNCATE TABLE $table");
    $dbhB->do("TRUNCATE TABLE $table");

    my $type = $tabletype{$table};
    my $val6 = $val{$type}{6};
    $sth{insert}{6}{$table}{A}->execute($val6);
    $sth{insert}{6}{$table}{B}->execute($val6);
}
$dbhA->commit();
$dbhB->commit();
$bct->ctl('kick sync swaptest 0');

for my $table (sort keys %tabletype) {

    my $type = $tabletype{$table};
    $t = qq{Insert on both sides does not choke the swap sync};

    $res = [[6]];
    bc_deeply($res, $dbhA, $sql{select}{$table}, $t);
    bc_deeply($res, $dbhB, $sql{select}{$table}, $t);
}

for my $table (sort keys %tabletype) {
    $SQL = "UPDATE $table SET inty = 77";
    $dbhA->do($SQL);
    $SQL = "UPDATE $table SET inty = 88";
    $dbhB->do($SQL);
}
$dbhA->commit();
$dbhB->commit();

for my $table (sort keys %tabletype) {

    $SQL = "SELECT inty FROM $table";
    $t = qq{Updates on both sides of swap sync work};

    $res = [[77]];
    bc_deeply($res, $dbhA, $sql{select}{$table}, $t);
    $res = [[88]];
    bc_deeply($res, $dbhB, $sql{select}{$table}, $t);
}

$bct->ctl('kick sync swaptest 0');

for my $table (sort keys %tabletype) {

    $SQL = "SELECT inty FROM $table";
    $t = qq{Expected values are there after swap sync where source wins};

    $res = [[77]];
    bc_deeply($res, $dbhA, $sql{select}{$table}, $t);
    $res = [[77]];
    bc_deeply($res, $dbhB, $sql{select}{$table}, $t);
}

## Delete from B, will also get deleted on A
for my $table (sort keys %tabletype) {
    $SQL = "DELETE FROM $table";
    $dbhB->do($SQL);
}
$dbhB->commit();

$bct->ctl(q{message "Begin swap delete test"});
$bct->ctl('kick sync swaptest 0');

for my $table (sort keys %tabletype) {

    $SQL = "SELECT inty FROM $table";
    $t = qq{Expected values are there post-delete after swap sync where source wins};

    $res = [];
    bc_deeply($res, $dbhB, $sql{select}{$table}, $t);
    $res = [];
    bc_deeply($res, $dbhA, $sql{select}{$table}, $t);
}

## Insert same rows to A and B, change B, extra rows to B
## Also tweak some sequences
for my $table (sort keys %tabletype) {

    my $type = $tabletype{$table};

    ## A only: 5
    my $val5 = $val{$type}{5};
    $sth{insert}{5}{$table}{A}->execute($val5);
    $SQL = "UPDATE $table SET inty = 55";
    $dbhA->do($SQL);

    ## B only: 6
    my $val6 = $val{$type}{6};
    $sth{insert}{6}{$table}{B}->execute($val6);
    $SQL = "UPDATE $table SET inty = 66";
    $dbhB->do($SQL);

    ## A and B: 7
    my $val7 = $val{$type}{7};
    $sth{insert}{7}{$table}{A}->execute($val7);
    $sth{insert}{7}{$table}{B}->execute($val7);

    ## Changes to B should get clobbered
    $SQL = "UPDATE $table SET inty = 77 WHERE inty <> 55";
    $dbhA->do($SQL);
    $SQL = "UPDATE $table SET inty = 777 WHERE inty <> 66";
    $dbhB->do($SQL);

    ## Change a sequence on A
    $SQL = q{SELECT setval('bucardo_test_seq1', 45)};
    $dbhA->do($SQL);
    $SQL = q{ALTER SEQUENCE bucardo_test_seq1 MAXVALUE 400};
    $dbhA->do($SQL);

}
$dbhA->commit();
$dbhB->commit();

$bct->ctl(q{message "Begin swap delete test"});
$bct->ctl('kick sync swaptest 0');

for my $table (sort keys %tabletype) {

    $SQL = "SELECT inty FROM $table ORDER BY 1";
    $t = qq{Expected values are there post-delete after swap sync where source wins};

    $res = [[55],[66],[77]];
    bc_deeply($res, $dbhA, $sql{select}{$table}, $t);
    $res = [[55],[66],[77]];
    bc_deeply($res, $dbhB, $sql{select}{$table}, $t);

}

$t = qq{Sequence bucardo_test_seq1 is copied to database B};

$SQL = 'SELECT sequence_name, last_value, increment_by, max_value, min_value, is_cycled FROM bucardo_test_seq1';
my $seqA = $dbhA->selectall_arrayref($SQL)->[0];
my $seqB = $dbhB->selectall_arrayref($SQL)->[0];
is_deeply($seqA, $seqB, $t);

## Make B the "master"
$command =
"bucardo_ctl update all tables standard_conflict=target";
$res = $bct->ctl($command);
like ($res, qr/"all tables"/, $t);

$command =
"bucardo_ctl reload sync swaptest";
$res = $bct->ctl($command);

## Delete all rows, some A, and some B, make sure both sides end up empty
for my $table (sort keys %tabletype) {
    $SQL = "DELETE FROM $table WHERE inty IN (55,66)";
    $dbhA->do($SQL);
    $SQL = "DELETE FROM $table WHERE inty IN (77)";
    $dbhB->do($SQL);
}
$dbhA->commit();
$dbhB->commit();

$bct->ctl('kick sync swaptest 0');

for my $table (sort keys %tabletype) {

    $SQL = "SELECT inty FROM $table ORDER BY 1";
    $t = qq{Deletes on both sides are carried over to the other};

    $res = [];
    bc_deeply($res, $dbhA, $sql{select}{$table}, $t);
    bc_deeply($res, $dbhB, $sql{select}{$table}, $t);
}


## Same scenario as above, but this time B should "win"
for my $table (sort keys %tabletype) {

    my $type = $tabletype{$table};

    ## A only: 5
    my $val5 = $val{$type}{5};
    $sth{insert}{5}{$table}{A}->execute($val5);
    $SQL = "UPDATE $table SET inty = 55";
    $dbhA->do($SQL);

    ## B only: 6
    my $val6 = $val{$type}{6};
    $sth{insert}{6}{$table}{B}->execute($val6);
    $SQL = "UPDATE $table SET inty = 66";
    $dbhB->do($SQL);

    ## A and B: 7
    my $val7 = $val{$type}{7};
    $sth{insert}{7}{$table}{A}->execute($val7);
    $sth{insert}{7}{$table}{B}->execute($val7);

    ## Changes to A should get clobbered
    $SQL = "UPDATE $table SET inty = 77 WHERE inty <> 55";
    $dbhA->do($SQL);
    $SQL = "UPDATE $table SET inty = 777 WHERE inty <> 66";
    $dbhB->do($SQL);
}
$dbhA->commit();
$dbhB->commit();

$bct->ctl(q{message "Begin swap delete test"});
$bct->ctl('kick sync swaptest 0');

for my $table (sort keys %tabletype) {

    $SQL = "SELECT inty FROM $table ORDER BY 1";
    $t = qq{Expected values are there post-delete after swap sync where target wins};

    $res = [[55],[66],[777]];
    bc_deeply($res, $dbhA, $sql{select}{$table}, $t);
    $res = [[55],[66],[777]];
    bc_deeply($res, $dbhB, $sql{select}{$table}, $t);
}

pass('Done with swap testing');

exit;
