#!/usr/bin/env perl
# -*-mode:cperl; indent-tabs-mode: nil-*-

## Test pushdelta functionality

use 5.008003;
use strict;
use warnings;
use Data::Dumper;
use lib 't','.';
use DBD::Pg;
use Test::More;

use BucardoTesting;
my $bct = BucardoTesting->new() or BAIL_OUT "Creation of BucardoTesting object failed\n";
$location = 'pushdelta';

my $numtabletypes = keys %tabletype;
plan tests => 43 + ($numtabletypes * 20);

pass("*** Beginning pushdelta tests");

use vars qw/ $dbhX $dbhA $dbhB $dbhC $dbhD $res $command $t %pkey $SQL %sth %sql/;

END {
    $bct->stop_bucardo($dbhX);
    $dbhX->disconnect();
    $dbhA->disconnect();
    $dbhB->disconnect();
    $dbhC->disconnect();
    $dbhD->disconnect();
}

## Get A, B, C, and D created, emptied out, and repopulated with sample data
$dbhA = $bct->repopulate_cluster('A');
$dbhB = $bct->repopulate_cluster('B');
$dbhC = $bct->repopulate_cluster('C');
$dbhD = $bct->repopulate_cluster('D');

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

$t = 'Adding database from cluster C works';
($dbuser,$dbport,$dbhost) = $bct->add_db_args('C');
$command =
"bucardo_ctl add db bucardo_test name=C user=$dbuser port=$dbport host=$dbhost";
$res = $bct->ctl($command);
like ($res, qr/Added database "C"/, $t);

$t = 'Adding database from cluster D works';
($dbuser,$dbport,$dbhost) = $bct->add_db_args('D');
$command =
"bucardo_ctl add db bucardo_test name=D user=$dbuser port=$dbport host=$dbhost";
$res = $bct->ctl($command);
like ($res, qr/Added database "D"/, $t);

## Teach Bucardo about all pushable tables, adding them to a new herd named "therd"
$t = q{Adding all tables on the master works};
$command =
"bucardo_ctl add tables all db=A herd=therd pkonly";
$res = $bct->ctl($command);
like ($res, qr/Creating herd: therd.*New tables added: \d/s, $t);

## Add all sequences, and add them to the newly created herd
$t = q{Adding all sequences on the master works};
$command =
"bucardo_ctl add sequences all db=A herd=therd";
$res = $bct->ctl($command);
like ($res, qr/New sequences added: \d/, $t);

## Add a new pushdelta sync that goes from A to B
$t = q{Adding a new pushdelta sync works};
$command =
"bucardo_ctl add sync pushdeltaAB type=pushdelta source=therd targetdb=B";
$res = $bct->ctl($command);
like ($res, qr/Added sync "pushdeltaAB/, $t);

## Create a database group consisting of A and B
$t = q{Adding dbgroup 'slaves' works};
$command =
"bucardo_ctl add dbgroup slaves B C";
$res = $bct->ctl($command);
like ($res, qr/\QAdded database "B" to group "slaves"\E.*
              \QAdded database "C" to group "slaves"\E.*
              \QAdded database group "slaves"/xsm, $t);

## We want to know when the sync has finished
$dbhX->do(q{LISTEN "bucardo_syncdone_pushdeltaAB"});
$dbhX->commit();

## Time to startup Bucardo
$bct->restart_bucardo($dbhX);

## Now for the meat of the tests

## Get the statement handles ready for each table type
for my $table (sort keys %tabletype) {

    $pkey{$table} = $table =~ /test5/ ? q{"id space"} : 'id';

    ## INSERT
    for my $x (1..6) {
        $SQL = $table =~ /0/
            ? "INSERT INTO $table($pkey{$table}) VALUES (?)"
                : "INSERT INTO $table($pkey{$table},data1,inty) VALUES (?,'foo',$x)";
        $sth{insert}{$x}{$table}{A} = $dbhA->prepare($SQL);
        if ('BYTEA' eq $tabletype{$table}) {
            $sth{insert}{$x}{$table}{A}->bind_param(1, undef, {pg_type => PG_BYTEA});
        }
    }

    ## SELECT
    $sql{select}{$table} = "SELECT inty FROM $table ORDER BY $pkey{$table}";
    $table =~ /0/ and $sql{select}{$table} =~ s/inty/$pkey{$table}/;

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
wait_for_notice($dbhX, 'bucardo_syncdone_pushdeltaAB', 5);

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
'bucardo_ctl update sync pushdeltaAB ping=0';
$res = $bct->ctl($command);

$command =
"bucardo_ctl reload sync pushdeltaAB";
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
$bct->ctl('kick sync pushdeltaAB 0');

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
$bct->ctl('kick sync pushdeltaAB 0');

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
$bct->ctl('kick sync pushdeltaAB 0');

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
$bct->ctl('kick sync pushdeltaAB 0');

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
$bct->ctl('kick sync pushdeltaAB 0');

## B should have the new rows
for my $table (sort keys %tabletype) {

    my $type = $tabletype{$table};
    $t = qq{COPY to A with pkey type $type makes it way to B};

    $res = [[9],[4],[5]];
    bc_deeply($res, $dbhB, $sql{select}{$table}, $t);
}

## Trim out a few rows from bucardo_delta so they don't automatically appear on C
for my $table (sort keys %tabletype) {

    my $type = $tabletype{$table};
    my $val4 = $val{$type}{4};

    $SQL = q{
DELETE FROM bucardo.bucardo_delta WHERE tablename =
  (SELECT oid FROM pg_class WHERE relname = ?)
  AND rowid = ?
};
    if ('BYTEA' eq $tabletype{$table}) {
        $SQL =~ s/rowid/DECODE(rowid,'base64')/;
        $val4 =~ s/\0/\\000/;
    }
    my $sth = $dbhA->prepare($SQL);
    my $count = $sth->execute($table,$val4);
    $t = qq{Row for pkey of type $type deleted from bucardo_delta};
    is ($count, 1, $t);
}
$dbhA->commit();

## Modify the sync and have it go to B *and* C
$command =
"bucardo_ctl update sync pushdeltaAB set targetgroup=slaves";
$res = $bct->ctl($command);

## Before the sync reload, C should not have anything
for my $table (sort keys %tabletype) {

    my $type = $tabletype{$table};
    $t = qq{Row with pkey of type $type does not exist on C yet};

    $res = [];
    bc_deeply($res, $dbhC, $sql{select}{$table}, $t);
}

$command =
"bucardo_ctl reload sync pushdeltaAB";
$res = $bct->ctl($command);

$bct->ctl('kick sync pushdeltaAB 0');

## After the sync is reloaded and kicked, C will have some rows
for my $table (sort keys %tabletype) {

    my $type = $tabletype{$table};
    $t = qq{Row with pkey of type $type does not exist on C yet};

    $res = [[9],[5]];
    bc_deeply($res, $dbhC, $sql{select}{$table}, $t);
}

## Do an update, and have it appear on both sides
for my $table (keys %tabletype) {
    my $type = $tabletype{$table};
    $SQL = "UPDATE $table SET inty=55 WHERE inty = 5";
    $dbhA->do($SQL);
}
$dbhA->commit();
$bct->ctl('kick sync pushdeltaAB 0');

for my $table (sort keys %tabletype) {

    my $type = $tabletype{$table};
    $t = qq{Row with pkey of type $type is replicated to B};

    $res = [[9],[4],[55]];
    bc_deeply($res, $dbhB, $sql{select}{$table}, $t);

    ## Appears on C as well because the bucardo_delta entries are still there!
    $t = qq{Row with pkey of type $type is replicated to C};
    $res = [[9],[55]];
    bc_deeply($res, $dbhC, $sql{select}{$table}, $t);
}

## Use onetimecopy to force C into compliance
$command = '
bucardo_ctl update sync pushdeltaAB set onetimecopy=2';
$bct->ctl($command);

## Since this is mode 2 (fullcopy only if target is empty), empty out C
## The other option is onetimecopy=1, which would unconditionally copy B and C
for my $table (sort keys %tabletype) {
    $SQL = "TRUNCATE TABLE $table";
    $dbhC->do($SQL);
}
$dbhC->commit();

## Reload and kick so the new onetimecopy takes effect
$command =
"bucardo_ctl reload sync pushdeltaAB";
$bct->ctl($command);

$bct->ctl('kick sync pushdeltaAB 0');

## C should now have all rows
for my $table (sort keys %tabletype) {

    my $type = $tabletype{$table};
    $t = qq{Row with pkey of type $type is now fully populated on C};

    $res = [[9],[4],[55]];
    bc_deeply($res, $dbhC, $sql{select}{$table}, $t);

}

## C should now have all rows
for my $seq (sort keys %sequences) {

    $t = qq{Sequence $seq is copied to database B};

    $SQL = "SELECT * FROM $seq";
    my $seqA = $dbhA->selectall_arrayref($SQL)->[0];
    my $seqB = $dbhB->selectall_arrayref($SQL)->[0];
    is_deeply($seqA, $seqB, $t);

    $t = qq{Sequence $seq is copied to database C};
    my $seqC = $dbhC->selectall_arrayref($SQL)->[0];
    is_deeply($seqA, $seqC, $t);

}

## Update all the sequences on A
for my $seq (sort keys %sequences) {
    $dbhA->do("SELECT nextval('$seq')");
}
$dbhA->commit();
$bct->ctl('kick sync pushdeltaAB 0');

for my $seq (sort keys %sequences) {

    $t = qq{Sequence $seq is copied to database B};

    $SQL = "SELECT sequence_name, last_value, increment_by, max_value, min_value, is_cycled FROM $seq";
    my $seqA = $dbhA->selectall_arrayref($SQL)->[0];
    my $seqB = $dbhB->selectall_arrayref($SQL)->[0];
    is_deeply($seqA, $seqB, $t);

    $t = qq{Sequence $seq is copied to database C};
    my $seqC = $dbhC->selectall_arrayref($SQL)->[0];
    is_deeply($seqA, $seqC, $t);

}


SKIP: {

    skip q{Sequence meta-information not replicated yet}, $numtabletypes * 2;

    ## Make some more invasive changes to the sequences
    ## Update all the sequences on A
    for my $seq (sort keys %sequences) {
        $dbhA->do("ALTER SEQUENCE $seq start 2 minvalue 2 maxvalue 500 cycle increment by 2");
    }
    $dbhA->commit();
    $bct->ctl('kick sync pushdeltaAB 0');

    for my $seq (sort keys %sequences) {

        $t = qq{Sequence $seq is copied to database B};

        $SQL = "SELECT sequence_name, last_value, increment_by, max_value, min_value, is_cycled FROM $seq";
        my $seqA = $dbhA->selectall_arrayref($SQL)->[0];
        my $seqB = $dbhB->selectall_arrayref($SQL)->[0];
        is_deeply($seqA, $seqB, $t);

        $t = qq{Sequence $seq is copied to database C};
        my $seqC = $dbhC->selectall_arrayref($SQL)->[0];
        is_deeply($seqA, $seqC, $t);
    }

} ## end of SKIP

## Test of truncation
SKIP: {

    if ($dbhA->{pg_server_version} < 80400) {
        skip 'Cannot test truncation replication unless server is 8.4 or higher', $numtabletypes;
    }

    $bct->ctl(q{message "Begin truncate tests"});
    for my $table (sort keys %tabletype) {
        $dbhA->do("TRUNCATE TABLE $table");
    }
    $dbhA->commit();
    $bct->ctl(q{message "Truncation complete"});
    $bct->ctl('kick sync pushdeltaAB 0');
    $bct->ctl(q{message "Post-truncation kick complete"});

    for my $table (sort keys %tabletype) {

        my $type = $tabletype{$table};
        $t = qq{Truncation of table with pkey of type $type is replicated to B};

        $res = [];
        bc_deeply($res, $dbhB, $sql{select}{$table}, $t);

        $t = qq{Truncation of table with pkey of type $type is replicated to C};

        bc_deeply($res, $dbhC, $sql{select}{$table}, $t);

    }

} ## end of SKIP

## Test out onetimecopy more completely

$t = qq{Sync attrib onetimecopy resets itself to 0 when complete};
$command = '
bucardo_ctl list sync pushdeltaAB -vv';
$res = $bct->ctl($command);
like ($res, qr{onetimecopy\s+=\s+0}, $t);

## Add a new rows (inty=99) to both slaves and a row (inty=17) to the master
$t = qq{A new row on slave is not automatically removed by a pushdelta sync};
$dbhB->do('INSERT INTO bucardo_test1(id,inty) VALUES(99,99)');
$dbhB->commit();
$dbhC->do('INSERT INTO bucardo_test1(id,inty) VALUES(99,99)');
$dbhC->commit();
$dbhA->do('INSERT INTO bucardo_test1(id,inty) VALUES (17,17)');
$dbhA->commit();
$bct->ctl('kick sync pushdeltaAB 0');

## Sanity check that both sides have the new row
$t = qq{New row created on slave database B};
$SQL = 'SELECT count(*) FROM bucardo_test1 WHERE inty = 99';
bc_deeply ([[1]], $dbhB, $SQL, $t);
$t = qq{New row created on slave database C};
bc_deeply ([[1]], $dbhC, $SQL, $t);
$t = qq{New row not created on slave database A};
bc_deeply ([[0]], $dbhA, $SQL, $t);

## Flip the sync to onetimecopy=2, then kick it off
$command = '
bucardo_ctl update sync pushdeltaAB set onetimecopy=2';
$bct->ctl($command);

$t = qq{Sync attrib onetimecopy accepts and keeps a setting of 2};
$command = '
bucardo_ctl list sync pushdeltaAB -vv';
$res = $bct->ctl($command);
like ($res, qr{onetimecopy\s+=\s+2}, $t);

$bct->ctl(q{message "Begin onetimecopy 2 tests"});

$command =
"bucardo_ctl reload sync pushdeltaAB";
$bct->ctl($command);
$bct->ctl('kick sync pushdeltaAB 0');

$t = qq{Sync attrib onetimecopy resets itself to 0 when complete};
$command = '
bucardo_ctl list sync pushdeltaAB -vv';
$res = $bct->ctl($command);
like ($res, qr{onetimecopy\s+=\s+0}, $t);

$t = q{Setting onetimecopy=2 does not overwrite tables on B with data in them};
bc_deeply ([[1]], $dbhB, $SQL, $t);

$t = q{Setting onetimecopy=2 does not overwrite tables on C with data in them};
bc_deeply ([[1]], $dbhC, $SQL, $t);

$t = q{Rows are copied from A to B};
$SQL = 'SELECT count(*) FROM bucardo_test1 WHERE inty = 17';
bc_deeply ([[1]], $dbhB, $SQL, $t);

$t = q{Rows are copied from A to C};
bc_deeply ([[1]], $dbhB, $SQL, $t);

## Flip the sync to onetimecopy=1, then kick it off

$bct->ctl(q{message "Begin onetimecopy 2 tests"});

$dbhA->do("UPDATE bucardo_test1 SET inty=18 WHERE inty=17");
$dbhA->commit();

$command = '
bucardo_ctl update sync pushdeltaAB set onetimecopy=1';
$bct->ctl($command);

$t = qq{Sync attrib onetimecopy accepts and keeps a setting of 1};
$command = '
bucardo_ctl list sync pushdeltaAB -vv';
$res = $bct->ctl($command);
like ($res, qr{onetimecopy\s+=\s+1}, $t);

$command =
"bucardo_ctl reload sync pushdeltaAB";
$bct->ctl($command);
$bct->ctl('kick sync pushdeltaAB 0');

$t = qq{Sync attrib onetimecopy resets itself to 0 when complete};
$command = '
bucardo_ctl list sync pushdeltaAB -vv';
$res = $bct->ctl($command);
like ($res, qr{onetimecopy\s+=\s+0}, $t);

$SQL = 'SELECT count(*) FROM bucardo_test1 WHERE inty = 99';

$t = q{Setting onetimecopy=1 overwrites tables on B with data in them};
bc_deeply ([[0]], $dbhB, $SQL, $t);

$t = q{Setting onetimecopy=1 overwrites tables on C with data in them};
bc_deeply ([[0]], $dbhC, $SQL, $t);

$t = q{Rows are copied from A to B};
$SQL = 'SELECT count(*) FROM bucardo_test1 WHERE inty = 18';
bc_deeply ([[1]], $dbhB, $SQL, $t);

$t = q{Rows are copied from A to C};
bc_deeply ([[1]], $dbhB, $SQL, $t);

## Use customode to solve a unique constraint issue

$dbhA->do('DELETE FROM bucardo_test1');
$dbhB->do('DELETE FROM bucardo_test1');

$SQL = q{INSERT INTO bucardo_test1(id,inty,email) VALUES (1,1,'zed')};
$dbhA->do($SQL);
$dbhA->commit();

$SQL = q{INSERT INTO bucardo_test1(id,inty,email) VALUES (2,2,'zed')};
$dbhB->do($SQL);
$dbhB->commit();

$dbhX->do(<<'END_CCINSERT'
INSERT INTO bucardo.customcode
    (name, about, whenrun, getrows, src_code)
VALUES
    ('solve_unique_email',
     'Solve a unique email problem',
     'exception',
     true,
$perl$
my $args = $_[0];

return if (exists $args->{dummy});

## If we have a unique email constraint, fix it up
my $info = $args->{rowinfo};
if ($info->{dbi_error} !~ /unique constraint "bucardo_test1_email_key"/) {
  die "Do not know how to handle this exception\n";
}

## We don't need any more information such as table name, pkey, etc. because 
## we know enough from that constraint name
## Perhaps make this a generic handler with a regex name later?

## Grab the row that failed
if ($info->{dbi_error} !~ /line \d+: "(.+)"/) {
  die "Could not extract COPY line\n";
}

my $fail = $1;
my @fail = map { $_ =~ /\\N/ ? undef : $_ } split /\t/ => $fail;

my $email = $fail[5];

## In this case, we'll remove the one on the target
my $targetdbh = $args->{targetdbh};
my $safemail;
eval {
  $safemail = $targetdbh->quote($email);
};
$@ and warn "customcode failure: $@\n";
eval {
$targetdbh->do("DELETE FROM bucardo_test1 WHERE email = $safemail");
};
$@ and warn "customcode failure: $@\n";
$args->{runagain} = 1;
return;
$perl$);
END_CCINSERT
);


$dbhX->do(q{INSERT INTO bucardo.customcode_map (code, goat) 
    SELECT 1, id FROM bucardo.goat WHERE tablename = 'bucardo_test1'});
$dbhX->commit();

$command =
"bucardo_ctl reload sync pushdeltaAB";
$res = $bct->ctl($command);

$SQL = 'SELECT id,email FROM bucardo_test1 ORDER BY 1';

bc_deeply ([[1,'zed']], $dbhA, $SQL, $t);

bc_deeply ([[2,'zed']], $dbhB, $SQL, $t);

$bct->ctl('kick sync pushdeltaAB 0');

bc_deeply ([[1,'zed']], $dbhA, $SQL, $t);

bc_deeply ([[1,'zed']], $dbhB, $SQL, $t);

exit;

