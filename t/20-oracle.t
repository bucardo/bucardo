#!/usr/bin/env perl
# -*-mode:cperl; indent-tabs-mode: nil-*-

## Test using Oracle as a database target

use 5.008003;
use strict;
use warnings;
use Data::Dumper;
use lib 't','.';
use DBD::Pg;
use Test::More;
use MIME::Base64;

use vars qw/ $bct $dbhX $dbhA $dbhB $dbhC $dbhD $res $command $t %pkey $SQL %sth %sql/;

## Must have the DBD::Oracle module
my $evalok = 0;
eval {
    require DBD::Oracle;
    $evalok = 1;
};
if (!$evalok) {
    plan (skip_all =>  'Cannot test Oracle unless the Perl module DBD::Oracle is installed');
}

## Oracle must be up and running
$evalok = 0;
my $dbh;
my $dbuser = 'system';
my $dbname = $dbuser;
my $sid = 'o';
my $host = '127.0.0.1';
my $pass = 'abcde';
eval {
    $dbh = DBI->connect("dbi:Oracle:host=$host;sid=$sid", $dbuser, $pass,
                         {AutoCommit=>0, PrintError=>0, RaiseError=>1});
    $evalok = 1;
};
if (!$evalok) {
    plan (skip_all =>  "Cannot test Oracle as we cannot connect to a running Oracle database: $@");
}

use BucardoTesting;

## For now, remove some tables that don't work
for my $num (3,5,6,8,10) {
    delete $tabletype{"bucardo_test$num"};
}

my $numtabletypes = keys %tabletype;
plan tests => 62;

## Create one table for each table type
for my $table (sort keys %tabletype) {

    my $pkeyname = $table =~ /test5/ ? q{"id space"} : 'id';
    my $pkindex = $table =~ /test2/ ? '' : 'PRIMARY KEY';

    eval {
        $dbh->do("DROP TABLE $table");
    };
    $@ and $dbh->rollback();

    $SQL = qq{
            CREATE TABLE $table (
                $pkeyname    $tabletypeoracle{$table} NOT NULL $pkindex};
    $SQL .= $table =~ /X/ ? "\n)" : qq{,
                data1 NVARCHAR2(100)  NULL,
                inty  SMALLINT        NULL,
                bite1 BLOB            NULL,
                bite2 BLOB            NULL,
                email NVARCHAR2(100)  NULL UNIQUE
            )
            };

    $dbh->do($SQL);

    if ($table =~ /test2/) {
        $dbh->do(qq{ALTER TABLE $table ADD CONSTRAINT "multipk" PRIMARY KEY ($pkeyname,data1)});
    }

}

$bct = BucardoTesting->new() or BAIL_OUT "Creation of BucardoTesting object failed\n";
$location = 'oracle';

pass("*** Beginning oracle tests");

END {
    $bct and $bct->stop_bucardo($dbhX);
    $dbhX and  $dbhX->disconnect();
    $dbhA and $dbhA->disconnect();
    $dbhB and $dbhB->disconnect();
    $dbhC and $dbhC->disconnect();
}

## Get Postgres database A and B and C created
$dbhA = $bct->repopulate_cluster('A');
$dbhB = $bct->repopulate_cluster('B');
$dbhC = $bct->repopulate_cluster('C');

## Create a bucardo database, and install Bucardo into it
$dbhX = $bct->setup_bucardo('A');

## Tell Bucardo about these databases

## Three Postgres databases will be source, source, and target
for my $name (qw/ A B C /) {
    $t = "Adding database from cluster $name works";
    my ($dbuser,$dbport,$dbhost) = $bct->add_db_args($name);
    $command = "bucardo add db $name dbname=bucardo_test user=$dbuser port=$dbport host=$dbhost";
    $res = $bct->ctl($command);
    like ($res, qr/Added database "$name"/, $t);
}

$t = 'Adding oracle database Q works';
$command =
"bucardo add db Q dbname=$dbuser type=oracle dbuser=$dbuser dbhost=$host conn=sid=$sid dbpass=$pass";
$res = $bct->ctl($command);
like ($res, qr/Added database "Q"/, $t);

## Teach Bucardo about all pushable tables, adding them to a new relgroup named "therd"
$t = q{Adding all tables on the master works};
$command =
"bucardo add tables all db=A relgroup=therd pkonly";
$res = $bct->ctl($command);
like ($res, qr/Creating relgroup: therd.*New tables added: \d/s, $t);

## Add all sequences, and add them to the newly created relgroup
$t = q{Adding all sequences on the master works};
$command =
"bucardo add sequences all db=A relgroup=therd";
$res = $bct->ctl($command);
like ($res, qr/New sequences added: \d/, $t);

## Create a new dbgroup
$t = q{Created a new dbgroup};
$command =
"bucardo add dbgroup qx A:source B:source C Q";
$res = $bct->ctl($command);
like ($res, qr/Created dbgroup "qx"/, $t);

## Create a new sync
$t = q{Created a new sync};
$command =
"bucardo add sync oracle relgroup=therd dbs=qx autokick=false";
$res = $bct->ctl($command);
like ($res, qr/Added sync "oracle"/, $t);

## Create a second sync, solely for multi-sync interaction issues
$bct->ctl('bucardo add dbgroup t1 A:source B C');
$bct->ctl('bucardo add sync tsync1 relgroup=therd dbs=t1 autokick=false status=inactive');

## Start up Bucardo with these new syncs
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

## Before the commit on A, B and C should be empty
for my $table (sort keys %tabletype) {
    my $type = $tabletype{$table};
    $t = qq{B has not received rows for table $table before A commits};
    $res = [];
    bc_deeply($res, $dbhB, $sql{select}{$table}, $t);
    bc_deeply($res, $dbhC, $sql{select}{$table}, $t);
}

## Commit, then kick off the sync
$dbhA->commit();
$bct->ctl('bucardo kick oracle 0');
$bct->ctl('bucardo kick oracle 0');

## Check B and C for the new rows
for my $table (sort keys %tabletype) {

    my $type = $tabletype{$table};
    $t = qq{Row with pkey of type $type gets copied to B};

    $res = [[1]];
    bc_deeply($res, $dbhB, $sql{select}{$table}, $t);
    bc_deeply($res, $dbhC, $sql{select}{$table}, $t);
}

## Check that Oracle has the new rows
for my $table (sort keys %tabletype) {
    $t = "Oracle table $table has correct number of rows after insert";
    $SQL = "SELECT * FROM $table";
    my $sth = $dbh->prepare($SQL);
    my $count = $sth->execute();
    #is ($count, 1, $t);

    $t = "Oracle table $table has correct entries";
    my $info = $sth->fetchall_arrayref({})->[0];
    my $type = $tabletype{$table};
    my $id = $val{$type}{1};
    my $pkeyname = $table =~ /test5/ ? 'ID SPACE' : 'ID';

    ## Datetime has no time zone thingy at the end
    $tabletypeoracle{$table} =~ /DATETIME/ and $id =~ s/\+.*//;

    is_deeply(
        $info,
        {
            $pkeyname => $id,
            INTY => 1,
            EMAIL => undef,
            BITE1 => undef,
            BITE2 => undef,
            DATA1 => 'foo',
        },

        $t);
}

## Update each row
for my $table (keys %tabletype) {
    $sth{update}{$table}{A}->execute(42);
}
$dbhA->commit();
$bct->ctl('bucardo kick oracle 0');

for my $table (keys %tabletype) {
    $t = "Oracle table $table has correct number of rows after update";
    $SQL = "SELECT * FROM $table";
    my $sth = $dbh->prepare($SQL);
    my $count = $sth->execute();
    #is ($count, 1, $t);

    $t = "Oracle table $table has updated value";
    my $info = $sth->fetchall_arrayref({})->[0];
    is ($info->{INTY}, 42, $t);
}

## Delete each row
for my $table (keys %tabletype) {
    $sth{deleteall}{$table}{A}->execute();
}
$dbhA->commit();
$bct->ctl('bucardo kick oracle 0');

for my $table (keys %tabletype) {
    $t = "Oracle table $table has correct number of rows after delete";
    $SQL = "SELECT * FROM $table";
    my $sth = $dbh->prepare($SQL);
    (my $count = $sth->execute()) =~ s/0E0/0/;
    $sth->finish();
    is ($count, 0, $t);
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
$bct->ctl('bucardo kick oracle 0');

for my $table (keys %tabletype) {
    $t = "Oracle table $table has correct number of rows after double insert";
    $SQL = "SELECT count(*) FROM $table";
    my $sth = $dbh->prepare($SQL);
    $sth->execute();
    my $count = $sth->fetchall_arrayref()->[0][0];
    is ($count, 2, $t);
}

## Delete one of the rows
for my $table (keys %tabletype) {
    $sth{deleteone}{$table}{A}->execute(2); ## inty = 2
}
$dbhA->commit();
$bct->ctl('bucardo kick oracle 0');

for my $table (keys %tabletype) {
    $t = "Oracle table $table has correct number of rows after single deletion";
    $SQL = "SELECT count(*) FROM $table";
    my $sth = $dbh->prepare($SQL);
    $sth->execute();
    my $count = $sth->fetchall_arrayref()->[0][0];
    is ($count, 1, $t);
}

## Insert two more rows, then truncate
for my $table (keys %tabletype) {
    my $type = $tabletype{$table};
    my $val3 = $val{$type}{3};
    $sth{insert}{3}{$table}{A}->execute($val3);
    my $val4 = $val{$type}{4};
    $sth{insert}{4}{$table}{A}->execute($val4);
}
$dbhA->commit();
$bct->ctl('bucardo kick oracle 0');

for my $table (keys %tabletype) {
    $t = "Oracle table $table has correct number of rows after more inserts";
    $SQL = "SELECT count(*) FROM $table";
    my $sth = $dbh->prepare($SQL);
    $sth->execute();
    my $count = $sth->fetchall_arrayref()->[0][0];
    is ($count, 3, $t);
}

$dbh->disconnect();
pass 'Finished Oracle tests';

exit;

