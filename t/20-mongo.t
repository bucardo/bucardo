#!/usr/bin/env perl
# -*-mode:cperl; indent-tabs-mode: nil-*-

## Test using MongoDB as a database target

use 5.008003;
use strict;
use warnings;
use Data::Dumper;
use lib 't','.';
use DBD::Pg;
use Test::More;
use MIME::Base64;

use vars qw/ $bct $dbhX $dbhA $dbhB $dbhC $dbhD $res $command $t %pkey $SQL %sth %sql/;

## Must have the MongoDB module
my $evalok = 0;
eval {
    require MongoDB;
    $evalok = 1;
};
if (!$evalok) {
	plan (skip_all =>  'Cannot test mongo unless the Perl module MongoDB is installed');
}

## MongoDB must be up and running
$evalok = 0;
my $conn;
eval {
    $conn = MongoDB::Connection->new({});
    $evalok = 1;
};
if (!$evalok) {
	plan (skip_all =>  'Cannot test mongo as we cannot connect to a running Mongo db');
}

use BucardoTesting;

## For now, remove the bytea table type as we don't have full mongo support yet
delete $tabletype{bucardo_test8};

## Also cannot handle multi-column primary keys
delete $tabletype{bucardo_test2};


my $numtabletypes = keys %tabletype;
plan tests => 125;

## Make sure we start clean by dropping the test database
my $dbname = 'bucardotest';
my $db = $conn->get_database($dbname);
$db->drop;

$t = qq{Test database "$dbname" has no collections};
my @names = $db->collection_names;
is_deeply (\@names, [], $t);

$bct = BucardoTesting->new() or BAIL_OUT "Creation of BucardoTesting object failed\n";
$location = 'mongo';

pass("*** Beginning mongo tests");

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
    $command = "bucardo add db bucardo_test name=$name user=$dbuser port=$dbport host=$dbhost";
    $res = $bct->ctl($command);
    like ($res, qr/Added database "$name"/, $t);
}

$t = 'Adding mongo database M works';
$command =
"bucardo add db $dbname name=M type=mongo";
$res = $bct->ctl($command);
like ($res, qr/Added database "M"/, $t);

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
"bucardo add dbgroup md A:source B:source C M";
$res = $bct->ctl($command);
like ($res, qr/Created database group "md"/, $t);

## Create a new sync
$t = q{Created a new sync};
$command =
"bucardo add sync mongo herd=therd dbs=md ping=false";
$res = $bct->ctl($command);
like ($res, qr/Added sync "mongo"/, $t);

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
$bct->ctl('bucardo kick mongo 0');
$bct->ctl('bucardo kick mongo 0');

## Check B and C for the new rows
for my $table (sort keys %tabletype) {

    my $type = $tabletype{$table};
    $t = qq{Row with pkey of type $type gets copied to B};

    $res = [[1]];
    bc_deeply($res, $dbhB, $sql{select}{$table}, $t);
    bc_deeply($res, $dbhC, $sql{select}{$table}, $t);
}

## Check that mongo has the new collection names
my %col;
@names = $db->collection_names;
for (@names) {
    $col{$_} = 1;
}

for my $table (sort keys %tabletype) {
    $t = "Table $table has a mongodb collection";
    ok(exists $col{$table}, $t);
}

## Check that mongo has the new rows
for my $table (sort keys %tabletype) {
    $t = "Mongo collection $table has correct number of rows after insert";
    my $col = $db->get_collection($table);
    my @rows = $col->find->all;
    my $count = @rows;
    is ($count, 1, $t);

    ## Remove the mongo internal id column
    delete $rows[0]->{_id};

    $t = "Mongo collection $table has correct entries";
    my $type = $tabletype{$table};
    my $id = $val{$type}{1};
    my $pkeyname = $table =~ /test5/ ? 'id space' : 'id';

    ## For now, binary is stored in escaped form, so we skip this one
    next if $table =~ /test8/;

    is_deeply(
        $rows[0],
        {
            $pkeyname => $id,
            inty => 1,
            email => undef,
            bite1 => undef,
            bite2 => undef,
            data1 => 'foo',
        },

        $t);
}

## Update each row
for my $table (keys %tabletype) {
    $sth{update}{$table}{A}->execute(42);
}
$dbhA->commit();
$bct->ctl('bucardo kick mongo 0');

for my $table (keys %tabletype) {
    $t = "Mongo collection $table has correct number of rows after update";
    my $col = $db->get_collection($table);
    my @rows = $col->find->all;
    my $count = @rows;
    is ($count, 1, $t);

    $t = "Mongo collection $table has updated value";
    is ($rows[0]->{inty}, 42, $t);
}

## Delete each row
for my $table (keys %tabletype) {
    $sth{deleteall}{$table}{A}->execute();
}
$dbhA->commit();
$bct->ctl('bucardo kick mongo 0');

for my $table (keys %tabletype) {
    $t = "Mongo collection $table has correct number of rows after delete";
    my $col = $db->get_collection($table);
    my @rows = $col->find->all;
    my $count = @rows;
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
$bct->ctl('bucardo kick mongo 0');

for my $table (keys %tabletype) {
    $t = "Mongo collection $table has correct number of rows after double insert";
    my $col = $db->get_collection($table);
    my @rows = $col->find->all;
    my $count = @rows;
    is ($count, 2, $t);
}

## Delete one of the rows
for my $table (keys %tabletype) {
    $sth{deleteone}{$table}{A}->execute(2); ## inty = 2
}
$dbhA->commit();
$bct->ctl('bucardo kick mongo 0');

for my $table (keys %tabletype) {
    $t = "Mongo collection $table has correct number of rows after single deletion";
    my $col = $db->get_collection($table);
    my @rows = $col->find->all;
    my $count = @rows;
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
$bct->ctl('bucardo kick mongo 0');

for my $table (keys %tabletype) {
    $t = "Mongo collection $table has correct number of rows after more inserts";
    my $col = $db->get_collection($table);
    my @rows = $col->find->all;
    my $count = @rows;
    is ($count, 3, $t);
}

for my $table (keys %tabletype) {
    $sth{truncate}{$table}{A}->execute();
}
$dbhA->commit();
$bct->ctl('bucardo kick mongo 0');

for my $table (keys %tabletype) {
    $t = "Mongo collection $table has correct number of rows after trunacte";
    my $col = $db->get_collection($table);
    my @rows = $col->find->all;
    my $count = @rows;
    is ($count, 0, $t);
}

pass('Done with mongo testing');

exit;

