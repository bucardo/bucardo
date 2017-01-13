#!/usr/bin/env perl
# -*-mode:cperl; indent-tabs-mode: nil-*-

## Test using MongoDB as a database target

## See the bottom of this file for notes on testing

use 5.008003;
use strict;
use warnings;
use Data::Dumper;
use lib 't','.';
use DBD::Pg;
use Test::More;
use MIME::Base64;
use File::Spec::Functions;
use File::Temp qw/ tempfile /;

use vars qw/ $bct $dbhX $dbhA $dbhB $dbhC $dbhD $res $command $t %pkey $SQL %sth %sql/;

my @mongoport = (00000,11111,22222,33333);
my @mongos = (1,2);

## Must have the MongoDB module
my $evalok = 0;
eval {
    require MongoDB;
    $evalok = 1;
};
if (!$evalok) {
    plan (skip_all =>  'Cannot test mongo unless the Perl module MongoDB is installed');
}

## Are we using an older version?
my $mongoversion = $MongoDB::VERSION;
my $oldversion = $mongoversion =~ /^0\./ ? 1 : 0;

## For creating the bucardo user on the mongo databases
my ($newuserfh, $newuserfilename) = tempfile( UNLINK => 1, SUFFIX => '.js');
print {$newuserfh} qq{
db.createUser(
  {
    user: "bucardo",
    pwd: "bucardo",
    roles: [ { role: "userAdminAnyDatabase", db: "admin" } ]
  }
)
};
close $newuserfh;

## All MongoDB databases must be up and running
my @conn;
my $mongotestdir = 'mongotest';
-e $mongotestdir or mkdir $mongotestdir;
for my $mdb (@mongos) {
    my $port = $mongoport[$mdb];
    my $mongodir = catfile($mongotestdir, "testmongo$port");
    my $restart = 0;
    if (! -e $mongodir) {
        mkdir $mongodir;
        $restart = 1;
    }
    else {
        ## Need to restart if not running
        my $lockfile = catfile($mongodir, 'mongod.lock');
        if (! -e $lockfile or ! -s $lockfile) {
            $restart = 1;
        }
    }
    if ($restart) {
        my $logfile = catfile($mongodir, 'mongod.log');
        my $COM = "mongod --dbpath $mongodir --port $port --logpath $logfile --fork";
        ## This will hang if more than one called: fixme!
        ## system $COM;
    }
    ## Create the bucardo user, just in case:
    my $COM = "mongo --quiet --port $port admin $newuserfilename 2>/dev/null";
    system $COM;

    $evalok = 0;
    my $dsn = "localhost:$mongoport[$mdb]";
    eval {
        $conn[$mdb] = $oldversion ? MongoDB::MongoClient->new(host => $dsn) : MongoDB->connect($dsn);
        $evalok = 1;
    };
    if (!$evalok) {
        plan (skip_all =>  "Cannot test mongo as we cannot connect to a running Mongo on $dsn $@");
    }
}

use BucardoTesting;

## For now, remove the bytea table type as we don't have full mongo support yet
delete $tabletype{bucardo_test8};

## Also cannot handle multi-column primary keys
delete $tabletype{bucardo_test2};

for my $key (keys %tabletype) {
    next if $key !~ /test1/;
    delete $tabletype{$key};
}


my $numtabletypes = keys %tabletype;

## Make sure we start clean by dropping the test databases
my (@names,@db);
for my $mdb (@mongos) {
    my $dbname = "btest$mdb";
    my $db = $db[$mdb] = $conn[$mdb]->get_database($dbname);
    $db->drop;
    $t = qq{Test database "$dbname" has no collections};
    @names = $db->collection_names;
    is_deeply (\@names, [], $t);
}

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
    $command = "bucardo add db $name dbname=bucardo_test user=$dbuser port=$dbport host=$dbhost";
    $res = $bct->ctl($command);
    like ($res, qr/Added database "$name"/, $t);
}

$t = 'Adding mongo database M works';
$command =
"bucardo add db M dbname=btest1 dbuser=bucardo dbpass=bucardo dbport=$mongoport[1] type=mongo";
$res = $bct->ctl($command);
like ($res, qr/Added database "M"/, $t);

$t = 'Adding mongo database N works';
$command = qq{bucardo add db N dbname=btest2 dbdsn="mongodb://localhost:$mongoport[2]" type=mongo};
$res = $bct->ctl($command);
like ($res, qr/Added database "N"/, $t);

$t = 'Adding mongo database O works';
$command = qq{bucardo add db O dbname=btest3 dbdsn="mongodb://localhost:$mongoport[3]" type=mongo};
$res = $bct->ctl($command);
like ($res, qr/Added database "O"/, $t);

## Teach Bucardo about all pushable tables, adding them to a new relgroup named "therd"
$t = q{Adding all tables on the master works};
$command =
"bucardo add tables all db=A relgroup=therd pkonly";
$res = $bct->ctl($command);
like ($res, qr/Creating relgroup: therd.*New tables added: \d/s, $t);

## Add a suffix to the end of each mongo target table on M
$SQL = q{INSERT INTO bucardo.customname(goat,newname,db)
SELECT id,tablename||'_pg','M' FROM goat};
$dbhX->do($SQL);

## Add all sequences, and add them to the newly created relgroup
$t = q{Adding all sequences on the master works};
$command =
"bucardo add sequences all db=A relgroup=therd";
$res = $bct->ctl($command);
like ($res, qr/New sequences added: \d/, $t);

## Create a new dbgroup
$t = q{Created a new dbgroup};
$command =
"bucardo add dbgroup md A:source B:source C M N O:fullcopy";
$res = $bct->ctl($command);
like ($res, qr/Created dbgroup "md"/, $t);

## Create a new sync
$t = q{Created a new sync};
$command =
"bucardo add sync mongo relgroup=therd dbs=md autokick=false";
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
            ? qq{INSERT INTO "$table"($pkey{$table}) VALUES (?)}
                : qq{INSERT INTO "$table"($pkey{$table},data1,inty) VALUES (?,'foo',$x)};
        $sth{insert}{$x}{$table}{A} = $dbhA->prepare($SQL);
        if ('BYTEA' eq $tabletype{$table}) {
            $sth{insert}{$x}{$table}{A}->bind_param(1, undef, {pg_type => PG_BYTEA});
        }
    }

    ## SELECT
    $sql{select}{$table} = qq{SELECT inty FROM "$table" ORDER BY $pkey{$table}};
    $table =~ /X/ and $sql{select}{$table} =~ s/inty/$pkey{$table}/;

    ## DELETE ALL
    $SQL = qq{DELETE FROM "$table"};
    $sth{deleteall}{$table}{A} = $dbhA->prepare($SQL);

    ## DELETE ONE
    $SQL = qq{DELETE FROM "$table" WHERE inty = ?};
    $sth{deleteone}{$table}{A} = $dbhA->prepare($SQL);

    ## TRUNCATE
    $SQL = qq{TRUNCATE TABLE "$table"};
    $sth{truncate}{$table}{A} = $dbhA->prepare($SQL);
    ## UPDATE
    $SQL = qq{UPDATE "$table" SET inty = ?};
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
$dbhB->commit();
$dbhC->commit();

## Commit, then kick off the sync
$dbhA->commit();
$bct->ctl('bucardo kick mongo 0');

## Check B and C for the new rows
for my $table (sort keys %tabletype) {

    my $type = $tabletype{$table};
    $t = qq{Row with pkey of type $type gets copied to B};

    $res = [[1]];
    bc_deeply($res, $dbhB, $sql{select}{$table}, $t);
    bc_deeply($res, $dbhC, $sql{select}{$table}, $t);
}
$dbhB->commit();
$dbhC->commit();

## Set the modified table names
my %tabletype2;
for my $table (keys %tabletype) {
    my $newname = $table.'_pg';
    $tabletype2{$newname} = $tabletype{$table};
}

## Check that all mongo databases have the new collection information
for my $mdb (@mongos) {
    my %col;
    my $db = $db[$mdb];
    @names = $db->collection_names;
    for (@names) {
        $col{$_} = 1;
    }

    for my $table (sort keys %tabletype2) {
        $table =~ s/_pg// if $mdb eq '2';
        $t = "Table $table has a mongodb collection in db $mdb";
        ok(exists $col{$table}, $t);
    }

    ## Check that mongo has the new rows
    for my $table (sort keys %tabletype2) {
        my $original_table = $table;
        $table =~ s/_pg// if $mdb eq '2';
        $t = "Mongo db $mdb collection $table has correct number of rows after insert";
        my $col = $db->get_collection($table);
        my @rows = $col->find->all;
        my $count = @rows;
        is ($count, 1, $t) or die;

        ## Remove the mongo internal id column
        delete $rows[0]->{_id};

        $t = "Mongo db $mdb collection $table has correct entries";
        my $type = $tabletype2{$original_table};
        my $id = $val{$type}{1};
        my $pkeyname = $table =~ /test5/ ? 'id space' : 'id';

        ## For now, binary is stored in escaped form, so we skip this one
        next if $table =~ /test8/;

        is_deeply(
            $rows[0],
            {
                $pkeyname => $id,
                inty  => 1,
                data1 => 'foo',
            },
            $t) or die;
    }

} ## end each mongo db


## Update each row, make sure it gets replicated to mongo
for my $table (keys %tabletype) {
    $sth{update}{$table}{A}->execute(42);
}
$dbhA->commit();
$bct->ctl('bucardo kick mongo 0');

for my $mdb (@mongos) {
    my $db = $db[$mdb];
    for my $table (keys %tabletype2) {
        $table =~ s/_pg// if $mdb eq '2';
        $t = "Mongo db $mdb collection $table has correct number of rows after update";
        my $col = $db->get_collection($table);
        my @rows = $col->find->all;
        my $count = @rows;
        is ($count, 1, $t);

        $t = "Mongo db $mdb collection $table has updated value";
        is ($rows[0]->{inty}, 42, $t);
    }
}

## Delete each row, make sure it gets replicated to mongo
for my $table (keys %tabletype) {
    $sth{deleteall}{$table}{A}->execute();
}
$dbhA->commit();
$bct->ctl('bucardo kick mongo 0');

for my $mdb (@mongos) {
    my $db = $db[$mdb];
    for my $table (keys %tabletype2) {
        $table =~ s/_pg// if $mdb eq '2';
        $t = "Mongo db $mdb collection $table has correct number of rows after delete";
        my $col = $db->get_collection($table);
        my @rows = $col->find->all;
        my $count = @rows;
        is ($count, 0, $t);
    }
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

for my $mdb (@mongos) {
    my $db = $db[$mdb];
    for my $table (keys %tabletype2) {
        $table =~ s/_pg// if $mdb eq '2';
        $t = "Mongo db $mdb collection $table has correct number of rows after double insert";
        my $col = $db->get_collection($table);
        my @rows = $col->find->all;
        my $count = @rows;
        is ($count, 2, $t);
    }
}

## Delete one of the rows
for my $table (keys %tabletype) {
    $sth{deleteone}{$table}{A}->execute(2); ## inty = 2
}
$dbhA->commit();
$bct->ctl('bucardo kick mongo 0');

for my $mdb (@mongos) {
    my $db = $db[$mdb];
    for my $table (keys %tabletype2) {
        $table =~ s/_pg// if $mdb eq '2';
        $t = "Mongo db $mdb collection $table has correct number of rows after single deletion";
        my $col = $db->get_collection($table);
        my @rows = $col->find->all;
        my $count = @rows;
        is ($count, 1, $t);
    }
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

for my $mdb (@mongos) {
    my $db = $db[$mdb];
    for my $table (keys %tabletype2) {
        $table =~ s/_pg// if $mdb eq '2';
        $t = "Mongo db $mdb collection $table has correct number of rows after more inserts";
        my $col = $db->get_collection($table);
        my @rows = $col->find->all;
        my $count = @rows;
        is ($count, 3, $t);
    }
}

for my $table (keys %tabletype) {
    $sth{truncate}{$table}{A}->execute();
}
$dbhA->commit();
$bct->ctl('bucardo kick mongo 0');

for my $mdb (@mongos) {
    my $db = $db[$mdb];
    for my $table (keys %tabletype2) {
        $t = "Mongo db $mdb collection $table has correct number of rows after truncate";
        my $col = $db->get_collection($table);
        my @rows = $col->find->all;
        my $count = @rows;
        is ($count, 0, $t);
    }
}

## Test customname again
undef %tabletype2;
for my $table (keys %tabletype) {
    my $newname = $table.'_pg';
    $tabletype2{$newname} = $tabletype{$table};
}


## Test of customname options
$dbhX->do('DELETE FROM bucardo.customname');

## Add a new suffix to the end of each table in this sync for mongo
$SQL = q{INSERT INTO bucardo.customname(goat,newname,db,sync)
SELECT id,tablename||'_pg','M','mongo' FROM goat};
$dbhX->do($SQL);
$dbhX->commit();

$bct->ctl('reload sync mongo');

## Insert two rows
for my $table (keys %tabletype) {
    my $type = $tabletype{$table};
    my $val3 = $val{$type}{3};
    $sth{insert}{3}{$table}{A}->execute($val3);
    my $val4 = $val{$type}{4};
    $sth{insert}{4}{$table}{A}->execute($val4);
}
$dbhA->commit();
$bct->ctl('bucardo kick mongo 0');

for my $mdb (@mongos) {
    my $db = $db[$mdb];
    for my $table (keys %tabletype2) {
        $table =~ s/_pg// if $mdb eq '2';
        $t = "Mongo db $mdb collection $table has correct number of rows after insert";
        my $col = $db->get_collection($table);
        my @rows = $col->find->all;
        my $count = @rows;
        is ($count, 2, $t);
    }
}

$t=q{Using customname, we can force a text string to an int};
my $CS = 'SELECT id, data1 AS data2inty::INTEGER, inty, email FROM bucardo.bucardo_test2';
## Set this one for this db and this sync
$bct->ctl('bucardo add cs db=M sync=mongo table=ttable');

$t=q{Using customname, we can restrict the columns sent};

$t=q{Using customname, we can add new columns and modify others};
## Set this one for all syncs

done_testing();

exit;

__END__
This can be handy to generate some test MongoDB databases:

mongod --dbpath mongotest/testmongo11111 --shutdown
mongod --dbpath mongotest/testmongo22222 --shutdown
mongod --dbpath mongotest/testmongo33333 --shutdown
sleep 2
rm -fr mongotest
mkdir -p mongotest/testmongo11111 mongotest/testmongo22222 mongotest/testmongo33333
sync
nohup mongod --dbpath mongotest/testmongo11111 --port 11111 --fork --logpath mongotest/mongod.11111.log --smallfiles --noprealloc --nojournal &
nohup mongod --dbpath mongotest/testmongo22222 --port 22222 --fork --logpath mongotest/mongod.22222.log --smallfiles --noprealloc --nojournal &
nohup mongod --dbpath mongotest/testmongo33333 --port 33333 --fork --logpath mongotest/mongod.33333.log --smallfiles --noprealloc --nojournal &
