#!/usr/bin/env perl
# -*-mode:cperl; indent-tabs-mode: nil-*-

## Test adding, dropping, and changing databases via bucardo
## Tests the main subs: add_database, list_databases, update_database, remove_database

use 5.008003;
use strict;
use warnings;
use Data::Dumper;
use lib 't','.';
use DBD::Pg;
use Test::More tests => 49;

use vars qw/$t $res $command $dbhX $dbhA $dbhB/;

use BucardoTesting;
my $bct = BucardoTesting->new({notime=>1})
    or BAIL_OUT "Creation of BucardoTesting object failed\n";
$location = '';

## Make sure A and B are started up
$dbhA = $bct->repopulate_cluster('A');
$dbhB = $bct->repopulate_cluster('B');

## Create a bucardo database, and install Bucardo into it
$dbhX = $bct->setup_bucardo('A');

## Grab connection information for each database
my ($dbuserA,$dbportA,$dbhostA) = $bct->add_db_args('A');
my ($dbuserB,$dbportB,$dbhostB) = $bct->add_db_args('B');

## Tests of basic 'add database' usage

$t = 'Add database with no argument gives expected help message';
$res = $bct->ctl('bucardo add db');
like ($res, qr/bucardo add db/, $t);

$t = q{Add database accepts both 'add database' and 'add db'};
$res = $bct->ctl('bucardo add database');
like ($res, qr/bucardo add db/, $t);

$t = q{Add database fails if not given a dbname};
$res = $bct->ctl('bucardo add database foobar');
like ($res, qr/must supply a database name/, $t);

$t = q{Add database fails for an invalid port};
$res = $bct->ctl('bucardo add database foo dbname=bar dbport=1');
like ($res, qr/Connection .+ failed.*could not connect to server/s, $t);

$t = q{Add database fails for non-existent host};
$res = $bct->ctl("bucardo add database bucardo_test dbname=bucardo_test user=$dbuserA port=$dbportA host=badbucardohost");
like ($res, qr/Connection .+ failed/s, $t); ## Could be timeout or bad hostname...

$t = q{Add database fails for non-existent database};
$res = $bct->ctl("bucardo add database foo dbname=bar user=$dbuserA port=$dbportA host=$dbhostA");
like ($res, qr/Connection .+ failed.*database "bar" does not exist/s, $t);

$t = q{Add database fails for non-existent user};
$res = $bct->ctl("bucardo add database bucardo_test dbname=bucardo_test user=nobob port=$dbportA host=$dbhostA");
like ($res, qr/Connection .+ failed.* "nobob" does not exist/s, $t);

$t = q{Add database works for non-existent cluster with --force flag};
$res = $bct->ctl('bucardo add database foo dbname=bar --force');
like ($res, qr/add anyway.*Added database "foo"/s, $t);

$t = 'Add database works for cluster A';
$res = $bct->ctl("bucardo add db A dbname=bucardo_test user=$dbuserA port=$dbportA host=$dbhostA");
is ($res, qq{Added database "A"\n}, $t);

$t = 'Upgrade correctly reports no schema changes are needed';
$res = $bct->ctl("bucardo upgrade");
like ($res, qr/No schema changes were needed/, $t);

$t = q{Add database fails if using the same internal name};
$res = $bct->ctl("bucardo add db A dbname=postgres user=$dbuserA port=$dbportA host=$dbhostA");
like ($res, qr/Cannot add database: the name "A" already exists/, $t);

$t = q{Add database works if same parameters given but different DB};
$res = $bct->ctl("bucardo add db A2 dbname=bucardo_test user=$dbuserA port=$dbportA host=$dbhostA");
like ($res, qr/Added database "A2"/, $t);

$t = 'Add database works for cluster B works with ssp=false';
$res = $bct->ctl("bucardo add db B dbname=bucardo_test user=$dbuserB port=$dbportB host=$dbhostB ssp=0");
like ($res, qr/Added database "B"/, $t);

$t = 'List databases gives expected results';
$res = $bct->ctl('bucardo list databases');
my $statA = qq{Database: A\\s+Status: active\\s+Conn: psql -p $dbportA -U $dbuserA -d bucardo_test -h $dbhostA};
my $statA2 = qq{Database: A2\\s+Status: active\\s+Conn: psql -p $dbportA -U $dbuserA -d bucardo_test -h $dbhostA};
my $statB = qq{Database: B\\s+Status: active\\s+Conn: psql -p $dbportB -U $dbuserB -d bucardo_test -h $dbhostB \\(SSP is off\\)};
my $statz = qq{Database: foo\\s+Status: active\\s+Conn: psql .*-d bar};
my $regex = qr{$statA\n$statA2\n$statB\n$statz}s;
like ($res, $regex, $t);

## Clear them out for some more testing
$t = q{Remove database works};
$res = $bct->ctl('bucardo remove db A B');
is ($res, qq{Removed database "A"\nRemoved database "B"\n}, $t);

## Tests of add database with group modifier

$t = 'Add database works when adding to a new dbgroup - role is source';
$res = $bct->ctl("bucardo add db A dbname=bucardo_test user=$dbuserA port=$dbportA host=$dbhostA group=group1");
like ($res, qr/Added database "A".*Created dbgroup "group1".*Added database "A" to dbgroup "group1" as source/s, $t);

$t = 'Add database works when adding to an existing dbgroup - role is target';
$res = $bct->ctl("bucardo add db B dbname=bucardo_test user=$dbuserB port=$dbportB host=$dbhostB group=group1");
like ($res, qr/Added database "B" to dbgroup "group1" as target/s, $t);

$t = 'Add database works when adding to an existing dbgroup as role source';
$bct->ctl('bucardo remove db B');
$res = $bct->ctl("bucardo add db B dbname=bucardo_test user=$dbuserB port=$dbportB host=$dbhostB group=group1:source");
like ($res, qr/Added database "B" to dbgroup "group1" as source/s, $t);

$t = q{Adding a database into a new group works with 'dbgroup'};
$bct->ctl('bucardo remove db B');
$res = $bct->ctl("bucardo add db B dbname=bucardo_test user=$dbuserB port=$dbportB host=$dbhostB dbgroup=group1:replica");
like ($res, qr/Added database "B" to dbgroup "group1" as target/s, $t);

## Tests for 'remove database'

$t = q{Remove database gives expected message when database does not exist};
$res = $bct->ctl('bucardo remove db foobar');
like ($res, qr/No such database "foobar"/, $t);

$t = q{Remove database works};
$res = $bct->ctl('bucardo remove db B');
like ($res, qr/Removed database "B"/, $t);

$t = q{Able to remove more than one database at a time};
$bct->ctl("bucardo add db B dbname=bucardo_test user=$dbuserB port=$dbportB host=$dbhostB");
$res = $bct->ctl('bucardo remove db A A2 B foo');
like ($res, qr/Removed database "A"\nRemoved database "A2"\nRemoved database "B"/ms, $t);

## Tests for 'list databases'

$t = q{List database returns correct message when no databases};
$res = $bct->ctl('bucardo list db');
like ($res, qr/No databases/, $t);

$bct->ctl("bucardo add db B dbname=bucardo_test user=$dbuserB port=$dbportB host=$dbhostB ssp=1");
$t = q{List databases shows the server_side_prepare setting};
$res = $bct->ctl('bucardo list database B -vv');
like ($res, qr/server_side_prepares = 1/s, $t);

$t = q{List databases accepts 'db' alias};
$res = $bct->ctl('bucardo list db');
like ($res, qr/Database: B/, $t);

## Tests for the "addall" modifiers

$t = q{Add database works with 'addalltables'};
$command =
"bucardo add db A dbname=bucardo_test user=$dbuserA port=$dbportA host=$dbhostA addalltables";
$res = $bct->ctl($command);
like ($res, qr/Added database "A"\nNew tables added: \d/s, $t);

$t = q{Remove database fails when it has referenced tables};
$res = $bct->ctl('bucardo remove db A');
like ($res, qr/remove all tables that reference/, $t);

$t = q{Remove database works when it has referenced tables and using --force};
$res = $bct->ctl('bucardo remove db A --force');
like ($res, qr/that reference database "A".*Removed database "A"/s, $t);

$t = q{Add database with 'addallsequences' works};
$res = $bct->ctl("bucardo remove dbgroup abc");
$command =
"bucardo add db A dbname=bucardo_test user=$dbuserA port=$dbportA host=$dbhostA addallsequences";
$res = $bct->ctl($command);
like ($res, qr/Added database "A"\nNew sequences added: \d/s, $t);

$t = q{Remove database respects the --quiet flag};
$res = $bct->ctl('bucardo remove db B --quiet');
is ($res, '', $t);

$t = q{Add database respects the --quiet flag};
$command =
"bucardo add db B dbname=bucardo_test user=$dbuserB port=$dbportB host=$dbhostB --quiet";
$res = $bct->ctl($command);
is ($res, '', $t);

$t = q{Update database gives proper error with no db};
$res = $bct->ctl('bucardo update db');
like ($res, qr/bucardo update/, $t);

$t = q{Update database gives proper error with no items};
$res = $bct->ctl('bucardo update db foobar');
like ($res, qr/bucardo update/, $t);

$t = q{Update database gives proper error with invalid database};
$res = $bct->ctl('bucardo update db foobar a=b');
like ($res, qr/Could not find a database named "foobar"/, $t);

$t = q{Update database gives proper error with invalid format};
$res = $bct->ctl('bucardo update db A blah blah');
like ($res, qr/update db:/, $t);

$res = $bct->ctl('bucardo update db A blah123#=123');
like ($res, qr/update db:/, $t);

$t = q{Update database gives proper error with invalid items};
$res = $bct->ctl('bucardo update db A foobar=123');
like ($res, qr/Cannot change "foobar"/, $t);

$t = q{Update database gives proper error with forbidden items};
$res = $bct->ctl('bucardo update db A cdate=123');
like ($res, qr/Sorry, the value of cdate cannot be changed/, $t);

$t = q{Update database works with a simple set};
$res = $bct->ctl('bucardo update db A port=1234');
like ($res, qr/Changed bucardo.db dbport from \d+ to 1234/, $t);

$t = q{Update database works when no change made};
$res = $bct->ctl('bucardo update db A port=1234');
like ($res, qr/No change needed for dbport/, $t);

$t = q{Update database works with multiple items};
$res = $bct->ctl('bucardo update db A port=12345 user=bob');
like ($res, qr/Changed bucardo.db dbport from \d+ to 1234/, $t);

$t = 'Update database works when adding to a new group';
$res = $bct->ctl('bucardo update db A group=group5');
like ($res, qr/Created dbgroup "group5".*Added database "A" to dbgroup "group5" as target/s, $t);

$t = 'Update database works when adding to an existing group';
$res = $bct->ctl('bucardo update db B group=group5');
like ($res, qr/Added database "B" to dbgroup "group5" as target/, $t);

$t = 'Update database works when changing roles';
$res = $bct->ctl('bucardo update db A group=group5:master');
like ($res, qr/Changed role for database "A" in dbgroup "group5" from target to source/, $t);

$t = 'Update database works when removing from a group';
$res = $bct->ctl('bucardo update db B group=group2:replica');
## new group, correct role, remove from group1!
like ($res, qr/Created dbgroup "group2".*Added database "B" to dbgroup "group2" as target.*Removed database "B" from dbgroup "group5"/s, $t);

$res = $bct->ctl('bucardo update db A status=inactive DBport=12345');
like ($res, qr/No change needed for dbport.*Changed bucardo.db status from active to inactive/s, $t);

$t = q{List database returns correct information};
$res = $bct->ctl('bucardo list dbs');
like ($res, qr/Database: A.*Status: inactive.*Database: B.*Status: active/s, $t);

$t = q{Remove database works};
$res = $bct->ctl('bucardo remove db A B --force');
like ($res, qr/that reference database "A".*Removed database "A".*Removed database "B"/s, $t);

$t = q{List database returns correct information};
$res = $bct->ctl('bucardo list dbs');
like ($res, qr/No databases/, $t);

exit;

END {
    $bct->stop_bucardo($dbhX);
    $dbhX and $dbhX->disconnect();
    $dbhA and $dbhA->disconnect();
    $dbhB and $dbhB->disconnect();
}
