#!/usr/bin/env perl
# -*-mode:cperl; indent-tabs-mode: nil-*-

## Test of customcode to handle exceptions

use 5.008003;
use strict;
use warnings;
use Data::Dumper;
use lib 't','.';
use DBD::Pg;
use Test::More;


use vars qw/ $dbhX $dbhA $dbhB $dbhC $res $command $t $SQL %pkey %sth %sql $sth $count $val /;

use BucardoTesting;

plan (skip_all =>  'Conflict resolution script requires Postgres 9.4 or later')
if BucardoTesting::pg_major_version() < 9.4;
my $bct = BucardoTesting->new({location => 'postgres'})
    or BAIL_OUT "Creation of BucardoTesting object failed\n";

#plan tests => 9999;

END {
    $bct and $bct->stop_bucardo($dbhX);
    $dbhX and $dbhX->disconnect();
    $dbhA and $dbhA->disconnect();
    $dbhB and $dbhB->disconnect();
    $dbhC and $dbhC->disconnect();
}

## Get A, B, and C created, emptied out, and repopulated with sample data
$dbhA = $bct->repopulate_cluster('A');
$dbhB = $bct->repopulate_cluster('B');
$dbhC = $bct->repopulate_cluster('C');

## Store into hashes for convenience

my %dbh = (A=>$dbhA, B=>$dbhB, C=>$dbhC);

## Create a bucardo database, and install Bucardo into it
$dbhX = $bct->setup_bucardo('A');

## Teach Bucardo about three databases
for my $name (qw/ A B C /) {
    $t = "Adding database from cluster $name works";
    my ($dbuser,$dbport,$dbhost) = $bct->add_db_args($name);
    $command = "bucardo add db $name dbname=bucardo_test user=$dbuser port=$dbport host=$dbhost";
    $res = $bct->ctl($command);
    like ($res, qr/Added database "$name"/, $t);
}

## Create a table with a non-primary key unique constraint
$SQL = q{DROP TABLE IF EXISTS employee CASCADE};
$dbhA->do($SQL); $dbhB->do($SQL); $dbhC->do($SQL);
$SQL = q{DROP TABLE IF EXISTS employee_conflict CASCADE};
$dbhA->do($SQL); $dbhB->do($SQL); $dbhC->do($SQL);

$SQL = q{
  CREATE TABLE employee (
    id SERIAL PRIMARY KEY,
    subid INT NOT NULL,
    fullname TEXT,
    email TEXT NOT NULL,
    active BOOL DEFAULT TRUE,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE UNIQUE INDEX sub_email_key ON employee(subid, LOWER(email)) WHERE active;
CREATE TABLE supplies(
    employee_id INT  NOT NULL REFERENCES employee(id),
    object      TEXT NOT NULL,
    count       INT  NOT NULL,
    PRIMARY KEY (employee_id, object)
);
};
$dbhA->do($SQL); $dbhB->do($SQL); $dbhC->do($SQL);

$SQL = q{CREATE TABLE employee_conflict (LIKE employee)};
$dbhA->do($SQL); $dbhB->do($SQL); $dbhC->do($SQL);

$dbhA->commit();$dbhB->commit();$dbhC->commit();

## Create a new herd for the table
$t = q{Adding tables to new herd 'exherd' works};
$res = $bct->ctl(q{bucardo add table employee herd=exherd});
like ($res, qr/Created the relgroup named "exherd".*are now part of/s, $t);
$res = $bct->ctl(q{bucardo add table supplies herd=exherd});
like ($res, qr/The following tables or sequences are now part of the relgroup "exherd":/s, $t);

## Create a new dbgroup going from A to B to C
$t = q{Created a new dbgroup exabc for A <=> B <=> C};
$res = $bct->ctl('bucardo add dbgroup exabc A:source B:source C:source');
like ($res, qr/Created dbgroup "exabc"/, $t);

## Create a new sync
$t = q{Created a new sync for dbgroup exabc};
$res = $bct->ctl('bucardo add sync exabc relgroup=exherd dbs=exabc status=active autokick=false');
like ($res, qr/Added sync "exabc"/, $t);

## Start listening for a syncdone message
$dbhX->do('LISTEN bucardo_syncdone_exabc');
$dbhX->commit();

## Start up Bucardo
$bct->restart_bucardo($dbhX);

## Add some rows and verify that basic replication is working
$SQL = 'INSERT INTO employee (id,subid,fullname,email) VALUES (?,?,?,?)';
my $insert_ea = $dbhA->prepare($SQL);
my $insert_eb = $dbhB->prepare($SQL);
my $insert_ec = $dbhC->prepare($SQL);
$SQL = 'INSERT INTO supplies (employee_id,object,count) VALUES (?,?,?)';
my $supply_ea = $dbhA->prepare($SQL);
my $supply_eb = $dbhB->prepare($SQL);
my $supply_ec = $dbhC->prepare($SQL);

$insert_ea->execute(100, 10, 'Alice',   'alice@acme'   );
$supply_ea->execute(100, 'staples', 1024);
$supply_ea->execute(100, 'pens', 3);
$insert_eb->execute(101, 10, 'Bob',     'bob@acme'     );
$supply_eb->execute(101, 'phone', 1);

$dbhA->commit(); $dbhB->commit(); $dbhC->commit();

$bct->ctl('bucardo kick sync exabc 0');

## We cool?
for my $db (qw/ A B C /) {
    my $dbh = $dbh{$db};
    my $result = $dbh->selectall_arrayref(
        'SELECT id FROM employee ORDER BY id'
    );
    $t = qq{Database $db has expected employee rows};
    is_deeply ($result, [[100],[101]], $t);
    $result = $dbh->selectall_arrayref(
        'SELECT employee_id,count FROM supplies ORDER BY employee_id, count'
    );
    $t = qq{Database $db has expected supply rows};
    is_deeply ($result, [[100,3],[100,1024],[101,1]], $t);
}

## Cause a unique index violation and confirm the sync dies
$insert_ec->execute(103, 10, 'Mallory2', 'MALLORY@ACME' );
$supply_ec->execute(103, 'staples', 42);
$insert_eb->execute(102, 10, 'Mallory1', 'mallory@acme' );
$supply_eb->execute(102, 'staples', 41);
$insert_ea->execute(104, 11, 'Mallory1', 'mallory@acme' );
$supply_ea->execute(104, 'staples', 10000);

# Add an inactive record.
$dbhB->do(
    'INSERT INTO employee (id,subid,fullname,email,active) VALUES (?,?,?,?,?)',
    undef, 105, 10, 'Mallory3', 'mallory@acme', 0,
);
$supply_eb->execute(105, 'pens', 3);

$dbhA->commit(); $dbhB->commit(); $dbhC->commit();

$bct->ctl('bucardo kick sync exabc 0');

## Check the status - should be bad
$res = $bct->ctl('bucardo status exabc');

$t = q{Sync exabc is marked as bad after a failed run};
like ($res, qr{Current state\s+:\s+Bad}, $t);

$t = q{Sync exabc shows a duplicate key violation};
like ($res, qr{ERROR.*sub_email_key}, $t);

## Add in a customcode exception handler
$res = $bct->ctl('bucardo add customcode email_exception whenrun=exception src_code=t/customcode.exception.bucardotest.pl relation=public.employee getdbh=1');

$t = q{Customcode exception handler was added for sync exabc};
like ($res, qr{Added customcode "email_exception"}, $t);

# $res = $bct->ctl('update customcode email_exception status=inactive');
# diag Dumper $res;
# exit;

## Reload the sync and verify the exception handler allows the sync to continue
$bct->ctl('bucardo reload exabc');

$bct->ctl('bucardo kick sync exabc 0');

## Status should now be good
$res = $bct->ctl('bucardo status exabc');
$t = q{Sync exabc is marked as good after a exception-handled run};
like ($res, qr{Current state\s+:\s+Good}, $t);

## Make sure all the rows are as we expect inside employee
## We cool?
$SQL = 'SELECT id,subid,email,fullname FROM employee ORDER BY id';
for my $db (qw/ A B C /) {
    my $dbh = $dbh{$db};
    my $result = $dbh->selectall_arrayref($SQL);
    $t = qq{Database $db has expected rows in employee};
    is_deeply ($result,[
               [100,10,'alice@acme','Alice'],
               [101,10,'bob@acme','Bob'],
               [102,10,'mallory@acme','Mallory1'],
               [104,11,'mallory@acme','Mallory1'],
               [105,10,'mallory@acme','Mallory3'],
       ],$t);
    $result = $dbh->selectall_arrayref(
        'SELECT employee_id,count FROM supplies ORDER BY employee_id, count'
    );
    $t = qq{Database $db has expected supply rows};
    is_deeply ($result, [[100,3],[100,1024],[101,1],[102,41],[104,10000],[105,3]], $t);
}

## Make sure all the rows are as we expect inside employee_conflict
$SQL = 'SELECT id,subid,email,fullname FROM employee_conflict';
for my $db (qw/ C /) {
    my $dbh = $dbh{$db};
    my $result = $dbh->selectall_arrayref($SQL);
    $t = qq{Database $db has expected rows in employee_conflict};
    is_deeply ($result,[
               [103,10,'MALLORY@ACME','Mallory2']
       ],$t);
}

## Test disabling the customcode
like $bct->ctl('update customcode email_exception status=inactive'),
    qr/\QChanged customcode "email_exception" status from 'active' to 'inactive'/,
    'Change email_exception status to "inactive"';

## Test goat-level customcode

done_testing();
exit;
