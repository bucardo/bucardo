#!/usr/bin/env perl
# -*-mode:cperl; indent-tabs-mode: nil-*-

## Test makedelta functionality

use 5.008003;
use strict;
use warnings;
use lib 't','.';
use DBD::Pg;
use Test::More;
use BucardoTesting;
my $bct = BucardoTesting->new({ location => 'makedelta' })
    or BAIL_OUT "Creation of BucardoTesting object failed\n";

END { $bct->stop_bucardo if $bct }

ok my $dbhA = $bct->repopulate_cluster('A'), 'Populate cluster A';
ok my $dbhB = $bct->repopulate_cluster('B'), 'Populate cluster B';
ok my $dbhC = $bct->repopulate_cluster('C'), 'Populate cluster C';
ok my $dbhX = $bct->setup_bucardo('A'), 'Set up Bucardo';

END { $_->disconnect for grep { $_ } $dbhA, $dbhB, $dbhC, $dbhX }

# Teach Bucardo about the databases.
for my $db (qw(A B C)) {
    my ($user, $port, $host) = $bct->add_db_args($db);
    like $bct->ctl(
        "bucardo add db $db dbname=bucardo_test user=$user port=$port host=$host"
    ), qr/Added database "$db"/, qq{Add database "$db" to Bucardo};
}

# Let's just deal with table bucardo_test1 (single column primary key)
# and bucardo_test2 (multi-column primary key).
# Create bucardo_test4 with makedelta off.
for my $num (1, 2, 4) {
    my $md = $num == 4 ? 'off' : 'on';
    like $bct->ctl("bucardo add table bucardo_test$num db=A relgroup=myrels makedelta=$md"),
        qr/Added the following tables/, "Add table bucardo_test$num";
}

# Create a sync for multi-master replication between A and B
like $bct->ctl('bucardo add sync deltatest1 relgroup=myrels dbs=A:source,B:source'),
    qr/Added sync "deltatest1"/, 'Create sync "deltatest1"';

# Create a sync for replication from B to C
like $bct->ctl('bucardo add sync deltatest2 relgroup=myrels dbs=B,C autokick=no'),
    qr/Added sync "deltatest2"/, 'Create sync "deltatest2"';

# Create an inactive sync from C to A. This is so makedelta on C tables works
like $bct->ctl('bucardo add sync deltafake relgroup=myrels dbs=C,A status=inactive autokick=no'),
    qr/Added sync "deltafake"/, 'Create sync "deltafake"';

# Listen in on things.
ok $dbhX->do('LISTEN bucardo_syncdone_deltatest1'),
    'Listen for syncdone_deltatest1';
ok $dbhX->do('LISTEN bucardo_syncdone_deltatest2'),
    'Listen for syncdone_deltatest2';
ok $dbhX->do('LISTEN bucardo_syncdone_deltatest3'), ## created below
    'Listen for syncdone_deltatest3';

# Start up Bucardo and wait for initial active sync to finish.
ok $bct->restart_bucardo($dbhX), 'Bucardo should start';
ok $bct->wait_for_notice($dbhX, [qw(
    syncdone_deltatest1
)]), 'The deltatest1 sync finished';

# Should have no rows.
$bct->check_for_row([], [qw(A B C)], undef, 'test[124]$');

# Let's add some data into A.bucardo_test1.
ok $dbhA->do(q{INSERT INTO bucardo_test1 (id, data1) VALUES (1, 'foo')}),
    'Insert a row into test1 on A';
$dbhA->commit;

ok $bct->wait_for_notice($dbhX, [qw(
    syncdone_deltatest1
)]), 'The deltatest1 sync has finished';

## The data should only go as far as B
$bct->check_for_row([], ['C'], undef, 'test[124]$');

## Bucardo will not fire off deltatest2 itself, so we kick it
$bct->ctl('bucardo kick sync deltatest2 0');

ok $bct->wait_for_notice($dbhX, [qw(
    syncdone_deltatest2
)]), 'The deltatest2 sync has finished';

# The row should be in A and B, as well as C!
is_deeply $dbhB->selectall_arrayref(
    'SELECT id, data1 FROM bucardo_test1'
), [[1, 'foo']], 'Should have the test1 row in B';

is_deeply $dbhC->selectall_arrayref(
    'SELECT id, data1 FROM bucardo_test1'
), [[1, 'foo']], 'Second sync moved row from B to C';

# Now let's insert into test2 on B.
# This will cause both syncs to fire
# deltatest1 (A<=>B) will copy the row from B to A
# deltatest2 (B=>C) will copy the row from B to C
ok $dbhB->do(q{INSERT INTO bucardo_test2 (id, data1) VALUES (2, 'foo')}),
    'Insert a row into test2 on B';
$dbhB->commit;

## Sync deltatest2 is not automatic, so we need to kick it
# Kick off the second sync
$bct->ctl('bucardo kick sync deltatest2 0');

ok $bct->wait_for_notice($dbhX, [qw(
    syncdone_deltatest1
    syncdone_deltatest2
)]), 'The deltatest1 and deltatest2 syncs finished';

is_deeply $dbhA->selectall_arrayref(
    'SELECT id, data1 FROM bucardo_test2'
), [[2, 'foo']], 'Should have the A test2 row in A';

is_deeply $dbhC->selectall_arrayref(
    'SELECT id, data1 FROM bucardo_test2'
), [[2, 'foo']], 'Should have the A test2 row in C';


# Finally, try table 4, which has no makedelta.
ok $dbhA->do(q{INSERT INTO bucardo_test4 (id, data1) VALUES (3, 'foo')}),
    'Insert a row into test4 on A';
$dbhA->commit;

ok $bct->wait_for_notice($dbhX, [qw(
    syncdone_deltatest1
)]), 'The deltatest1 sync finished';

# Kick off the second sync
$bct->ctl('bucardo kick sync deltatest2 0');

is_deeply $dbhB->selectall_arrayref(
    'SELECT id, data1 FROM bucardo_test4'
), [[3, 'foo']], 'Should have the test4 row in B';

is_deeply $dbhC->selectall_arrayref(
    'SELECT id, data1 FROM bucardo_test4'
), [], 'Should have no test4 row row in C';

$dbhA->commit();
$dbhB->commit();
$dbhC->commit();

##############################################################################
# Okay, what if we have C be a target from either A or B?
like $bct->ctl('bucardo remove sync deltatest2'),
    qr/Removed sync "deltatest2"/, 'Remove sync "deltatest2"';
like $bct->ctl('bucardo add sync deltatest3 relgroup=myrels dbs=A:source,B:source,C'),
   qr/Added sync "deltatest3"/, 'Created sync "deltatest3"';

ok $bct->restart_bucardo($dbhX), 'Bucardo restarted';

ok $dbhA->do(q{INSERT INTO bucardo_test2 (id, data1) VALUES (3, 'howdy')}),
    'Insert a row into test2 on A';
$dbhA->commit;

ok $bct->wait_for_notice($dbhX, [qw(
   syncdone_deltatest1
   syncdone_deltatest3
)]), 'Syncs deltatest1 and deltatest3 finished';

is_deeply $dbhB->selectall_arrayref(
    'SELECT id, data1 FROM bucardo_test2'
), [[2, 'foo'], [3, 'howdy']], 'Should have the A test2 row in B';


is_deeply $dbhC->selectall_arrayref(
    'SELECT id, data1 FROM bucardo_test2'
), [[2, 'foo'], [3, 'howdy']], 'Should have the A test2 row in C';

done_testing();
