#!/usr/bin/env perl
# -*-mode:cperl; indent-tabs-mode: nil-*-

## Test makedelta functionality

use 5.008003;
use strict;
use warnings;
use Data::Dumper;
use lib 't','.';
use DBD::Pg;
use Test::More tests => 48;
#use Test::More 'no_plan';

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

# Let's just deal with table bucardo_test1 (sindle col pk) and bucardo_test2
# (multi-col pk). Create bucardo_test4 with makedelta off.
for my $num (1, 2, 4) {
    my $md = $num == 4 ? 'off' : 'on';
    like $bct->ctl("bucardo add table bucardo_test$num db=A relgroup=myrels makedelta=$md"),
        qr/Added the following tables/, "Add table bucardo_test$num";
}

# Create a new dbgroup for multi-master replication between A and B
like $bct->ctl('bucardo add dbgroup delta1 A:source B:source'),
    qr/Created database group "delta1"/, 'Create relgroup delta1';

# Create a sync for this group.
like $bct->ctl('bucardo add sync deltatest1 relgroup=myrels dbs=delta1'),
    qr/Added sync "deltatest1"/, 'Create sync "deltatest1"';

# Create a new dbgroup and sync to copy the tables from B to C.
like $bct->ctl('bucardo add dbgroup delta2 B:source C:target'),
    qr/Created database group "delta2"/, 'Create relgroup delta2';
like $bct->ctl('bucardo add sync deltatest2 relgroup=myrels dbs=delta2'),
    qr/Added sync "deltatest2"/, 'Create sync "deltatest2"';

# Listen in on things.
ok $dbhX->do('LISTEN bucardo_syncdone_deltatest1'),
    'Listen for syncdone_deltatest1';
ok $dbhX->do('LISTEN bucardo_syncdone_deltatest2'),
    'Listen for syncdone_deltatest2';

# Start up Bucardo and wait for initial syncs to finish.
ok $bct->restart_bucardo($dbhX), 'Bucardo should start';
ok $bct->wait_for_notice($dbhX, 'bucardo_syncdone_deltatest1'),
    'The sync deltatest1 sync should finish';
ok $bct->wait_for_notice($dbhX, 'bucardo_syncdone_deltatest2'),
    'The sync deltatest2 sync should finish';

# Should have no rows.
$bct->check_for_row([], [qw(A B C)], undef, 'test[124]$');

# Let's add some data into A.bucardo_test1.
ok $dbhA->do(q{INSERT INTO bucardo_test1 (id, data1) VALUES (1, 'foo')}),
    'Insert a row into test1 on A';
$dbhA->commit;

ok $bct->wait_for_notice($dbhX, 'bucardo_syncdone_deltatest1'),
    'Second deltatest1 sync should finish';
ok $bct->wait_for_notice($dbhX, 'bucardo_syncdone_deltatest2'),
    'Second deltatest2 sync should finish';

# Make sure we don't enter a circular repliation loop between A and B.
eval { $bct->wait_for_notice($dbhX, 'bucardo_syncdone_deltatest1', 1, 0, 0) };
like $@, qr/\QGave up waiting for notice "bucardo_syncdone_deltatest1"/,
    'Should not have another deltatest1 sync';

# The row should be in all three databases.
is_deeply $dbhB->selectall_arrayref(
    'SELECT id, data1 FROM bucardo_test1'
), [[1, 'foo']], 'Should have the test1 row in B';

is_deeply $dbhC->selectall_arrayref(
    'SELECT id, data1 FROM bucardo_test1'
), [[1, 'foo']], 'Should have the test1 row in C';

# Excellent. Now let's insert into test2 on B.
ok $dbhB->do(q{INSERT INTO bucardo_test2 (id, data1) VALUES (2, 'foo')}),
    'Insert a row into test2 on B';
$dbhB->commit;

ok $bct->wait_for_notice($dbhX, 'bucardo_syncdone_deltatest1'),
    'Then the third deltatest1 sync should finish';
ok $bct->wait_for_notice($dbhX, 'bucardo_syncdone_deltatest2'),
    'Then the third deltatest2 sync should finish';

# Make sure we don't enter a circular repliation loop between A and B.
eval { $bct->wait_for_notice($dbhX, 'bucardo_syncdone_deltatest1', 1, 0, 0) };
like $@, qr/\QGave up waiting for notice "bucardo_syncdone_deltatest1"/,
    'Again should not have a duplicate deltatest1 sync';

is_deeply $dbhA->selectall_arrayref(
    'SELECT id, data1 FROM bucardo_test2'
), [[2, 'foo']], 'Should have the A test2 row in A';

is_deeply $dbhC->selectall_arrayref(
    'SELECT id, data1 FROM bucardo_test2'
), [[2, 'foo']], 'Should have the C test2 row in C';

# Finally, try table 4, which has no makedelta.
ok $dbhA->do(q{INSERT INTO bucardo_test4 (id, data1) VALUES (3, 'foo')}),
    'Insert a row into test4 on A';
$dbhA->commit;

ok $bct->wait_for_notice($dbhX, 'bucardo_syncdone_deltatest1'),
    'Wait for the fourth deltatest1 sync should finish';

# Make sure we don't enter a circular repliation loop between A and B.
eval { $bct->wait_for_notice($dbhX, 'bucardo_syncdone_deltatest1', 1, 0, 0) };
like $@, qr/\QGave up waiting for notice "bucardo_syncdone_deltatest1"/,
    'Again should not have a duplicate deltatest1 sync';

# Should have no deltatest2 sync, either.
eval { $bct->wait_for_notice($dbhX, 'bucardo_syncdone_deltatest2', 1, 0, 0) };
like $@, qr/\QGave up waiting for notice "bucardo_syncdone_deltatest2"/,
    'Should have no deltatest2 sync triggered from table 4';

is_deeply $dbhB->selectall_arrayref(
    'SELECT id, data1 FROM bucardo_test4'
), [[3, 'foo']], 'Should have the test4 row in B';

is_deeply $dbhC->selectall_arrayref(
    'SELECT id, data1 FROM bucardo_test4'
), [], 'Should have no test4 row row in C';
