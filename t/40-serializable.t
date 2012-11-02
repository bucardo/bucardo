#!/usr/bin/perl -w

use strict;
use warnings;
use lib 't';
use Test::More;
use BucardoTesting;

my $bct = BucardoTesting->new({location => 'postgres'})
    or BAIL_OUT 'Creation of BucardoTesting object failed';

END { $bct->stop_bucardo if $bct }

my $dbh = $bct->connect_database('A');
END { $dbh->disconnect if $dbh }

# Skip the tests if we can't mock the serialization failure.
plan skip_all => "Cannot mock serialization failure on Postgres $dbh->{pg_server_version}"
    if $dbh->{pg_server_version} < 80400;

# We are a go!
plan tests => 27;
$dbh->disconnect;
$dbh = undef;

ok my $dbhA = $bct->repopulate_cluster('A'), 'Populate cluster A';
ok my $dbhB = $bct->repopulate_cluster('B'), 'Populate cluster B';
ok my $dbhX = $bct->setup_bucardo('A'), 'Set up Bucardo';

END { $_->disconnect for grep { $_ } $dbhA, $dbhB, $dbhX }

# Teach Bucardo about the databases.
for my $db (qw(A B)) {
    my ($user, $port, $host) = $bct->add_db_args($db);
    like $bct->ctl(
        "bucardo add db $db dbname=bucardo_test user=$user port=$port host=$host"
    ), qr/Added database "$db"/, qq{Add database "$db" to Bucardo};
}

# Let's just deal with table bucardo_test1 and bucardo_test2.
for my $num (1, 2) {
    like $bct->ctl("bucardo add table bucardo_test$num db=A relgroup=myrels"),
        qr/Added the following tables/, "Add table bucardo_test$num";
}

# Create a new database group going from A to B
like $bct->ctl('bucardo add dbgroup serial1 A:source B:target'),
    qr/Created database group "serial1"/, 'Create relgroup serial1';

# Create a sync for this group.
like $bct->ctl('bucardo add sync serialtest1 relgroup=myrels dbs=serial1'),
    qr/Added sync "serialtest1"/, 'Create sync "serialtest1"';

# Set up a rule to mock a serialization failure on B.bucardo_test2.
ok $bct->mock_serialization_failure($dbhB, 'bucardo_test2'),
    'Mock serialization failure on bucardo_test2';
END {
    $bct->unmock_serialization_failure($dbhB, 'bucardo_test2')
        if $bct && $dbhB;
}

# Listen in on things.
ok $dbhX->do('LISTEN bucardo_syncdone_serialtest1'),
    'Listen for syncdone';
ok $dbhX->do('LISTEN bucardo_syncsleep_serialtest1'),
    'Listen for syncsleep';

# Start up Bucardo.
ok $bct->restart_bucardo($dbhX), 'Bucardo should start';

ok $bct->wait_for_notice($dbhX, 'bucardo_syncdone_serialtest1'),
    'The sync should finish';

# Should have no rows.
$bct->check_for_row([], [qw(A B)], undef, 'test[12]$');

# Let's add some data into A.bucardo_test1.
ok $dbhA->do(q{INSERT INTO bucardo_test1 (id, data1) VALUES (1, 'foo')}),
    'Insert a row into test1';
$dbhA->commit;

ok $bct->wait_for_notice($dbhX, 'bucardo_syncdone_serialtest1'),
    'Second sync should finish';

# The row should be in both databases.
is_deeply $dbhB->selectall_arrayref(
    'SELECT id, data1 FROM bucardo_test1'
), [[1, 'foo']], 'Should have the test1 row in B';

# Excellent. Now let's insert into test2.
ok $dbhA->do(q{INSERT INTO bucardo_test2 (id, data1) VALUES (2, 'foo')}),
    'Insert a row into test2';
$dbhA->commit;

ok $bct->wait_for_notice($dbhX, 'bucardo_syncsleep_serialtest1'),
    'Should get a syncsleep message';

ok $bct->wait_for_notice($dbhX, 'bucardo_syncdone_serialtest1'),
    'Then the third sync should finish';

is_deeply $dbhB->selectall_arrayref(
    'SELECT id, data1 FROM bucardo_test2'
), [[2, 'foo']], 'Should have the B test2 row despite serialization failure';
