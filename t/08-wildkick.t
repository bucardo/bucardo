#!perl

# Test multi-column primary keys

use 5.008003;
use strict;
use warnings;
use Data::Dumper;
use lib 't','.';
use DBD::Pg;
use Test::More 'no_plan';

no warnings 'redefine';
use BucardoTesting;
use warnings;

my $bct = BucardoTesting->new() or BAIL_OUT "Creation of BucardoTesting object failed\n";

pass(q{*** Beginning 'wildkick' tests});

$bct->drop_database('all');

## Prepare a clean Bucardo database on A
my $dbhA = $bct->blank_database('A');
my $dbhB = $bct->blank_database('B');

## Server A is the master, the rest are slaves
my $dbhX = $bct->setup_bucardo(A => $dbhA);
$bct->add_test_databases('A B');

for my $i (1..5) {
    # Create some tables
    my $create = qq{
        CREATE TABLE swap$i (
            id   INTEGER PRIMARY KEY,
            data TEXT);
    };

    for my $dbh (($dbhA, $dbhB)) {
        $dbh->do($create);
        $dbh->commit();
    }

    # Add herds for each table and add the tables to the herd
    $bct->ctl('add herd herd'.$i);
    my $result = $bct->ctl(qq{add table swap$i db=A herd=herd$i});
    like($result, qr{Added table}, "Added test table $i");

    # Add syncs
    $bct->ctl(qq{add sync test$i source=herd$i type=pushdelta targetdb=B});
}

# See if the syncs work
$bct->restart_bucardo($dbhX);
for my $i (1..5) {
    $dbhA->do(qq{INSERT INTO swap$i (id, data) VALUES (1, 'test')});
    $dbhX->do(qq{LISTEN bucardo_syncdone_test$i});
    $dbhA->commit();
    $dbhX->commit();

    $bct->ctl(qq{kick test$i});
    eval {
        wait_for_notice($dbhX, "bucardo_syncdone_test$i", 10);
    };
    ok(! $@, "Sync $i pushed");
    my $res = $dbhB->selectall_arrayref("SELECT * FROM swap$i");
    $dbhB->commit();
    is_deeply($res, [['1', 'test']], "Sync test$i worked");
}
pass('All syncs work. Now starting the real test');

# Now that we have a set of working syncs, we'll try kicking all of 'em
## First, get ready to listen for the kicks
for my $i (1..5) {
    $dbhA->do(qq{INSERT INTO swap$i (id, data) VALUES (2, 'test2')});
    $dbhA->commit();
}

# Now do the kicking
$bct->ctl(q{kick test%});

# See what happened
my %notifies;
eval {
    local $SIG{ALRM} = sub { die 'Timeout: '.Dumper(%notifies); };
    alarm 15;
    N: {
        while (my $n = $dbhX->func('pg_notifies')) {
            if ($n->[0] =~ /bucardo_syncdone_test(\d)/) {
                #print 'Found '.($n->[0])."\n";
                $notifies{ $n->[0] } = 1;
                my @keys = keys %notifies;
                last N if (@keys == 5);
            }
        }
        sleep .1;
        redo N;
    }
    alarm 0;
};
ok(!$@, "No errors on waiting for notifications: $@");
my @keys = keys %notifies;
is(@keys, 5, 'All syncs returned');

END {
    $bct->stop_bucardo($dbhX);
    $dbhX->disconnect();
    $dbhA->disconnect();
    $dbhB->disconnect();
}
