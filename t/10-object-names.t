#!/usr/bin/env perl
# -*-mode:cperl; indent-tabs-mode: nil-*-

## Test handling of object names

use 5.008003;
use strict;
use warnings;
use lib 't','.';
use DBD::Pg;
use Test::More tests => 14;

use BucardoTesting;
my $bct = BucardoTesting->new({ location => 'makedelta' })
    or BAIL_OUT "Creation of BucardoTesting object failed\n";

END { $bct->stop_bucardo if $bct }

ok my $dbhA = $bct->repopulate_cluster('A'), 'Populate cluster A';
ok my $dbhB = $bct->repopulate_cluster('B'), 'Populate cluster B';
ok my $dbhC = $bct->repopulate_cluster('C'), 'Populate cluster C';
ok my $dbhD = $bct->repopulate_cluster('D'), 'Populate cluster D';
ok my $dbhX = $bct->setup_bucardo('A'), 'Set up Bucardo';

END { $_->disconnect for grep { $_ } $dbhA, $dbhB, $dbhC, $dbhD, $dbhX }

# Teach Bucardo about the databases.
for my $db (qw(A B C D)) {
    my ($user, $port, $host) = $bct->add_db_args($db);
    like $bct->ctl(
        "bucardo add db $db dbname=bucardo_test user=$user port=$port host=$host"
    ), qr/Added database "$db"/, qq{Add database "$db" to Bucardo};
}

for my $arr ((['A','B'], ['C','D'])) {
    my ($src, $dest) = @$arr;
    like $bct->ctl("bucardo add table bucardo_test1 db=$src relgroup=myrels_$src"),
        qr/Added the following tables/, "Added table in db $src ";
    like $bct->ctl("bucardo add sync test_$src relgroup=myrels_$src dbs=$src:source,$dest:target"),
        qr/Added sync "test_$src"/, "Create sync from $src to $dest";
}

# Now remove syncs, for easier testing
map { $bct->ctl('bucardo remove sync $_') } qw/A C/;

# Remove a table from just database C
like $bct->ctl('bucardo remove table bucardo_test1 db=C'),
    qr/Removed the following tables:\s+\n\s+public.bucardo_test1/,
    "Removed table from just one database";


END {
    $bct and $bct->stop_bucardo();
    $dbhX and $dbhX->disconnect();
    $dbhA and $dbhA->disconnect();
    $dbhB and $dbhB->disconnect();
    $dbhC and $dbhC->disconnect();
    $dbhD and $dbhD->disconnect();
}
