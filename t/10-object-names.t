#!/usr/bin/env perl
# -*-mode:cperl; indent-tabs-mode: nil-*-

## Test handling of object names

use 5.008003;
use strict;
use warnings;
use utf8;
use open qw( :std :utf8 );
use charnames ':full';
use lib 't','.';
use DBD::Pg;
use Test::More;
use Encode qw/encode_utf8/;

my $dbdpgversion = $DBD::Pg::VERSION;
(my $majorversion = $dbdpgversion) =~ s/^(\d+).*/$1/;

if ($majorversion < 3) {
    plan (skip_all =>  "Test skipped unless DBD::Pg is version 3 or higher: this is $dbdpgversion");
}
plan tests => 20;

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

$_->{pg_enable_utf8} = 0 for grep { $_ } $dbhA, $dbhB, $dbhC, $dbhD, $dbhX;

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
like $bct->ctl('bucardo remove table public.bucardo_test1 db=C'),
    qr/Removed the following tables:\s*\n\s+public.bucardo_test1 \(DB: C\)/,
    "Removed table from just one database";

## Test non-ASCII characters in table names
## XXX Probably ought to test non-ASCII schemas as well, as well as different client_encoding values

for my $dbh (($dbhA, $dbhB)) {
    $dbh->do(encode_utf8(qq/CREATE TABLE test_büçárđo ( pkey_\x{2695} INTEGER PRIMARY KEY, data TEXT );/));
    $dbh->commit;
}

## XXX TODO: Make sync names and relgroup names with non-ASCII characters work
like $bct->ctl(encode_utf8('bucardo add table test_büçárđo db=A relgroup=unicode')),
    qr/Added the following tables/, "Added table in db A";
like($bct->ctl("bucardo add sync test_unicode relgroup=unicode dbs=A:source,B:target"),
    qr/Added sync "test_unicode"/, "Create sync from A to B")
    or BAIL_OUT "Failed to add test_unicode sync";

$dbhA->do(encode_utf8("INSERT INTO test_büçárđo (pkey_\x{2695}, data) VALUES (1, 'Something')"));
$dbhA->commit;

## Get Bucardo going
$bct->restart_bucardo($dbhX);

## Kick off the sync.
my $timer_regex = qr/\[0\s*s\]\s+(?:[\b]{6}\[\d+\s*s\]\s+)*/;
like $bct->ctl('kick sync test_unicode 0'),
    qr/^Kick\s+test_unicode:\s+${timer_regex}DONE!/,
    'Kick test_unicode' or die 'Sync failed, no point continuing';

my $res = $dbhB->selectall_arrayref(encode_utf8('SELECT * FROM test_büçárđo'));
ok($#$res == 0 && $res->[0][0] == 1 && $res->[0][1] eq 'Something', 'Replication worked');

END {
    $bct and $bct->stop_bucardo();
    $dbhX and $dbhX->disconnect();
    $dbhA and $dbhA->disconnect();
    $dbhB and $dbhB->disconnect();
    $dbhC and $dbhC->disconnect();
    $dbhD and $dbhD->disconnect();
}
