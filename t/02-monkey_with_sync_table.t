#!/usr/bin/perl -- -*-cperl-*-

## Test all ways of accessing the sync table

use strict;
use warnings;
use lib 't','.';
use Test::More tests => 99;

use BucardoTesting;
my $bct = BucardoTesting->new();

my ($t,$i);

## Start with a clean schema and databases (don't care what's in them)

my $dbh = $bct->setup_database({db => 'bucardo', clean => 1, dropschema => 0});

$t = 'Calling bucardo_ctl from command-line works';
$i = $bct->ctl('--help');
like($i, qr{ping}, $t);

$t = q{Calling bucardo_ctl with 'add sync' gives expected message};
$i = $bct->ctl('add sync');
like($i, qr{Usage: add sync <name>}, $t);

$bct->scrub_bucardo_tables($dbh);

## Create and return handles for some test databases
my $dbhA = $bct->setup_database({db => 'A'});
my $dbhB = $bct->setup_database({db => 'B'});

## A sync has some prereqs.

## Add the two databases to the db table:
$t=q{Add database works};
my $ctlargs = $bct->add_db_args('A');
$i = $bct->ctl("add database $ctlargs");
like($i, qr{Database added}, $t);

$ctlargs = $bct->add_db_args('B');
$i = $bct->ctl("add database $ctlargs");
like($i, qr{Database added}, $t);

## Add a herd
$t=q{Add herd works};
$i = $bct->ctl("add herd testherd1");
like($i, qr{Herd added}, $t);

$t=q{Running add sync gives an error if no herd members};
$i = $bct->ctl("add sync testsync1 source=testherd1 type=swap targetdb=B");
like($i, qr{Herd has no members}, $t);

## Add a table to the herd
$t=q{Add table works};
$i = $bct->ctl("add table bucardo_test1 db=A herd=testherd1");
like($i, qr{Table added:}, $t);

$t=q{Add sync works};
$i = $bct->ctl("add sync testsync1 source=testherd1 type=pushdelta targetdb=B");
like($i, qr{Sync added:}, $t);

pass("done");
