#!perl

## Test all ways of accessing the goat table

use strict;
use warnings;
use lib 't','.';
use Test::More tests => 3;

use BucardoTesting;
my $bct = BucardoTesting->new();

my ($t,$i);

## Start with schema and databases left over from 02-monkey_with_db_tables.t
my $dbhA = $bct->blank_database('A');
#my $dbh = $bct->setup_bucardo(A => $dbhA);

## Now let's add the database in three ways: SQL, Moose, bucardo_ctl

## For now, let's try bucardo_ctl

$t = 'Calling bucardo_ctl from command-line works';
$i = $bct->ctl('--help');
like($i, qr{ping}, $t);

$t = q{Calling bucardo_ctl with 'add table' gives expected message};
$i = $bct->ctl('add table');
like($i, qr{Usage: add table <name>}, $t);

#$i = $bct->ctl("add table bucardo_test1 db=A");
#like($i, qr{Table added: bucardo_test1}, $t);

pass("done");

