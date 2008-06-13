#!/usr/bin/perl -- -*-cperl-*-

## Test all ways of accessing the herd table

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

$t = q{Calling bucardo_ctl with 'add herd' gives expected message};
$i = $bct->ctl('add herd');
like($i, qr{Usage: add herd <name>}, $t);

## Create and return handles for some test databases
my $dbhA = $bct->setup_database({db => 'A'});

$t=q{Running add herd works as expeceted};
$i = $bct->ctl("add herd testherd1");
like($i, qr{Herd added: testherd1}, $t);

pass("done");

