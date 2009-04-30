#!/usr/bin/perl -- -*-cperl-*-

## Cleanup all test remnants

use 5.008003;
use strict;
use warnings;
use DBI;
use Test::More tests => 5;
use lib 't','.';
use BucardoTesting;

use vars qw/$SQL $sth $t/;

pass("*** Cleaning up Bucardo testing artifacts");

## Remove all temporary files
pass('Removed all temporary files');
## *.bc.tmp

## Remove all test databases
my $bct = BucardoTesting->new({name => 'cleanup'});
$bct->drop_database('all');
pass('Removed all test databases');

## Remove all test users
#$bct->drop_users('all');
#pass('Removed all test users');

## Shutdown the helper program
pass('Shutdown the helper program');

