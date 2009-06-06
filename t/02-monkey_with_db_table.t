#!perl

## Test all ways of accessing the db table

use strict;
use warnings;
use lib 't','.';
use Test::More tests => 8;
use BucardoTesting;
my $bct = BucardoTesting->new();

my ($t,$i);

## Start with a clean schema and databases (don't care what's in them)

my $dbhA = $bct->blank_database('A');
my $dbh = $bct->setup_bucardo(A => $dbhA);
$bct->scrub_bucardo_tables($dbh);

## Now let's add the database in three ways: SQL, Moose, bucardo_ctl

## For now, let's try bucardo_ctl

$t = 'Calling bucardo_ctl from command-line works';
$i = $bct->ctl('--help');
like($i, qr{ping}, $t);

$t = q{Calling bucardo_ctl with 'add' gives expected message};
$i = $bct->ctl('add');
like($i, qr{Usage: add <item_type>}, $t);

$t = q{Calling bucardo_ctl with 'add xxx' gives expected message};
$i = $bct->ctl('add xxx');
like($i, qr{Cannot add: unknown type}, $t);

$t = q{Calling bucardo_ctl with 'add db' gives expected message};
$i = $bct->ctl('add db');
like($i, qr{Usage: add db <name>}, $t);

$t = q{Calling bucardo_ctl with 'add database' gives expected message};
$i = $bct->ctl('add database');
like($i, qr{Usage: add database <name>}, $t);

# Create another database for testing
my $dbhB = $bct->blank_database('B');

## Note: this cannot be tested exhaustively unless we initdb and control port and host ourselves
my $ctlargs = $bct->add_db_args('A');
#$com = qq{add database A 'user=$user port=1234 | fff=123 | host="two names"'};
#warn "ctlargs: $ctlargs\n";
$i = $bct->ctl("add database bucardo_test $ctlargs"); ## Default to user bucardo?
like($i, qr{Database added: A}, $t);

## Note: this cannot be tested exhaustively unless we initdb and control port and host ourselves
$ctlargs = $bct->add_db_args('B');
$i = $bct->ctl("add database bucardo_test $ctlargs");
like($i, qr{Database added: B}, $t);

$dbh->disconnect();
$dbhA->disconnect();

pass("done");

