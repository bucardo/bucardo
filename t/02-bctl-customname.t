#!/usr/bin/env perl
# -*-mode:cperl; indent-tabs-mode: nil-*-

## Test adding, dropping, and listing customnames via bucardo
## Tests the main subs: add_customname, list_customname, and remove_customname

use 5.008003;
use strict;
use warnings;
use Data::Dumper;
use lib 't','.';
use DBD::Pg;
use Test::More tests => 18;

use vars qw/$t $res $command $dbhX $dbhA $dbhB/;

use BucardoTesting;
my $bct = BucardoTesting->new({notime=>1})
    or BAIL_OUT "Creation of BucardoTesting object failed\n";
$location = '';

## Make sure A and B are started up
$dbhA = $bct->repopulate_cluster('A');

## Create a bucardo database, and install Bucardo into it
$dbhX = $bct->setup_bucardo('A');

## Grab connection information for each database
my ($dbuserA,$dbportA,$dbhostA) = $bct->add_db_args('A');

## Add database A along with all tables
$command =
"bucardo add db A dbname=bucardo_test user=$dbuserA port=$dbportA host=$dbhostA addalltables";
$res = $bct->ctl($command);
like ($res, qr/Added database "A"\nNew tables added: \d/s, $t);

$t = 'Add customname with no argument gives expected help message';
$res = $bct->ctl('bucardo add customname');
like ($res, qr/add customname/, $t);

$t = 'Add customname with a single argument gives expected help message';
$res = $bct->ctl('bucardo add customname foobar');
like ($res, qr/add customname/, $t);

$t = 'Add customname with an invalid table name gives expected error message';
$res = $bct->ctl('bucardo add customname nosuchtable foobar');
like ($res, qr/Could not find/, $t);

$t = 'Add customname with an invalid table number gives expected error message';
$res = $bct->ctl('bucardo add customname 12345 foobar');
like ($res, qr/Could not find/, $t);

$t = 'Add customname with an invalid sync gives expected error message';
$res = $bct->ctl('bucardo add customname bucardo_test1 foobar sync=abc');
like ($res, qr/No such sync/, $t);

$t = 'Add customname with an invalid database gives expected error message';
$res = $bct->ctl('bucardo add customname bucardo_test1 foobar database=abc');
like ($res, qr/No such database/, $t);

$t = 'Add customname with an invalid db gives expected error message';
$res = $bct->ctl('bucardo add customname bucardo_test1 foobar db=abc');
like ($res, qr/No such database/, $t);

$t = 'Add customname with a valid name works';
$res = $bct->ctl('bucardo add customname bucardo_test1 foobar');
like ($res, qr/Transformed public.bucardo_test1 to foobar/, $t);

$t = 'List customname shows the expected output';
$res = $bct->ctl('bucardo list customname');
like ($res, qr/1\. Table: public.bucardo_test1 => foobar/, $t);

$t = 'List customname shows the expected output with no matching entries';
$res = $bct->ctl('bucardo list customname anc');
like ($res, qr/No matching/, $t);

$t = 'List customname shows the expected output using an exact name';
$res = $bct->ctl('bucardo list customname public.bucardo_test1');
like ($res, qr/1\. Table: public.bucardo_test1 => foobar/, $t);

$t = 'List customname shows the expected output using a regex';
$res = $bct->ctl('bucardo list customname pub%');
like ($res, qr/1\. Table: public.bucardo_test1 => foobar/, $t);

$t = q{Remove customname with no argument gives expected help message};
$res = $bct->ctl('bucardo remove customname');
like ($res, qr/bucardo remove/, $t);

$t = q{Remove customname with non-numeric argument gives expected help message};
$res = $bct->ctl('bucardo remove customname foobar');
like ($res, qr/bucardo remove/, $t);

$t = q{Remove customname with invalid argument gives expected error message};
$res = $bct->ctl('bucardo remove customname 1234');
like ($res, qr/number 1234 does not exist/, $t);

$t = q{Remove customname with valid argument gives expected message};
$res = $bct->ctl('bucardo remove customname 1');
like ($res, qr/Removed customcode 1: public.bucardo_test1 => foobar/, $t);

$t = 'List customname shows the expected output';
$res = $bct->ctl('bucardo list customname');
like ($res, qr/No customnames have been added yet/, $t);

exit;

END {
    $bct->stop_bucardo($dbhX);
    $dbhX and $dbhX->disconnect();
}
