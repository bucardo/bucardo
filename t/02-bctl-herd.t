#!/usr/bin/env perl
# -*-mode:cperl; indent-tabs-mode: nil-*-

## Test adding, dropping, and changing herds via bucardo
## Tests the main subs: add_herd, list_herds, update_herd, remove_herd

use 5.008003;
use strict;
use warnings;
use Data::Dumper;
use lib 't','.';
use DBD::Pg;
use Test::More tests => 47;

use vars qw/$t $res $command $dbhX $dbhA $dbhB/;

use BucardoTesting;
my $bct = BucardoTesting->new({notime=>1})
    or BAIL_OUT "Creation of BucardoTesting object failed\n";
$location = '';

## Make sure A and B are started up
$dbhA = $bct->repopulate_cluster('A');
$dbhB = $bct->repopulate_cluster('B');

## Create a bucardo database, and install Bucardo into it
$dbhX = $bct->setup_bucardo('A');

## Grab connection information for each database
my ($dbuserA,$dbportA,$dbhostA) = $bct->add_db_args('A');
my ($dbuserB,$dbportB,$dbhostB) = $bct->add_db_args('B');

## Tests of basic 'add herd' usage

$t = 'Add herd with no argument gives expected help message';
$res = $bct->ctl('bucardo add herd');
like ($res, qr/Usage: add herd/, $t);

$t = q{Add herd works for a new herd};
$res = $bct->ctl('bucardo add herd foobar');
like ($res, qr/Created herd "foobar"/, $t);

$t = q{Add herd gives expected message if herd already exists};
$res = $bct->ctl('bucardo add herd foobar');
like ($res, qr/Herd "foobar" already exists/, $t);

$t = q{Add herd gives expected message when adding a single table that does not exist};
$res = $bct->ctl('bucardo add herd foobar nosuchtable');
like ($res, qr/No databases have been added yet/, $t);

## Add two postgres databases
for my $name (qw/ A B /) {
    $t = "Adding database from cluster $name works";
    my ($dbuser,$dbport,$dbhost) = $bct->add_db_args($name);
    $command = "bucardo add db $name dbname=bucardo_test user=$dbuser port=$dbport host=$dbhost";
    $res = $bct->ctl($command);
    like ($res, qr/Added database "$name"/, $t);
}

$t = q{Add herd works when adding a single table};
$bct->ctl("bucardo add database bucardo_test user=$dbuserA port=$dbportA host=$dbhostA addalltables");
$res = $bct->ctl('bucardo add herd foobar bucardo_test1');
is ($res, qq{Herd "foobar" already exists
Added the following tables:
  public.bucardo_test1
$newherd_msg "foobar":
  public.bucardo_test1\n}, $t);

$t = q{Add herd works when adding multiple tables};

$t = q{Add herd works when adding a single sequence};

$t = q{Add herd works when adding multiple sequences};

$t = q{Add herd works when adding same name table and sequence};

$t = q{Add herd works when adding tables via schema wildcards};

$t = q{Add herd works when adding tables via table wildcards};

exit;

## end add herd?

exit;

END {
    $bct->stop_bucardo($dbhX);
    $dbhX and $dbhX->disconnect();
    $dbhA and $dbhA->disconnect();
    $dbhB and $dbhB->disconnect();
}
