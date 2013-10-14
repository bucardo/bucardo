#!/usr/bin/env perl
# -*-mode:cperl; indent-tabs-mode: nil-*-

## Test adding, dropping, and changing relgroups via bucardo
## Tests the main subs: add_relgroup, list_relgroups, update_relgroup, remove_relgroup

use 5.008003;
use strict;
use warnings;
use Data::Dumper;
use lib 't','.';
use DBD::Pg;
use Test::More tests => 7;

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

## Tests of basic 'add relgroup' usage

$t = 'Add relgroup with no argument gives expected help message';
$res = $bct->ctl('bucardo add relgroup');
like ($res, qr/add relgroup/, $t);

$t = q{Add relgroup works for a new relgroup};
$res = $bct->ctl('bucardo add relgroup foobar');
like ($res, qr/Created relgroup "foobar"/, $t);

$t = q{Add relgroup gives expected message if relgroup already exists};
$res = $bct->ctl('bucardo add relgroup foobar');
like ($res, qr/Relgroup "foobar" already exists/, $t);

$t = q{Add relgroup gives expected message when adding a single table that does not exist};
$res = $bct->ctl('bucardo add relgroup foobar nosuchtable');
like ($res, qr/No databases have been added yet/, $t);

## Add two postgres databases
for my $name (qw/ A B /) {
    $t = "Adding database from cluster $name works";
    my ($dbuser,$dbport,$dbhost) = $bct->add_db_args($name);
    $command = "bucardo add db $name dbname=bucardo_test user=$dbuser port=$dbport host=$dbhost";
    $res = $bct->ctl($command);
    like ($res, qr/Added database "$name"/, $t);
}

$t = q{Add relgroup works when adding a single table};

# If we do this here, we'll have problems. The next test adds a table called
# bucardo_test1, which will be found in this new bucardo_test database. But
# because there's no dot in the table name in the call adding the foobar herd,
# bucardo will try to find other tables with similar names, and will search in
# the newly-added database A to do so, where it will find and add a
# bucardo_test1 table. It will then try adding that table to the herd as well,
# and fail, because you can't have tables from different databases in the same
# herd. This behavior seems pessimal.
#$res = $bct->ctl("bucardo add database bucardo_test db=bucardo_test user=$dbuserA port=$dbportA host=$dbhostA addalltables");
$res = $bct->ctl('bucardo add relgroup foobar bucardo_test1');
is ($res, qq{Relgroup "foobar" already exists
Added the following tables or sequences:
  public.bucardo_test1 (DB: A)
$newherd_msg "foobar":
  public.bucardo_test1\n}, $t);

$t = q{Add relgroup works when adding multiple tables};

$t = q{Add relgroup works when adding a single sequence};

$t = q{Add relgroup works when adding multiple sequences};

$t = q{Add relgroup works when adding same name table and sequence};

$t = q{Add relgroup works when adding tables via schema wildcards};

$t = q{Add relgroup works when adding tables via table wildcards};

exit;

## end add relgroup?

exit;

END {
    $bct->stop_bucardo($dbhX);
    $dbhX and $dbhX->disconnect();
    $dbhA and $dbhA->disconnect();
    $dbhB and $dbhB->disconnect();
}
