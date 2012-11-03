#!/usr/bin/env perl
# -*-mode:cperl; indent-tabs-mode: nil-*-

## Test adding, dropping, and changing database groups via bucardo
## Tests the main subs: add_dbgroup, remove_dbgroup, update_dbgroup, list_dbgroups

use 5.008003;
use strict;
use warnings;
use Data::Dumper;
use lib 't','.';
use DBD::Pg;
use Test::More tests => 24;

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

## Tests of basic 'add dbgroup' usage

$t = 'Add database group with no argument gives expected help message';
$res = $bct->ctl('bucardo add dbg');
like ($res, qr/add dbgroup/, $t);

$t = q{Add database group accepts both 'add dbg' and 'add dbgroup'};
$res = $bct->ctl('bucardo add dbgroup');
like ($res, qr/add dbgroup/, $t);

$t = q{Add database group fails with invalid characters};
$res = $bct->ctl('bucardo add dbgroup foo^barz');
like ($res, qr/Invalid characters/, $t);

$t = 'Add database group works';
$res = $bct->ctl('bucardo add dbg foobar');
like ($res, qr/Created database group "foobar"/, $t);

$t = q{Adding a database group with the same name fails};
$res = $bct->ctl('bucardo add dbg foobar');
is ($res, '', $t);

$t = 'Listing of database groups looks correct';
$res = $bct->ctl('bucardo list dbgroups');
chomp $res;
is ($res, 'Database group: foobar', $t);

$t = q{Listing of database groups with the 'dbg' alias works};
$res = $bct->ctl('bucardo list dbg');
chomp $res;
is ($res, 'Database group: foobar', $t);

$t = q{Adding an invalid database via add dbgroup gives expected message};
$res = $bct->ctl('bucardo add dbgroup foobar A');
like ($res, qr/"A" does not exist/, $t);

$t = q{Adding a database via add dbgroup gives expected message};
$bct->ctl("bucardo add db A dbname=bucardo_test user=$dbuserA port=$dbportA host=$dbhostA");
$res = $bct->ctl('bucardo add dbgroup foobar A');
like ($res, qr/Added database "A" to group "foobar" as target/, $t);

$t = q{Adding a database in source role via add dbgroup gives expected message};
$bct->ctl("bucardo add db B dbname=bucardo_test user=$dbuserB port=$dbportB host=$dbhostB");
$res = $bct->ctl('bucardo add dbgroup foobar B:master');
like ($res, qr/Added database "B" to group "foobar" as source/, $t);

$t = 'Listing of database groups looks correct';
$res = $bct->ctl('bucardo list dbgroups');
chomp $res;
is ($res, 'Database group: foobar  Members: A:target B:source', $t);

## Remove

$t = 'Removal of non-existent database group gives expected message';
$res = $bct->ctl('bucardo remove dbgroup bunko');
like ($res, qr/No such database group: bunko/, $t);

$t = 'Removal of a database group works';
$res = $bct->ctl('bucardo remove dbgroup foobar');
like ($res, qr/Removed database group "foobar"/, $t);

$t = 'Removal of two database groups works';
$bct->ctl('bucardo add dbgroup foobar1');
$bct->ctl('bucardo add dbgroup foobar2');
$res = $bct->ctl('bucardo remove dbgroup foobar1 foobar2');
like ($res, qr/Removed database group "foobar1".*Removed database group "foobar2"/s, $t);

$t = 'Removal of dbgroup fails if used in a sync';
$bct->ctl('bucardo add herd therd bucardo_test1');
$bct->ctl('bucardo add dbgroup foobar3 A:source B');
$bct->ctl('bucardo add sync mysync herd=therd dbs=foobar3');
$res = $bct->ctl('bucardo remove dbgroup foobar3');
$res =~ s/\s+$//ms;
is ($res, q/Error running bucardo: Cannot remove database group "foobar3": it is being used by one or more syncs/, $t);

$t = 'Removal of dbgroup works if used in a sync and the --force argument used';
$res = $bct->ctl('bucardo remove dbgroup foobar3 --force');
like ($res, qr/Dropping all syncs that reference the dbgroup "foobar3".*Removed database group "foobar3"/s, $t);

## Update

$bct->ctl('bucardo add dbgroup foobar');

$t = 'Update dbgroup with no arguments gives expected message';
$res = $bct->ctl('bucardo update dbgroup foobar');
like ($res, qr/update/, $t);

$t = 'Update dbgroup with invalid group gives expected message';
$res = $bct->ctl('bucardo update dbgroup foobar3 baz');
like ($res, qr/Could not find a database group named "foobar3"/, $t);

$t = 'Update dbgroup works with adding a single database';
$res = $bct->ctl('bucardo update dbgroup foobar A');
like ($res, qr/Added database "A" to group "foobar" as target/, $t);

$t = 'Update dbgroup works with adding multiple databases';
$res = $bct->ctl('bucardo update dbgroup foobar A:master B:master');
like ($res, qr/Changed role of database "A" in group "foobar" from target to source.*Added database "B" to group "foobar" as source/s, $t);

$t = 'Update dbgroup fails when new name is invalid';
$res = $bct->ctl('bucardo update dbgroup foobar newname=foobaz#');
like ($res, qr/Invalid database group name "foobaz#"/, $t);

$t = 'Update dbgroup works when changing the name';
$res = $bct->ctl('bucardo update dbgroup foobar name=foobaz');
like ($res, qr/Changed database group name from "foobar" to "foobaz"/, $t);

$t = q{Removing all database groups};
$res = $bct->ctl('bucardo remove dbg foobaz');
like ($res, qr/Removed database group "foobaz"/, $t);

$t = q{List database returns correct information};
$res = $bct->ctl('bucardo list dbgroups');
like ($res, qr/No database groups/, $t);

exit;

END {
    $bct->stop_bucardo($dbhX);
    $dbhX and $dbhX->disconnect();
    $dbhA and $dbhA->disconnect();
    $dbhB and $dbhB->disconnect();
}
