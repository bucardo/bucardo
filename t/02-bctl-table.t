#!/usr/bin/env perl
# -*-mode:cperl; indent-tabs-mode: nil-*-

## Test adding, dropping, and changing tables via bucardo
## Tests the main subs: add_table, list_table, update_table, remove_table

use 5.008003;
use strict;
use warnings;
use Data::Dumper;
use lib 't','.';
use DBD::Pg;
use Test::More tests => 32;

use vars qw/$t $res $expected $command $dbhX $dbhA $dbhB $SQL/;

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

## Tests of basic 'add table' usage

$t = 'Add table with no argument gives expected help message';
$res = $bct->ctl('bucardo add table');
like ($res, qr/Usage: add table/, $t);

$t = q{Add table fails when no databases have been created yet};
$res = $bct->ctl('bucardo add table foobarz');
like ($res, qr/No databases have been added yet/, $t);

$bct->ctl("bucardo add db A dbname=bucardo_test user=$dbuserA port=$dbportA host=$dbhostA");

$t = q{Add table fails when the table does not exist};
$res = $bct->ctl('bucardo add table foobarz');
like ($res, qr/Did not find matches.*  foobarz/s, $t);

## Clear out each time, gather a list afterwards

sub empty_goat_table() {
    $SQL = 'TRUNCATE TABLE herdmap, herd, goat CASCADE';
    $dbhX->do($SQL);
    $dbhX->commit();
}

empty_goat_table();
$t = q{Add table works for a single valid schema.table entry};
$res = $bct->ctl('bucardo add table public.bucardo_test1');
is ($res, qq{$addtable_msg:\n  public.bucardo_test1\n}, $t);

$t = q{Add table fails for a single invalid schema.table entry};
$res = $bct->ctl('bucardo add table public.bucardo_notest1');
is ($res, qq{$nomatch_msg:\n  public.bucardo_notest1\n}, $t);

$t = q{Add table works for a single valid table entry (no schema)};
$res = $bct->ctl('bucardo add table bucardo_test2');
is ($res, qq{$addtable_msg:\n  public.bucardo_test2\n}, $t);

$t = q{Add table fails for a single invalid table entry (no schema)};
$res = $bct->ctl('bucardo add table bucardo_notest2');
is ($res, qq{$nomatch_msg:\n  bucardo_notest2\n}, $t);

$dbhA->do('DROP SCHEMA IF EXISTS tschema CASCADE');
$dbhA->do('CREATE SCHEMA tschema');
$dbhA->do('CREATE TABLE tschema.bucardo_test3 (a int)');
$dbhA->commit();

$t = q{Add table works for multiple matching valid table entry (no schema)};
$res = $bct->ctl('bucardo add table bucardo_test3');
is ($res, qq{$addtable_msg:\n  public.bucardo_test3\n  tschema.bucardo_test3\n}, $t);

$t = q{Add table works for a single valid middle wildcard entry};
$res = $bct->ctl('bucardo add table b%_test4');
is ($res, qq{$addtable_msg:\n  public.bucardo_test4\n}, $t);

$t = q{Add table works for a single valid beginning wildcard entry};
$res = $bct->ctl('bucardo add table %_test5');
is ($res, qq{$addtable_msg:\n  public.bucardo_test5\n}, $t);

$t = q{Add table works for a single valid ending wildcard entry};
$res = $bct->ctl('bucardo add table drop%');
is ($res, qq{$addtable_msg:\n  public.droptest\n}, $t);

$t = q{Add table works for a single valid middle wildcard entry};
$res = $bct->ctl('bucardo add table b%_test6');
is ($res, qq{$addtable_msg:\n  public.bucardo_test6\n}, $t);

$t = q{Add table fails for a single invalid wildcard entry};
$res = $bct->ctl('bucardo add table b%_notest');
is ($res, qq{$nomatch_msg:\n  b%_notest\n}, $t);

$t = q{Add table works for a single valid schema wildcard entry};
$res = $bct->ctl('bucardo add table %.bucardo_test7');
is ($res, qq{$addtable_msg:\n  public.bucardo_test7\n}, $t);

$t = q{Add table fails for a single invalid schema wildcard entry};
$res = $bct->ctl('bucardo add table %.notest');
is ($res, qq{$nomatch_msg:\n  %.notest\n}, $t);

$t = q{Add table works for a single valid table wildcard entry};
$res = $bct->ctl('bucardo add table public.bucard%8');
is ($res, qq{$addtable_msg:\n  public.bucardo_test8\n}, $t);

$t = q{Add table fails for a single invalid table wildcard entry};
$res = $bct->ctl('bucardo add table public.no%test');
is ($res, qq{$nomatch_msg:\n  public.no%test\n}, $t);

$t = q{Add table works for a single valid schema and table wildcard entry};
$res = $bct->ctl('bucardo add table pub%.bucard%9');
is ($res, qq{$addtable_msg:\n  public.bucardo_test9\n}, $t);

$t = q{Add table fails for a single invalid schema and table wildcard entry};
$res = $bct->ctl('bucardo add table pub%.no%test');
is ($res, qq{$nomatch_msg:\n  pub%.no%test\n}, $t);

$t = q{Add table does not re-add existing tables};
$res = $bct->ctl('bucardo add table bucard%');
is ($res, qq{$addtable_msg:\n  public.bucardo_test10\n}, $t);

$t = q{'bucardo list tables' returns expected result};
$res = $bct->ctl('bucardo list tables');
$expected =
q{Table: public.bucardo_test1   DB: A  PK: id (int2)
Table: public.bucardo_test2   DB: A  PK: id|data1 (int4|text)
Table: public.bucardo_test3   DB: A  PK: id (int8)
Table: public.bucardo_test4   DB: A  PK: id (text)
Table: public.bucardo_test5   DB: A  PK: id space (date)
Table: public.bucardo_test6   DB: A  PK: id (timestamp)
Table: public.bucardo_test7   DB: A  PK: id (numeric)
Table: public.bucardo_test8   DB: A  PK: id (bytea)
Table: public.bucardo_test9   DB: A  PK: id (int_unsigned)
Table: public.bucardo_test10  DB: A  PK: id (timestamptz)
Table: public.droptest        DB: A  PK: none
Table: tschema.bucardo_test3  DB: A  PK: none
};
is ($res, $expected, $t);

## Remove them all, then try adding in various combinations
empty_goat_table();
$t = q{Add table works with multiple entries};
$res = $bct->ctl('bucardo add table pub%.bucard%9 public.bucardo_test1 nada bucardo3 buca%2');
is ($res, qq{$nomatch_msg:\n  bucardo3\n  nada\n$addtable_msg:\n  public.bucardo_test1\n  public.bucardo_test2\n  public.bucardo_test9\n}, $t);

$t = q{Add table works when specifying the ping option};
$res = $bct->ctl('bucardo add table bucardo_test4 ping=true');
is ($res, qq{$addtable_msg:\n  public.bucardo_test4\n}, $t);

$t = q{'bucardo list tables' returns expected result};
$res = $bct->ctl('bucardo list tables');
$expected =
q{Table: public.bucardo_test1  DB: A  PK: id (int2)
Table: public.bucardo_test2  DB: A  PK: id|data1 (int4|text)
Table: public.bucardo_test4  DB: A  PK: id (text) ping:true
Table: public.bucardo_test9  DB: A  PK: id (int_unsigned)
};
is ($res, $expected, $t);

$t = q{Add table works when specifying the rebuild_index and ping options};
$res = $bct->ctl('bucardo add table bucardo_test5 ping=false rebuild_index=1');
is ($res, qq{$addtable_msg:\n  public.bucardo_test5\n}, $t);

$t = q{'bucardo list tables' returns expected result};
$res = $bct->ctl('bucardo list tables');
$expected =
q{Table: public.bucardo_test1  DB: A  PK: id (int2)
Table: public.bucardo_test2  DB: A  PK: id|data1 (int4|text)
Table: public.bucardo_test4  DB: A  PK: id (text) ping:true
Table: public.bucardo_test5  DB: A  PK: id space (date) ping:false rebuild_index:1
Table: public.bucardo_test9  DB: A  PK: id (int_unsigned)
};
is ($res, $expected, $t);

empty_goat_table();

$t = q{Add table works when adding to a new herd};
$res = $bct->ctl('bucardo add table bucardo_test1 herd=foobar');
$expected =
qq{$addtable_msg:
  public.bucardo_test1
Created the herd named "foobar"
$newherd_msg "foobar":
  public.bucardo_test1
};
is ($res, $expected, $t);

$t = q{Add table works when adding to an existing herd};
$res = $bct->ctl('bucardo add table bucardo_test5 herd=foobar');
is ($res, qq{$addtable_msg:\n  public.bucardo_test5\n$oldherd_msg "foobar":\n  public.bucardo_test5\n}, $t);

$t = q{Add table works when adding multiple tables to a new herd};
$res = $bct->ctl('bucardo add table "public.buc*3" %.bucardo_test2 herd=foobar2');
$expected =
qq{$addtable_msg:
  public.bucardo_test2
  public.bucardo_test3
Created the herd named "foobar2"
$newherd_msg "foobar2":
  public.bucardo_test2
  public.bucardo_test3
};
is ($res, $expected, $t);

$t = q{Add table works when adding multiple tables to an existing herd};
$res = $bct->ctl('bucardo add table bucardo_test6 %.%do_test4 herd=foobar2');
$expected =
qq{$addtable_msg:
  public.bucardo_test4
  public.bucardo_test6
$newherd_msg "foobar2":
  public.bucardo_test4
  public.bucardo_test6
};
is ($res, $expected, $t);

## Tests of basic 'delete table' usage

$t = q{Delete table works for a single entry};
$res = $bct->ctl('bucardo remove table public.bucardo_test4');
$expected =
qq{$deltable_msg:
  public.bucardo_test4
};
is ($res, $expected, $t);

$t = q{Delete table works for multiple entries};
$res = $bct->ctl('bucardo remove table public.bucardo_test3 public.bucardo_test2');
$expected =
qq{$deltable_msg:
  public.bucardo_test2
  public.bucardo_test3
};
is ($res, $expected, $t);


END {
    $bct->stop_bucardo($dbhX);
    $dbhX and $dbhX->disconnect();
    $dbhA and $dbhA->disconnect();
    $dbhB and $dbhB->disconnect();
}
