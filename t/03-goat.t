#!perl

## Test all goat-related actions
## Creating goats and removing via Moose
## Creating goats and removing via bucardo_ctl
## Proper goat triggers

use 5.008003;
use strict;
use warnings;
use DBI;
use Test::More tests => 73;
use lib 't','.';
use BucardoTesting;

my $info = {
			name => 'goat'
			};
my $bctest = BucardoTesting->new($info);



__DATA__


my $rebuildschema = 1;

my ($SQL,$sth,$t);

our $location = 'goat';
require 't/bucardo.test.pl';

pass("*** Beginning 'goat' tests");

## For the goat pkey tests, we need the main bucardo db and one other

my $dbh = setup_database('master', {rebuild => $rebuildschema});
my $dbh1 = setup_database(1, { rebuild => 0 });

$dbh->{AutoCommit} = 1;
$dbh->do("DELETE FROM goat");
$dbh->do("DELETE FROM db");
$SQL = "INSERT INTO db(name,dbhost,dbport,dbname,dbuser,dbpass) VALUES (?,?,?,?,?,?)";
$sth = $dbh->prepare($SQL);

my $bc = get_bc();
$sth->execute('one',
	$bc->{DBHOST1},
	$bc->{DBPORT1},
	$bc->{TESTDB1},
	$bc->{TESTBC1},
	$bc->{TESTPW1}
);

$dbh1->{AutoCommit} = 1;
$dbh1->{RaiseError} = 0;
$dbh1->do("DROP TABLE bctest");
$dbh1->{RaiseError} = 1;
$dbh1->do(q{CREATE TABLE bctest(a int, b text, c date, "user" int, "foo bar" int, e bigint, f smallint)});
$dbh1->do("SET client_min_messages = 'warning'");

$SQL = "INSERT INTO goat(db,schemaname,tablename) VALUES (?,'bucardo_schema',?)";
my $ins = $dbh->prepare($SQL);
$SQL = "UPDATE goat SET pkey=? WHERE tablename='bctest'";
my $upd = $dbh->prepare($SQL);
$SQL = "UPDATE goat SET pkey = ? WHERE tablename = 'bctest'";
my $redo = $dbh->prepare($SQL);

sub get_goat {
	$SQL = "SELECT pkey, pkeytype, qpkey FROM goat WHERE tablename = 'bctest'";
	return $dbh->selectall_arrayref($SQL)->[0];
}
sub get_pkey     { return get_goat()->[0]; }
sub get_pkeytype { return get_goat()->[1]; }
sub get_qpkey    { return get_goat()->[2]; }

$t=q{ Insert to 'goat' with a missing 'db' value fails};
eval { $ins->execute(undef,'foobar'); };
like($@, qr{provide a db}, $t);

$t=q{ Insert to 'goat' with a missing 'tablename' value fails};
eval { $ins->execute('one',undef); };
like($@, qr{provide a table}, $t);

$t=q{ Insert to 'goat' with a invalid 'db' value fails};
eval { $ins->execute('bad_db','foobar'); };
like($@, qr{find a database}, $t);

$t=q{ Insert to 'goat' with a non-existent table fails};
eval { $ins->execute('one','bad_table'); };
like($@, qr{Table not found}, $t);

$t=q{ Insert to 'goat' with a table in the wrong schema fails};
$SQL = "INSERT INTO goat(db,schemaname,tablename) VALUES (?,?,?)";
$sth = $dbh->prepare($SQL);
eval { $sth->execute('one','bad_schema','bctest'); };
like($@, qr{Table not found}, $t);

$t=q{ Insert to 'goat' with a table without a unique index creates empty entries};
eval { $ins->execute('one','bctest'); };
is(get_pkey(), '', $t);
is(get_qpkey(), '', $t);
is(get_pkeytype(), '', $t);

$t=q{ Automatic pkey picking for table 'goat' does not use non-unique indexes};
$dbh1->do("CREATE INDEX bci1 ON bctest(a)");
eval { $redo->execute(undef); };
is(get_pkey(), '', $t);

$t=q{ Automatic pkey picking for table 'goat' does not use a unique expressional index};
$dbh1->do("CREATE UNIQUE INDEX bci2 ON bctest((lower(b)))");
eval { $redo->execute(undef); };
is(get_pkey(), '', $t);

$t=q{ Automatic pkey picking for table 'goat' does not use a unique conditional index};
$dbh1->do("CREATE UNIQUE INDEX bci3 ON bctest(b) WHERE a <> 2");
eval { $redo->execute(undef); };
is(get_pkey(), '', $t);

$t=q{ Automatic pkey picking for table 'goat' picks a unique index correctly};
$dbh1->do("CREATE UNIQUE INDEX bci4 ON bctest(b)");
eval { $redo->execute(undef); };
is($@, q{}, $t);
is(get_pkey(), 'b', $t);

$t=q{ Insert to 'goat' with a table with a unique index populates pkey correctly};
$dbh->do("DELETE FROM goat");
eval { $ins->execute('one','bctest'); };
is($@, q{}, $t);
is(get_pkey(), 'b', $t);

$t=q{ Insert to 'goat' with a table with single-col unique index populates pkeytype correctly};
is(get_pkeytype(), 'text', $t);

$t=q{ Insert to 'goat' with a table with single-col unique index populates qpkeytype correctly};
is(get_qpkey(), 'b', $t);

$t=q{ Removing unique indexes from a table resets pkey on 'goat' when pkey set to null};
$dbh1->do("DROP INDEX bci4");
eval { $redo->execute(undef); };
is(get_pkey(), '', $t);
$t=q{ Removing unique indexes from a table resets pkeytype on 'goat' when pkey set to null};
is(get_pkeytype(), '', $t);
$t=q{ Removing unique indexes from a table resets qpkeytype on 'goat' when pkey set to null};
is(get_qpkey(), '', $t);

$t=q{ Removing unique indexes from a table resets pkey on 'goat' when pkey set empty};
$dbh1->do("CREATE UNIQUE INDEX bci4 ON bctest(b)");
eval { $redo->execute(undef); };
$dbh1->do("DROP INDEX bci4");
eval { $redo->execute(''); };
is($@, q{}, $t);
is(get_pkey(), '', $t);

$t=q{ Setting pkey on 'goat' to a value when there are no unique indexes gives an error};
eval { $redo->execute('b'); };
like($@, qr{no unique constraint}, $t);

$t=q{ Insert to 'goat' with a table with multi-col unique index populates pkey correctly};
$dbh1->do("CREATE UNIQUE INDEX bci5 ON bctest(b,a,c)");
$dbh->do("DELETE FROM goat");
eval { $ins->execute('one','bctest'); };
is($@, q{}, $t);
is(get_pkey(), 'b|a|c', $t);

$t=q{ Insert to 'goat' with a table with multi-col unique index populates pkeytype correctly};
is(get_pkeytype(), 'text|int4|date', $t);

$t=q{ Insert to 'goat' with a table with multi-col unique index populates qpkey correctly};
is(get_qpkey(), 'b|a|c', $t);

$t=q{ Insert to 'goat' with a table with single-col primary key populates pkey correctly};
$dbh1->do("DROP INDEX bci5");
$dbh1->do("ALTER TABLE bctest ADD CONSTRAINT bcip PRIMARY KEY (a)");
$dbh->do("DELETE FROM goat");
eval { $ins->execute('one','bctest'); };
is($@, q{}, $t);
is(get_pkey(), 'a', $t);

$t=q{ Insert to 'goat' with a table with single-col primary key populates pkeytype correctly};
is(get_pkeytype(), 'int4', $t);

$t=q{ Insert to 'goat' with a table with single-col primary key populates qpkey correctly};
is(get_qpkey(), 'a', $t);

$t=q{ Insert to 'goat' with a table with multi-col primary key populates pkey correctly};
$dbh1->do("ALTER TABLE bctest DROP CONSTRAINT bcip");
$dbh1->do("ALTER TABLE bctest ADD CONSTRAINT bcip PRIMARY KEY (e,a,c)");
$dbh->do("DELETE FROM goat");
eval { $ins->execute('one','bctest'); };
is($@, q{}, $t);
is(get_pkey(), 'e|a|c', $t);

$t=q{ Insert to 'goat' with a table with multi-col primary key populates pkeytype correctly};
is(get_pkeytype(), 'int8|int4|date', $t);

$t=q{ Insert to 'goat' with a table with multi-col primary key populates qpkey correctly};
is(get_qpkey(), 'e|a|c', $t);

$t=q{ Insert to 'goat' with a table with primary key and unique index chooses the primary key};
$dbh1->do("CREATE UNIQUE INDEX bci7 ON bctest(e)");
$dbh->do("DELETE FROM goat");
eval { $ins->execute('one','bctest'); };
is($@, q{}, $t);
is(get_pkey(), 'e|a|c', $t);

$t=q{ Insert to 'goat' with a table with multiple unique indexes chooses one with least columns};
$dbh1->do("ALTER TABLE bctest DROP CONSTRAINT bcip");
$dbh1->do("CREATE UNIQUE INDEX bci8 ON bctest(a,c,b)");
$dbh->do("DELETE FROM goat");
eval { $ins->execute('one','bctest'); };
is($@, q{}, $t);
is(get_pkey(), 'e', $t);
$dbh1->do("DROP INDEX bci7");
$dbh1->do("CREATE UNIQUE INDEX bci9 ON bctest(e,a)");
$dbh->do("DELETE FROM goat");
eval { $ins->execute('one','bctest'); };
is($@, q{}, $t);
is(get_pkey(), 'e|a', $t);

$t=q{ Insert to 'goat' with a table with multiple unique indexes chooses newest if number of cols the same};
$dbh1->do("CREATE UNIQUE INDEX bci10 ON bctest(a,b)");
$dbh->do("DELETE FROM goat");
eval { $ins->execute('one','bctest'); };
is($@, q{}, $t);
is(get_pkey(), 'a|b', $t);

$t=q{ Insert to 'goat' with a table with odd multi-col primary key populates pkey correctly};
$dbh1->do(q{ALTER TABLE bctest ADD CONSTRAINT bcip PRIMARY KEY ("user",a,"foo bar",e)});
$dbh->do("DELETE FROM goat");
eval { $ins->execute('one','bctest'); };
is($@, q{}, $t);
is(get_pkey(), 'user|a|foo bar|e', $t);
is(get_pkeytype(), 'int4|int4|int4|int8', $t);
is(get_qpkey(), '"user"|a|"foo bar"|e', $t);

$t=q{ Setting pkeytype manually on update is not allowed};
eval { $dbh->do("UPDATE goat SET pkeytype='foo'"); };
like($@, qr{Cannot set pkeytype}, $t);

$t=q{ Setting qpkey manually on update is not allowed};
eval { $dbh->do("UPDATE goat SET qpkey='foo'"); };
like($@, qr{Cannot set qpkey}, $t);

$t=q{ Setting pkeytype manually on insert is not allowed};
$dbh->do("DELETE FROM goat");
$SQL = "INSERT INTO goat(db,schemaname,tablename,pkeytype) VALUES ('one','bucardo_schema','bctest','foo')";
eval { $dbh->do($SQL); };
like($@, qr{Cannot set pkeytype}, $t);

$t=q{ Setting qpkey manually on insert is not allowed};
$SQL = "INSERT INTO goat(db,schemaname,tablename,qpkey) VALUES ('one','bucardo_schema','bctest','foo')";
eval { $dbh->do($SQL); };
like($@, qr{Cannot set qpkey}, $t);

$t=q{ Inserting to 'goat' with an invalid pkey fails};
$SQL = "INSERT INTO goat(db,schemaname,tablename,pkey) VALUES ('one','bucardo_schema','bctest','foo')";
eval { $dbh->do($SQL); };
like($@, qr{matching unique}, $t);

$t=q{ Inserting to 'goat' with an invalid pkey (invalid index) fails};
$SQL = "INSERT INTO goat(db,schemaname,tablename,pkey) VALUES ('one','bucardo_schema','bctest','a')";
eval { $dbh->do($SQL); };
like($@, qr{matching unique}, $t);

$t=q{ Inserting to 'goat' with an valid pkey works};
$SQL = "INSERT INTO goat(db,schemaname,tablename,pkey) VALUES ('one','bucardo_schema','bctest','a|c|b')";
eval { $dbh->do($SQL); };
is($@, q{}, $t);

$t=q{ Updating 'goat' pkey to an invalid value fails};
$SQL = "UPDATE goat SET pkey = 'foo' WHERE tablename = 'bctest'";
eval { $dbh->do($SQL); };
like($@, qr{matching unique}, $t);

$t=q{ Updating 'goat' pkey to an invalid value (invalid index) fails};
$SQL = "UPDATE goat SET pkey = 'a' WHERE tablename = 'bctest'";
eval { $dbh->do($SQL); };
like($@, qr{matching unique}, $t);

$t=q{ Updating 'goat' pkey to an valid value works};
$SQL = "UPDATE goat SET pkey = 'e|a' WHERE tablename = 'bctest'";
eval { $dbh->do($SQL); };
is($@, q{}, $t);

$t=q{ Setting column 'pkey' on 'goat' to NULL causes automatic population};
$SQL = "UPDATE goat SET pkey = NULL WHERE tablename = 'bctest'";
eval { $dbh->do($SQL); };
is($@, q{}, $t);
is(get_pkey(), 'user|a|foo bar|e', $t);

pass("*** Finished 'goat' tests");
$dbh->disconnect();

