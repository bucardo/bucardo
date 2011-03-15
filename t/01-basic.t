#!perl

## Basic tests of Things That Should Always Work
## Any failures immediately call BAIL_OUT

use strict;
use warnings;
use lib 't','.';
use Test::More tests => 4;
use BucardoTesting;

my $t=q{File Bucardo.pm compiles without errors};
eval {
    require Bucardo;
};
is($@, q{}, $t);
$@ and BAIL_OUT qq{Cannot continue until Bucardo.pm compiles cleanly\n};

$t=q{File bucardo_ctl compiles without errors};
$ENV{BUCARDO_CTL_TEST} = 1;
eval {
    require 'bucardo_ctl';
};
$ENV{BUCARDO_CTL_TEST} = 0;
is($@, q{}, $t);
$@ and BAIL_OUT qq{Cannot continue until bucardo_ctl compiles cleanly\n};

$t=qq{Helper module BucardoTesting.pm compiles without errors};
eval {
    require BucardoTesting;
};
is($@, q{}, $t);
$@ and BAIL_OUT qq{Cannot continue until BucardoTesting cleanly\n};

$t=q{BucardoTesting->new() works};
my $bct;
eval {
    $bct = BucardoTesting->new();
};
is($@, q{}, $t);
$@ and BAIL_OUT qq{Cannot continue until BucardoTesting->new() works\n};

