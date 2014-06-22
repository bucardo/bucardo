#!/usr/bin/env perl
# -*-mode:cperl; indent-tabs-mode: nil-*-

## Basic tests of Things That Should Always Work
## Any failures of important files immediately call BAIL_OUT

use strict;
use warnings;
use Data::Dumper;
use lib 't','.';
use Test::More;
use BucardoTesting;

my @important_files = qw{Bucardo.pm bucardo };

opendir my $dh, 't' or die qq{Could not open the 't' directory: are you running this from the right place?\n};
my @test_files = grep { /\.t$/ } readdir $dh;
closedir $dh or warn qq{Could not close the 't' directory: $!\n};

opendir $dh, 'scripts' or die qq{Could not open the 'scripts' directory};
my @script_files = grep { /^[a-z]/ } readdir $dh;
closedir $dh or warn qq{Could not close the 'scripts' directory: $!\n};

if (! eval { require CGI; } ) {
    @script_files = grep { ! /bucardo-report/ } @script_files;
}

plan tests => @important_files + @test_files + @script_files;

for my $file (@important_files) {
    my $t=qq{File $file compiles without errors};
    eval {
        require $file;
    };
    is($@, q{}, $t);
    $@ and BAIL_OUT qq{Cannot continue until $file compiles cleanly\n};
}

for my $file (@test_files) {
    my $t=qq{File $file compiles without errors};
    my $com = "perl -c t/$file 2>&1";
    my $res = qx{$com};
    chomp $res;
    is($res, qq{t/$file syntax OK}, $t);
}

for my $file (@script_files) {
    my $t=qq{File $file compiles without errors};
    my $com = "perl -c scripts/$file 2>&1";
    my $res = qx{$com};
    chomp $res;
    is($res, qq{scripts/$file syntax OK}, $t);
}

exit;

