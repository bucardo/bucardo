#!perl

use 5.008003;
use strict;
use warnings;
use Test::More;

if (! $ENV{RELEASE_TESTING}) {
	plan (skip_all =>  'Test skipped unless environment variable RELEASE_TESTING is set');
}

eval {
	require Perl::Critic;
};
if ($@) {
   plan (skip_all =>  'Perl::Critic needed to run this test');
}
eval {
	require Test::Perl::Critic;
};
if ($@) {
   plan (skip_all =>  'Test::Perl::Critic needed to run this test');
}

## Gotta have a profile
my $PROFILE = '.perlcriticrc';
if (! -e $PROFILE) {
	plan (skip_all =>  qq{Perl::Critic profile "$PROFILE" not found\n});
}

plan tests => 2;
Test::Perl::Critic->import( -profile => $PROFILE );
critic_ok('Bucardo.pm');
critic_ok('bucardo_ctl');


