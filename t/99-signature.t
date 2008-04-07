#!perl

## Test that our SIGNATURE file is valid - requires TEST_SIGNATURE env

use strict;
use warnings;
use Test::More;
select(($|=1,select(STDERR),$|=1)[1]);

if (!$ENV{TEST_SIGNATURE}) {
	plan skip_all => 'Set the environment variable TEST_SIGNATURE to enable this test';
}
plan tests => 1;

SKIP: {
	if (!eval { require Module::Signature; 1 }) {
		skip 'Must have Module::Signature to test SIGNATURE file', 1;
	}
	elsif ( !-e 'SIGNATURE' ) {
		fail 'SIGNATURE file was not found';
	}
	elsif ( ! -s 'SIGNATURE') {
		fail 'SIGNATURE file was empty';
	}
	else {
		my $ret = Module::Signature::verify();
		if ($ret eq Module::Signature::SIGNATURE_OK()) {
			pass 'Valid SIGNATURE file';
		}
		else {
			fail 'Invalid SIGNATURE file';
		}
	}
}
