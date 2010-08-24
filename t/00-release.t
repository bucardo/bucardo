#!/usr/bin/env perl
# -*-mode:cperl; indent-tabs-mode: nil-*-

## Make sure the version number is consistent in all places
## Ensure the bucardo.schema file has no tabs in it

use 5.006;
use strict;
use warnings;
use Data::Dumper;
use Test::More;
use lib 't','.';

if (! $ENV{RELEASE_TESTING}) {
	plan (skip_all =>  'Test skipped unless environment variable RELEASE_TESTING is set');
}

## Grab all files from the MANIFEST to generate a test count
my $file = 'MANIFEST';
my @mfiles;
open my $mfh, '<', $file or die qq{Could not open "$file": $!\n};
while (<$mfh>) {
	next if /^#/;
	push @mfiles => $1 if /(\S.+)/o;
}
close $mfh or warn qq{Could not close "$file": $!\n};

plan tests => 1 + @mfiles;

my %v;
my $vre = qr{(\d+\.\d+\.\d+\_?\d*)};

## Grab version from various files
$file = 'META.yml';
open my $fh, '<', $file or die qq{Could not open "$file": $!\n};
while (<$fh>) {
	push @{$v{$file}} => [$1,$.] if /version\s*:\s*$vre/;
}
close $fh or warn qq{Could not close "$file": $!\n};

$file = 'Makefile.PL';
open $fh, '<', $file or die qq{Could not open "$file": $!\n};
while (<$fh>) {
	push @{$v{$file}} => [$1,$.] if /VERSION = '$vre'/;
}
close $fh or warn qq{Could not close "$file": $!\n};

$file = 'Bucardo.pm';
open $fh, '<', $file or die qq{Could not open "$file": $!\n};
while (<$fh>) {
	push @{$v{$file}} => [$1,$.] if (/VERSION = '$vre'/ or /document describes version $vre/);
}
close $fh or warn qq{Could not close "$file": $!\n};

$file = 'Bucardo.pm.html';
open $fh, '<', $file or die qq{Could not open "$file": $!\n};
while (<$fh>) {
	push @{$v{$file}} => [$1,$.] if /document describes version $vre/;
}
close $fh or warn qq{Could not close "$file": $!\n};

$file = 'bucardo_ctl';
open $fh, '<', $file or die qq{Could not open "$file": $!\n};
while (<$fh>) {
	push @{$v{$file}} => [$1,$.] if (/VERSION = '$vre'/ or /document describes version $vre/);
}
close $fh or warn qq{Could not close "$file": $!\n};

$file = 'bucardo_ctl.html';
open $fh, '<', $file or die qq{Could not open "$file": $!\n};
while (<$fh>) {
	push @{$v{$file}} => [$1,$.] if /document describes version $vre/;
}
close $fh or warn qq{Could not close "$file": $!\n};

$file = 'Changes';
open $fh, '<', $file or die qq{Could not open "$file": $!\n};
while (<$fh>) {
	if (/^$vre/) {
		push @{$v{$file}} => [$1,$.];
		last;
	}
}
close $fh or warn qq{Could not close "$file": $!\n};

$file = 'README';
open $fh, '<', $file or die qq{Could not open "$file": $!\n};
while (<$fh>) {
	push @{$v{$file}} => [$1,$.] if (/is version $vre/ or /TEST VERSION \($vre/);
}
close $fh or warn qq{Could not close "$file": $!\n};

$file = 'bucardo.schema';
open $fh, '<', $file or die qq{Could not open "$file": $!\n};
while (<$fh>) {
	push @{$v{$file}} => [$1,$.] if (/\-\- Version $vre/ or /version\|$vre/);
}
close $fh or warn qq{Could not close "$file": $!\n};

my $good = 1;
my $lastver;
for my $filename (keys %v) {
	for my $glob (@{$v{$filename}}) {
		my ($ver,$line) = @$glob;
		if (! defined $lastver) {
			$lastver = $ver;
		}
		elsif ($ver ne $lastver) {
			$good = 0;
		}
	}
}

if ($good) {
	pass "All version numbers are the same ($lastver)";
}
else {
	fail 'All version numbers were not the same!';
	for my $filename (sort keys %v) {
		for my $glob (@{$v{$filename}}) {
			my ($ver,$line) = @$glob;
			diag "File: $filename. Line: $line. Version: $ver\n";
		}
	}
}

## Make sure all files in the MANIFEST are "clean": no tabs, no unusual characters

for my $mfile (@mfiles) {
	file_is_clean($mfile);
}

exit;

sub file_is_clean {

	my $file = shift or die;

	if (!open $fh, '<', $file) {
		fail qq{Could not open "$file": $!\n};
		return;
	}
	$good = 1;
	my $inside_copy = 0;
	while (<$fh>) {
		if (/^COPY .+ FROM stdin/i) {
			$inside_copy = 1;
		}
		if (/^\\./ and $inside_copy) {
			$inside_copy = 0;
		}
		if (/\t/ and $file ne 'Makefile.PL' and $file !~ /\.html$/ and ! $inside_copy) {
			diag "Found a tab at line $. of $file\n";
			$good = 0;
		}
		if (! /^[\S ]*/) {
			diag "Invalid character at line $. of $file: $_\n";
			$good = 0; die;
		}
	}
	close $fh or warn qq{Could not close "$file": $!\n};

	if ($good) {
		pass "The $file file has no tabs or unusual characters";
	}
	else {
		fail "The $file file did not pass inspection!";
	}

}

exit;
