#!perl

## Make sure the version number is consistent in all places

use 5.006;
use strict;
use warnings;
use Data::Dumper;
use Test::More;
use lib 't','.';

if (! $ENV{RELEASE_TESTING}) {
	plan (skip_all =>  'Test skipped unless environment variable RELEASE_TESTING is set');
}

plan tests => 1;

my %v;
my $vre = qr{(\d+\.\d+\.\d+\_?\d*)};

## Grab version from various files
my $file = 'META.yml';
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

exit;
