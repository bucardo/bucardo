#!perl

## Cleanup any mess we made

use 5.008003;
use strict;
use warnings;
use Test::More tests => 1;

for my $letter ('A'..'Z') {
	my $dir = "bucardo_test_database_$letter";
	next if ! -d $dir;
	my $pidfile = "$dir/postmaster.pid";
	next if ! -e $pidfile;
	open my $fh, '<', $pidfile or die qq{Could not open "$pidfile": $!\n};
	<$fh> =~ /^(\d+)/ or die qq{File "$pidfile" did not start with a number!\n};
	my $pid = $1;
	close $fh or die qq{Could not close "$pidfile": $!\n};
	kill 15 => $pid;
	sleep 1;
	if (kill 0 => $pid) {
		kill 9 => $pid;
	}
}

pass 'Test databases are shut down';

