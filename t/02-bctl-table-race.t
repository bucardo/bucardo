#!/usr/bin/env perl
# -*-mode:cperl; indent-tabs-mode: nil-*-

## Test for race condition when adding tables to a sync in parallel
##
## This test demonstrates that concurrent "bucardo add table ... relgroup=X" commands
## can fail or produce inconsistent results when multiple processes add different tables
## to the same relgroup/herd simultaneously.
##
## The race condition occurs because:
## 1. Each bucardo process loads the global $GOAT and $HERD hashes at startup
## 2. Multiple processes check if a table is already in the herd
## 3. The check uses the in-memory hash, not the current database state
## 4. If two processes are adding different tables concurrently, both checks pass
## 5. Both processes insert their tables and call load_bucardo_info() before committing
## 6. The reload sees an incomplete state, causing hash corruption
## 7. Some tables fail to be added properly, resulting in missing entries
##
## Expected behavior with bucardo 5.6.0 (and presumably earlier): TEST FAILS
## - Some tables will not be added to the herdmap
## - Errors about uninitialized values in the $GOAT hash
## - Exit codes are 0 but actual count doesn't match expected
##
## Expected behavior after fix: TEST PASSES
## - All tables should be successfully added to the herdmap
## - No errors about uninitialized values
## - Actual count matches expected count

use 5.008003;
use strict;
use warnings;
use lib 't','.';
use DBD::Pg;
use Test::More;
use File::Temp qw(tempfile);
use Time::HiRes qw(sleep time);

use vars qw/$t/;

use BucardoTesting;
my $bct = BucardoTesting->new({notime=>1})
    or BAIL_OUT "Creation of BucardoTesting object failed\n";
$location = 'setup';

## Configuration
my $NUM_TABLES = 100;      ## Total tables to create and add
my $NUM_THREADS = 10;      ## Parallel threads
my $NUM_SYNCS = 3;         ## Number of syncs to create
my $TABLES_PER_THREAD = int($NUM_TABLES / $NUM_THREADS);

plan tests => $NUM_TABLES;

## Make sure A and B are started up
my $dbhA = $bct->repopulate_cluster('A');
my $dbhB = $bct->repopulate_cluster('B');

## Create a bucardo database, and install Bucardo into it
my $dbhX = $bct->setup_bucardo('A');

## Grab connection information
my ($dbuserA,$dbportA,$dbhostA) = $bct->add_db_args('A');
my ($dbuserB,$dbportB,$dbhostB) = $bct->add_db_args('B');

## Add databases to bucardo
$bct->ctl("bucardo add db A dbname=bucardo_test user=$dbuserA port=$dbportA host=$dbhostA");
$bct->ctl("bucardo add db B dbname=bucardo_test user=$dbuserB port=$dbportB host=$dbhostB");

## Create many test tables
for my $i (1..$NUM_TABLES) {
    $dbhA->do("CREATE TABLE race_table_$i (id INTEGER PRIMARY KEY, data TEXT)");
}
$dbhA->commit();

## Create multiple syncs, each with its own herd
my @sync_names;
for my $sync_num (1..$NUM_SYNCS) {
    my $sync_name = "race_sync_$sync_num";
    my $herd_name = "race_herd_$sync_num";
    push @sync_names, $sync_name;
    $bct->ctl("bucardo add sync $sync_name relgroup=$herd_name dbs=A:source,B:target");
}

## Ensure test output is not buffered
$| = 1;

## Create a barrier file for synchronization
my ($barrier_fh, $barrier_file) = tempfile(UNLINK => 1);
close $barrier_fh;

## Track child processes
my @children;
my %child_output;
my %child_errors;
my @all_exit_codes;

## Create temp files for capturing output
my @output_files;
my @error_files;
for (1..$NUM_THREADS) {
    my ($out_fh, $out_file) = tempfile(UNLINK => 1);
    my ($err_fh, $err_file) = tempfile(UNLINK => 1);
    close $out_fh;
    close $err_fh;
    push @output_files, $out_file;
    push @error_files, $err_file;
}

## Create a completion flag file to signal when table-adding threads are done
my ($done_fh, $done_file) = tempfile(UNLINK => 1);
close $done_fh;

## Fork a validation thread that validates all syncs every 5 seconds
my $validator_pid = fork();
if (!defined $validator_pid) {
    die "Failed to fork validator: $!";
}
if ($validator_pid == 0) {
    ## Validator child process
    my ($val_out_fh, $val_out_file) = tempfile(UNLINK => 1);
    my ($val_err_fh, $val_err_file) = tempfile(UNLINK => 1);
    open STDOUT, '>', $val_out_file or die "Can't redirect STDOUT: $!";
    open STDERR, '>', $val_err_file or die "Can't redirect STDERR: $!";

    ## Wait for other threads to start
    sleep 2;

    ## Keep validating until done file is marked complete
    while (1) {
        ## Check if we're done
        open my $dfh, '<', $done_file or die "Cannot open done file: $!";
        my $done = <$dfh> || '';
        close $dfh;
        last if $done =~ /complete/;

        ## Run validate on all syncs
        for my $sync_name (@sync_names) {
            my $output = $bct->ctl("bucardo validate $sync_name");
            print STDOUT "Validate $sync_name: $output\n";
            STDOUT->flush();
        }

        sleep 5;
    }

    exit(0);
}

## Fork threads - each adds different tables in parallel
for my $thread_num (1..$NUM_THREADS) {
    my $pid = fork();

    if (!defined $pid) {
        die "Failed to fork: $!";
    }

    if ($pid == 0) {
        ## Child process
        my $out_file = $output_files[$thread_num-1];
        my $err_file = $error_files[$thread_num-1];
        open STDOUT, '>', $out_file or die "Can't redirect STDOUT: $!";
        open STDERR, '>', $err_file or die "Can't redirect STDERR: $!";

        ## Wait at the barrier
        my $ready = 0;
        while (!$ready) {
            open my $bfh, '+<', $barrier_file or die "Cannot open barrier: $!";
            flock($bfh, 2);
            my $count = <$bfh> || 0;
            chomp $count;
            $count++;
            seek($bfh, 0, 0);
            truncate($bfh, 0);
            print $bfh "$count\n";
            $ready = 1 if $count >= $NUM_THREADS;
            close $bfh;
            sleep 0.001 unless $ready;
        }

        ## Each thread adds its assigned tables to different syncs in round-robin fashion
        my $start_table = ($thread_num - 1) * $TABLES_PER_THREAD + 1;
        my $end_table = $thread_num * $TABLES_PER_THREAD;

        my $success_count = 0;
        my $failure_count = 0;

        for my $i ($start_table..$end_table) {
            my $table_name = "race_table_$i";

            ## Pick which sync/herd to add this table to (round-robin)
            my $sync_index = ($i - 1) % $NUM_SYNCS;
            my $herd_name = "race_herd_" . ($sync_index + 1);

            ## Add table to the relgroup
            my $output = $bct->ctl("bucardo add table $table_name relgroup=$herd_name db=A");

            if ($output =~ /Added the following tables/ || $output =~ /already in the relgroup/) {
                print STDOUT "PASS $i\n";
                $success_count++;
            } else {
                print STDOUT "FAIL $i\n";
                print STDERR "Table $table_name failed: $output\n";
                $failure_count++;
            }
            STDOUT->flush();
        }

        exit($failure_count);
    }

    ## Parent: track child PID
    push @children, $pid;
}

## Track which tables were successfully added (based on child process output)
my %table_results;
my $first_failure = 0;

## Wait for all children to complete and collect all their results first
for my $i (0..$#children) {
    my $pid = $children[$i];
    waitpid($pid, 0);
    my $exit_code = $? >> 8;
    push @all_exit_codes, $exit_code;

    ## Read all results from this child
    open my $out_fh, '<', $output_files[$i];
    while (my $line = <$out_fh>) {
        if ($line =~ /^(PASS|FAIL) (\d+)/) {
            $table_results{$2} = $1;
        }
    }
    close $out_fh;

    open my $err_fh, '<', $error_files[$i];
    $child_errors{$i+1} = do { local $/; <$err_fh> };
    close $err_fh;
}

## Signal validator thread to stop
open my $dfh, '>', $done_file or die "Cannot open done file: $!";
print $dfh "complete\n";
close $dfh;

## Wait for validator thread to finish
waitpid($validator_pid, 0);

## Now output test results in order, one at a time
## Add small delay between results to show progress
TESTLOOP: for my $table_num (1..$NUM_TABLES) {
    $t = "Table race_table_$table_num was added to the herd";

    my $result = $table_results{$table_num} || 'FAIL';

    if ($result eq 'PASS') {
        pass($t);
        ## Small delay to make progress visible
        select(undef, undef, undef, 0.05);
    } else {
        fail($t);

        ## First failure - show diagnostics and skip remaining
        if (!$first_failure) {
            $first_failure = $table_num;

            ## Check database state for all herds
            $dbhX = $bct->connect_database('A', 'bucardo');
            my $total_actual_count = 0;

            diag("");
            diag("=== RACE CONDITION DETECTED ===");
            diag("First failure at table race_table_$table_num");

            for my $sync_num (1..$NUM_SYNCS) {
                my $herd_name = "race_herd_$sync_num";
                my $sth = $dbhX->prepare(q{
                    SELECT COUNT(*) FROM bucardo.herdmap
                    WHERE herd = ?
                });
                $sth->execute($herd_name);
                my ($actual_count) = $sth->fetchrow_array();
                $total_actual_count += $actual_count;
                diag("Herd $herd_name has $actual_count tables");
            }

            diag("Expected $NUM_TABLES tables total across all herds, but found $total_actual_count");
            diag("Missing: " . ($NUM_TABLES - $total_actual_count) . " tables");

            ## Show error outputs from children
            for my $thread_num (sort keys %child_errors) {
                if ($child_errors{$thread_num}) {
                    diag("Thread $thread_num errors:");
                    diag($child_errors{$thread_num});
                }
            }

            ## Skip remaining tests
            my $remaining = $NUM_TABLES - $table_num;
            skip("Skipping remaining tests due to race condition failure", $remaining);

            last TESTLOOP;
        }
    }
}

## Reconnect to check final state (only if no failures were detected)
if (!$first_failure) {
    $dbhX = $bct->connect_database('A', 'bucardo');

    my $total_tables_added = 0;
    my @all_duplicates;

    ## Check each herd
    for my $sync_num (1..$NUM_SYNCS) {
        my $herd_name = "race_herd_$sync_num";

        ## Get list of tables that were actually added to this herdmap
        my $sth2 = $dbhX->prepare(q{
            SELECT g.schemaname || '.' || g.tablename as tablename
            FROM bucardo.herdmap hm
            JOIN bucardo.goat g ON (hm.goat = g.id)
            WHERE hm.herd = ?
            ORDER BY g.tablename
        });
        $sth2->execute($herd_name);
        my $added_tables_final = $sth2->fetchall_hashref('tablename');

        ## Check for duplicate entries in this herdmap
        $sth2 = $dbhX->prepare(q{
            SELECT g.schemaname || '.' || g.tablename as tablename, COUNT(*) as cnt
            FROM bucardo.herdmap hm
            JOIN bucardo.goat g ON (hm.goat = g.id)
            WHERE hm.herd = ?
            GROUP BY g.schemaname, g.tablename
            HAVING COUNT(*) > 1
        });
        $sth2->execute($herd_name);
        my $duplicates_final = $sth2->fetchall_arrayref();

        my $tables_in_herd = scalar(keys %$added_tables_final);
        $total_tables_added += $tables_in_herd;

        if (@$duplicates_final) {
            push @all_duplicates, @$duplicates_final;
        }
    }

    if ($total_tables_added != $NUM_TABLES) {
        diag("");
        diag("=== RACE CONDITION DETECTED (Final Verification) ===");
        diag("Expected $NUM_TABLES tables total across all herds, but found $total_tables_added");
        diag("Missing: " . ($NUM_TABLES - $total_tables_added) . " tables");

        if (@all_duplicates) {
            diag("Duplicate entries found: " . scalar(@all_duplicates));
            for my $dup (@all_duplicates) {
                diag("  " . $dup->[0] . " appears " . $dup->[1] . " times");
            }
        }
    }
}

END {
    $bct->stop_bucardo($dbhX) if $dbhX;
    $dbhX->disconnect() if $dbhX;
    $dbhA->disconnect() if $dbhA;
    $dbhB->disconnect() if $dbhB;
}
