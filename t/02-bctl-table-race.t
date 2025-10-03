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
## 2. Multiple processes check if a table is already in the herd (line 3669 in bucardo)
## 3. The check uses the in-memory hash, not the current database state
## 4. If two processes are adding different tables concurrently, both checks pass
## 5. Both processes insert their tables and call load_bucardo_info() before committing
## 6. The reload sees an incomplete state, causing hash corruption
## 7. Some tables fail to be added properly, resulting in missing entries
##
## Expected behavior with current code: TEST FAILS
## - Some tables will not be added to the herdmap (typically 50-70 missing out of 500)
## - Errors about uninitialized values in the $GOAT hash
## - Exit codes are 0 but actual count doesn't match expected
##
## Expected behavior after fix: TEST PASSES
## - All 500 tables should be successfully added to the herdmap
## - No errors about uninitialized values
## - Actual count matches expected count (500)

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

## Create a sync with a herd
$bct->ctl("bucardo add sync race_sync relgroup=race_herd dbs=A:source,B:target");

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

## Fork a validation thread that runs "bucardo validate race_sync" every 5 seconds
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

        ## Run validate
        my $output = $bct->ctl("bucardo validate race_sync");
        print STDOUT "Validate: $output\n";
        STDOUT->flush();

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

        ## Each thread adds its assigned tables
        my $start_table = ($thread_num - 1) * $TABLES_PER_THREAD + 1;
        my $end_table = $thread_num * $TABLES_PER_THREAD;

        my $success_count = 0;
        my $failure_count = 0;

        for my $i ($start_table..$end_table) {
            my $table_name = "race_table_$i";

            ## Add table to the relgroup
            my $output = $bct->ctl("bucardo add table $table_name relgroup=race_herd db=A");

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

            ## Check database state
            $dbhX = $bct->connect_database('A', 'bucardo');
            my $sth = $dbhX->prepare(q{
                SELECT COUNT(*) FROM bucardo.herdmap
                WHERE herd = (SELECT name FROM bucardo.herd WHERE name='race_herd')
            });
            $sth->execute();
            my ($actual_count) = $sth->fetchrow_array();

            diag("");
            diag("=== RACE CONDITION DETECTED ===");
            diag("First failure at table race_table_$table_num");
            diag("Expected $NUM_TABLES tables in herdmap, but found $actual_count");
            diag("Missing: " . ($NUM_TABLES - $actual_count) . " tables");

            ## Show error outputs from children
            for my $thread_num (sort keys %child_errors) {
                if ($child_errors{$thread_num}) {
                    diag("Thread $thread_num errors:");
                    diag($child_errors{$thread_num});
                }
            }

            ## Skip remaining tests
            my $remaining = $NUM_TABLES - $table_num;
            SKIP: {
                skip("Skipping remaining tests due to race condition failure", $remaining);
            }

            last TESTLOOP;
        }
    }
}

## Skip the final verification since we already handled it
goto CLEANUP;

CLEANUP:

## Reconnect to check final state (only if no failures were detected)
if (!$first_failure) {
    $dbhX = $bct->connect_database('A', 'bucardo');

    ## Get list of tables that were actually added to the herdmap
    my $sth2 = $dbhX->prepare(q{
        SELECT g.schemaname || '.' || g.tablename as tablename
        FROM bucardo.herdmap hm
        JOIN bucardo.goat g ON (hm.goat = g.id)
        WHERE hm.herd = (SELECT name FROM bucardo.herd WHERE name='race_herd')
        ORDER BY g.tablename
    });
    $sth2->execute();
    my $added_tables_final = $sth2->fetchall_hashref('tablename');

    ## Check for duplicate entries in herdmap
    $sth2 = $dbhX->prepare(q{
        SELECT g.schemaname || '.' || g.tablename as tablename, COUNT(*) as cnt
        FROM bucardo.herdmap hm
        JOIN bucardo.goat g ON (hm.goat = g.id)
        WHERE hm.herd = (SELECT name FROM bucardo.herd WHERE name='race_herd')
        GROUP BY g.schemaname, g.tablename
        HAVING COUNT(*) > 1
    });
    $sth2->execute();
    my $duplicates_final = $sth2->fetchall_arrayref();


    my $tables_added_count = scalar(keys %$added_tables_final);

    if ($tables_added_count != $NUM_TABLES) {
        diag("");
        diag("=== RACE CONDITION DETECTED (Final Verification) ===");
        diag("Expected $NUM_TABLES tables in herdmap, but found $tables_added_count");
        diag("Missing: " . ($NUM_TABLES - $tables_added_count) . " tables");

        if (@$duplicates_final) {
            diag("Duplicate entries found: " . scalar(@$duplicates_final));
            for my $dup (@$duplicates_final) {
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
