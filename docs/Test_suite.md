---
title: Test suite
permalink: /Test_suite/
---

The Bucardo test suite is a good test to prove a system can run Bucardo successfully, and is recommended on new installations. It tests several common, and some uncommon Bucardo setups, described here. These tests follow Perl's common Test::

|File name|Description|
|---------|-----------|
|00_release.t|Simple sanity checks to make sure the various files are the correct versions|
|01-basic.t|Make sure various files parse cleanly|
|02-monkey_with_db_table.t|Tests adding databases in various ways|
|02-monkey_with_goat_table.t|Tests adding goats in various ways|
|02-monkey_with_herd.t|Tests adding herds in various ways|
|02-monkey_with_sync_table.t|Tests adding syncs in various ways|
|02-simple.t|Creates a simple pushdelta sync between two test databases and ensures it works|
|03-goat.t|Currently contains no tests|
|04-pushdelta.t|Creates a pushdelta sync and tests correct handling of database constraints and triggers|
|04-pushdelta_twosyncs.t|Similar to 04-pushdelta.t, but with two slave databases|
|05-fullcopy.t|Tests tables and sequences in a fullcopy sync, including testing customselect|
|06-multicolpk.t|Tests multi-column primary keys in a pushdelta sync|
|06-multicolpushdelta.t|Similar to 06-multicolpk.t, including more difficult scenarios|
|07-multicolswap.t|Tests multi-column primary keys in a swap sync|
|08-wildkick.t|Tests using bucardo_ctl's kick command with a wildcard instead of an exact sync name|
|09-uniqueconstraint.t|Tests handling of various scenarios involving unique constraints|
|10-makedelta.t|Creates three databases (A, B, and C), and uses makedelta to replicate rows from A to B, and then from B to C.|
|11-customselect.t|Tests transforming rows during replication using customselect|
|12-addtable.t|Tests adding a new table to an existing sync|
|13-ddl.t|Currently contains no tests|
|14-truncate.t|Tests proper handling of TRUNCATE commands using truncate triggers (available in PostgreSQL 8.4 and later)|
|15-star.t|Creates a commonly requested but currently unusable replication scheme, where three databases (A, B, and C) are organized in a star-like pattern, with A as the hub, and B and C as spokes. Rows are replicated from A to both spokes, and from each spoke into A. This won't perform well in practice, because there's no mechanism in place to make sure rows don't get replicated from, for instance, C to A, and back to C.|
|98-cleanup.t|Removes test databases and other leftovers of the test suite|
|99-perlcritic.t|Runs perlcritic against the Bucardo code. This runs only if the RELEASE_TESTING environment variable is set|
|99-signature.t|Tests Bucardo's package signature to make sure the source hasn't been tampered with. This runs only if the RELEASE_TESTING environment variable is set|
|99-spellcheck.t|If Test::SpellChecker is installed and RELEASE_TESTING is set, spell checks as much of Bucardo as possible.|
|99-yaml.t|Tests the package metadata to ensure it is well formed, if RELEASE_TESTING is set|

[Category:Bucardo](/Category:Bucardo "wikilink")
