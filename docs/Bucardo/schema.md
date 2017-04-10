---
title: Bucardo schema
permalink: /Bucardo/schema/
---

The main [Bucardo](/Bucardo "wikilink") schema is contained in the **bucardo.schema** file. This file is processed when running *bucardo_ctl install* to create the Bucardo control database. There are also some tables and functions that are created on the remote databases. All tables and functions are always in the 'bucardo' schema.

Main Bucardo tables
-------------------

-   [bucardo_config](/Bucardo/table/bucardo_config "wikilink")
    -   Holds global configuration information: use **bucardo_ctl show all** to view
-   [db](/Bucardo/table/db "wikilink")
    -   Contains information about each replicated database. Use **bucardo_ctl list dbs** to view.
-   [dbgroup](/Bucardo/table/dbgroup "wikilink")
    -   Contains the names of database groups. View with **bucardo_ctl list dbgroups**'
-   [dbmap](/Bucardo/table/dbmap "wikilink")
    -   Maps databases to database groups (many to many)
-   [goat](/Bucardo/table/goat "wikilink")
    -   Contains information on specific items to be replicated: tables or sequences. View with **bucardo_ctl list goats**
-   [herd](/Bucardo/table/herd "wikilink")
    -   A group of goats is a herd: this contains the names of all herds. View with **bucardo_ctl list herds**
-   [herdmap](/Bucardo/table/herdmap "wikilink")
    -   Maps goats to herds (many to many)
-   [sync](/Bucardo/table/sync "wikilink")
    -   A single named replication event. Links a specific source herd to a remote database or database group. View with **bucardo_ctl list syncs**
-   [customcode](/Bucardo/table/customcode "wikilink")
    -   Perl subroutines that fire at some point in the replication process. Contains code for conflict handling and exception handling. View with **bucardo_ctl list codes**
-   [customcode_map](/Bucardo/table/customcode_map "wikilink")
    -   Maps customcodes to a specific sync or a goat
-   [upgrade_log](/Bucardo/table/upgrade_log "wikilink")
    -   Populated when you run **bucardo_ctl upgrade**
-   [bucardo_rate](/Bucardo/table/bucardo_rate "wikilink")
    -   Track latency and speed of sync, when the "track_rates" column of a sync is set to true
-   [bucardo_custom_trigger](/Bucardo/table/bucardo_custom_trigger "wikilink")
    -   Allows replacement of the standard trigger for replication of only some rows in a table.
-   [bucardo_log_message](/Bucardo/table/bucardo_log_message "wikilink")
    -   Used internally for the messaging feature, e.g. **bucardo_ctl message Foobar**
-   [q](/Bucardo/table/q "wikilink")
    -   Internal table used by Bucardo to coordinate active syncs
-   [audit_pid](/Bucardo/table/audit_pid "wikilink")
    -   Lists the PIDs of active processes. No longer on by default.
-   [db_connlog](/Bucardo/table/db_connlog "wikilink")
    -   A historical record of connection attempts to remote databases. Rarely used or needed.

Main Bucardo functions
----------------------

-   [bucardo_purge_q_table(interval)](/Bucardo/function/bucardo_purge_q_table "wikilink")
-   [validate_goat()](/Bucardo/function/validate_goat "wikilink")
-   [validate_sync()](/Bucardo/function/validate_sync "wikilink")
-   [validate_all_syncs()](/Bucardo/function/validate_all_syncs "wikilink")

Remote Bucardo tables
---------------------

Each database that is used as a source for a [swap](/swap "wikilink") or [pushdelta](/pushdelta "wikilink") sync has the following tables installed into the bucardo schema:

-   [bucardo_delta](/Bucardo/table/bucardo_delta "wikilink")
    -   Stores which rows have changed for each replicated table
-   [bucardo_track](/Bucardo/table/bucardo_track "wikilink")
    -   Stores which rows have been replicated to which remote targets
-   [bucardo_delta_targets](/Bucardo/table/bucardo_delta_targets "wikilink")
    -   Maps tables to remote databases
-   [bucardo_sequences](/Bucardo/table/bucardo_sequences "wikilink")
    -   Tracks current status of replicated sequences
-   [bucardo_truncate_trigger](/Bucardo/table/bucardo_truncate_trigger "wikilink")
    [bucardo_truncate_trigger_log](/Bucardo/table/bucardo_truncate_trigger_log "wikilink")
    -   Tracks truncation of replicated tables

Remote Bucardo functions
------------------------

-   [bucardo_purge_delta(interval)](/Bucardo/function/bucardo_purge_delta "wikilink")
-   [bucardo_compress_delta()](/Bucardo/function/bucardo_compress_delta "wikilink")
-   [bucardo_audit()](/Bucardo/function/bucardo_audit "wikilink")
