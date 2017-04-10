---
title: Bucardo Conflict Handling
permalink: /Bucardo/Conflict_Handling/
---

Bucardo needs to have ways to solve conflicts when replicating, as more than one source database can be written to at the same time. To do this, one must use a conflict strategy (at a table or sync level), or use a 'conflict' [customcode](/Bucardo/customcode "wikilink").

Built-in conflict strategies
----------------------------

Bucardo has a small selection of built-in conflict strategies, which can be set per-table or per-sync. A table-level setting will override a sync-level setting. Both tables and syncs have an attribute named **conflict_strategy** that can be set when creating or updating syncs and tables. The default conflict_strategy for syncs is **bucardo_latest**. Tables have no default value. For example, to change a sync named foobar to a conflict_strategy of bucardo_latest_all_tables, run:

` bucardo update sync conflict_strategy=bucardo_latest_all_tables`

### bucardo_latest

The default strategy for all syncs. When a conflict arises, Bucardo will scan all source databases and find out when the conflicted table was last changed. It then creates a list of preferred databases with the most recently changed source database at the top. Then it uses this list to solve conflicts for each row. Other tables are ignored. The list of databases is not cached, and thus the list is generated anew each time the sync runs and a conflict is encountered.

### bucardo_latest_all_tables

A similar strategy to "bucardo_latest", but all tables in the sync are compared across all source databases to generate the list of most-recently-updates databases. This value is only computed once per sync run, so if more than one table has a conflict, the first table will perform the lookup and the others will used the cached list.

### bucardo_abort

Forces Bucardo to stop the sync if a conflict arises. Only useful in specialized cases, as a lack of a complete conflict solution will stop the sync anyway.

Database list conflict strategy
-------------------------------

Another option that can be given to conflict_strategy is a simple list of databases, separated by spaces. This indicates the preferred order of "winning" databases. Conflicting rows will consult this list and declare the first database found in the list that is part of the conflict as the winner. The database names given are the names as created by the "bucardo add database" command. As an example, if we have three databases (alpha, bravo, and charlie), we can create a sync and give it a preferred ordering like so:

` bucardo add db alpha,bravo,charlie dbname=sales dbhost=east,west,central`
` bucardo add sync foobar dbs=alpha:s,beta:s,charlie:s tables=all conflict='beta alpha charlie'`

In the example above, any rows caused by a conflict in all three databases will be won by database beta, which means the row from that database will overwrite the same row in databases alpha and charlie. If a conflict is only between alpha and charlie, alpha will win.

While it is still possible to resolve all conflicts by only listing N-1 of the databases (in the above example, leaving out "charlie", as it will never win against any others), it is better to explicitly list them all. Future versions of Bucardo may enforce this.

Custom conflict strategies with customcodes
-------------------------------------------

Conflicts can also be resolved by the use of [customcodes](/customcodes "wikilink"), which are Perl scripts which can be associated with a sync. The script will receive some information from Bucardo, and is responsible for letting Bucardo know how to handle the conflict. More than one customcode can be defined per sync, and they will be executed in alphabetic order.

The script will be passed in a hashref as the first and only argument. This hashref includes information about Bucardo, the current sync and table, and the conflicting rows. It also contains [safe database handles](/DBIx::Safe "wikilink") to each database involved in the sync. It contains a few other items as well: see below for the complete list.

To resolve a conflict, the customcode has two major options: set an overall winner, or declare a winner itself for each row. To set an overall winner, the customcode should set a value (list of preferred databases) for one of these four keys in the hashref:

-   tablewinner
    -   Declares the winning databases for the current table only, and only for this run. When the sync runs again and finds a conflict, the customcode will be run again. No information is cached with this option.
-   tablewinner_always
    -   Declares the winning databases for the current table only, but caches the list and will re-use it until the sync itself is restarted. Subsequent runs in which the table has a conflict will use the cached list and the customcode will not run.
-   syncwinner
    -   Declares the winning databases for every table in the sync. The information is only cached in case other tables have conflicts in this round, otherwise everything starts fresh the next time the sync runs.
-   syncwinner_always
    -   Declares the winning database for every table in the sync, and caches the list until the sync is restarted.

The other way for the customcode to resolve conflicts is to tell Bucardo the winning database for each row. To do this, it should modify the key in the hashref named "conflicts". This key contains a hash of primary keys which have conflicts, and the values of this hash are a list of conflicting databases. The job of the customcode is to turn that hash of databases into a single database name - a simple string.

The hashref passed to a conflict customcode has the following keys:

-   schemaname
    -   Schema the conflicting table is in
-   tablename
    -   Table causing the conflict
-   conflicts
    -   A hash with all conflicting rows, with the primary keys as the hash keys
-   version
    -   The version of Bucardo
-   syncname
    -   What sync has called this code
-   shared
    -   A special hash that can be used to maintain information across customcode invocations
-   message
    -   Set this to output a string to the Bucardo logs
-   warning
    -   Set this to output a string as a Bucardo warning
-   error
    -   Set this to cause an exception, which will usually stop the sync
-   skip
    -   Set this to any value to tell Bucardo to skip this customcode
-   lastcode
    -   Set this to any value to tell Bucardo to not bother firing any more customcodes for this particular conflict.
-   dbinfo
    -   Hashref with database names as the keys containing detailed information about each databases involved in the current sync.
-   dbh
    -   Hashref with database names as the keys containing DBIx::Safe versions of the DBI handles to each databases.
