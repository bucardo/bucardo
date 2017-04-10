---
title: Bucardo add sync
permalink: /Bucardo/add_sync/
---

The **add sync** command is used to create a new [Bucardo sync](/Bucardo_sync "wikilink").

Example:

` bucardo add sync alpha herd=gill dbs=A,B,C`

Creates a new sync named **alpha** which replicates tables in the herd **gill** and replicates from source database **A** to target databases **B** and **C**

Usages:

` bucardo add sync `<name>` herd=`<herdname>` dbs=`<database group>

` bucardo add sync `<name>` herd=`<herdname>` dbs=`<list of databases>

` bucardo add sync `<name>` tables=products,categories,sales dbs=`<list of databases>

### Required arguments:

-   herd
    -   The [Bucardo herd](/Bucardo_herd "wikilink") containing the tables and sequences to be replicated
-   dbs
    -   The [Bucardo database group](/Bucardo_database_group "wikilink") to use in this sync, or a comma-separated list of databases. If the latter, a new database group with the same name as the sync will be created. By default, the first database will be considered the [source](/source_database "wikilink"), and all others [targets](/target_database "wikilink"). To specify the [role](/database_role "wikilink") of a database, add a colon and the role. For example, to create a sync with three source databases and two targets:

` bucardo add sync foobar herd=myherd dbs=A:source,B:target,C:target,D:source,E:source`

Because the first database given always defaults to a source role, and all others default to a target role, the above sync could also be created with:

` bucardo add sync foobar herd=myherd dbs=A,B,C,D:source,E:source`

### Optional arguments:

-   tables
    -   A comma-separated list of tables which should be replicated by this sync. A new [herd](/Bucardo_herd "wikilink") will be created with the same name as the sync to hold these tables.
-   status
    -   The initial status of this sync. Defaults to "active". The only other choice at the moment is "inactive"
-   rebuild_index
    -   Whether to rebuild indexes after each sync, defaults to off (0)
-   onetimecopy
    -   Controls if we switch to fullcopy mode for normal targets. Default is 0 (off). A setting of 1 indicates a normal onetimecopy. A setting of 2 indicates that we only copy if the source table is not-empty and the target table is empty. After a successful sync, Bucardo will flip this value back to 0 itself. See [onetimecopy](/Bucardo/onetimecopy "wikilink") for more information.
-   ping
    -   Determine if triggers are created that signal Bucardo to run when a table on one of the source databases for this sync has changes. Defaults to 1 (on).

### See also:

-   [list_sync](/Bucardo/list_sync "wikilink")
-   [update_sync](/Bucardo/update_sync "wikilink")
-   [remove_sync](/Bucardo/remove_sync "wikilink")

[Category:Bucardo](/Category:Bucardo "wikilink")
