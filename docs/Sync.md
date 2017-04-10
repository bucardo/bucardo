---
title: Sync
permalink: /Sync/
---

[Bucardo](/Bucardo "wikilink") replication events happens in **syncs**, which replicate one or more tables and come in one of three types:

-   [fullcopy](/fullcopy "wikilink")
-   [pushdelta](/pushdelta "wikilink")
-   [swap](/swap "wikilink")

To add a new sync, run:

` bucardo_ctl add sync `<syncname>` source=xx targetdb=yy targetgroup=zz type=`<type>` tables=x,y,z`

-   *syncname* is what you want to name this sync - keep it short and descriptive
-   *source* is the name of a [source herd](/source_herd "wikilink")
-   *targetdb* is the name of a [database](/database "wikilink")
-   *targetgroup* is the name of a [database group](/database_group "wikilink") (note: only one of targetdb or targetgroup is required)
-   *type* is one of the three sync types: pushdelta, fullcopy, or swap
-   *tables* is an optional list of tables that should be immediately added to the new sync. A [herd](/herd "wikilink") with the same name as the sync itself will be automatically created to contain these tables.

To modify an existing sync, run:

` bucardo_ctl update sync name=value`

Where name is one of the columns of the [sync table](/sync_table "wikilink")

To add a database to an existing sync the **update dbgroup** command should be used (see [Bucardo/update_dbgroup](/Bucardo/update_dbgroup "wikilink")).

[Category:Bucardo](/Category:Bucardo "wikilink")
