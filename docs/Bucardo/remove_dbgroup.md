---
title: Bucardo remove dbgroup
permalink: /Bucardo/remove_dbgroup/
---

__NOTOC__

The **remove dbgroup** command removes one or more database groups from Bucardo's internal tables.

Example:

` bucardo remove dbgroup foo`

Removes the database group named **foo**

Usage:

` bucardo remove dbgroup `<name(s)>` <--force>`

If there are any syncs that are using the database groups to be removed, the command will fail unless the --force option is given.

### Internals

Removed database groups will cause a delete from the [bucardo.dbgroup table](/bucardo.dbgroup_table "wikilink") and deletes from the [bucardo.dbmap table](/bucardo.dbmap_table "wikilink"). The --force option may cause deletes from the [bucardo.sync table](/bucardo.sync_table "wikilink").

### See also:

-   [add_dbgroup](/Bucardo/add_dbgroup "wikilink")
-   [list_dbgroup](/Bucardo/list_dbgroup "wikilink")
-   [update_dbgroup](/Bucardo/update_dbgroup "wikilink")
