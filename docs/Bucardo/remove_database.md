---
title: Bucardo remove database
permalink: /Bucardo/remove_database/
---

__NOTOC__

The **remove database** command removes one or more databases from Bucardo's internal tables.

Example:

` bucardo remove database A B`

Removes the database entries **A** and **B**.

Usage:

` bucardo remove database `<name(s)>` <--force>`

If there are any tables that Bucardo knows about that reside in this database, or any database groups that this database belongs to, the remove command will fail unless the --force option is given. The alternate form **remove db** is also accepted.

### Internals

Removed databases will cause a delete from the [bucardo.db table](/bucardo.db_table "wikilink"). Database group mappings will cause deletes from the [bucardo.dbmap table](/bucardo.dbmap_table "wikilink"). Table removals will cause deletes from the [bucardo.goat table](/bucardo.goat_table "wikilink").

### See also:

-   [add_database](/Bucardo/add_database "wikilink")
-   [list_database](/Bucardo/list_database "wikilink")
-   [update_database](/Bucardo/update_database "wikilink")
