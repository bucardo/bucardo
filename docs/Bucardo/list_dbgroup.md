---
title: Bucardo list dbgroup
permalink: /Bucardo/list_dbgroup/
---

__NOTOC__

The **list dbgroup** command is used to list information about one or more Bucardo database groups.

Example:

` bucardo list dbgroups`

Shows a list of all database groups, one per line, in alphabetical order.

Usage:

` bucardo list dbgroup `<name(s)>

If one or more group names are given, only lists the given ones. Wildcards can be used. To view detailed information, use the **--vv** (very verbose) argument.

### Examples

` bucardo list dbgroup`

` Database group: alpha  Members: db1:source db2:target`
` Database group: beta   Members: db1:source db2:source db3:target`

### Internals

Information is read from the [bucardo.dbgroup table](/bucardo.dbgroup_table "wikilink") and [bucardo.dbmap](/bucardo.dbmap "wikilink") tables.

### See also:

-   [add_dbgroup](/Bucardo/add_dbgroup "wikilink")
-   [update_dbgroup](/Bucardo/update_dbgroup "wikilink")
-   [remove_dbgroup](/Bucardo/remove_dbgroup "wikilink")
