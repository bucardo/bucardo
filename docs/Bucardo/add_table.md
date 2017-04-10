---
title: Bucardo add table
permalink: /Bucardo/add_table/
---

__NOTOC__

The **add table** command is used to teach Bucardo about a table.

Example:

` bucardo add table sales`

Adds the table named **sales** to Bucardo.

Usage:

` bucardo add table `<name(s)>` [herd=herdname]`

Adds one or more tables: the schema is optional: if not given, will add all tables with that name regardless of schema. Wildcards can be used as well: the preferred form is a percent sign (%) as the wildcard. A herd can be specified as well: all new tables will be added to this herd. The herd will be created if it does not already exist.

### Internals

New tables cause an insert to the [bucardo.goat table](/bucardo.goat_table "wikilink"). A new herd causes an insert to the [bucardo.herd table](/bucardo.herd_table "wikilink"). Adding a table to a herd results in an insert to the [bucardo.herdmap table](/bucardo.herdmap_table "wikilink").

### See also:

-   [list_table](/Bucardo/list_table "wikilink")
-   [update_table](/Bucardo/update_table "wikilink")
-   [remove_table](/Bucardo/remove_table "wikilink")
