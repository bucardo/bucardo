---
title: Bucardo update table
permalink: /Bucardo/update_table/
---

__NOTOC__

The **update table** command is used to change an existing table entry in the Bucardo database.

Example:

` bucardo update table t1 ping=true`

Updates the table **t1**: sets the ping to true

Usage:

` bucardo update table name [setting=value setting2=value2 ...]`

To see a list of items that can be changed, issue:

` bucardo list table name -vv`

Note: the following fields cannot be changed: id, cdate, db, reltype

### Internals

Changes will update the [bucardo.goat table](/bucardo.goat_table "wikilink").

### See also:

-   [add_table](/Bucardo/add_table "wikilink")
-   [list_table](/Bucardo/list_table "wikilink")
-   [remove_table](/Bucardo/remove_table "wikilink")
