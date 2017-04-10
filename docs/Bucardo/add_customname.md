---
title: Bucardo add customname
permalink: /Bucardo/add_customname/
---

The **add customname** command creates a new Bucardo custom name mapping. This allows the tables involved in replication to have different names on different databases.

Example:

` bucardo add customname public.foobar public.baz`

Creates a mapping across all databases and syncs such that replication from the 'public.foobar' table will go to the 'public.baz' table on the targets.

Usage:

` bucardo add customname oldname newname [db=name] [sync=name]`

Maps an existing table to a new name on the target. The oldname must contain the schema as well as the table name (if the source database supports schemas). You can limit it to one or more databases, and/or to one or more syncs.

### Internals

New customnames are inserted to the [bucardo.customname table](/bucardo.customname_table "wikilink").

### See also:

-   [list_customname](/Bucardo/list_customname "wikilink")
-   [remove_customname](/Bucardo/remove_customname "wikilink")
