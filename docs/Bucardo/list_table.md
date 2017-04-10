---
title: Bucardo list table
permalink: /Bucardo/list_table/
---

__NOTOC__

The **list table** command is used to list information about one or more tables that Bucardo knows about. It can also be called as **list tables**

Example:

` bucardo list tables`

Shows a list of all tables, one per line, in alphabetical order. Also shows which database and syncs they belong to. The number at the start of the line is the internal id for the table, and is primarily used to feed to the [remove_table](/Bucardo/remove_table "wikilink") command.

Usage:

` bucardo list table `<name(s)>

If one or more table names is given, only lists the given ones. Wildcards can be used. To view detailed information, use the **--vv** (very verbose) argument.

### Examples

` bucardo list tables`
` `
`1. Table: public.t1  Database: db1 Syncs: alpha,beta,charlie`
`3. Table: public.t2  Database: db1 Syncs: alpha,beta,charlie`
`2. Table: public.t3  Database: db1 Syncs: alpha,beta,charlie`
`4. Table: public.t4  Database: db1 Syncs: alpha,beta,charlie`
`5. Table: public.t5  Database: db1`

`bucardo list table %4`

`4. Table: public.t4  Database: db1 Syncs: alpha,beta,charlie`

`bucardo list table t1 -vv`

1. Table: public.t1 Database: db1 Syncs: alpha,beta,charlie

`   analyze_after_copy   = 1`
`   cdate                = 2011-11-25 20:18:04.434984-05`
`   customselect         = NULL`
`   db                   = db1`
`   delta_bypass         = 0`
`   delta_bypass_count   = NULL`
`   delta_bypass_min     = NULL`
`   delta_bypass_percent = NULL`
`   ghost                = 0`
`   has_delta            = 0`
`   id                   = 1`
`   makedelta            = NULL`
`   ping                 = NULL`
`   pkey                 = id`
`   pkeytype             = int4`
`   qpkey                = id`
`   rebuild_index        = 0`
`   reltype              = table`
`   schemaname           = public`
`   standard_conflict    = NULL`
`   strict_checking      = 1`
`   tablename            = t1`
`   vacuum_after_copy    = 1`
` `

### Internals

Information is read from the [bucardo.goat table](/bucardo.goat_table "wikilink") for the main information, from the [bucardo.herdmap](/bucardo.herdmap "wikilink") and the [bucardo.sync table](/bucardo.sync_table "wikilink") table for the sync information.

### See also:

-   [add_table](/Bucardo/add_table "wikilink")
-   [update_table](/Bucardo/update_table "wikilink")
-   [remove_table](/Bucardo/remove_table "wikilink")
