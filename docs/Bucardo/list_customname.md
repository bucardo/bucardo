---
title: Bucardo list customname
permalink: /Bucardo/list_customname/
---

__NOTOC__

The **list customname** command is used to list information about one or more Bucardo custom name mappings.

Example:

` bucardo list customnames`

Shows a list of all custom name mappings with their internal ID.

Usage:

` bucardo list customnames [number] [schema.tablename]`

Without any arguments, lists all custom names. Arguments can be a number, representing the internal ID (mainly used for removing custom names), or the argument can be a fully qualified table name.

### Examples

` bucardo list customnames`

` 1. Table: public.t1 => foobar`
` 2. Table: public.sales => qs Sync: alpha`

### Internals

Information is read from the [bucardo.customname](/bucardo.customname "wikilink") table

### See also:

-   [add_customname](/Bucardo/add_customname "wikilink")
-   [remove_customname](/Bucardo/remove_customname "wikilink")
