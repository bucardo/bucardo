---
title: Bucardo remove customname
permalink: /Bucardo/remove_customname/
---

__NOTOC__

The **remove customname** command removes one or more custom name mappings from Bucardo's internal tables.

Example:

` bucardo remove customname 1 4`

Removes the custom names with internal IDs of 1 and 4

Usage:

` bucardo remove customname `<numbers>

The numbers are internal IDs that can be seen by issuing the [list customname](/bucardo/list_customname "wikilink") command.

### Internals

Removed custom names will cause a delete from the [bucardo.customname table](/bucardo.customname_table "wikilink").

### See also:

-   [add_customname](/Bucardo/add_customname "wikilink")
-   [list_customname](/Bucardo/list_customname "wikilink")
