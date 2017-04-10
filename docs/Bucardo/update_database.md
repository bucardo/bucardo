---
title: Bucardo update database
permalink: /Bucardo/update_database/
---

__NOTOC__

The **update database** command is used to change an existing database entry in the Bucardo database.

Example:

` bucardo update database foo server_side_prepares=0 port=5433`

Updates the database **foo**: sets the server_side_prepares option off, and changes the connection port to 5433.

Usage:

` bucardo update database name [setting=value setting2=value2 ...]`

The alternate form **update db** is also accepted. To see a list of items that can be changed, run:

` bucardo list database name -vv`

Note that not all fields can be changed (e.g. cdate)

### Internals

Changes will update the [bucardo.db table](/bucardo.db_table "wikilink").

### See also:

-   [add_database](/Bucardo/add_database "wikilink")
-   [list_database](/Bucardo/list_database "wikilink")
-   [remove_database](/Bucardo/remove_database "wikilink")
