---
title: Bucardo update dbgroup
permalink: /Bucardo/update_dbgroup/
---

__NOTOC__

The **update dbgroup** command is used to change an existing Bucardo database group.

Example:

` bucardo update dbgroup foobar s1 s2 s3:source`

Updates the database group **foobar**, setting it to contain the servers s1, s3, and s3. Both s1 and s3 will be source databases, and s2 will be a target.

This command should be used to add a new database to an existing sync. If the bucardo schema is still not created on the new db the sync needs then to be validated.

Example:

`  bucardo add db s3...`
`  bucardo update dbgroup `<dbgroupname>` s1 s2 s3:source`
`  bucardo validate sync `<syncname>

Usage:

` bucardo update dbgroup name [servers] [name=newname]`

Use the name= form to change the name of an existing database group. Otherwise, provide a list of all servers in this group, and the existing servers will be overwritten.

### Internals

Changes will update the [bucardo.dbmap table](/bucardo.dbmap_table "wikilink").

### See also:

-   [add_dbgroup](/Bucardo/add_dbgroup "wikilink")
-   [list_dbgroup](/Bucardo/list_dbgroup "wikilink")
-   [remove_dbgroup](/Bucardo/remove_dbgroup "wikilink")
