---
title: Bucardo table
permalink: /Bucardo/table/
---

To add a table to Bucardo, run:

` bucardo_ctl add table `*`tablename`*

where *tablename* can be of the form **mytable** or **myschema.mytable**.

To add all the tables in the database, run:

` bucardo_ctl add all tables`

Note that adding a table simply lets Bucardo know that the table is there, but it will not be replicated until you add it to a [sync](/sync "wikilink")

[Category:Bucardo](/Category:Bucardo "wikilink")
