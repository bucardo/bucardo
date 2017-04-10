---
title: Fullcopy
permalink: /Fullcopy/
---

**fullcopy** is a type of Bucardo [sync](/sync "wikilink") that copies an entire table from one database (the master) to one or more slave databases. Any rows on the slaves are removed first, and the table is populated using the COPY command. This is the only sync that can be used for tables that have no primary key and no unique index. Because it copies the entire table every time, it is not efficient and should not be run often for large tables. It is far better in most cases to add a unique index to the table and then use a [pushdelta](/pushdelta "wikilink") sync to keep things up to date.

The contents of the table can be modified as they are copied over, by setting the [customselect](/customselect "wikilink") field of the sync. This should be a SELECT statement that returns some subset of the original table's columns. It must contain at least the columns used in the primary key or unique index.

[Category:Bucardo](/Category:Bucardo "wikilink")
