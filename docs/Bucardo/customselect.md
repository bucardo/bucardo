---
title: Bucardo customselect
permalink: /Bucardo/customselect/
---

CustomSelect / CustomCols
-------------------------

The **customselect** feature allows you to control what rows & columns get synchronized from one database to another. This functionality works both on the fly (when CRUD invokes a trigger), and when doing a [fullcopy](/Bucardo/fullcopy "wikilink").

The command takes a SELECT statement, and is helpful in the following scenarios:

-   Tables differ in structure and/or field names between the source and destination databases
-   You want to join different tables from the source, into a single table in the destination
-   You want to apply a filter on what actually gets synchronized (by adding a WHERE clause)
-   Any combination of the above!

The test file **t/20-postgres.t** has a working example (grep for 'customcols').

Example Usage
-------------

The basic idea is to replace the default 'SELECT \* FROM table' with a modified select list, by calling:

`$ bucardo add customcols mytable "SELECT a,b, foo AS bar"`

You can optionally constrain to a certain sync:

`$ bucardo add customcols mytable "SELECT a,b, foo AS bar" sync=mysync`

And/or to a certain database (which is probably what you want to do here):

`$ bucardo add customcols mytable "SELECT a,b, foo AS bar" db=mymongodb`

Notes
-----

1.  You can't sync from a view, you can only sync from tables
2.  The same view definition can be adapted to become the \`customcols\` query
