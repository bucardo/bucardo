---
title: Onetimecopy
permalink: /Onetimecopy/
---

The **onetimecopy** mode of a sync instructs it to temporarily switch from a [pushdelta](/pushdelta "wikilink") mode to a [fullcopy](/fullcopy "wikilink") mode. In other words, it will unconditionally copy over all rows for each table in the sync from the master to the slaves. When finished, Bucardo will automatically set this value back to 0.

If this value is set to **2**, tables will only be copied if:

-   there is at least one row on the source
-   there are no rows on the target

This is handy for adding new tables to a [sync](/sync "wikilink"), in which the target table is empty.

If the target table is \*not\* empty, but is not identical to the source, the best way to get them in sync is to truncate or delete all the rows from the slave, and then set onetimecopy to a value of 2. (While you can do the same thing by simply setting it to 1, that will also copy over all the rows for every other table in the sync).

To change the onetimecopy value of a sync, just run:

` bucardo_ctl update sync `<syncname>` onetimecopy=2`

A onetimecopy event will appear as a [fullcopy](/fullcopy "wikilink") sync in the [web stats](/web_stats "wikilink") page.

[Category:Bucardo](/Category:Bucardo "wikilink")
