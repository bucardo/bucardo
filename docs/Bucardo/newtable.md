---
title: Bucardo newtable
permalink: /Bucardo/newtable/
---

Adding a new table to an existing sync in Bucardo
-------------------------------------------------

Adding a new table to an existing [sync](/sync "wikilink") is a fairly easy process. This process that Bucardo already knows about the table: if not, just run:

` bucardo_ctl add all tables`

The next step is:

` bucardo_ctl update sync `<syncname>` add table `<tab1>` `<tab2>` ...`

This adds one or more tables to an existing sync, by adding them to the source [herd](/herd "wikilink") for this sync. If the tables are already in the sync, no changes are made.

` bucardo_ctl validate `<syncname>

This tells Bucardo to run the validate_sync() function, which makes any changes needed on the remote databases (such as adding triggers).

` bucardo_ctl update sync onetimecopy=2`

This instructs the sync to enter [onetimecopy](/onetimecopy "wikilink") mode. This step is not needed if the sync is [fullcopy](/fullcopy "wikilink"). The value of 2 means that only empty tables will be copied over. We do this to ensure that all rows in the new table are copied from the master to the slave. Once they are all copied, only the differences are copied from that point forward.

Optionally, you can ask Bucardo to defer index processing until the end of the COPY:

` bucardo_ctl update sync onetimecopy=2 rebuild_index=1`

This modifies the system tables to "turn off" all indexes on the table before it copies the data in, and then runs a REINDEX immediately afterward. For large tables, this can be a significant speedup.

` bucardo_ctl reload `<syncname>

This lets the Bucardo daemon know that the sync has changed, and to stop it from running, read in the new information from the database, and start it up again.

After that is done, you can tail the [log.bucardo](/log.bucardo "wikilink") file and watch the changes being made. For large tables, you should see it stop for a while on a line that looks like this:

` KID Running on test_target: COPY public.mytable FROM STDIN`

Once finished, you can check on the sync's status. Right after the onetimecopy, the number of inserts for the last run will be very high, with no updates or deletes.

` bucardo_ctl status `<syncname>

[Category:Bucardo](/Category:Bucardo "wikilink")
