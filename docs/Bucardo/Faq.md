---
title: Bucardo FAQ
permalink: /Bucardo/FAQ/
---

Bucardo Frequently Asked Questions
----------------------------------

If you have a question that is not answered here, try checking the main [Bucardo](/Bucardo "wikilink") page and see the links and community information there.

### What is Bucardo?

Bucardo is a replication program for two or more Postgres databases. Specifically, it is an asynchronous, multi-master, master-slave table-based replication system. It is written in Perl, and uses extensive use of triggers, PL/PgSQL, and PL/PerlU.

### What are the requirements for use?

Bucardo is a Perl script that requires the following modules to be installed before it can be run:

-   DBI (at least version 1.51)
-   DBD::Pg (2.0+)
-   Sys::Hostname (1.11+)
-   Sys::Syslog (0.13+)
-   DBIx::Safe (1.2.4+)

Bucardo requires a database to install the main bucardo schema on. This database must be Postgres version 8.1 or higher, and must have both the languages Pl/Pgsql and Pl/perlU available. In addition, the install script requires installation as a superuser: creating a new user named 'bucardo' for this purpose is highly recommended.

Databases involved in replication must be Postgres version 8.1 or higher. The language Pl/pgsql must be available as well, unless the database is only used as a target for "fullcopy" syncs, in which case 8.1 is the only requirement.

Bucardo requires a Unix-like system. Currently, it has only been tested on Linux variants, but it should work on BSD, Solaris, and other similar systems. Bucardo will not currently work on Windows, but the ability to do so is probably not that difficult to achieve at this point so let us know if you'd like to help with that.

### Can Bucardo do master/slave replication?

Absolutely. While Bucardo can do master/master replication, many people use it only for master/slave replication (one master database sending changes to one or more slave databases).

### Can Bucardo replicate between more than two masters?

Yes, you can have as many sources (masters) and targets (slaves) as you like.

### Does Bucardo need to run on the database that is being replicated?

The Bucardo program itself can run anywhere, and does not have to be on any of the servers involved in replication. The primary advantage to having it run on the same server is reduced network time. A disadvantage is putting all your eggs in one basket.

### Why does Bucardo give me warnings on startup about mismatched sequences?

This is a consequence of the bucardo user having different search paths on different Postgres servers. This has been fixed in version 4.5.0.

### How fast will replication occur?

There is no simple answer to this question, as it depends on how many tables you are replicating in one [sync](/sync "wikilink"), how fast your network is, how busy your database is, etc. As a general rule of thumb, however, changes make it to the other databases within a matter of one or two seconds.

### Can Bucardo replicate DDL?

No, Bucardo relies on triggers, and Postgres does not yet provide DDL triggers or triggers on its system tables.

### What does "Could not add to q" mean

This message looks bad, but is in fact innocuous. It simply means that Bucardo is getting signaled to sync more quickly than it can complete a sync. That's quite normal, and Bucardo will catch up.

[Category:Bucardo](/Category:Bucardo "wikilink")
