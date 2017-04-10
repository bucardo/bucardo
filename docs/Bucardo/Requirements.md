---
title: Bucardo Requirements
permalink: /Bucardo/Requirements/
---

[Bucardo](/Bucardo "wikilink") has a few requirements before it can run.

-   Any databases being replicated must be version 8.0 or greater.
-   Any "source" databases used in [pushdelta](/pushdelta "wikilink") or [swap](/swap "wikilink") syncs must have the [plpgsql](/plpgsql "wikilink") language available.
-   The database Bucardo itself uses must have plpgsql and plperlu available

Bucardo itself is a Perl daemon that communicates with a master Bucardo database and the databases that are being replicated. The box that it lives on must have:

-   Perl, at least version 5.8.3
-   DBI. at least version 1.51
-   DBD::Pg, at least version 2.0.0
-   Sys::Hostname, at least version 1.11
-   Sys::Syslog, at least version 0.13
-   [DBIx::Safe](/DBIx::Safe "wikilink"), at least version 1.2.4

In addition, the Bucardo daemon will not currently work on [Windows](/Bucardo/Windows "wikilink") boxes. However, you can have a Bucardo daemon on a Linux box that replicates Postgres between two Window boxes.

[Category:Bucardo](/Category:Bucardo "wikilink")
