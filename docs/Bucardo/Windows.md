---
title: Bucardo Windows
permalink: /Bucardo/Windows/
---

Bucardo will not work on Windows boxes, as it has some Unix-specific features. However, the only part of Bucardo that needs to run on a non-Windows box is the Perl daemon: the Bucardo database and all databases to be replicated can be running on Windows.

It would be nice to get Bucardo working at some point as a native Windows service. Some of the factors that are currently preventing it from running on Windows:

-   Use of fork() and setsid()
-   Heavy use of PIDs
-   Sys::Syslog module
