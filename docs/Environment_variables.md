---
title: Environment variables
permalink: /Environment_variables/
---

All the environment variables used by [Bucardo](/Bucardo "wikilink")

Items on initial setup via Makefile.PL
--------------------------------------

-   INSTALL_BUCARDODIR
    -   A directory in which everything will be installed. Useful for testing. Sample usage: **INSTALL_BUCARDODIR=. perl Makefile.PL**

Items used by the [bucardo script](/bucardo_script "wikilink")
--------------------------------------------------------------

-   BUCARDO_DATEFORMAT
    -   How timestamps are displayed, e.g. **bucardo status**. Defaults to **Mon DD, YYYY HH24:MI:SS**
-   BUCARDO_SHORTDATEFORMAT
    -   How the time part of timestamps are displayed. Defaults to **HH24:MI:SS**
-   HOME
    -   Used to help locate a valid [.bucardorc](/.bucardorc "wikilink") file
-   BUCARDO_CONFIRM
    -   If set, forces a confirmation prompt after all actions
-   BUCARDO_DEBUG
    -   Prints varying amounts of output to stderr when set. Defaults to 0, verbosity increases as the value does.

Items used when installing Bucardo via the [bucardo script](/bucardo_script "wikilink")
---------------------------------------------------------------------------------------

-   HOME
    -   Where the .pgpass file is written (if the bucardo superuser is created)
-   PGHOST
    -   The host Postgres is listening on. Defaults to none.
-   PGORT
    -   The port Postgres is listening on. Defaults to **5432**
-   DBUSER
    -   The name of the Postgres user to connect as. Defaults to **postgres**
-   DBNAME
    -   The name of the database to connect to. Defaults to **postgres**
-   PGBINDIR
    -   Where the Postgres binaries (esp. psql) are located. If not set, they must be in your path.
-   USER
    -   If connection as user 'postgres' fails during install, we will try the value of USER.

Items used by the Bucardo daemon
--------------------------------

-   BUCARDO_DRYRUN
    -   If set, will attempt to rollback all changes before the final commits in a sync.
-   BUCARDO_EMAIL_DEBUG_FILE
    -   Location of a file containing a copy of outgoing emails. Overrides the configuration value **email_debug_file**
-   LC_ALL, LC_MESSAGES, LANG
    -   Consulted in the order given to try and determine the correct language to use

Items used by the testing suite
-------------------------------

-   BUCARDO_DEBUG
    -   Integer. Various levels of debug output
-   RELEASE_TESTING
    -   If set, all the housekeeping/maintainer tests will be run. Should not be set by anyone but maintainers.
-   PGSERVICEFILE
    -   Location of the Postgres service file - used by some tests
-   BUCARDO_TESTBAIL
    -   When set, all tests are stopped when the first failing test is encountered.
-   USER
    -   Used to set temporary directory and file names
-   PGBINDIR
    -   Location of Postgres binaries
-   PGBINDIRA .. PGBINDIRZ
    -   Allows per-database setting of binaries based on the test databases A,B,C,D, etc. Useful for testing across Postgres versions.
