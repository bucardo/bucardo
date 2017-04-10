---
title: Bucardo add database
permalink: /Bucardo/add_database/
---

__NOTOC__

The **add database** command is used to teach Bucardo about a database that will be involved in replication. It is usually the first step performed after the initial [install](/Bucardo/install "wikilink").

Example:

` bucardo add database A host=example.com dbname=sales`

This will creates a new database entry named **A** which resides on host **example.com** and is named **sales**. Note that "sales" is the actual database name that Bucardo will connect to, while "A" is how Bucardo refers to this specific database, for example when calling [add dbgroup](/add_dbgroup "wikilink"). A connection to the database will be attempted right away: see the Verification section below.

Usage:

` bucardo add database `<name>` `<dbname=value>` [optional arguments]`

The alternate form **add db** is also accepted.

### Required arguments:

-   dbname (can also use 'db')
    -   The database to connect to.

### Optional arguments:

-   dbtype (can also use 'type')
    -   The type of database we are connecting to. Defaults to 'postgres'. Other options are drizzle, mongo, mysql, oracle, redis, and sqlite.
-   dbuser (can also use 'username' or 'user')
    -   The username to connect as. Defaults to 'bucardo'.
-   dbpass (can also use 'password')
    -   The password to connect with. Please avoid if possible by using things such as [Postgres' pgpass file](http://www.postgresql.org/docs/current/static/libpq-pgpass.html).
-   dbport (can also use 'port')
    -   The database port to connect to.
-   dbhost (can also use 'host')
    -   The database host to connect to.
-   dbconn (can also use 'conn')
    -   Additional connection parameters. For example, to specify a SID when using an Oracle target:

` bucardo add db foobar type=oracle host=example.com user=scott conn=sid=abc`

-   status
    -   Defaults to 'active'; the only other choice is 'inactive'
-   dbgroup
    -   Which internal [database group](/database_group "wikilink") to put this database into. Will be created if needed.
-   addalltables
    -   Automatically add all tables inside of this database. For finer control, see [add_table](/add_table "wikilink")
-   addallsequences
    -   Automatically add all sequences inside of this database
-   server_side_prepares (can also use 'ssp')
    -   For Postgres databases only, determines if we should use server-side prepares. The default is 1 (on). This may need to be 0 if you are using a connection pooler such as PgBouncer which may get confused by usage of server-side prepares.
-   dbservice
    -   For Postgres databases, the "service name" to use

### Verification

Before a new database is added, a simple connection test if performed to make sure everything is working. Thus, you will need to have the database up and running, as well as have installed any extra Perl modules needed to reach the database. The additional Perl modules you need depend on the database type:

-   Postgres: DBD::Pg
-   Drizzle: DBD::Drizzle
-   Mongo: MongoDB
-   Oracle: DBD::Oracle
-   Redis: Redis
-   SQLite: DBD::SQLite

### Internals

New databases cause an insert to the [bucardo.db table](/bucardo.db_table "wikilink"). A new database group will cause an insert to the [bucardo.dbgroup table](/bucardo.dbgroup_table "wikilink"). Databases added to that group will cause an insert to the [bucardo.dbmap table](/bucardo.dbmap_table "wikilink"). Adding tables and sequences will cause inserts to the [bucardo.goat table](/bucardo.goat_table "wikilink").

### See also:

-   [list_database](/Bucardo/list_database "wikilink")
-   [update_database](/Bucardo/update_database "wikilink")
-   [remove_database](/Bucardo/remove_database "wikilink")
