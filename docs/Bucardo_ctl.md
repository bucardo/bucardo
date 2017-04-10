---
title: Bucardo ctl
permalink: /Bucardo_ctl/
---

**bucardo_ctl** is the main interface to Bucardo - it is used to start, stop, and control Bucardo.

You can tell bucardo_ctl where to find the main [Bucardo database](/Bucardo_database "wikilink") by use of the following arguments:

` --dbport`
` --dbhost`
` --dbname`
` --dbuser`

Additional bucardo_ctl arguments include:

` --quiet=0`
` --verbose=0`
` --bcverbose=1`
` --sendmail=0`
` --extraname=''`
` --debugfilesep=0`
` --debugdir='.'`
` --debugname=''`
` --debugsyslog=1  Enables/Disables Syslog`
` --debugfile=1    Enables/Disables local log file ./log.bucardo`
` --cleandebugs=0`

Rather than enter those every time, you may place the arguments into a [bucardorc](/bucardorc "wikilink") file. All of the arguments below, except for "install", require that enough options exist to find the main Bucardo database.

### Installing and upgrading Bucardo

To install Bucardo for the first time, simply enter

` bucardo_ctl install`

See the [Bucardo installation](/Bucardo/Installation "wikilink") page for complete details.

To upgrade Bucardo, use:

` bucardo_ctl upgrade`

See the [Bucardo upgrade](/Bucardo/Upgrade "wikilink") page for complete details.

### Controlling Bucardo

#### Starting Bucardo

To start Bucardo, simply enter:

` bucardo_ctl start "Reason for starting"`

The reason is optional but recommended.

#### Stopping Bucardo

To stop Bucardo, simply enter:

` bucardo_ctl stop "Reason for stopping"`

Again, the reason is optional but recommended.

#### Restarting Bucardo

Restarting is just:

` bucardo_ctl restart "Reason for restart"`

#### Checking that Bucardo is alive

To send a "ping" to the [MCP](/MCP "wikilink") process of a running Bucardo, use:

` bucardo_ctl ping [timeout]`

If successful, an exit value of 0 will be returned. The string returned by this command is Nagios-friendly, and will start with either OK or CRITICAL. The optional timeout argument indicates how long to wait for a response before giving up and returning a critical failure. The default time is 15 seconds. To wait forever, enter a timeout of 0.

### Bucardo configuration

Bucardo stores important configuration variables in the database inside the bucardo_config table. See the [Bucardo configuration](/Bucardo/configuration "wikilink") page for a complete list.

#### Viewing configuration values

To view all of the configuration settings:

` bucardo_ctl show all`

To view one or more specific items, enter their names:

` bucardo_ctl show kick_sleep log_showline`

Note that names are actually regular expressions, so that entering:

` bucardo_ctl show kid`

will list all configuration parameters that have the letters 'kid' inside of them.

#### Changing configuration values

To change a configuration, use:

` bucardo_ctl set name=value`

For example, to change the syslog logging facility to LOG_LOCAL3:

` bucardo_ctl set syslog_facility=LOG_LOCAL3`

#### Reloading the configuration settings

To tell a running Bucardo to re-read the configuration table:

` bucardo_ctl reload_config`

### Controlling syncs

Bucardo works by running one or more replication events called [syncs](/sync "wikilink"). The main interface for controlling these is bucardo_ctl

#### Kicking a sync

Syncs are fired by changes to the underlying tables, or manually started by [kicking](/kick "wikilink") them. To kick a sync, use:

` bucardo_ctl kick `<syncname>` [timeout]`

The optional timeout argument tells how long bucardo_ctl will wait for a response from Bucardo indicating that the sync has finished. If no timeout argument is given, the program sends the kick signal and returns immediately. If a value of "0" is given, bucardo_ctl will wait indefinitely for the sync to finish, and also give a running tab of how long the sync has taken.

Multiple syncs arguements can be given. If not timeout is given, they will all be kicked at once. Otherwise, they will be kicked in the order given, each starting when the previous one has completed. For example, to kick the syncs "sales" and "marketing", while while waiting for each to finish, you could use:

` bucardo_ctl kick sales marketing 0`

#### Reloading a sync

To reload a sync:

` bucardo_ctl reload `<syncname>

One or more named syncs can be reloaded this way. Each will be reloaded in turn, and bucardo_ctl will let you know when each has been reloaded. When a sync is reloaded, the [MCP](/MCP "wikilink") process will stop the existing sync, reload information about the sync from the database, and start it up again. This is typically used when you want to make changes to an existing sync that is already running, e.g. the [onetimecopy](/onetimecopy "wikilink") attribute.

#### Activating a sync

To activate a sync that is not currently running, use:

` bucardo_ctl activate `<syncname>

#### Deactivating a sync

To deactivate a sync that is currently active and running, use:

` bucardo_ctl deactivate `<syncname>

### Viewing information

To view general status about the currently running Bucardo process, use:

` bucardo_ctl status`

This will list general information about each sync

#### Detailed sync information

` bucardo_ctl status `<syncname>

This will show detailed information about a specific sync, including the last time it successfully ran, the number of rows transferred, and the last time it failed.

#### Listing syncs

To get a list of all [syncs](/sync "wikilink"):

` bucardo_ctl list syncs`

This list will show the sync name, it's type ([fullcopy](/fullcopy "wikilink"), [pushdelta](/pushdelta "wikilink"), or [swap](/swap "wikilink")), the source [herd](/herd "wikilink"), the target database (or database group), and current status. For more details on a specific sync, use the 'status' command above.

#### Listing databases

To get a list of all known databases:

` bucardo_ctl list dbs`

This will show the name of the database (as used by Bucardo, not its actual name in Postgres), its status, and the connection string used by Bucardo to connect to it.

#### Listing database groups

To get a list of all known [database groups](/database_group "wikilink"):

` bucardo_ctl list dbgroups`

#### Listing tables

To list all known tables:

` bucardo_ctl list tables`

#### Listing sequences

To list all known sequences:

` bucardo_ctl list sequences`

#### Listing herds

To list all [herds](/herd "wikilink"):

` bucardo_ctl list herds`

To list only one or more specific herds, add their names:

` bucardo_ctl list herd `<herdname>

To get a list of all tables that belong to a herd, use the verbose argument:

` bucardo_ctl list herd `<herdname>` --verbose`

### Adding things

To add new items, the general syntax is:

` bucardo_ctl add `<thing>` `<name>` additional_information`

#### Adding a database

Bucardo needs to know how to connect to each database involved in replication. You can teach it about a new database by using:

` bucardo_ctl add db `<dbname>` [options]`

The "dbname" is the name of the database inside of Postgres. The other optional arguments are entered in the format name=value and can include:

-   name: the internal name used by Bucardo to refer to this database
-   port: the port this database runs on. Defaults to 5432.
-   host: the host this database is on. Defaults to no host (Unix socket)
-   user: the user to connect as. Defaults to 'bucardo'
-   pass: the password to connect with. Don't use this, use a .pgpass file instead!
-   conn: Any additional information add to the connection string, e.g. sslmode=require
-   sourcelimit: The maximum number of replication events that can run at one time using this database as a source. Defaults to 0 (no limit)
-   targetlimit: The maximum number of replication events that can run at one time using this database as a target. Defaults to 0 (no limit)
-   pgpass: Full path and filename of a [Bucardo/pgpass](/Bucardo/pgpass "wikilink") file to use for this connection

For example, to add three new databases on different hosts:

` bucardo_ctl add database sales name=sales_master host=int-db-sales1 `
` bucardo_ctl add database sales name=sales_slave1 host=int-db-sales2 `
` bucardo_ctl add database sales name=sales_slave2 host=int-db-sales3`

#### Adding a database group

Databases can be grouped together, so that one master can push to a group of slave databases rather than a single database. To create a new named group:

` bucardo_ctl add dbgroup [db db]`

An optional list of databases to add to this group can be given. For example:

` bucardo_ctl add dbgroup sales sales_slave1 sales_slave2`

#### Adding tables

Bucardo needs to know about all tables that might be used in replication. Adding a tables is simply:

` bucardo_ctl add table `<tablename>` db=dbname`

The tablename can be schema qualified, but does not have to be. The "dbname" refers to the internal name Bucardo uses to identify databases. The "db=dbname" can be left off if there is only one database in the db table. Note that you only need to add tables from the source database(s).

An easier way to add tables is to simply run:

` bucardo_ctl add all tables [db=dbname]`

This will not actually change these tables or replicate them, it will merely tell Bucardo about them. Thus, it is safe to run this command at any time.

#### Adding sequences

Bucardo can also replicate sequences that it knows about. To add a sequence:

` bucardo_ctl add sequence `<seqname>` db=dbname`

To add all sequences:

` bucardo_ctl add all sequences [db=dbname]`

All the notes that apply to 'add table' above apply here as well.

#### Adding a herd

A [herd](/herd "wikilink") is a named group of tables that are replicated together. To add a herd:

` bucardo_ctl add herd `<name>` [goat goat]`

The list of goats are tables or sequences that should be part of this herd.

#### Adding a sync

To add a sync:

` bucardo_ctl add sync `<name>` source=`<herdname>` type=`<synctype>` target`

The name is simply an internal name used by Bucardo. Keep it short but descriptive: it is used quite often in day to day use. The source is the name of the herd that we are replicating from. The type is one of [fullcopy](/fullcopy "wikilink"), [pushdelta](/pushdelta "wikilink"), or [swap](/swap "wikilink"). The target is either a database (targetdb=<dbname>) or a database group (targetgroup=<groupname>.

As a shortcut for creating new syncs, you can also give a comma-separated list of tables, like so:

` bucardo_ctl add sync abc source=db1 targetdb=db2 tables=sales,marketing,userdb`

This will create a herd of the same name as the sync if it does not already exist, add the tables to it, and then create the sync.

Other options that can be added to 'add sync', in the format name=value:

-   onetimecopy: set the [onetimecopy](/onetimecopy "wikilink") value for this sync
-   status: set the initial status for the sync. Defaults to 'active'
-   lifetime: set the [lifetime](/lifetime "wikilink") for this sync - how long to run before the sync is restarted
-   maxkicks: sets the [maxkicks](/maxkicks "wikilink") for this sync - how many times it runs before being restarted
-   makedelta: set the [makedelta](/makedelta "wikilink") value for this sync. Defaults to 0.

### Other actions

#### Sending a message to the log file

To write a custom message to the log file that a current Bucardo process is writing to, use:

` bucardo_ctl message "Your message here"`

The message will be written by the [MCP](/MCP "wikilink") process to the logs in the format:

` MESSAGE (date): string`

where date is the timestamp the message was added, and string was the message provided

[Category:Bucardo](/Category:Bucardo "wikilink")
