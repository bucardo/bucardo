---
title: Bucardo pgbench example
permalink: /Bucardo/pgbench_example/
---

This page describes the steps needed to replicate a sample database, created by the pgbench utility, with Bucardo. This will demonstrate simply master to slave behavior, using the [pushdelta](/pushdelta "wikilink") and [fullcopy](/fullcopy "wikilink") sync types.

Install Bucardo
---------------

The first step is to install Bucardo. Detailed instructions can be found on the [installation page](/Bucardo/install "wikilink"), but the quick steps are:

### Install Perl modules

Bucardo requires the following Perl modules to be installed:

-   DBD::Pg
-   DBIx::Safe

### Download and unpack Bucardo

The latest version of Bucardo can be found at [the download page](/Bucardo/download "wikilink"). Alternatively, you can pull the development version from git by doing:

` git clone `[`git://bucardo.org/bucardo.git`](git://bucardo.org/bucardo.git)

Either way, you should end up in a bucardo directory, and ready for the next step.

### make and install

Run the following commands:

` perl Makefile.PL`
` make`

The following step is optional but recommended:

` make test`

Finally, install as a user with appropriate rights. One way to do this is:

` sudo make install`

You should now have a global [bucardo_ctl](/bucardo_ctl "wikilink") file available. Test that you can run it and that you are using the correct version:

` bucardo_ctl --version`

### Create and populate the database

Bucardo needs a central database. The install option of bucardo_ctl will create and install this database for you. All you need to provide is the location of a Postgres instance you want to use, and a valid PID directory. For this example, we'll use the default values of no host, port 5432, and a user named 'Postgres'. We'll use the **/tmp/bucardo** directory as our piddir value.

` mkdir /tmp/bucardo`
` bucardo_ctl install --piddir=/tmp/bucardo`

You will need to enter a "P" to tell it to proceed. If all goes well, you should see a message like this:

`$ bucardo_ctl install --piddir=/tmp/bucardo`
`This will install the bucardo database into an existing Postgres cluster.`
`Postgres must have been compiled with Perl support,`
`and you must connect as a superuser`
`We will create a new superuser named 'bucardo',`
`and make it the owner of a new database named 'bucardo'`
`Current connection settings:`
`1. Host:          `<none>
`2. Port:          5432`
`3. User:          postgres`
`4. PID directory: /tmp/bucardo`
`Enter a number to change it, P to proceed, or Q to quit: p`
`Postgres version is: 8.4`
`Attempting to create and populate the bucardo database and schema`
`Database creation is complete`
`Connecting to database 'bucardo' as user 'bucardo'`
`Updated configuration setting "piddir"`
`Installation is now complete.`
`If you see any unexpected errors above, please report them to bucardo-general@bucardo.org`
`You should probably check over the configuration variables next, by running:`
`bucardo_ctl show all`
`Change any setting by using: bucardo_ctl set foo=bar`

That's it! Time to setup our test databases. NOTE: In this example the source/master and target/slave databases reside within a single instance of he Postgres server, and this installation step is normally only required for the source/master node. In the real world where source/master and target/slave are hosted in separate Postgres servers, each slave node will need to have the role 'bucardo' manually created before proceeding to the next step.

Setup the pgbench databases
---------------------------

The **pgbench** utility that comes with Postgres can be used to create some simple test tables in an existing database. Let's create two databases, test1 (the master), and test2 (the slave).

` createdb test1`
` createdb test2`

Next, we'll install the pgbench files on each.

` pgbench -i test1`
` pgbench -i test2`

Now that we have some data, let's get Bucardo to replicate it.

Add the databases
-----------------

Bucardo needs to know about each database it needs to talk to. The [bucardo_ctl](/bucardo_ctl "wikilink") program does this with the [add db](/add_db "wikilink") option.

` bucardo_ctl add db test1`
` bucardo_ctl add db test2`

We've kept it simple for this example, but you generally will end up replicating databases with the same name, and thus should add an extra internal database name. Since we did not provide one, they default to the actual database names.

Add the tables
--------------

Bucardo also needs to know about any tables that it may be called on to replicate. Adding tables by the [add table](/add_table "wikilink") command does not actually start replicating them. In this case, we're going to use the handy **add all tables** feature. Tables are grouped together inside of Bucardo into [herds](/herd "wikilink"), so we'll also place the newly added tables into a named herd. Finally, the history table has no primary key or unique index, so we cannot replicate it by using the [pushdelta](/pushdelta "wikilink") method, so we're going to exclude it from the alpha herd, using the [-T](/-T "wikilink") switch, and add it in the next setup with the [-t](/-t "wikilink") switch.

` $ bucardo_ctl add all tables db=test1 -T history --herd=alpha --verbose`
` New tables:`
`   public.accounts`
`   public.branches`
`   public.tellers`
` New tables added: 3`
` Already added: 0`
` $ bucardo_ctl add all tables db=test1 -t history --herd=beta --verbose`
` New tables:`
`   public.history`
` New tables added: 1`
` Already added: 0`

Add the syncs
-------------

A [sync](/sync "wikilink") is a named replication event. Each sync has a source herd; because we created two herds above, we'll go ahead and create two syncs as well. One will be a [pushdelta](/pushdelta "wikilink") sync, the other will be a [fullcopy](/fullcopy "wikilink") sync.

` $ bucardo_ctl add sync benchdelta source=alpha targetdb=test2 type=pushdelta`
` Added sync "benchdelta"`

` $ bucardo_ctl add sync benchcopy source=beta targetdb=test2 type=fullcopy`
` Added sync "benchcopy"`

We are ready to kick off Bucardo at this point. Before we do, let's use the **list** options to bucardo_ctl to check everything out.

` $ bucardo_ctl list herds`
` Herd: alpha Members: public.branches, public.tellers, public.accounts`
`   Used in syncs: benchdelta`
` Herd: beta  Members: public.history`
`   Used in syncs: benchcopy`

` $ bucardo_ctl list syncs`
` Sync: benchcopy   (fullcopy )  beta  =>  test2  (Active)`
` Sync: benchdelta  (pushdelta)  alpha =>  test2  (Active)`

` $ bucardo_ctl list dbs`
` Database: test1  Status: active  Conn: psql -p 5432 -U bucardo -d test1`
` Database: test2  Status: active  Conn: psql -p 5432 -U bucardo -d test2`

` $ bucardo_ctl list tables`
` Table: public.accounts  DB: test1  PK: aid (int4)`
` Table: public.branches  DB: test1  PK: bid (int4)`
` Table: public.history   DB: test1  PK: none`
` Table: public.tellers   DB: test1  PK: tid (int4)`

Start Bucardo
-------------

The final step is to fire it up:

` bucardo_ctl start`

After a few seconds, the prompt will return. There will be a log file in the current directory called **log.bucardo** that you can look through. To disable the logfile and just rely on syslog use the [--debugfile=0](/--debugfile=0 "wikilink") argument. You can also verify that the Bucardo daemons are running by doing a:

` ps -Afw | grep -i Bucardo`

Test Replication
----------------

To verify that things are working properly, let's get some baseline counts:

` $ psql -d test1 -At -c 'select count(*) from tellers'`
` 10`

` $ psql -d test2 -At -c 'select count(*) from tellers'`
` 10`

` $ psql -d test1 -c 'select * from tellers where tid = 1'`
`  tid | bid | tbalance | filler `
` -----+-----+----------+--------`
`    1 |   1 |        0 |`
` (1 row)`

` $ psql -d test2 -c 'select * from tellers where tid = 1'`
`  tid | bid | tbalance | filler`
` -----+-----+----------+--------`
`    1 |   1 |        0 |`
` (1 row)`

Now let's make changes to that record, and verify that it gets propagated to the slave (test2)

` $ psql -d test1 -c 'update tellers set bid=999 where tid = 1'`
` UPDATE 1`

` $ psql -d test2 -c 'select * from tellers where tid = 1'`
`  tid | bid | tbalance | filler`
` -----+-----+----------+--------`
`    1 | 999 |        0 |`

How about the history table, which has not primary key? We cannot track row by row changes, and don't want to copy the whole thing every time the table changes, so we've got to [kick](/kick "wikilink") that sync manually when we want to change it:

` $ psql -d -At test1 -c 'select count(*) from history'`
` 0`
` $ psql -d -At test2 -c 'select count(*) from history'`
` 0`

` $ pgbench -t3 test1`

` $ psql -At -d test1 -c 'select count(*) from history'`
` 3`
` $ psql -At -d test2 -c 'select count(*) from history'`
` 3`

` $ bucardo_ctl kick benchcopy`

` $ psql -At -d test1 -c 'select count(*) from history'`
` 3`
` $ psql -At -d test2 -c 'select count(*) from history'`
` 3`

This ends the demonstration. Feel free to play around more. To stop Bucardo when done, just issue:

` bucardo_ctl stop`

As you experiment, you might also want to look at the syncs in more detail with:

` bucardo_ctl status`
` bucardo_ctl status benchdelta`
` bucardo_ctl status benchcopy`

__NOTOC__ [Category:Bucardo](/Category:Bucardo "wikilink")
