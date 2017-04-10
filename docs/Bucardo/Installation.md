---
title: Bucardo Installation
permalink: /Bucardo/Installation/
---

[Bucardo](/Bucardo "wikilink") Installation

This page describes how to install Bucardo. If you want a packaged version, skip to **Installation From Packages**, FreeBSD users can skip to **[Installation From FreeBSD Ports](/Bucardo/Installation#Installation_From_FreeBSD_Ports "wikilink")**. For the impatient, here's the short version:

Quick version
-------------

` * Install DBIx::Safe and boolean`
` * Download and untar the latest Bucardo`
` * perl Makefile.PL && make && sudo make install`
` * bucardo install`

The rest of this document goes into details about the installation process.

Install DBIx::Safe
------------------

Bucardo requires the Perl module DBIx::Safe to be installed. Some distributions have it available as a package, in which case the installation is as simple as:

` yum install perl-DBIx-Safe`
` # or`
` apt-get install libdbix-safe-perl`

To install it manually, grab the [latest DBIx::Safe tarball](http://bucardo.org/downloads/dbix_safe.tar.gz), then unpack the tarball and install as a normal Perl module:

` tar xvfz dbix_safe.tar.gz`
` cd DBIx-Safe-1.2.5`
` perl Makefile.PL`
` make`
` make test`
` sudo make install`

Other Perl prerequisites that may or may not be installed already on your system include:

` DBI`
` DBD::Pg`
` Test::Simple`
` boolean`

Download and unpack the Bucardo tarball
---------------------------------------

The latest version of Bucardo can be found on [the Bucardo download page](/Bucardo#Obtaining_Bucardo "wikilink"). Untar it and switch to the directory:

` tar xvfz Bucardo-5.4.1.tar.gz`
` cd Bucardo-5.4.1`

Install the Bucardo software
----------------------------

Once in the directory:

` perl Makefile.PL`
` make`
` sudo make install`

The last step (make install) needs to be run as an account that can install to system directories.

If you want to install all the files to a single directory, for testing purposes, you can define the environment variable `INSTALL_BUCARDODIR` before running `perl Makefile.PL`. Thus, the first step would become:

` INSTALL_BUCARDODIR=/tmp/bucardotest perl Makefile.PL`

Create the Bucardo database
---------------------------

Bucardo needs to be installed into a database. This database must have the pl/perlu language available. For systems installed via packaging, installing Pl/PerlU may be as simple as:

`yum install postgresql-plperl`
`# or`
`apt-get install postgresql-plperl-9.0`

Once you've decided where you want the Bucardo database to be installed, run:

`bucardo install`

You will have an opportunity to change the default parameters:

` This will install the bucardo database into an existing Postgres cluster.`
` Postgres must have been compiled with Perl support,`
` and you must connect as a superuser`
` `
` We will create a new superuser named 'bucardo',`
` and make it the owner of a new database named 'bucardo'`
` `
` Current connection settings:`
` 1. Host:          `<none>
` 2. Port:          5432`
` 3. User:          postgres`
` 4. Database:      postgres`
` 5. PID directory: /var/run/bucardo`

Note that the installation will create the bucardo Postgres account without a password, and then attempt to connect to it to continue the installation. However this may fail depending on your pg_hba.conf settings. Some workarounds include:

:\* The stock pg_hba.conf includes "trust" entries for local connections. Use a trust method connection (perhaps temporarily) to allow it to connect as the bucardo user.

:\* For pg_hba.conf's "md5" method, create the bucardo Postgres user ahead of time with a password and set that in the .pgpass file of the user running the installation.

:\* Use the "ident" method in pg_hba.conf, and create both a bucardo system account and a bucardo Postgres account. The ident method is enabled by default in Debian-based packages, and will allow the installation to log in if run under the bucardo system account.

If all goes well at this point, Bucardo is installed. NOTE: Although you do not need to run the install script on each slave node in your cluster, you will need to manually create the 'bucardo' role on each slave node before proceeding with the installation and configuration.

TIP: If you run into errors during install or in subsequent steps, the best thing to do is to completely remove the bucardo-owned objects and start fresh with the 'bucardo install' step. This includes doing a cascaded drop of the 'bucardo' schema and the 'bucardo' role. This should completely remove any traces of bucardo and allow you to run the installation step cleanly again.

Installation From Packages
--------------------------

Bucardo has been packaged in both rpm and deb formats. The Fedora Project has built rpm's and has made them available through their EPEL repo. Packages are also available in Debian's "testing" distribution, and are planned for inclusion in Debian's "squeeze" release and Ubuntu's "Maverick" release.

Installation From FreeBSD Ports
-------------------------------

Bucardo is available in the FreeBSD ports system. The FreeBSD project has included a port for compiling and installed Bucardo from source, and also provided packages in the old **pkg_\*** tools format as well as the new **PKGNG** packaging system. To install from source on FreeBSD:

`cd /usr/ports/databases/p5-Bucardo`
`sudo make install`

To install prebuilt packages using the new packaging system on FreeBSD:

`sudo pkg install databases/p5-Bucardo`

The installation process will automatically install all the dependancies such as **DBIx::Safe**

Configuring Replication
-----------------------

Add databases:

`bucardo add database `<dbname>

Add tables and sequences:

` bucardo add all tables`
` bucardo add all sequences`

Add [syncs](/sync "wikilink"):

` bucardo add sync `<syncname>` type=`<synctype>` source=`<db>` targetdb=`<db>` tables=tab1,tab2,...`

Start Bucardo:

` bucardo start`

__NOTOC__ [Category:Bucardo](/Category:Bucardo "wikilink")
