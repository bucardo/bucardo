---
title: Bucardo Upgrade
permalink: /Bucardo/Upgrade/
---

Upgrading Bucardo
-----------------

To upgrade Bucardo, install the new Bucardo file by downloading the [latest version](/Bucardo#Obtaining_Bucardo "wikilink"), and then running:

` perl Makefile.PL`
` make`
` make install`

Then upgrade your existing Bucardo database by running:

` bucardo_ctl upgrade`

This will modify your existing Bucardo schema as needed. You should also validate all your syncs by running:

` bucardo_ctl validate all`

[Category:Bucardo](/Category:Bucardo "wikilink")
