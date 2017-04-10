---
title: Bucardo removetable
permalink: /Bucardo/removetable/
---

To remove a table or a sequence from an existing sync:

` bucardo_ctl update sync `<syncname>` remove table foobar`
` `
` bucardo_ctl update sync `<syncname>` remove sequence foobar_seq`

This will not change any running syncs: to do that, you should run:

` bucardo_ctl reload `<syncname>

[Category:Bucardo](/Category:Bucardo "wikilink")
