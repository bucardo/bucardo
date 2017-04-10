---
title: Bucardo Sequences
permalink: /Bucardo/Sequences/
---

Postgres **sequences** can be replicated.

Pushdelta syncs and sequences
-----------------------------

In most cases, you will not need to replicate sequences, unless your slaves are not read only, or if you want your slaves to be ready for [failover](/failover "wikilink"). If you do decide to replicate them, just treat them like you would a table - add them to the [goat table](/goat_table "wikilink"), gather them into [herds](/herds "wikilink"), and associate them with one or more [syncs](/syncs "wikilink").

Swap syncs and sequences
------------------------

If you are using a [swap sync](/swap_sync "wikilink"), the best practice is to \*not\* replicate sequences, but to make sure that they are different on both sides, such that an insert on database A will never conflict with an insert on database B. There are three general ways to do this:

1.  Use interleaving sequences. On database A, define the sequence as START WITH 1 INCREMENT BY 2. On database B, define the sequence as START WITH 2 INCREMENT BY 2. Thus, the two sequences will never have the same value.
2.  Use different ranges. For example, database A would use a sequence of START WITH 1, while database B uses START WITH 100000000. This is not foolproof, as A can eventually catch up with B, although you can define A as MAXVALUE 99999999.
3.  Use a common sequence. This relies on one or both of the databases using a function that makes a call to an external sequence.

[Category:Bucardo](/Category:Bucardo "wikilink")
