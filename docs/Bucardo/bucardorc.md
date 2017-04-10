---
title: Bucardo bucardorc
permalink: /Bucardo/bucardorc/
---

Options for [bucardo_ctl](/bucardo_ctl "wikilink") can be specified either at the command line or inside configuration files. There are three files that are checked in order, and the first one that is found is used:

-   .bucardorc in the current directory
-   .bucardorc in the user's home directory
-   /etc/bucardorc

The format of each file is simply **name=value**, where name is any of the options passed to the bucardo_ctl program. Any command line options will override the options in the bucardorc file.

If the option **--no-bucardorc** is given to bucardo_ctl, none of the bucardorc files will be used.

You can also specify the location of a bucardorc file to use, by using **--bucardorc=filename**. If this option is used, all other default locations above will be ignored, and the program will error out if the file is not found.

[Category:Bucardo](/Category:Bucardo "wikilink")
