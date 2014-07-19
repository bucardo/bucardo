## ctest2.pl - a conflict handler for Bucardo
use strict;
use warnings;

my $info = shift;

## Walk through all conflicted rows and set a winning list
for my $row (keys %{ $info->{conflicts}}) {
    $info->{conflicts}{$row} = 'B';
}
## We don't want any other customcodes to fire: we have handled this!
$info->{lastcode} = 1;

return;
