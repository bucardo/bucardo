## ctest1.pl - a conflict handler for Bucardo
use strict;
use warnings;

my $info = shift;

## If this table is named 'work', do nothing
if ($info->{tablename} eq 'work') {
    $info->{skip} = 1;
}
else {
    ## Winning databases, in order
    $info->{tablewinner} = 'B A C';
}

return;
