use Test;
use strict;
BEGIN { plan tests => 1; };

my $script='group_by_derived.pql';
system("perl ./examples/make_sample_data.pl 20 | perl ./scripts/pequel ./examples/$script -silent -noheader -prefix ./examples > ./t/$script.OUT 2> ./t/$script.ERR");
my $count=0;
open(RESULT, "./t/$script.ERR");
while (<RESULT>) { chomp; s/\s+//g; $count++ if (length > 1); }
close(RESULT);
open(ERR, "./t/$script.ERR"); $/=undef; my $err = <ERR>; close(ERR);
warn ("WARNING:$err") if ($count);
unlink("./t/$script.ERR");
warn ("WARNING:no output from $script") if (!$count && !-s "./t/$script.OUT");
print "@{[ $count || !-s qq{./t/$script.OUT} ? 'not ' : '' ]}ok 1";
unlink("./t/$script.OUT");
