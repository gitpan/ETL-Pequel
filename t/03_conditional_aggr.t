use Test;
use strict;
BEGIN { plan tests => 1; };

my $script='examples/conditional_aggr.pql';
system("perl examples/make_sample_data.pl 20 | scripts/pequel $script -silent > /dev/null 2> t/result");

open(RESULT, "t/result");
my $count=0;
while (<RESULT>) { chomp; s/\s+//g; $count++ if (length > 1); }
close(RESULT);
system("cat t/result");
unlink("t/result");
print "not ok 1" if ($count);
print "ok 1" if (!$count);
