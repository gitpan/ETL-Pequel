use Test;
use strict;
BEGIN { plan tests => 1; };

my $script='copy_output.pql';
system("./scripts/pequel ./examples/$script -silent -noheader -prefix ./examples > ./t/$script.OUT 2> ./t/$script.ERR");
my $count=0;
open(RESULT, "./t/$script.ERR");
while (<RESULT>) { chomp; s/\s+//g; $count++ if (length > 1); }
close(RESULT);
warn ("WARNING:" . `cat ./t/$script.ERR`) if ($count);
unlink("./t/$script.ERR");
warn ("WARNING:no output from $script") if (!$count && !-s "./t/$script.OUT");
print "@{[ $count || !-s qq{./t/$script.OUT} ? 'not ' : '' ]}ok 1";
unlink("./t/$script.OUT");
