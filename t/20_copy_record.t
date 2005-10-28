use Test;
use strict;
BEGIN { plan tests => 1; };

my $script='copy_record.pql';
system("scripts/pequel examples/$script -silent -prefix ./examples > /dev/null 2> t/result");

open(RESULT, "t/result");
my $count=0;
while (<RESULT>) { chomp; s/\s+//g; $count++ if (length > 1); }
close(RESULT);
warn ("WARNING:" . `cat t/result`) if ($count);
#unlink("t/result");
print "not ok 1" if ($count);
print "ok 1" if (!$count);
