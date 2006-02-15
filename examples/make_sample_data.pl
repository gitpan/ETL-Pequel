
my $NUMRECS = shift || 1000;

$\="\n";
$,="|";

#print 
#	"PRODUCT",
#	"COST_PRICE",
#	"DESCRIPTION",
#	"SALES_CODE",
#	"SALES_PRICE",
#	"SALES_QTY",
#	"SALES_DATE",
#	"LOCATION";

my @BRANDS = qw{Compaq Dell Toshiba IBM Fujitsu HP Philips Cannon};
my @MEM = qw{128 256 512 1GB};
my @DISK = qw{10GB 20GB 30GB 40GB 60GB};
my @PROC = qw{P3-1200 P3-880 P3-900 P3-1300 P3-1400 P4-1600 P4-1700 P4-1800 P4-1900 P4-2000};
my @LANG = qw{EN IT FR SP GR};
my @LOC = qw{SYD NSW NT QLD MEL VIC SA WA PER ALIC};
my @DAY = (1..31);
my @MONTH = (1..12);
my @YEAR = qw{1999 2000 2001 2002};
my @SMAN = qw{MG PL KO WER KJH RT JK BG SA HGY};

foreach my $r (1..$NUMRECS)
{

    my $prod_1 = chr(rand(12)+65);
    my $prod_2 = sprintf("%03d", rand(20)+100);
    my $prod_3 = chr(rand(25)+65) . chr(rand(25)+65) . chr(rand(25)+65);
    my $prod_4 = sprintf("%02d", rand(10)+1);
    my $product = $prod_1 . $prod_2 . $prod_3 . $prod_4;
#    my $location = $prod_1 . $prod_2;
    my $cost = sprintf("%d.%02d", rand(3000)+1, rand(99));
    my $model = $prod_4 * 1000;
    my $description = $BRANDS[rand(@BRANDS)] . " " . $model . " " . $LANG[rand(@LANG)] . " " . $PROC[rand(@PROC)] . "/" . $MEM[rand(@MEM)] . "/" . $DISK[rand(@DISK)];
    my $sales_code = substr($description, 0, 1) . $model;
	my $location = $LOC[rand(@LOC)];

	my $sales_price = sprintf("%.2f", $cost + rand(1000));
	my $sales_qty = rand(2)+1 == 1 ? sprintf("%d", int(rand(100)+1)) : sprintf("%d", int(rand(10)+1));
	my $sales_date = sprintf("%02d/%02d/%04d", $DAY[rand(@DAY)], $MONTH[rand(@MONTH)], $YEAR[rand(@YEAR)]);
#	my $sales_date = sprintf("%04d%02d%02d", $YEAR[rand(@YEAR)], $MONTH[rand(@MONTH)], $DAY[rand(@DAY)]);

	my @sman_list; foreach (0..rand(10)) { push(@sman_list, $SMAN[rand(@SMAN)]); }
	my $sman_list = join(',', @sman_list);

    print
        $product,
        $cost,
        $description,
        $sales_code,
		$sales_price,
		$sales_qty,
		$sales_date,
		$location,
		$sman_list;

    foreach my $n (1..rand(10))
    {
        $cost = sprintf("%d.%02d", rand(3000)+1, rand(99));
        $sales_code = substr($description, 0, 1) . (($prod_4 + int(rand(8))) * 1000);
		$location = $LOC[rand(@LOC)];

		$sales_price = sprintf("%.2f", $cost + rand(1000));
		$sales_qty = rand(2)+1 == 1 ? sprintf("%d", int(rand(10)+1)) : sprintf("%d", int(rand(100)+1));
		$sales_date = sprintf("%02d/%02d/%04d", $DAY[rand(@DAY)], $MONTH[rand(@MONTH)], $YEAR[rand(@YEAR)]);
#		$sales_date = sprintf("%04d%02d%02d", $YEAR[rand(@YEAR)], $MONTH[rand(@MONTH)], $DAY[rand(@DAY)]);
		undef @sman_list; foreach (0..rand(10)) { push(@sman_list, $SMAN[rand(@SMAN)]); }
		$sman_list = join(',', @sman_list);

        print
            $product,
            $cost,
            $description,
            $sales_code,
			$sales_price,
			$sales_qty,
			$sales_date,
			$location,
			$sman_list;
    }

}
