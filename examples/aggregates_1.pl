# vim: syntax=perl ts=4 sw=4
#:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
#  Script       : aggregates_1.pl
#	Description  : Example perl module access.
#:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
use lib './lib';
use ETL::Pequel;
use strict;

my $p = ETL::Pequel->new();
my $s;

$s = $p->section(ETL::Pequel::OPTIONS);
$s->addItem(name => 'header', 		value => 1);
$s->addItem(name => 'optimize', 	value => 1);
$s->addItem(name => 'hash', 		value => 1);
$s->addItem(name => 'nulls', 		value => 1);
$s->addItem(name => 'doc_title', 	value => "Aggregates Example Script");
$s->addItem(name => 'doc_email', 	value => "sample\@youraddress.com");
$s->addItem(name => 'doc_version', 	value => '2.4');

$s = $p->section(ETL::Pequel::DESCRIPTION);
$s->addItem(value => 'Demonstrates aggregation and use of various aggregate function.');

$s = $p->section(ETL::Pequel::INPUT_SECTION);
$s->addItem(name => 'PRODUCT_CODE', 	type => 'string');
$s->addItem(name => 'COST_PRICE', 		type => 'string');
$s->addItem(name => 'DESCRIPTION', 		type => 'string');
$s->addItem(name => 'SALES_CODE', 		type => 'string');
$s->addItem(name => 'SALES_PRICE', 		type => 'string');
$s->addItem(name => 'SALES_QTY', 		type => 'string');
$s->addItem(name => 'SALES_DATE', 		type => 'string');
$s->addItem(name => 'LOCATION', 		type => 'string');
$s->addItem(name => 'SALES_TOTAL', 		type => 'string', operator => '=>', calc => 'SALES_QTY * SALES_PRICE');

$s = $p->section(ETL::Pequel::SORT_BY);
$s->addItem(fld => 'LOCATION');
$s->addItem(fld => 'PRODUCT_CODE');

$s = $p->section(ETL::Pequel::GROUP_BY);
$s->addItem(fld => 'LOCATION');
$s->addItem(fld => 'PRODUCT_CODE');

$s = $p->section(ETL::Pequel::OUTPUT_SECTION);
$s->addItem(type => 'string', 	field => 'LOCATION', 		clause => 'LOCATION');
$s->addItem(type => 'string', 	field => 'PRODUCT_CODE', 	clause => 'PRODUCT_CODE');
$s->addItem(type => 'decimal', 	field => 'MIN_COST_PRICE', 	clause => 'min COST_PRICE');
$s->addItem(type => 'decimal', 	field => 'MAX_COST_PRICE', 	clause => 'max COST_PRICE');
$s->addItem(type => 'decimal',	field => 'AVG_SALES_PRICE',	clause => 'mean SALES_PRICE');
$s->addItem(type => 'numeric',	field => '_AVG_SALES_QTY',	clause => 'mean SALES_QTY');
$s->addItem(type => 'decimal',	field => 'SALES_TOTAL',		clause => 'sum SALES_TOTAL');
$s->addItem(type => 'decimal',	field => 'SALES_TOTAL_2',	clause => 'sum SALES_TOTAL');
$s->addItem(type => 'decimal',	field => 'RANGE_COST',		clause => 'range COST_PRICE');
$s->addItem(type => 'numeric',	field => 'MODE_SALES_CODE',	clause => 'mode SALES_CODE');
$s->addItem(type => 'numeric',	field => 'AVGS',			clause => '= _AVG_SALES_QTY * 2');

$p->prepare();
$p->generate();

if ($p->check() =~ /syntax\s+ok/i)
{
	$p->engine->printToFile("$0.2.code");
	$p->execute();
}
