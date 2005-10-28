require 5.004;
use strict;
require ExtUtils::MakeMaker;
#	INSTALLSITELIB	=> '$(PREFIX)/lib',

my %opts = (
    NAME        	=> 'ETL::Pequel',
#	PREFIX			=> '/product/perl',
    VERSION_FROM 	=> 'lib/ETL/Pequel/Main.pm',
#	INSTALLSCRIPT 	=> '/product/scripts',
#	INSTALLSITELIB	=> '$(PREFIX)/lib',		# this will place .pm's in /product/perl/lib
	INST_SCRIPT		=> 'scripts',
	INST_LIB		=> 'lib',
# Uncomment the following if you intend to use Inline:
#   PREREQ_PM => {
#       Data::Dumper => 2.09,
#       Digest::MD5 => 2.09,
#   	Parse::RecDescent => 1.80,
#       File::Spec => 0.8,
#   	Inline => 0.44,
#   	MIME::Base64 => 3.05,
#   	Text::Balanced => 1.95,
#   },
    dist       		=>
    {
        SUFFIX       => ".gz",
        DIST_DEFAULT => 'all tardist',
        COMPRESS     => "gzip -9vf",
#		TARFLAGS	 => '--owner=root --group=users cvf'
    }
);

if ($ExtUtils::MakeMaker::VERSION >= 5.43) {
    $opts{'AUTHOR'} = 'Mario Gaffiero (gaffie@users.sourceforge.net)';
}
ExtUtils::MakeMaker::WriteMakefile(%opts);
