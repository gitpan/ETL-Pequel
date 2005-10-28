#!/usr/bin/perl
# ----------------------------------------------------------------------------------------------------
#  Name		: ETL::Pequel::Type.pm
#  Created	: 14 January 2005
#  Author	: Mario Gaffiero (gaffie)
#
# Copyright 1999-2005 Mario Gaffiero.
# 
# This file is part of Pequel(TM).
# 
# Pequel is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
# 
# Pequel is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with Pequel; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
# ----------------------------------------------------------------------------------------------------
# Modification History
# When          Version     Who     What
# ----------------------------------------------------------------------------------------------------
# TO DO:
# ----------------------------------------------------------------------------------------------------
require 5.005_62;
use strict;
use attributes qw(get reftype);
use warnings;
use vars qw($VERSION $BUILD);
$VERSION = "1.1-1";
$BUILD = 'Tue Jan 27 15:45:31 EST 2005';
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Element;
#> maybe combine with Pequel::Element (Pequel::Collection::Element)
	use ETL::Pequel::Collection;	#+++++
	use base qw(ETL::Pequel::Collection::Element);

	our $this = __PACKAGE__;

	sub BEGIN
	{
		our @attr =
		qw(
			useList
			sourceSectionName
			sourceFieldName
			timeStamp
		);
		eval ("sub attr { my \$self = shift; return (\$self->SUPER::attr, qw(@{[ join(' ', @attr) ]})); } ");
		foreach (@attr)
		{
			eval
			("
				sub $_ : method
				{
					my \$self = shift;
					\$self->{\$this}->{@{[ uc($_) ]}} = shift if (\@_);
					return \$self->{\$this}->{@{[ uc($_) ]}};
				}
			");
		}
	}

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_);
		bless($self, $class);

		$self->useList($params{'use_list'} || $params{'useList'} || ETL::Pequel::Collection::Vector->new);
		$self->sourceSectionName($params{'sourceSectionName'});
		$self->sourceFieldName($params{'sourceFieldName'});

		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Data;
	use ETL::Pequel::Collection;	#+++++
	use base qw(ETL::Pequel::Collection::Vector);

	sub new : method
	{
		my $self = shift;
		my $param = shift;
		my $class = ref($self) || $self;
		$self = $class->SUPER::new(@_);
		bless($self, $class);
		# Set all default values:
		$self->add(ETL::Pequel::Type::Element->new(name => 'string',	value => 1, PARAM => $param));
		$self->add(ETL::Pequel::Type::Element->new(name => 'numeric',value => 2, PARAM => $param));
		$self->add(ETL::Pequel::Type::Element->new(name => 'decimal',value => 3, PARAM => $param));
		$self->add(ETL::Pequel::Type::Element->new(name => 'date',	value => 4, PARAM => $param));
		$self->add(ETL::Pequel::Type::Element->new(name => 'time',	value => 5, PARAM => $param));
		$self->add(ETL::Pequel::Type::Element->new(name => 'array',	value => 6, PARAM => $param));

		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
1;
