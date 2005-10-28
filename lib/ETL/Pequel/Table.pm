#!/usr/bin/perl
# ----------------------------------------------------------------------------------------------------
#  Name		: ETL::Pequel::Table.pm
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
#use Pequel::Base;
#use Pequel::Collection;
#use Pequel::Field;
use vars qw($VERSION $BUILD);
$VERSION = "1.1-1";
$BUILD = 'Tue Jan 27 15:45:31 EST 2005';
# ----------------------------------------------------------------------------------------------------
{	
	package ETL::Pequel::Table;	# --> contains vector of all tables (ETL::Pequel::Table::Element)
	use ETL::Pequel::Collection;	#+++++
	use base qw(ETL::Pequel::Collection::Vector);

	our $this = __PACKAGE__;

	sub BEGIN
	{
		our @attr =
		qw(
			lastSequence
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
		$self = $class->SUPER::new(@_);
		bless($self, $class);

		$self->lastSequence(0);

		return $self;
	}

	sub setTableSequence : method
	{
		my $self = shift;
		my $table = shift;	#--> ETL::Pequel::Table::Element;

		if ($table->sequence == 0)
		{
			$self->lastSequence($self->lastSequence+1);
			$table->sequence($self->lastSequence);
		}

#<		if ($table->sequence != 0)
#<		{
#<			# Table already in sequence, but we still need to set the startFieldNumber for refkey
#<			# if its another one:
#<			if (!defined($table->refKeyList->last->startFieldNumber))
#<			{
#<				foreach (grep($_->sequence == $self->lastSequence, $self->root->tables->toArray))
#<				{
#<					$table->refKeyList->last->startFieldNumber(
#<						$_->refKeyList->last->startFieldNumber + $_->fields->size +1);
#<						$_->fields->size +1);
#<				}
#<			}
#<		}
#<		else
#<		{
#<			$self->lastSequence($self->lastSequence+1);
#<			$table->sequence($self->lastSequence);
#<			if ($self->lastSequence == 1)	# this is the first table in sequence
#<			{
#<				$table->refKeyList->last->startFieldNumber($self->root->s_inputFields->items->size);
#<			}
#<			else
#<			{
#<				foreach (grep($_->sequence == $self->lastSequence-1, $self->root->tables->toArray))
#<				{
#<					$table->refKeyList->last->startFieldNumber(
#<						$_->refKeyList->last->startFieldNumber + $_->fields->size +1);
#<				}
#<			}
#<		}
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Table::Refkey::Element;
	use ETL::Pequel::Field;	#+++++
	use base qw(ETL::Pequel::Field::Element);

	our $this = __PACKAGE__;

	sub BEGIN
	{
		our @attr =
		qw(
			tableField
			table
			referenceFieldList
			startFieldNumber
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

		$self->startFieldNumber($params{'start_field_number'});
		$self->referenceFieldList(ETL::Pequel::Collection::Vector->new);
		$self->table($params{'table'});
		$self->tableField($params{'table_field'});

		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Table::Field::Element;
	use ETL::Pequel::Field;	#+++++
	use base qw(ETL::Pequel::Field::Element);

	our $this = __PACKAGE__;

	sub BEGIN
	{
		our @attr =
		qw(
			column
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

		$self->column($params{'col'} || $params{'column'});		

		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Table::Data::Element;
	use ETL::Pequel::Collection;	#+++++
	use base qw(ETL::Pequel::Collection::Element);

	sub new : method
	{
        my $self = shift;
        my $class = ref($self) || $self;
        my %params = @_;
        $self = $class->SUPER::new(@_);
        bless($self, $class);

		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
{	
	package ETL::Pequel::Table::Data;	# --> contains vector of all tables (ETL::Pequel::Table::Data::Element)
	use ETL::Pequel::Collection;	#+++++
	use base qw(ETL::Pequel::Collection::Vector);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		$self = $class->SUPER::new(@_);
		bless($self, $class);

		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
1;
