#!/usr/bin/perl
# ----------------------------------------------------------------------------------------------------
#  Name		: ETL::Pequel::Param.pm
#  Created	: 23 September 2005
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
#
# ----------------------------------------------------------------------------------------------------
# Modification History
# When          Version     Who     What
# 29/09/2005	2.3-2		gaffie	New -- replaces Pequel::Base module and 'root'.
# ----------------------------------------------------------------------------------------------------
# TO DO:
# ----------------------------------------------------------------------------------------------------
require 5.005_62;
use strict;
use attributes qw(get reftype);
use warnings;
use constant DEBUG => 0;
use vars qw($VERSION $BUILD);
$VERSION = "2.4-3";
$BUILD = 'Tuesday November  1 08:45:13 GMT 2005';
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Param;
	use vars qw($AUTOLOAD);

	our $this = __PACKAGE__;
	sub BEGIN
	{
		our @attr =
		qw(
			parser
			tables	  
			datatypes	  
			datetypes	  
			monthtypes	  
			options	  
			sections 
			macros	  
			aggregates		
			dbtypes		  
			error
			pequel_script
			pequel_script_disallow
			ifields
			SCRIPT
			ENGINE
			SORT_ASC
			SORT_DES
			PEQUEL_EXEC_NAME
			VERSION
			LOCAL
			root
			packages
		);
		eval ("sub attr { my \$self = shift; return (qw(@{[ join(' ', @attr) ]})); } ");
		foreach (@attr)
		{
			eval
			("
				sub @{[ __PACKAGE__ ]}::$_ : method
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
		my $proto = shift;
		my $class = ref($proto) || $proto;
		my $self = {};
		bless($self, $class);

		use ETL::Pequel::Script;
		$self->SCRIPT(ETL::Pequel::Script->new(PARAM => $self));

		use ETL::Pequel::Engine;
		$self->ENGINE(ETL::Pequel::Engine->new(PARAM => $self));

		$self->SORT_ASC(1);
		$self->SORT_DES(2);
		$self->PEQUEL_EXEC_NAME('pequel');
		$self->VERSION("@{[ $self->PEQUEL_EXEC_NAME ]} Version @{[ $::VERSION ]}, Build: @{[ $::BUILD ]}");

		use ETL::Pequel::Error;
		$self->error(ETL::Pequel::Error->new(PARAM => $self));

		# pequel_script is a container for external pequel scripts
		$self->pequel_script(ETL::Pequel::Collection::Vector->new());
		$self->pequel_script_disallow(ETL::Pequel::Collection::Vector->new());
		$self->ifields(ETL::Pequel::Collection::Vector->new());
		$self->packages(ETL::Pequel::Collection::Vector->new());

		use ETL::Pequel::Table;
		$self->tables(ETL::Pequel::Table->new);

		use ETL::Pequel::Type;
		$self->datatypes(ETL::Pequel::Type::Data->new($self));

		use ETL::Pequel::Type::Date;
		$self->datetypes(ETL::Pequel::Type::Date->new);
		$self->monthtypes(ETL::Pequel::Type::Month->new);

		use ETL::Pequel::Type::Option;
		$self->options(ETL::Pequel::Type::Option->new($self));

		use ETL::Pequel::Type::Section;
		$self->sections(ETL::Pequel::Type::Sections->new($self));

		use ETL::Pequel::Type::Macro;
		$self->macros(ETL::Pequel::Type::Macro->new($self));

		use ETL::Pequel::Type::Aggregate;
		$self->aggregates(ETL::Pequel::Type::Aggregate->new($self));

		use ETL::Pequel::Parse;
		$self->parser(ETL::Pequel::Parse->new(PARAM => $self));

		use ETL::Pequel::Type::Db;
		$self->dbtypes(ETL::Pequel::Type::Db->new($self));

		use ETL::Pequel::Type::Db::Oracle;	#+++++
		use ETL::Pequel::Type::Db::Sqlite;	#+++++
		$self->dbtypes->addAll
		(
			ETL::Pequel::Type::Db::Oracle->new($self),
			ETL::Pequel::Type::Db::Sqlite->new($self),
#>			ETL::Pequel::Type::Db::Sybase->new($self),
#>			ETL::Pequel::Type::Db::Mysql->new($self),
		);

		return $self;
	}

	sub section : method
	{
		my $self = shift;
		my $name = shift;
		return $self->sections->find($name);
	}

	sub properties : method
	{
		my $self = shift;
		my $name = shift;
		if ($self->sections->find('options')->items->find($name))
		{
			if (int(@_) > 0)
			{
#>				$self->section('options')->items->find($name)->value($_[0]);
				$self->sections->find('options')->items->find($name)->value($_[0]);
				$self->options->find($name)->value($_[0]);
			}
			return $self->sections->find('options')->items->find($name)->value;
		}
		if ($self->options->find($name))
		{
			if (int(@_) > 0) {
				$self->options->find($name)->value($_[0])
			}
			return $self->options->find($name)->value;
		}
		if ($self->options->getAlias($name))
		{
			return $self->properties($self->options->getAlias($name)->name, @_);
		}
		$self->error->warning("[20001] Unknown property '$name' [ @{[ join('|', caller()) ]} ] -- ignored.");
		return 0;
	}

	sub getscriptname : method
	{
		my $self = shift;
		my $sn = shift || $self->properties('script_name');
		$sn =~ s/.*\://;
		$sn =~ s/.*\///;
		return $sn;
	}

	sub getfilepath : method
	{
		my $self = shift;
		my $ofl = shift;
		$ofl =~ s/^.*://; # remove any tags 'pequel:' 'file:' ...
		$ofl = "@{[ $self->properties('prefix') ]}/$ofl"
			if ($ofl !~ /^\// && $self->properties('prefix') ne '');
#? What about $PEQUEL_PREFIX ???
		return $ofl;
	}

	sub c_numOutputFields : method
	{
		my $self = shift;
		my $num=0;
		if ($self->properties('transfer'))
		{
			foreach (grep($_->name !~ /^_/, $self->sections->find('input section')->items->toArray))
			{
				$num++;
			}
		}
		return $self->sections->find('output section')->items->size + $num;
	}

	sub c_numInputFields : method
	{
		my $self = shift;
		foreach (reverse $self->sections->find('input section')->items->toArray)
		{
			return $_->number if ($_->operator eq '');
		}
		return $self->sections->find('input section')->items->last->number;
	}
}
# ----------------------------------------------------------------------------------------------------
1;
