#!/usr/bin/perl
# ----------------------------------------------------------------------------------------------------
#  Name		: ETL::Pequel::Type::Db.pm
#  Created	: 15 March 2005
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
# 30/08/2005	2.2-8		gaffie	removed default ConfigFlags and ConfigOptimize
# 30/08/2005	2.2-8		gaffie	bug fix in codeInlineConfigOptimize
# ----------------------------------------------------------------------------------------------------
# TO DO:
# ----------------------------------------------------------------------------------------------------
require 5.005_62;
use strict;
use attributes qw(get reftype);
use warnings;
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Db::Element;
	use ETL::Pequel::Type;	#+++++
	use base qw(ETL::Pequel::Type::Element);

	our $this = __PACKAGE__;

	sub BEGIN
	{
		our @attr =
		qw(
			dataSourceFilename
			username
			password
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
		$self->dataSourceFilename($params{'data_source_filename'});		
		$self->username($params{'username'});		
		$self->password($params{'password'});		
		return $self;
	}

	sub codeInlineClose : method {}
	sub codeInlineInit : method {}
	sub codeInlineOpen : method {}
	sub codeInlinePragma : method {}
	sub codeInlinePrep : method {}
	sub codeInlineReset : method {}
	sub codeInlineDecl : method  {}
	sub codeInlineValueDecl : method  {}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Db::Vector;
	use ETL::Pequel::Collection;	#+++++
	use base qw(ETL::Pequel::Collection::Vector);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_);
		bless($self, $class);
		return $self;
	}

    sub cacheRecs : method 
	{ 
		my $self = shift; 
		return $self->PARAM->properties('cache_recs') ? "[current_cache_rec]" : "";
	}

	sub codeConnect : method {}
	sub codeInlineClose : method {}
	sub codeInlineInit : method {}
	sub codeInlineOpen : method {}
	sub codeInlinePragma : method {}
	sub codeInlinePrep : method {}
	sub codeInlineReset : method {}
	sub codeInlineDecl : method  {}
	sub codeInlineValueDecl : method  {}
	sub codeInlineConfigFlags : method {}
	sub codeInlineConfigLibs : method {}
	sub codeInlineConfigIncludes : method {}
	sub codeInlineIncludes : method {}
	sub codeInlineFunctions : method {}
	sub codeDisconnect : method {}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Db;
	use ETL::Pequel::Collection;	#+++++
	use base qw(ETL::Pequel::Collection::Vector);
#	use ETL::Pequel::Type::Db::Oracle;	#+++++
#	use ETL::Pequel::Type::Db::Sqlite;	#+++++

	our $this = __PACKAGE__;

	sub BEGIN
	{
		our @attr =
		qw(
			tableList
			PARAM
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
		my $param = shift;
		my $class = ref($self) || $self;
		$self = $class->SUPER::new(@_);
		bless($self, $class);
		$self->PARAM($param);

#		$self->addAll
#		(
#			ETL::Pequel::Type::Db::Oracle->new,
#			ETL::Pequel::Type::Db::Sqlite->new,
#>			ETL::Pequel::Type::Db::Mysql->new,
#		);
		$self->tableList(ETL::Pequel::Collection::Vector->new);
		return $self;
	}

	sub codeConnect : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code::Perl->new(PARAM => $self->PARAM);
		foreach my $db ($self->toArray)
		{ 
			$c->addAll($db->codeConnect) 
				if (grep($_->useList, $db->toArray));
		}
		return $c;
	}

	sub codeDisconnect : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code::Perl->new(PARAM => $self->PARAM);
		foreach my $db ($self->toArray)
		{ 
			$c->addAll($db->codeDisconnect) 
				if (grep($_->useList, $db->toArray));
		}
		return $c;
	}

	sub codeInlineConfigLibs : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code::C->new(PARAM => $self->PARAM);
		foreach my $db ($self->toArray)
		{ 
			if (grep($_->useList, $db->toArray))
			{
				$c->addNonl(" ");
				$c->addAll($db->codeInlineConfigLibs);
			}
		}
		return $c;
	}

	sub codeInlineConfigIncludes : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code::C->new(PARAM => $self->PARAM);
		foreach my $db ($self->toArray)
		{ 
			if (grep($_->useList, $db->toArray))
			{
				$c->addNonl(" ");
				$c->addAll($db->codeInlineConfigIncludes);
			}
		}
		return $c;
	}

	sub codeInlineConfigFlags : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code::C->new(PARAM => $self->PARAM);
#<		$c->addNonl("-pthread -O3");
		foreach my $db ($self->toArray)
		{ 
			if (grep($_->useList, $db->toArray))
			{
				$c->addNonl(" ");
				$c->addAll($db->codeInlineConfigFlags);
			}
		}
		return $c;
	}

	sub codeInlineConfigOptimize : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code::C->new(PARAM => $self->PARAM);
#<		$c->addNonl("-O3");
		foreach my $db ($self->toArray)
		{ 
			if (grep($_->useList, $db->toArray))
			{
				$c->addNonl(" ");
				$c->addAll($db->codeInlineConfigOptimize);
			}
		}
		return $c;
	}

	sub codeInlineClose : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code::C->new(PARAM => $self->PARAM);
		foreach my $db ($self->toArray)
		{ 
			$c->addAll($db->codeInlineClose) 
				if (grep($_->useList, $db->toArray));
		}
		return $c;
	}

	sub codeInlineInit : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code::C->new(PARAM => $self->PARAM);
		foreach my $db ($self->toArray)
		{ 
			$c->addAll($db->codeInlineInit) 
				if (grep($_->useList, $db->toArray));
		}
		return $c;
	}

	sub codeInlineOpen : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code::C->new(PARAM => $self->PARAM);
		foreach my $db ($self->toArray)
		{ 
			$c->addAll($db->codeInlineOpen) 
				if (grep($_->useList, $db->toArray));
		}
		return $c;
	}

	sub codeInlinePragma : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code::C->new(PARAM => $self->PARAM);
		foreach my $db ($self->toArray)
		{ 
			$c->addAll($db->codeInlinePragma) 
				if (grep($_->useList, $db->toArray));
		}
		return $c;
	}

	sub codeInlinePrep : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code::C->new(PARAM => $self->PARAM);
		foreach my $db ($self->toArray)
		{ 
			$c->addAll($db->codeInlinePrep) 
				if (grep($_->useList, $db->toArray));
		}
		return $c;
	}

	sub codeInlineReset : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code::C->new(PARAM => $self->PARAM);
		foreach my $db ($self->toArray)
		{ 
			$c->addAll($db->codeInlineReset) 
				if (grep($_->useList, $db->toArray));
		}
		return $c;
	}

	sub codeInlineDecl : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code::C->new(PARAM => $self->PARAM);
		foreach my $db ($self->toArray)
		{ 
			$c->addAll($db->codeInlineDecl) 
				if (grep($_->useList, $db->toArray));
		}
		return $c;
	}

	sub codeInlineValueDecl : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code::C->new(PARAM => $self->PARAM);
		foreach my $db ($self->toArray)
		{ 
			$c->addAll($db->codeInlineValueDecl) 
				if (grep($_->useList, $db->toArray));
		}
		return $c;
	}

	sub codeInlineIncludes : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code::C->new(PARAM => $self->PARAM);
		foreach my $db ($self->toArray)
		{ 
			$c->addAll($db->codeInlineIncludes) 
				if (grep($_->useList, $db->toArray));
		}
		return $c;
	}

	sub codeInlineFunctions : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code::C->new(PARAM => $self->PARAM);
		foreach my $db ($self->toArray)
		{ 
			$c->addAll($db->codeInlineFunctions) 
				if (grep($_->useList, $db->toArray));
		}
		return $c;
	}

	sub codeInlineFieldNamesDecl : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);

		foreach ($self->PARAM->sections->find('input section')->items->toArray)
		{
			$c->add(sprintf("#define %-@{[ $self->maxHeaderLen+1 ]}s %4d", 
				$_->id, $_->number-1));
		}
		$c->add;
		my $next_fldnum = $self->PARAM->sections->find('input section')->items->size;
		foreach my $t (sort { $a->sequence <=> $b->sequence } $self->PARAM->tables->toArray)
		{
			foreach my $k ($t->refKeyList->toArray)
			{
				$c->add(sprintf("#define %-@{[ $self->maxTableHeaderLen ]}s %4d", 
					"_I_@{[ $t->name ]}_@{[ $k->name ]}_FLD_KEY", $next_fldnum++));

				foreach my $f ($t->fields->toArray)
				{
					$c->add(sprintf("#define %-@{[ $self->maxTableHeaderLen ]}s %4d", 
						"_I_@{[ $t->name ]}_@{[ $k->name ]}_FLD_@{[ $f->name ]}",
						$next_fldnum++));
				}
			}
		}
		return $c;
	}

	sub maxHeaderLen : method
	{
		my $self = shift;
		my $maxheader=0;
		foreach ($self->PARAM->sections->find('input section')->items->toArray, $self->PARAM->sections->find('output section')->items->toArray)
		{
			$maxheader = length($_->name) if ($maxheader < length($_->name));
		}
		return $maxheader + 5;
	}

	sub maxTableHeaderLen : method
	{
		my $self = shift;

		# need to increase:
		my $maxheader=0;
		foreach my $t ($self->PARAM->tables->toArray)
		{
			foreach my $k ($t->refKeyList->toArray)
			{
				foreach my $f ($t->fields->toArray)
				{
					$maxheader = length($t->name) + length($k->name) + length($f->name) 
						if ($maxheader < length($t->name) + length($k->name) +  length($f->name));
				}
			}
		}
		return $maxheader+10;
	}
}
1;
# ----------------------------------------------------------------------------------------------------
__END__
