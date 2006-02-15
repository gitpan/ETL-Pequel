#!/usr/bin/perl
# ----------------------------------------------------------------------------------------------------
#  Name		: ETL::Pequel::Field.pm
#  Created	: 28 January 2005
#  Author	: Mario Gaffiero (gaffie)
#
# Copyright 1999-2006 Mario Gaffiero.
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
# 11/11/2005	2.4-5		gaffie	implement new option show_synonyms -- 
# ----------------------------------------------------------------------------------------------------
# TO DO:
# ----------------------------------------------------------------------------------------------------
require 5.005_62;
use strict;
use attributes qw(get reftype);
use warnings;
use vars qw($VERSION $BUILD);
$VERSION = "2.4-3";
$BUILD = 'Tuesday November  1 08:45:13 GMT 2005';
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Field::Element;
	use ETL::Pequel::Type;
	use base qw(ETL::Pequel::Type::Element);

	our $this = __PACKAGE__;

	sub BEGIN
	{
		# Create the class attributes
		our @attr =
		qw(
			type
			dateType
			calc
			calcOrig
			operator
			direction
			inputField
			outputField
			id
			synonym
			codeVar
			ref
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

		$self->type($params{'type'} || ETL::Pequel::Type::Data->new);
		$self->dateType($params{'date_type'} || ETL::Pequel::Type::Date->new);
		$self->calc($params{'calc'});
		$self->calcOrig($params{'calc_orig'});
		$self->operator($params{'operator'} || '');
		$self->direction($params{'direction'} || $self->PARAM->SORT_ASC);	#--> Pequel::Type::Sort->...
		$self->inputField($params{'input_field'});
		$self->outputField($params{'output_field'});
		$self->id($params{'id'});
		$self->synonym($params{'synonym'});
		$self->codeVar($params{'code_var'});
		$self->comment($params{'comment'});
		$self->ref($params{'ref'});

		return $self;
	}

    sub compile : method 
	{ 
		my $self = shift; 
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Field::Input::Element;
	use base qw(ETL::Pequel::Field::Element);

	our $this = __PACKAGE__;

	sub BEGIN
	{
		our @attr =
		qw(
			refTableList
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

		$self->synonym("_I_@{[ $self->name ]}");
		$self->PARAM->properties('show_synonyms') 
			? $self->id("_I_@{[ $self->name ]}")
			: $self->id("@{[ $self->PARAM->sections->find('input section')->items->size() ]}"); # Input field nums 0 base.
#<		$self->id("_I_@{[ $self->name ]}");
		$self->codeVar("\$I_VAL[@{[ $self->id ]}]");
		$self->refTableList(ETL::Pequel::Collection::Vector->new);

		return $self;
	}

#?	sub compile : method
#?	{
#?		my $self = shift;
#?		$self->root->parser->compile($self->calc, $self);
#?	}
#?	
#?	sub equals : method
#?	{
#?		my $self = shift;
#?		my $element = shift;
#?		return 0 if (!$self->SUPER::equals($element));
#?		return 0 if (!$self->check_equals($self->calcFmt, $element->calcFmt));
#?		return 1;
#?	}
#?	
#?	sub set : method
#?	{
#?		my $self = shift;
#?		my $element = shift;
#?		$self->SUPER::set($element);
#?		$self->calcFmt($element->calcFmt);
#?	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Field::Output::Element;
	use base qw(ETL::Pequel::Field::Element);

	our $this = __PACKAGE__;

	sub BEGIN
	{
		our @attr =
		qw(
			clause
			listDelimiter
			aggregate
			inputField
			condition
			serialStart
			calculated
			CFlds
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

		$self->listDelimiter($params{'list_delimiter'});
		$self->aggregate($params{'aggregate'});
		$self->inputField($params{'input_field'});
		$self->condition($params{'condition'});
		$self->serialStart($params{'serial_start'});
		$self->synonym("_O_@{[ $self->name ]}");
		$self->PARAM->properties('show_synonyms') 
			? $self->id("_O_@{[ $self->name ]}")
			: $self->id("@{[ $self->PARAM->sections->find('output section')->items->size()+1 ]}"); # Output field nums 1 base.
#<		$self->id("_O_@{[ $self->name ]}");
		$self->codeVar($self->PARAM->properties('hash') 
			? "\$O_VAL{\$key}{@{[ $self->id ]}}" 
			: "\$O_VAL[@{[ $self->id ]}]");

		$self->CFlds($params{'cflds'} || ETL::Pequel::Collection::Vector->new);
			# contains ETL::Pequel::Field::Element

		$self->clause($params{'clause'});

		return $self;
	}

	sub compile : method
	{
		my $self = shift;

		my $clause = $self->clause;
		return unless ($clause ne '');
		$clause =~ s/^\s*//;
		$clause =~ s/^=/= /;
		$clause =~ s/\band\b/ && /gi;
		my @clause_words = split(/\s+/, $clause, -1);
		my $aggregate = shift(@clause_words);
	
		$self->listDelimiter($self->PARAM->properties('default_list_delimiter'));
	
		if ($aggregate =~ /values_all\(|values_uniq\(/)
		{
			$aggregate =~ s/(.*)\s*\((.*)\)/$1/;
			$self->listDelimiter($2 eq '\s' ? ' ' : "$2");
		}
		if ($aggregate eq '=')
		{
			my $calculate = join(' ', @clause_words);
			$self->PARAM->error->fatalError("[7001] Invalid where clause specified for output field @{[ $self->name ]}") 
				if ($calculate eq '');
	
			if ((my @found = $self->PARAM->sections->exists('input section')->items->extract($calculate)) != 0)
			{
				$self->PARAM->error->fatalError
				("[7002] Calculated output fields may not contain input fields (@{[ join(', ', map($_->name, @found)) ]})");
			}
			$self->PARAM->error->fatalError
			("[7003] Calculated output field name (@{[ $self->name ]}) cannot be the same as input field name")
				if ($self->PARAM->sections->exists('input section')->items->extract($self->name) != 0);
			$self->calculated(1);

			# Set calc() here ONLY:
			$self->calc($calculate);

			$self->aggregate($self->PARAM->aggregates->exists("="));
			$self->comment("_CALCULATED_");
		}
		elsif ($self->PARAM->aggregates->exists($aggregate))
		{
			my $ifield_name = shift(@clause_words);
#? 	Need group-all directive
#?	 		$self->root->error->fatalError
#?			("(@{[ $self->name ]}) group_by field(s) must be specified before aggregate functions can be used")
#? 			unless ($self->root->s_group_by->items->size || $aggregate eq 'serial');
#? 			$self->{GROUP_ALL} = 1 if (!@{$self->{GROUPFIELD}});

			$self->PARAM->error->fatalError("[7003] Invalid where clause specified for output field @{[ $self->name ]}")
				if ($aggregate =~ /count|flag/ && $ifield_name ne '*');

			$self->PARAM->error->fatalError
			("[7004] Invalid 'serial' aggregate clause specified for output field @{[ $self->name ]}")
				if ($aggregate eq 'serial' && $ifield_name !~ /\d+/);

			my $itype = $self->PARAM->datatypes->exists('array') if ($ifield_name =~ s/^@//);
			if 
			(
				$aggregate !~ /count|flag|serial/ 
				&& $self->inputField($self->PARAM->sections->exists('input section')->items->exists($ifield_name)) == 0
			)
			{
				$self->PARAM->error->fatalError("[7005] Field '@{[ $ifield_name ]}' not defined in input section")
			}
			$self->inputField->type($itype) if (defined($self->inputField) && defined($itype) && $itype->name eq 'array');

			$self->aggregate($self->PARAM->aggregates->exists($aggregate));

			$self->condition(join(' ', @clause_words[1..$#clause_words]));	# remove the 'where'

			$self->PARAM->error->fatalError("[7006] Invalid where clause: '@{[ $self->condition ]}'")
				if ($self->condition && $clause_words[0] !~ /where/);

			$self->serialStart($ifield_name) if ($aggregate eq 'serial');
#<			$self->aggregate->useCount($self->aggregate->useCount+1);		#??? Maybe need to move down...
			$self->aggregate->useList->add($self);		#??? Maybe need to move down...
			$self->comment("_AGGREGATE_");
		}
		elsif (join(' ', @clause_words) ne '')
		{
			$self->PARAM->error->fatalError("[7007] Invalid aggregate '$aggregate' for output field '@{[ $self->name ]}'")
		}
		else    # just print input field
		{
			my $ifield_name = $aggregate;
			$self->PARAM->error->fatalError("[7008] Field '@{[ $ifield_name ]}' not defined in input section")
				if ($self->inputField($self->PARAM->sections->exists('input section')->items->exists($ifield_name)) == 0);
			$self->comment("_INPUT_");
		}
		$self->PARAM->error->fatalError
		("[7009] Invalid type '@{[ $self->type->name ]}' used for aggregate '@{[ $self->aggregate->name ]}', for output field '@{[ $self->name ]}'") 
			if ($self->aggregate && !$self->aggregate->allowType->exists($self->type->name));

		$self->PARAM->error->fatalError("[7010] Invalid type '@{[ $self->type->name ]}' for output field '@{[ $self->name ]}'") 
			unless ($self->PARAM->datatypes->exists($self->type->name));

		$self->PARAM->error->fatalError("[7011] Invalid date type '@{[ $self->dateType->name ]}' used for output field '@{[ $self->name ]}'") 
			if ($self->type->name eq 'date' && !$self->PARAM->datetypes->exists($self->dateType->name));

		$self->type->useList->add($self);
		$self->dateType->useList->add($self) if ($self->type->name eq 'date');

		if (defined($self->condition))
		{
			# not quite though...especially when using subs eg: "substr(NAME,1,1) eq 'A'" not same as "NAME eq "..."
			# need to work with left_op and right_op...
			foreach 
			(
				$self->PARAM->sections->exists('input section')->items->extract($self->condition), 
				$self->PARAM->sections->exists('output section')->items->extract($self->condition), 
				$self->PARAM->tables->extract($self->condition)
			)
			{ 
				$self->CFlds->add($_);
			}
			# Now remove all: fields, macros, operators, literals, regexps, then add remaining stuff to CFLDS

			# Input field has preference over Output field.
			#$self->{OUTPUT}->{$field_number}->{CONDITION} = $self->_compile_output($condition, 1); 
		}
	}
}
# ----------------------------------------------------------------------------------------------------
1;
