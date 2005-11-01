#!/usr/bin/perl
# ----------------------------------------------------------------------------------------------------
#  Name		: ETL::Pequel::Type::Aggregate.pm
#  Created	: 13 February 2005
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
# 22/09/2005	2.3-2		gaffie	Removed Pequel::Base usage and (almost) all refs to Pequel::root.
# ----------------------------------------------------------------------------------------------------
# TO DO:
# - nested aggregates eg: AVG(MAX(salary)) :This calculation evaluates the inner aggregate 
#	(MAX(salary)) for each group defined by the GROUP BY clause (department_id), and aggregates 
#	the results again.
# - corr: returns the coefficient of correlation of a set of number pairs. 
# - covar_pop: returns the population covariance of a set of number pairs. 
# - covar_samp returns the sample covariance of a set of number pairs.
# - cume_dist calculates the cumulative distribution of a value in a group of values. 
# - dense_rank computes the rank of a row in an ordered group of rows.
# - rank calculates the rank of a value in a group of values. 
# - count <field>: counts only non-null fields
# ----------------------------------------------------------------------------------------------------
require 5.005_62;
use strict;
use attributes qw(get reftype);
use warnings;
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Aggregate::Element;
	use ETL::Pequel::Type;	#+++++
	use ETL::Pequel::Code;	#+++++
	use base qw(ETL::Pequel::Type::Element);

	our $this = __PACKAGE__;

	sub BEGIN
	{
		our @attr =
		qw(
			rightOp
			level
			allowType
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
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_);
		bless($self, $class);

		$self->level($params{'level'});
		$self->PARAM($params{'PARAM'});

#<		use Pequel::Type;
#<		$self->datatypes(Pequel::Type::Data->new);
#<		$self->sections($params{'sections'}); # ptr to Type::Sections object
#<		$self->options($params{'options'}); # ptr to Type::Option object
#<		$self->aggregates($params{'aggregates'}); # ptr to Type::Aggregate object

		$self->allowType
		(
			ETL::Pequel::Type::Data->new
			(
				$self->PARAM->datatypes->exists('string'),
				$self->PARAM->datatypes->exists('numeric'),
				$self->PARAM->datatypes->exists('decimal'),
				$self->PARAM->datatypes->exists('date')
			)
		) 
		unless ($params{'type'});

		return $self;
	}

	sub properties : method
	{
		my $self = shift;
		my $name = shift;
		my $value = shift || undef;
		if ($self->PARAM->sections->exists('options')->items->exists($name))
		{
			$self->PARAM->sections->exists('options')->items->exists($name)->value($value) if (defined($value));
			return $self->PARAM->sections->exists('options')->items->exists($name)->value;
		}
		if ($self->PARAM->options->exists($name))
		{
			$self->PARAM->options->exists($name)->value($value) if (defined($value));
			return $self->PARAM->options->exists($name)->value;
		}
		return 0;
	}

    sub codeInit : method 
	{ 
		my $self = shift; 
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		return $c;
	}

    sub codeReset : method 
	{ 
		my $self = shift; 
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		return $c;
	}

    sub codeOutputFinal : method 
	{ 
		my $self = shift; 
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		return $c;
	}

    sub codeOutput : method 
	{ 
		my $self = shift; 
#>		my $parser = shift;
		my $ofld = shift;

		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);

		if ($ofld->inputField && $ofld->inputField->calc)
		{
			$c->addNonl("@{[ $ofld->inputField->codeVar ]}");
			$c->addNonl("@{[ $ofld->inputField->operator eq '=~' ? ' =~ '  : ' = ' ]}");
			$c->add("@{[ $self->PARAM->parser->compile($ofld->inputField->calc) ]};");
		}
		$self->rightOp($self->PARAM->parser->compile($ofld->inputField->name)) if ($ofld->inputField);
		return $c;
	}

	sub hash : method
	{
		my $self = shift; 
		return $self->properties('hash') ? '{$key}' : '';
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Aggregate::equals;
	use base qw(ETL::Pequel::Type::Aggregate::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_,
			name => $params{'name'} || '=',				
			level => 2,
		);
		bless($self, $class);
		return $self;
	}

    sub codeOutputFinal : method 
	{ 
		my $self = shift; 
		my $ofld = shift;
		my $c = $self->SUPER::codeOutputFinal($ofld);
		$c->addNonl("@{[ $ofld->codeVar ]} = ");
		$c->addNonl($self->PARAM->parser->compileOutput($ofld->calc));
		$c->add(";");
		return $c;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Aggregate::Sum;
	use base qw(ETL::Pequel::Type::Aggregate::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_,
			name => $params{'name'} || 'sum',				
			level => 1,
		);
		bless($self, $class);
		$self->allowType(ETL::Pequel::Type::Data->new
		(
			$self->PARAM->datatypes->exists('decimal'), 
			$self->PARAM->datatypes->exists('numeric'), 
			$self->PARAM->datatypes->exists('date')
		));

		return $self;
	}

    sub codeOutput : method 
	{ 
		my $self = shift; 
		my $ofld = shift;

		my $c = $self->SUPER::codeOutput($ofld);
		$c->addNonl("@{[ $ofld->codeVar ]} += @{[ $self->rightOp ]}");
		$c->addNonl(" unless (@{[ $self->rightOp ]} eq '')") if (!$self->properties('nonulls'));
		$c->add(";");
		return $c;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Aggregate::SumDistinct;
	use base qw(ETL::Pequel::Type::Aggregate::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_,
			name => $params{'name'} || 'sum_distinct',				
			level => 1,
		);
		bless($self, $class);
		$self->allowType(ETL::Pequel::Type::Data->new
		(
			$self->PARAM->datatypes->exists('decimal'), 
			$self->PARAM->datatypes->exists('numeric'), 
			$self->PARAM->datatypes->exists('date')
		));

		return $self;
	}

    sub codeInit : method 
	{ 
		my $self = shift; 
		my $c = $self->SUPER::codeInit;
		$c->add("my \%SUM_DISTINCT_KEYS;");
		return $c;
	}

    sub codeReset : method 
	{ 
		my $self = shift; 
		my $c = $self->SUPER::codeReset;
		$c->add("\%SUM_DISTINCT_KEYS = undef;");
		return $c;
	}

    sub codeOutput : method 
	{ 
		my $self = shift; 
		my $ofld = shift;

		my $c = $self->SUPER::codeOutput($ofld);
		$c->add("@{[ $ofld->codeVar ]} += @{[ $self->rightOp ]}");
		$c->over();
		$c->addNonl(" if (");
		$c->addNonl("@{[ $self->rightOp ]} ne '' && ") if (!$self->properties('nonulls'));
		$c->addNonl("++\$SUM_DISTINCT_KEYS@{[ $self->hash ]}\{@{[ $ofld->id ]}}{qq{@{[ $self->rightOp ]}}} == 1)");
		$c->add(";");
		$c->back();
		return $c;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Aggregate::ValuesUniq;
	use base qw(ETL::Pequel::Type::Aggregate::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_,
			name => $params{'name'} || 'values_uniq',				
			level => 2,
		);
		bless($self, $class);
		return $self;
	}

    sub codeInit : method 
	{ 
		my $self = shift; 
		my $c = $self->SUPER::codeInit;
		$c->add("my \%VALUES_UNIQ;");
		return $c;
	}

    sub codeReset : method 
	{ 
		my $self = shift; 
		my $c = $self->SUPER::codeReset;
		$c->add("\%VALUES_UNIQ = undef;");
		return $c;
	}

    sub codeOutputFinal : method 
	{ 
		my $self = shift; 
		my $ofld = shift;
		my $c = $self->SUPER::codeOutputFinal($ofld);
		$c->addNonl("@{[ $ofld->codeVar ]} = ");
		$c->addNonl("join(qq{@{[ $self->properties('default_list_delimiter') ]}}, ");
		$c->addNonl("grep(length, sort keys \%{\$VALUES_UNIQ@{[ $self->hash ]}\{@{[ $ofld->id ]}}}))");
		$c->add(";");
		return $c;
	}

    sub codeOutput : method 
	{ 
		my $self = shift; 
		my $ofld = shift;
		my $c = $self->SUPER::codeOutput($ofld);
#<		if ($ofld->type->name eq 'array')
		if ($ofld->inputField->type->name eq 'array')
		{
			$c->addNonl("map(\$VALUES_UNIQ@{[ $self->hash ]}\{@{[ $ofld->id ]}}{\$_}++, ");
			$c->add("split(/\\s*@{[ $self->properties('default_list_delimiter') ]}\\s*/, @{[ $self->rightOp ]}));");
		}
		else
		{
			$c->add("\$VALUES_UNIQ@{[ $self->hash ]}\{@{[ $ofld->id ]}}{qq{@{[ $self->rightOp ]}}}++;");
		}
		return $c;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Aggregate::ValuesAll;
	use base qw(ETL::Pequel::Type::Aggregate::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_,
			name => $params{'name'} || 'values_all',				
			level => 2,	# level 2 means same as having codeOutputFinal()
		);
		bless($self, $class);
		return $self;
	}

    sub codeInit : method 
	{ 
		my $self = shift; 
		my $c = $self->SUPER::codeInit;
		$c->add("my \%VALUES_ALL;");
		return $c;
	}

    sub codeReset : method 
	{ 
		my $self = shift; 
		my $c = $self->SUPER::codeReset;
		$c->add("\%VALUES_ALL = undef;");
		return $c;
	}

    sub codeOutputFinal : method 
	{ 
		my $self = shift; 
		my $ofld = shift;
		my $c = $self->SUPER::codeOutputFinal($ofld);
		$c->addNonl("@{[ $ofld->codeVar ]} = ");
		$c->addNonl("join(qq{@{[ $self->properties('default_list_delimiter') ]}}, ");
		$c->addNonl("grep(length, \@{\$VALUES_ALL@{[ $self->hash ]}\{@{[ $ofld->id ]}}}))");
		$c->add(";");
		return $c;
	}

    sub codeOutput : method 
	{ 
		my $self = shift; 
		my $ofld = shift;
		my $c = $self->SUPER::codeOutput($ofld);
		if ($ofld->type->name eq 'array')
		{
			$c->add("push(\@{\$VALUES_ALL@{[ $self->hash ]}\{@{[ $ofld->id ]}}}, split(/\\s*@{[ $self->properties('default_list_delimiter') ]}\\s*/, @{[ $self->rightOp ]}));");
		}
		else
		{
			$c->add("push(\@{\$VALUES_ALL@{[ $self->hash ]}\{@{[ $ofld->id ]}}}, qq{@{[ $self->rightOp ]}});");
		}
		return $c;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Aggregate::Count;
	use base qw(ETL::Pequel::Type::Aggregate::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_,
			name => $params{'name'} || 'count',				
			level => 1,
		);
		bless($self, $class);
		return $self;
	}

    sub codeOutput : method 
	{ 
		my $self = shift; 
		my $ofld = shift;
		my $c = $self->SUPER::codeOutput($ofld);
		$c->add("@{[ $ofld->codeVar ]}++;");
		return $c;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Aggregate::Minimum;
	use base qw(ETL::Pequel::Type::Aggregate::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_,
			name => $params{'name'} || 'minimum',				
			level => 1,
		);
		bless($self, $class);
		return $self;
	}

    sub codeOutput : method 
	{ 
		my $self = shift; 
		my $ofld = shift;
		my $c = $self->SUPER::codeOutput($ofld);

		if ($ofld->type->name eq 'date')
		{
			$c->add("if (!defined(@{[ $ofld->codeVar ]})) { @{[ $ofld->codeVar ]} = @{[ $self->rightOp ]}; }");
			$c->add("else");
			$c->openBlock("{");
			$c->addAll($ofld->dateType->codeCmpDate($self->rightOp, $ofld->codeVar));
			$c->add("if (\$cmp == -1) { @{[ $ofld->codeVar ]} = @{[ $self->rightOp ]}; }");
			$c->closeBlock;
		}
		elsif ($ofld->type->name eq 'string')
		{
			$c->add("@{[ $ofld->codeVar ]} = @{[ $self->rightOp ]}");
			$c->over;
			$c->add("if (!defined(@{[ $ofld->codeVar ]}) || @{[ $self->rightOp ]} lt @{[ $ofld->codeVar ]});");
			$c->back;
		}
		else
		{
			$c->add("@{[ $ofld->codeVar ]} = @{[ $self->rightOp ]}");
			$c->over;
			$c->add("if (!defined(@{[ $ofld->codeVar ]}) || @{[ $self->rightOp ]} < @{[ $ofld->codeVar ]});");
			$c->back;
		}
		return $c;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Aggregate::Min;
	use base qw(ETL::Pequel::Type::Aggregate::Minimum);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_,
			name => $params{'name'} || 'min',				
		);
		bless($self, $class);
		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Aggregate::Maximum;
	use base qw(ETL::Pequel::Type::Aggregate::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_,
			name => $params{'name'} || 'maximum',				
			level => 1,
		);
		bless($self, $class);
		return $self;
	}

    sub codeOutput : method 
	{ 
		my $self = shift; 
		my $ofld = shift;
		my $c = $self->SUPER::codeOutput($ofld);
		if ($ofld->type->name eq 'date')
		{
			$c->add("if (!defined(@{[ $ofld->codeVar ]})) { @{[ $ofld->codeVar ]} = @{[ $self->rightOp ]}; }");
			$c->add("else");
			$c->openBlock("{");
			$c->addAll($ofld->dateType->codeCmpDate($self->rightOp, $ofld->codeVar));
			$c->add("if (\$cmp == 1) { @{[ $ofld->codeVar ]} = @{[ $self->rightOp ]}; }");
			$c->closeBlock;
		}
		elsif ($ofld->type->name eq 'string')
		{
			$c->add("@{[ $ofld->codeVar ]} = @{[ $self->rightOp ]}");
			$c->over;
			$c->add("if (!defined(@{[ $ofld->codeVar ]}) || @{[ $self->rightOp ]} gt @{[ $ofld->codeVar ]});");
			$c->back;
		}
		else
		{
			$c->add("@{[ $ofld->codeVar ]} = @{[ $self->rightOp ]}");
			$c->over;
			$c->add("if (!defined(@{[ $ofld->codeVar ]}) || @{[ $self->rightOp ]} > @{[ $ofld->codeVar ]});");
			$c->back;
		}
		return $c;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Aggregate::Max;
	use base qw(ETL::Pequel::Type::Aggregate::Maximum);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_,
			name => $params{'name'} || 'max',				
		);
		bless($self, $class);
		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Aggregate::Last;
	use base qw(ETL::Pequel::Type::Aggregate::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_,
			name => $params{'name'} || 'last',				
			level => 1,
		);
		bless($self, $class);
		return $self;
	}

    sub codeOutput : method 
	{ 
		my $self = shift; 
		my $ofld = shift;
		my $c = $self->SUPER::codeOutput($ofld);
		$c->add("@{[ $ofld->codeVar ]} = @{[ $self->rightOp ]};");
		return $c;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Aggregate::First;
	use base qw(ETL::Pequel::Type::Aggregate::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_,
			name => $params{'name'} || 'first',				
			level => 1,
		);
		bless($self, $class);
		return $self;
	}

    sub codeOutput : method 
	{ 
		my $self = shift; 
		my $ofld = shift;
		my $c = $self->SUPER::codeOutput($ofld);
		$c->add("@{[ $ofld->codeVar ]} = @{[ $self->rightOp ]} if (!defined(@{[ $ofld->codeVar ]}));");
		return $c;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Aggregate::Flag;
	use base qw(ETL::Pequel::Type::Aggregate::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_,
			name => $params{'name'} || 'flag',				
			level => 1,
		);
		bless($self, $class);
		return $self;
	}

    sub codeOutput : method 
	{ 
		my $self = shift; 
		my $ofld = shift;
		my $c = $self->SUPER::codeOutput($ofld);
		$c->add("@{[ $ofld->codeVar ]} = 1;");
		return $c;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Aggregate::Distinct;
	use base qw(ETL::Pequel::Type::Aggregate::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_,
			name => $params{'name'} || 'distinct',				
			level => 1,
		);
		bless($self, $class);
		return $self;
	}

    sub codeInit : method 
	{ 
		my $self = shift; 
		my $c = $self->SUPER::codeInit;
		$c->add("my \%DISTINCT;");
		return $c;
	}

    sub codeReset : method 
	{ 
		my $self = shift; 
		my $c = $self->SUPER::codeReset;
		$c->add("\%DISTINCT = undef;");
		return $c;
	}

    sub codeOutput : method 
	{ 
		my $self = shift; 
		my $ofld = shift;
		my $c = $self->SUPER::codeOutput($ofld);
		$c->add("@{[ $ofld->codeVar ]}++ ");
		$c->over();
		$c->add("if (defined(@{[ $self->rightOp ]}) && ++\$DISTINCT@{[ $self->hash ]}\{@{[ $ofld->id ]}}{qq{@{[ $self->rightOp ]}}} == 1);");
		$c->back();
		return $c;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Aggregate::CountDistinct;
	use base qw(ETL::Pequel::Type::Aggregate::Distinct);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_,
			name => $params{'name'} || 'count_distinct',				
			level => 1,
		);
		bless($self, $class);
		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Aggregate::AvgDistinct;
	use base qw(ETL::Pequel::Type::Aggregate::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_,
			name => $params{'name'} || 'avg_distinct',				
			level => 2,
		);
		bless($self, $class);
		$self->allowType(ETL::Pequel::Type::Data->new
		(
			$self->PARAM->datatypes->exists('decimal'), 
			$self->PARAM->datatypes->exists('numeric'), 
			$self->PARAM->datatypes->exists('date')
		));
		return $self;
	}

    sub codeInit : method 
	{ 
		my $self = shift; 
		my $c = $self->SUPER::codeInit;
		$c->add("my \%AVG_DISTINCT;");
		$c->add("my \%AVG_DISTINCT_KEYS;");
		return $c;
	}

    sub codeReset : method 
	{ 
		my $self = shift; 
		my $c = $self->SUPER::codeReset;
		$c->add("\%AVG_DISTINCT = undef;");
		$c->add("\%AVG_DISTINCT_KEYS = undef;");
		return $c;
	}

    sub codeOutputFinal : method 
	{ 
		my $self = shift; 
		my $ofld = shift;
		my $c = $self->SUPER::codeOutputFinal($ofld);
		$c->addNonl("@{[ $ofld->codeVar ]} = ");
		$c->addNonl("(\$AVG_DISTINCT@{[ $self->hash ]}\{@{[ $ofld->id ]}}{_COUNT} == 0");
		$c->addNonl(" ? 0 : \$AVG_DISTINCT@{[ $self->hash ]}\{@{[ $ofld->id ]}}{_SUM} / \$AVG_DISTINCT@{[ $self->hash ]}\{@{[ $ofld->id ]}}{_COUNT})");
		$c->add(";");
		return $c;
	}

    sub codeOutput : method 
	{ 
		my $self = shift; 
		my $ofld = shift;
		my $c = $self->SUPER::codeOutput($ofld);
		$c->add("\$AVG_DISTINCT@{[ $self->hash ]}\{@{[ $ofld->id ]}}{_SUM} += @{[ $self->rightOp ]}");
		$c->over();
		$c->addNonl(" if (++\$AVG_DISTINCT_KEYS@{[ $self->hash ]}\{@{[ $ofld->id ]}}{qq{@{[ $self->rightOp ]}}} == 1)");
		$c->add(";");
		$c->back();
		$c->add("\$AVERAGE_DISTINCT@{[ $self->hash ]}\{@{[ $ofld->id ]}}{_COUNT}++");
		$c->over();
		$c->addNonl(" if (\$AVG_DISTINCT_KEYS@{[ $self->hash ]}\{@{[ $ofld->id ]}}{qq{@{[ $self->rightOp ]}}} == 1)");
		$c->add(";");
		$c->back();
		return $c;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Aggregate::Avg;
	use base qw(ETL::Pequel::Type::Aggregate::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_,
			name => $params{'name'} || 'avg',				
			level => 2,
		);
		bless($self, $class);
		$self->allowType(ETL::Pequel::Type::Data->new
		(
			$self->PARAM->datatypes->exists('decimal'), 
			$self->PARAM->datatypes->exists('numeric'), 
			$self->PARAM->datatypes->exists('date')
		));
		return $self;
	}

    sub codeInit : method 
	{ 
		my $self = shift; 
		my $c = $self->SUPER::codeInit;

#>		need better way to do this...
		if 
		(
			$self->name eq 'avg'
			|| ($self->name eq 'mean' && $self->PARAM->aggregates->exists('avg')->useList->size == 0)
		)
		{
			$c->add("my \%AVERAGE;");
		}
		return $c;
	}

    sub codeReset : method 
	{ 
		my $self = shift; 
		my $c = $self->SUPER::codeReset;
		if 
		(
			$self->name eq 'avg'
			|| ($self->name eq 'mean' && $self->PARAM->aggregates->exists('avg')->useList->size == 0)
		)
		{
			$c->add("\%AVERAGE = undef;");
		}
		return $c;
	}

    sub codeOutputFinal : method 
	{ 
		my $self = shift; 
		my $ofld = shift;
		my $c = $self->SUPER::codeOutputFinal($ofld);
		$c->addNonl("@{[ $ofld->codeVar ]} = ");
		$c->addNonl("(\$AVERAGE@{[ $self->hash ]}\{@{[ $ofld->id ]}}{_COUNT} == 0");
		$c->addNonl(" ? 0 : \$AVERAGE@{[ $self->hash ]}\{@{[ $ofld->id ]}}{_SUM} / \$AVERAGE@{[ $self->hash ]}\{@{[ $ofld->id ]}}{_COUNT})");
		$c->add(";");
		return $c;
	}

    sub codeOutput : method 
	{ 
		my $self = shift; 
		my $ofld = shift;
		my $c = $self->SUPER::codeOutput($ofld);
		$c->add("\$AVERAGE@{[ $self->hash ]}\{@{[ $ofld->id ]}}{_SUM} += @{[ $self->rightOp ]};");
		$c->add("\$AVERAGE@{[ $self->hash ]}\{@{[ $ofld->id ]}}{_COUNT}++;");
		return $c;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Aggregate::Mean;
	use base qw(ETL::Pequel::Type::Aggregate::Avg);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_,
			name => $params{'name'} || 'mean',				
		);
		bless($self, $class);
		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Aggregate::Median;
	use base qw(ETL::Pequel::Type::Aggregate::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_,
			name => $params{'name'} || 'median',				
			level => 2,
		);
		bless($self, $class);
		$self->allowType(ETL::Pequel::Type::Data->new
		(
			$self->PARAM->datatypes->exists('decimal'), 
			$self->PARAM->datatypes->exists('numeric'), 
			$self->PARAM->datatypes->exists('date')
		));
		return $self;
	}

    sub codeInit : method 
	{ 
		my $self = shift; 
		my $c = $self->SUPER::codeInit;
		$c->add("my \%MEDIAN;");
		$c->add("my \%MEDIAN_COUNT;");
		return $c;
	}

    sub codeReset : method 
	{ 
		my $self = shift; 
		my $c = $self->SUPER::codeReset;
		$c->add("\%MEDIAN = undef;");
		$c->add("\%MEDIAN_COUNT = undef;");
		return $c;
	}

    sub codeOutputFinal : method 
	{ 
		my $self = shift; 
		my $ofld = shift;
		my $c = $self->SUPER::codeOutputFinal($ofld);
		my $v1 = "\$MEDIAN_COUNT@{[ $self->hash ]}\{@{[ $ofld->id ]}}/2-1";
		my $v2 = "\$MEDIAN_COUNT@{[ $self->hash ]}\{@{[ $ofld->id ]}}/2";
		$c->addNonl("@{[ $ofld->codeVar ]} = ");
		$c->addNonl("\$MEDIAN_COUNT@{[ $self->hash ]}\{@{[ $ofld->id ]}} % 2 == 0");
		$c->addNonl(' ? &{sub{($_[0] + $_[1]) / 2}}((( sort {$a <=> $b} keys %{$MEDIAN' . "@{[ $self->hash ]}\{@{[ $ofld->id ]}}" . '} )[' . "$v1, $v2" . '])[0,1])');
		$c->addNonl(' : (sort {$a <=> $b} keys %{$MEDIAN' . "@{[ $self->hash ]}\{@{[ $ofld->id ]}}" . '} )[(($MEDIAN_COUNT' . "@{[ $self->hash ]}\{@{[ $ofld->id ]}}" . '+1)/2)-1]');
		$c->add(";");
		return $c;
	}

    sub codeOutput : method 
	{ 
		my $self = shift; 
		my $ofld = shift;
		my $c = $self->SUPER::codeOutput($ofld);
		$c->addNonl("\$MEDIAN_COUNT@{[ $self->hash ]}\{@{[ $ofld->id ]}}++ ");
		$c->add("if (++\$MEDIAN@{[ $self->hash ]}\{@{[ $ofld->id ]}}{qq{@{[ $self->rightOp ]}}} == 1);");
		return $c;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Aggregate::Variance;
	use base qw(ETL::Pequel::Type::Aggregate::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_,
			name => $params{'name'} || 'variance',				
			level => 2,
		);
		bless($self, $class);
		$self->allowType(ETL::Pequel::Type::Data->new
		(
			$self->PARAM->datatypes->exists('decimal'), 
			$self->PARAM->datatypes->exists('numeric'), 
			$self->PARAM->datatypes->exists('date')
		));
		return $self;
	}

    sub codeInit : method 
	{ 
		my $self = shift; 
		my $c = $self->SUPER::codeInit;
		$c->add("my \%VARIANCE;");
		return $c;
	}

    sub codeReset : method 
	{ 
		my $self = shift; 
		my $c = $self->SUPER::codeReset;
		$c->add("\%VARIANCE = undef;");
		return $c;
	}

    sub codeOutputFinal : method 
	{ 
		my $self = shift; 
		my $ofld = shift;
		my $c = $self->SUPER::codeOutputFinal($ofld);
		$c->addNonl("@{[ $ofld->codeVar ]} = ");
		$c->addNonl("(\$VARIANCE@{[ $self->hash ]}\{@{[ $ofld->id ]}}{_SUM_SQUARES}");
		$c->addNonl(" / (\$VARIANCE@{[ $self->hash ]}\{@{[ $ofld->id ]}}{_COUNT} == 0");
		$c->addNonl(" ? 1 : \$VARIANCE@{[ $self->hash ]}\{@{[ $ofld->id ]}}{_COUNT}))");
		$c->addNonl("- ((\$VARIANCE@{[ $self->hash ]}\{@{[ $ofld->id ]}}{_SUM}");
		$c->addNonl(" / \$VARIANCE@{[ $self->hash ]}\{@{[ $ofld->id ]}}{_COUNT}) ** 2)");
		$c->add(";");
		return $c;
	}

    sub codeOutput : method 
	{ 
		my $self = shift; 
		my $ofld = shift;
		my $c = $self->SUPER::codeOutput($ofld);
		$c->add("\$VARIANCE@{[ $self->hash ]}\{@{[ $ofld->id ]}}{_SUM} += @{[ $self->rightOp ]};");
		$c->add("\$VARIANCE@{[ $self->hash ]}\{@{[ $ofld->id ]}}{_SUM_SQUARES} += @{[ $self->rightOp ]} ** 2;");
		$c->add("\$VARIANCE@{[ $self->hash ]}\{@{[ $ofld->id ]}}{_COUNT}++;");
		return $c;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Aggregate::Stddev;
	use base qw(ETL::Pequel::Type::Aggregate::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_,
			name => $params{'name'} || 'stddev',				
			level => 2,
		);
		bless($self, $class);
		$self->allowType(ETL::Pequel::Type::Data->new
		(
			$self->PARAM->datatypes->exists('decimal'), 
			$self->PARAM->datatypes->exists('numeric'), 
			$self->PARAM->datatypes->exists('date')
		));
		return $self;
	}

    sub codeInit : method 
	{ 
		my $self = shift; 
		my $c = $self->SUPER::codeInit;
		$c->add("my \%STDDEV;");
		return $c;
	}

    sub codeReset : method 
	{ 
		my $self = shift; 
		my $c = $self->SUPER::codeReset;
		$c->add("\%STDDEV = undef;");
		return $c;
	}

    sub codeOutputFinal : method 
	{ 
		my $self = shift; 
		my $ofld = shift;
		my $c = $self->SUPER::codeOutputFinal($ofld);
		$c->addNonl("@{[ $ofld->codeVar ]} = ");
		$c->addNonl("sqrt((\$STDDEV@{[ $self->hash ]}\{@{[ $ofld->id ]}}{_SUM_SQUARES}");
		$c->addNonl(" / (\$STDDEV@{[ $self->hash ]}\{@{[ $ofld->id ]}}{_COUNT} == 0");
		$c->addNonl(" ? 1 : \$STDDEV@{[ $self->hash ]}\{@{[ $ofld->id ]}}{_COUNT}))");
		$c->addNonl("- ((\$STDDEV@{[ $self->hash ]}\{@{[ $ofld->id ]}}{_SUM}");
		$c->addNonl(" / \$STDDEV@{[ $self->hash ]}\{@{[ $ofld->id ]}}{_COUNT}) ** 2))");
		$c->add(";");
		return $c;
	}

    sub codeOutput : method 
	{ 
		my $self = shift; 
		my $ofld = shift;
		my $c = $self->SUPER::codeOutput($ofld);
		$c->add("\$STDDEV@{[ $self->hash ]}\{@{[ $ofld->id ]}}{_SUM} += @{[ $self->rightOp ]};");
		$c->add("\$STDDEV@{[ $self->hash ]}\{@{[ $ofld->id ]}}{_SUM_SQUARES} += @{[ $self->rightOp ]} ** 2;");
		$c->add("\$STDDEV@{[ $self->hash ]}\{@{[ $ofld->id ]}}{_COUNT}++;");
		return $c;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Aggregate::Range;
	use base qw(ETL::Pequel::Type::Aggregate::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_,
			name => $params{'name'} || 'range',				
			level => 2,
		);
		bless($self, $class);
		$self->allowType(ETL::Pequel::Type::Data->new
		(
			$self->PARAM->datatypes->exists('decimal'), 
			$self->PARAM->datatypes->exists('numeric'), 
			$self->PARAM->datatypes->exists('date')
		));
		return $self;
	}

    sub codeInit : method 
	{ 
		my $self = shift; 
		my $c = $self->SUPER::codeInit;
		$c->add("my \%RANGE;");
		return $c;
	}

    sub codeReset : method 
	{ 
		my $self = shift; 
		my $c = $self->SUPER::codeReset;
		$c->add("\%RANGE = undef;");
		return $c;
	}

    sub codeOutputFinal : method 
	{ 
		my $self = shift; 
		my $ofld = shift;
		my $c = $self->SUPER::codeOutputFinal($ofld);
		$c->addNonl("@{[ $ofld->codeVar ]} = ");
		$c->addNonl("\$RANGE@{[ $self->hash ]}\{@{[ $ofld->id ]}}{_MAX} - \$RANGE@{[ $self->hash ]}\{@{[ $ofld->id ]}}{_MIN}");
		$c->add(";");
		return $c;
	}

    sub codeOutput : method 
	{ 
		my $self = shift; 
		my $ofld = shift;
		my $c = $self->SUPER::codeOutput($ofld);
		$c->add("\$RANGE@{[ $self->hash ]}\{@{[ $ofld->id ]}}{_MIN} = @{[ $self->rightOp ]}");
		$c->over;
		$c->add("if");
		$c->openBlock("(");
		$c->add("!defined(\$RANGE@{[ $self->hash ]}\{@{[ $ofld->id ]}}{_MIN})");
		$c->add("|| @{[ $self->rightOp ]} < \$RANGE@{[ $self->hash ]}\{@{[ $ofld->id ]}}{_MIN}");
		$c->closeBlock(");");
		$c->back;

		$c->add("\$RANGE@{[ $self->hash ]}\{@{[ $ofld->id ]}}{_MAX} = @{[ $self->rightOp ]}");
		$c->over;
		$c->add("if");
		$c->openBlock("(");
		$c->add("!defined(\$RANGE@{[ $self->hash ]}\{@{[ $ofld->id ]}}{_MAX})");
		$c->add("|| @{[ $self->rightOp ]} > \$RANGE@{[ $self->hash ]}\{@{[ $ofld->id ]}}{_MAX}");
		$c->closeBlock(");");
		$c->back;
		return $c;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Aggregate::Mode;
	use base qw(ETL::Pequel::Type::Aggregate::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_,
			name => $params{'name'} || 'mode',				
			level => 2,
		);
		bless($self, $class);
		return $self;
	}

    sub codeInit : method 
	{ 
		my $self = shift; 
		my $c = $self->SUPER::codeInit;
		$c->add("my \%MODE;");
		return $c;
	}

    sub codeReset : method 
	{ 
		my $self = shift; 
		my $c = $self->SUPER::codeReset;
		$c->add("\%MODE = undef;");
		return $c;
	}

    sub codeOutputFinal : method 
	{ 
		my $self = shift; 
		my $ofld = shift;
		my $c = $self->SUPER::codeOutputFinal($ofld);
		$c->addNonl("@{[ $ofld->codeVar ]} = ");
		$c->addNonl("join(' ', &{sub{my \@top; foreach my \$k (sort { \$MODE@{[ $self->hash ]}\{@{[ $ofld->id ]}}{\$b} <=> \$MODE@{[ $self->hash ]}\{@{[ $ofld->id ]}}{\$a} } ");
		$c->addNonl("keys %{\$MODE@{[ $self->hash ]}\{@{[ $ofld->id ]}}} )");
		$c->addNonl("{ last if (\$MODE@{[ $self->hash ]}\{@{[ $ofld->id ]}}{\$k} != \$MODE@{[ $self->hash ]}\{@{[ $ofld->id ]}}{\$_[0]}); push(\@top, \$k);} \@top; }}");
		$c->addNonl("((sort { \$MODE@{[ $self->hash ]}\{@{[ $ofld->id ]}}{\$b} <=> \$MODE@{[ $self->hash ]}\{@{[ $ofld->id ]}}{\$a} } keys %{\$MODE@{[ $self->hash ]}\{@{[ $ofld->id ]}}} )[0]))");
		$c->add(";");
		return $c;
	}

    sub codeOutput : method 
	{ 
		my $self = shift; 
		my $ofld = shift;
		my $c = $self->SUPER::codeOutput($ofld);
		$c->add("\$MODE@{[ $self->hash ]}\{@{[ $ofld->id ]}}{qq{@{[ $self->rightOp ]}}}++;");
		return $c;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Aggregate::Serial;
	use base qw(ETL::Pequel::Type::Aggregate::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_,
			name => $params{'name'} || 'serial',				
			level => 2,
		);
		bless($self, $class);
		return $self;
	}

    sub codeInit : method 
	{ 
		my $self = shift; 
		my $c = $self->SUPER::codeInit;
		# or $self->root->t_aggregate->codeInit; --> see above
		map
		(
			$c->add("my \$_SERIAL_@{[ $_->name ]} = @{[ $_->serialStart ]};"),
			$self->useList->toArray		#--> list of: ETL::Pequel::Field::Output::Element;
		);
		return $c;
	}

    sub codeOutputFinal : method 
	{ 
		my $self = shift; 
		my $ofld = shift;
		my $c = $self->SUPER::codeOutputFinal($ofld);
		$c->addNonl("@{[ $ofld->codeVar ]} = ");
		$c->addNonl("++\$_SERIAL_@{[ $ofld->name ]}");
		$c->add(";");
		return $c;
	}

    sub codeOutput : method 
	{ 
		my $self = shift; 
#>		my $parser = shift;
		return ETL::Pequel::Code->new(PARAM => $self->PARAM);
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Aggregate;
	use ETL::Pequel::Collection;	#+++++
	use base qw(ETL::Pequel::Collection::Vector);

	sub new : method
	{
		my $self = shift;
		my $param = shift;
		my $class = ref($self) || $self;
		$self = $class->SUPER::new(@_); 
		bless($self, $class);

#<		my $options = $params{'options'}; # ptr to Type::Option object

		# Set all default values:
		$self->add(ETL::Pequel::Type::Aggregate::Count->new(PARAM => $param));
		$self->add(ETL::Pequel::Type::Aggregate::CountDistinct->new(PARAM => $param));
		$self->add(ETL::Pequel::Type::Aggregate::Min->new(PARAM => $param));
		$self->add(ETL::Pequel::Type::Aggregate::Max->new(PARAM => $param));
		$self->add(ETL::Pequel::Type::Aggregate::Minimum->new(PARAM => $param));
		$self->add(ETL::Pequel::Type::Aggregate::Maximum->new(PARAM => $param));
		$self->add(ETL::Pequel::Type::Aggregate::Sum->new(PARAM => $param));
		$self->add(ETL::Pequel::Type::Aggregate::SumDistinct->new(PARAM => $param));
		$self->add(ETL::Pequel::Type::Aggregate::First->new(PARAM => $param));
		$self->add(ETL::Pequel::Type::Aggregate::Last->new(PARAM => $param));
		$self->add(ETL::Pequel::Type::Aggregate::Flag->new(PARAM => $param));
		$self->add(ETL::Pequel::Type::Aggregate::Distinct->new(PARAM => $param));	#--> CountDistinct
		$self->add(ETL::Pequel::Type::Aggregate::Avg->new(PARAM => $param));
		$self->add(ETL::Pequel::Type::Aggregate::AvgDistinct->new(PARAM => $param));
		$self->add(ETL::Pequel::Type::Aggregate::equals->new(PARAM => $param));
		$self->add(ETL::Pequel::Type::Aggregate::Serial->new(PARAM => $param));
		$self->add(ETL::Pequel::Type::Aggregate::ValuesAll->new(PARAM => $param));
		$self->add(ETL::Pequel::Type::Aggregate::ValuesUniq->new(PARAM => $param));
		$self->add(ETL::Pequel::Type::Aggregate::Mean->new(PARAM => $param));
		$self->add(ETL::Pequel::Type::Aggregate::Median->new(PARAM => $param));
		$self->add(ETL::Pequel::Type::Aggregate::Variance->new(PARAM => $param));
		$self->add(ETL::Pequel::Type::Aggregate::Stddev->new(PARAM => $param));
		$self->add(ETL::Pequel::Type::Aggregate::Range->new(PARAM => $param));
		$self->add(ETL::Pequel::Type::Aggregate::Mode->new(PARAM => $param));
#>v3	$self->add(ETL::Pequel::Type::Aggregate::Element->new(name => 'quartiles',			level => 2));
#>v3	$self->add(ETL::Pequel::Type::Aggregate::Element->new(name => 'interquartilerange',	level => 2));
#>v3	$self->add(ETL::Pequel::Type::Aggregate::Element->new(name => 'percentiles',		level => 2));
#>v3	$self->add(ETL::Pequel::Type::Aggregate::Element->new(name => 'trimean',			level => 2));
#>v3	$self->add(ETL::Pequel::Type::Aggregate::Element->new(name => 'trimedianmean',		level => 2));
#>v3	$self->add(ETL::Pequel::Type::Aggregate::Element->new(name => 'valid_vals',			level => 1));
#>v3	$self->add(ETL::Pequel::Type::Aggregate::Element->new(name => 'missing_vals',		level => 1));

		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
1;
