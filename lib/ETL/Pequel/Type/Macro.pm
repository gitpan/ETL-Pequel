#!/usr/bin/perl
# ----------------------------------------------------------------------------------------------------
#  Name		: ETL::Pequel::Type::Macro.pm
#  Created	: 16 February 2005
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
# 26/10/2005	2.3-6		gaffie	&input_field_count(), &input_record_count() macros.
# 26/10/2005	2.3-6		gaffie	&arr_pack(), &pack() macros.
# 19/10/2005	2.3-5		gaffie	&sprintf() macro.
# 09/09/2005	2.2-9		gaffie	Array field macros -- now assuming all args to be array field using &to_array().
# 01/09/2005	2.2-9		gaffie	Bug fix in compile() for arr_avg
# 01/09/2005	2.2-9		gaffie	All array macros now will parse any param as an array field.
# 01/09/2005	2.2-9		gaffie	Fixed arr_size macro.
# ----------------------------------------------------------------------------------------------------
# TO DO:
# ----------------------------------------------------------------------------------------------------
require 5.005_62;
use strict;
use attributes qw(get reftype);
use warnings;
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::Element;
	use ETL::Pequel::Collection;
	use ETL::Pequel::Type;
	use base qw(ETL::Pequel::Type::Element);

	our $this = __PACKAGE__;

	sub BEGIN
	{
		our @attr =
		qw(
			usageFmt
			eval
			external
			args	
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

		$self->eval($params{'eval'});
		$self->external($params{'external'} || 0);
#>		$self->allow($params{'allow'});
#>		$self->usage($params{'usage'});
#>		$self->description($params{'description'});

#>		use ETL::Pequel::Type::Date;
#>		$self->t_date(ETL::Pequel::Type::Date->new);
#>		$self->t_month(ETL::Pequel::Type::Month->new);

		return $self;
	}

#?	# check args validity
#?	sub checkArgs : method
#?	{
#?	}

    sub codeInit : method 
	{ 
		my $self = shift; 
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		return $c;
	}

    sub inWords : method 
	{ 
		my $self = shift; 
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		return $c;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::Date;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new (@_, name => $params{'name'} || 'date');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		my $type = $_[1] || $self->PARAM->properties('default_datetype');

		my $o_date = $self->PARAM->datetypes->exists($type) || $self->PARAM->datetypes->addUserType($type);
		$self->PARAM->error->fatalError("[4001] Invalid date type '$type'") if (!$o_date);
		$o_date->useList->add($self);
		return $o_date->codeToCCYYMMDD($_[0])->sprint;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::MonthsSince;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new (@_, name => $params{'name'} || 'months_since');
		bless($self, $class);
		return $self;
	}

	sub codeInit : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		$c->add("use constant _MONTHS_SINCE_TODAY_YEAR => int @{[ (localtime)[5] + 1900 ]}; # Macro:@{[ $self->name ]}()");
		$c->add("use constant _MONTHS_SINCE_TODAY_MONTH => int @{[ (localtime)[4]+1 ]}; # Macro:@{[ $self->name ]}()");
		return $c;
	}

	sub compile : method
	{
		my $self = shift;
		my $type = $_[1] || $self->PARAM->properties('default_datetype');

		my $o_date = $self->PARAM->datetypes->exists($type) || $self->PARAM->datetypes->addUserType($type);
		$self->PARAM->error->fatalError("[4002] Invalid date type '$type'") if (!$o_date);
		$o_date->useList->add($self);

		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		if ($o_date->m->len == 3)
		{
			$c->addNonl("$_[0] !~ /@{[ $o_date->regEx ]}/ ? 0 : ");
			$c->addNonl("(");
			$c->addNonl(	"(_MONTHS_SINCE_TODAY_YEAR - int(substr($_[0], @{[ $o_date->y->pos ]}, @{[ $o_date->y->len ]}))) * 12");
			$c->addNonl(")");
			$c->addNonl(" + ");
			$c->addNonl("(");
			$c->addNonl(	"_MONTHS_SINCE_TODAY_MONTH - int(\$MONTH_NUM{substr($_[0], @{[ $o_date->m->pos ]}, @{[ $o_date->m->len ]})})");
			$c->addNonl(")");
		}
		else
		{
			$c->addNonl("$_[0] !~ /@{[ $o_date->regEx ]}/ ? 0 : ");
			$c->addNonl("(");
			$c->addNonl(	"(_MONTHS_SINCE_TODAY_YEAR - int(substr($_[0], @{[ $o_date->y->pos ]}, @{[ $o_date->y->len ]}))) * 12");
			$c->addNonl(")");
			$c->addNonl(" + ");
			$c->addNonl("(");
			$c->addNonl(	"_MONTHS_SINCE_TODAY_MONTH - int(substr($_[0], @{[ $o_date->m->pos ]}, @{[ $o_date->m->len ]}))");
			$c->addNonl(")");
		}
		return $c->sprint;
#		return ("((_MONTHS_SINCE_TODAY_YEAR - (unpack('a4 a2', $_[0]))[0]) * 12) + (_MONTHS_SINCE_TODAY_MONTH - (unpack('a4 a2', $_[0]))[1])");
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::MonthsBetween;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new (@_, name => $params{'name'} || 'months_between',
			fmt => 'date-from, date-to [, date-type ]',
		);
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		my $type = $_[2] || $self->PARAM->properties('default_datetype');

		my $o_date = $self->PARAM->datetypes->exists($type) || $self->PARAM->datetypes->addUserType($type);
		$self->PARAM->error->fatalError("[4003] Invalid date type '$type'") if (!$o_date);
		$o_date->useList->add($self);

		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);

		if ($o_date->m->len == 3)
		{
			$c->addNonl("$_[0] !~ /@{[ $o_date->regEx ]}/ || $_[1] !~ /@{[ $o_date->regEx ]}/ ? 0 : abs(");
			$c->addNonl("(");
			$c->addNonl(	"(");
			$c->addNonl(		"int(substr($_[1], @{[ $o_date->y->pos ]}, @{[ $o_date->y->len ]}))");
			$c->addNonl(		" - int(substr($_[0], @{[ $o_date->y->pos ]}, @{[ $o_date->y->len ]}))");
			$c->addNonl(	") * 12");
			$c->addNonl(")");
			$c->addNonl(" + ");
			$c->addNonl("(");
			$c->addNonl(	"int(\$MONTH_NUM{substr($_[1], @{[ $o_date->m->pos ]}, @{[ $o_date->m->len ]})})");
			$c->addNonl(	" - int(\$MONTH_NUM{substr($_[0], @{[ $o_date->m->pos ]}, @{[ $o_date->m->len ]})})");
			$c->addNonl("))");
		}
		else
		{
			$c->addNonl("$_[0] !~ /@{[ $o_date->regEx ]}/ || $_[1] !~ /@{[ $o_date->regEx ]}/ ? 0 : abs(");
			$c->addNonl("(");
			$c->addNonl(	"(");
			$c->addNonl(		"int(substr($_[1], @{[ $o_date->y->pos ]}, @{[ $o_date->y->len ]}))");
			$c->addNonl(		" - int(substr($_[0], @{[ $o_date->y->pos ]}, @{[ $o_date->y->len ]}))");
			$c->addNonl(	") * 12");
			$c->addNonl(")");
			$c->addNonl(" + ");
			$c->addNonl("(");
			$c->addNonl(	"int(substr($_[1], @{[ $o_date->m->pos ]}, @{[ $o_date->m->len ]}))");
			$c->addNonl(	" - int(substr($_[0], @{[ $o_date->m->pos ]}, @{[ $o_date->m->len ]}))");
			$c->addNonl("))");
		}
		return $c->sprint;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::DateLastDay;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new (@_, name => $params{'name'} || 'date_last_day',
			fmt => 'date, [, date-type ]',
		);
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		my $type = $_[1] || $self->PARAM->properties('default_datetype');

		my $o_date = $self->PARAM->datetypes->exists($type) || $self->PARAM->datetypes->addUserType($type);
		$self->PARAM->error->fatalError("[4007] Invalid date type '$type'") if (!$o_date);
		$o_date->useList->add($self);

		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);

		my $codeDay = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		($o_date->m->len == 3)
			? $codeDay->add("&y($_[0]) % 4 == 0 ? \$_LAST_DAY_LEAP[ \$MONTH_NUM{substr($_[0], @{[ $o_date->m->pos ]}, @{[ $o_date->m->len ]})} ] : \$_LAST_DAY[ \$MONTH_NUM{substr($_[0], @{[ $o_date->m->pos ]}, @{[ $o_date->m->len ]})} ]")
			: $codeDay->add("&y($_[0]) % 4 == 0 ? \$_LAST_DAY_LEAP[ substr($_[0], @{[ $o_date->m->pos ]}, @{[ $o_date->m->len ]}) ] : \$_LAST_DAY[ substr($_[0], @{[ $o_date->m->pos ]}, @{[ $o_date->m->len ]}) ]");
		my $codeMonth = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		$codeMonth->add("substr($_[0], @{[ $o_date->m->pos ]}, @{[ $o_date->m->len ]})");
		my $codeYear = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		$codeYear->add("substr($_[0], @{[ $o_date->y->pos ]}, @{[ $o_date->y->len ]})");

		$c->add("");
		$c->over;
		$c->addAll($o_date->codeFmtDate($codeDay, $codeMonth, $codeYear));
		$c->back;
		return $c->sprint;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::LastDay;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new (@_, name => $params{'name'} || 'last_day',
			fmt => 'date, [, date-type ]',
		);
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		my $type = $_[1] || $self->PARAM->properties('default_datetype');

		my $o_date = $self->PARAM->datetypes->exists($type) || $self->PARAM->datetypes->addUserType($type);
		$self->PARAM->error->fatalError("[4011] Invalid date type '$type'") if (!$o_date);
		$o_date->useList->add($self);

		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		if ($o_date->m->len == 3)
		{
			$c->add("&y($_[0]) % 4 == 0");
			$c->add("? \$_LAST_DAY_LEAP[ \$MONTH_NUM{substr($_[0], @{[ $o_date->m->pos ]}, @{[ $o_date->m->len ]})} ]");
			$c->add(": \$_LAST_DAY[ \$MONTH_NUM{substr($_[0], @{[ $o_date->m->pos ]}, @{[ $o_date->m->len ]})} ]");
		}
		else
		{
			$c->add("&y($_[0]) % 4 == 0");
			$c->add("? \$_LAST_DAY_LEAP[ substr($_[0], @{[ $o_date->m->pos ]}, @{[ $o_date->m->len ]}) ]");
			$c->add(": \$_LAST_DAY[ substr($_[0], @{[ $o_date->m->pos ]}, @{[ $o_date->m->len ]}) ]");
		}
		return $c->sprint;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::DateNextDay;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new (@_, name => $params{'name'} || 'date_next_day',
			fmt => 'date, [, date-type ]',
		);
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		my $type = $_[1] || $self->PARAM->properties('default_datetype');

		my $o_date = $self->PARAM->datetypes->exists($type) || $self->PARAM->datetypes->addUserType($type);
		$self->PARAM->error->fatalError("[4008] Invalid date type '$type'") if (!$o_date);
		$o_date->useList->add($self);

		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);

		my $codeMonthEx = ($o_date->m->len == 3)
			? "\$MONTH_NUM{substr($_[0], @{[ $o_date->m->pos ]}, @{[ $o_date->m->len ]})}"
			: "substr($_[0], @{[ $o_date->m->pos ]}, @{[ $o_date->m->len ]})";
		my $codeDayEx = "substr($_[0], @{[ $o_date->d->pos ]}, @{[ $o_date->d->len ]})";
		my $codeYearEx = "substr($_[0], @{[ $o_date->y->pos ]}, @{[ $o_date->y->len ]})";

		my $codeDay = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		$codeDay->add("$codeDayEx == ");
		$codeDay->add("(");
		$codeDay->over;
			$codeDay->add("&last_day($_[0])");
		$codeDay->back;
		$codeDay->add(")");
		$codeDay->add("? 1");
		$codeDay->add(": 1 + int($codeDayEx)");

		my $codeMonth = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		$codeMonth->add("$codeDayEx == ");
		$codeMonth->add("(");
		$codeMonth->over;
			$codeMonth->add("&last_day($_[0])");
		$codeMonth->back;
		$codeMonth->add(")");
		$codeMonth->add("?");
		$codeMonth->add("(");
		$codeMonth->over;
			$codeMonth->add("$codeMonthEx == 12");
			$codeMonth->add("? 1");
			$codeMonth->add(": 1 + int($codeMonthEx)");
		$codeMonth->back;
		$codeMonth->add(")");
		$codeMonth->add(": $codeMonthEx");

		my $codeYear = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		$codeYear->add("$codeYearEx +");
		$codeYear->add("(");
		$codeYear->over;
			$codeYear->add("$codeMonthEx == 12 && $codeDayEx == 31");
			$codeYear->add("? 1");
			$codeYear->add(": 0");
		$codeYear->back;
		$codeYear->add(")");

		$c->add("");
		$c->over;
		$c->addAll($o_date->codeFmtDate($codeDay, $codeMonth, $codeYear));
		$c->back;
		return $c->sprint;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::DayNumber;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new (@_, name => $params{'name'} || 'day_number',
			fmt => 'date [, date-type ]',
		);
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		my $type = $_[1] || $self->PARAM->properties('default_datetype');

		my $o_date = $self->PARAM->datetypes->exists($type) || $self->PARAM->datetypes->addUserType($type);
		$self->PARAM->error->fatalError("[4010] Invalid date type '$type'") if (!$o_date);
		$o_date->useList->add($self);

		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		$c->add("&d($_[0]) + (&y($_[0]) % 4 == 0 ? \$_FIRST_DAYNUMBER_LEAP[&m($_[0])] : \$_FIRST_DAYNUMBER[&m($_[0])])");
		return $c->sprint;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::AddDays;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new (@_, name => $params{'name'} || 'add_days',
			fmt => 'date, num-days, [, date-type ]',
		);
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		# 1) convert date to a day number, 
		# 2) add days, 
		# 3) convert new daynumber to date 
		my $self = shift;
		my $date = $_[0];
		my $days = $_[1];
		my $type = $_[2] || $self->PARAM->properties('default_datetype');

		my $o_date = $self->PARAM->datetypes->exists($type) || $self->PARAM->datetypes->addUserType($type);
		$self->PARAM->error->fatalError("[4009] Invalid date type '$type'") if (!$o_date);
		$o_date->useList->add($self);

		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		# $year = &y($_[0]);
		# $daynum = $year % 4 == 0 ? $DAYNUMBER_LEAP{$_[0]} : $DAYNUMBER{$_[0]};
		# $daynum += $_[1];
		# while ( $daynum > ($year % 4 == 0  ? $DAYNUMBER_LEAP{&to_date(31,12,$year)} : $DAYNUMBER{&to_date(31,12,$year)}) ) 
		# { 
		#	$daynum -= ($year % 4 == 0 ? $DAYNUMBER_LEAP{&to_date(31,12,$year)} : $DAYNUMBER{&to_date(31,12,$year)}); 
		#	$year++; 
		# }
		# $year % 4 == 0 ? $DAYNUMBER_TO_DATE_LEAP{$daynum} : $DAYNUMBER_TO_DATE{$daynum}

		return $c->sprint;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::Y;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new (@_, name => $params{'name'} || 'y',
			fmt => 'date [, date-type ]',
		);
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		my $type = $_[1] || $self->PARAM->properties('default_datetype');

		my $o_date = $self->PARAM->datetypes->exists($type) || $self->PARAM->datetypes->addUserType($type);
		$self->PARAM->error->fatalError("[4004] Invalid date type '$type'") if (!$o_date);
		$o_date->useList->add($self);

		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		$c->addNonl("substr($_[0], @{[ $o_date->y->pos ]}, @{[ $o_date->y->len ]})");
		return $c->sprint;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::D;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new (@_, name => $params{'name'} || 'd',
			fmt => 'date [, date-type ]',
		);
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		my $type = $_[1] || $self->PARAM->properties('default_datetype');

		my $o_date = $self->PARAM->datetypes->exists($type) || $self->PARAM->datetypes->addUserType($type);
		$self->PARAM->error->fatalError("[4005] Invalid date type '$type'") if (!$o_date);
		$o_date->useList->add($self);

		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		$c->addNonl("substr($_[0], @{[ $o_date->d->pos ]}, @{[ $o_date->d->len ]})");
		return $c->sprint;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::M;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'm',
			fmt => 'date [, date-type ]',
		);
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		my $type = $_[1] || $self->PARAM->properties('default_datetype');

		my $o_date = $self->PARAM->datetypes->exists($type) || $self->PARAM->datetypes->addUserType($type);
		$self->PARAM->error->fatalError("[4006] Invalid date type '$type'") if (!$o_date);
		$o_date->useList->add($self);

		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		if ($o_date->m->len == 3)
		{
			$c->addNonl("substr($_[0], @{[ $o_date->m->pos ]}, @{[ $o_date->m->len ]})");
		}
		else
		{
			$c->addNonl("substr($_[0], @{[ $o_date->m->pos ]}, @{[ $o_date->m->len ]})");
		}
		return $c->sprint;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::Map;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'map',
			fmt => 'table, input_field | output_field',
		);
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		return 
		(
			"join
			(
				\"@{[ $self->PARAM->properties('default_list_delimiter') ]}\",
					map
					(
						@{[ $_[0] =~ /^%/ ? '' : '%' ]}$_[0](\$_),
						split(\/\\s*@{[ $self->PARAM->properties('default_list_delimiter') ]}\\s*\/,$_[1],-1)
					)
			)"
		);
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::Today;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'today');
		bless($self, $class);
		return $self;
	}

	sub codeInit : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);

#! localtime needs to be interpreted at runtime, not during code generation...
		$c->add("use constant _TODAY_YEAR => int @{[ (localtime)[5] + 1900 ]}; # Macro:@{[ $self->name ]}()");
		$c->add("use constant _TODAY_MONTH => int @{[ (localtime)[4]+1 ]}; # Macro:@{[ $self->name ]}()");
		$c->add("use constant _TODAY_DAY => int @{[ (localtime)[3] ]}; # Macro:@{[ $self->name ]}()");
		$c->addNonl(qq/use constant _TODAY => @{[ sprintf("%04d", (localtime)[5] + 1900) ]}/);
		$c->addNonl(qq/@{[ sprintf("%02d", (localtime)[4]+1) ]}/);
		$c->add    (qq/@{[ sprintf("%02d", (localtime)[3]) ]}; # Macro:@{[ $self->name ]}()/);
		return $c;
	}

	sub compile : method
	{
		my $self = shift;
		return ("_TODAY");
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::MonthsSinceCache;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'months_since_c');
		bless($self, $class);
		return $self;
	}

	sub codeInit : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		$c->add("use constant _MONTHS_SINCE_C_TODAY_YEAR => int @{[ (localtime)[5] + 1900 ]}; # Macro:@{[ $self->name ]}()");
		$c->add("use constant _MONTHS_SINCE_C_TODAY_MONTH => int @{[ (localtime)[4]+1 ]}; # Macro:@{[ $self->name ]}()");
		$c->add("my %_CACHE_MONTHS_SINCE;");	# if ($self->root->o_cache_months_since);
		return $c;
	}

	sub compile : method
	{
		my $self = shift;
#>		return ("$_CACHE_MONTHS_SINCE{$_[0]} || ((_MONTHS_SINCE_C_TODAY_YEAR - int(substr($_[0], 0, 4))) * 12) + (_MONTHS_SINCE_C_TODAY_MONTH - int(substr($_[0], 4, 2)))");
		return ("$_[0] !~ /\\d+/ ? 0 : ((_MONTHS_SINCE_C_TODAY_YEAR - int(substr($_[0], 0, 4))) * 12) + (_MONTHS_SINCE_C_TODAY_MONTH - int(substr($_[0], 4, 2)))");
#		return ("((_MONTHS_SINCE_C_TODAY_YEAR - (unpack('a4 a2', $_[0]))[0]) * 12) + (_MONTHS_SINCE_C_TODAY_MONTH - (unpack('a4 a2', $_[0]))[1])");
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::ToArray;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'to_array',
			fmt => 'input_field | output_field',
		);
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		return ("split(\/\\s*@{[ $self->PARAM->properties('default_list_delimiter') ]}\\s*\/,$_[0],-1)");
	}
}
# ----------------------------------------------------------------------------------------------------
{
#>	ETL::Pequel::Type::Macro::Array::Size;	in Type/Macro/Array/Array.pm
	package ETL::Pequel::Type::Macro::ArrSize;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'arr_size');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
#<		return (int(@_) > 1)
#<			? join(" + 1 + ", map("(@{[ $_ =~ /^@/ ? substr($_, 1) : $_ ]} =~ y/@{[ $self->root->o_default_list_delimiter ]}//)", @_))
#<			: "@{[ $_[0] =~ /^@/ ? substr($_[0], 1) : $_[0] ]} =~ y/@{[ $self->root->o_default_list_delimiter ]}//";
#>		Should be:
		foreach (@_) { $_ = "&to_array($_)" unless (/^@/); }
		return join(' + ', map("int($_)", @_));
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::ArrSort;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'arr_sort');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		foreach (@_) { $_ = "&to_array($_)" unless (/^@/); }
		return ("join('@{[ $self->PARAM->properties('default_list_delimiter') ]}', sort(@{[ join(',', @_) ]}))");
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::ArrReverse;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'arr_reverse');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		foreach (@_) { $_ = "&to_array($_)" unless (/^@/); }
		return ("join('@{[ $self->PARAM->properties('default_list_delimiter') ]}', reverse(@{[ join(',', @_) ]}))");
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::ArrValuesUniq;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new (@_, name => $params{'name'} || 'arr_values_uniq');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		foreach (@_) { $_ = "&to_array($_)" unless (/^@/); }
		return ("&{sub { my \%uniq; foreach (@{[ join(',', @_) ]}) { \$uniq{\$_}++; } return join('@{[ $self->PARAM->properties('default_list_delimiter') ]}', keys \%uniq); }}");
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::ArrSum;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new (@_, name => $params{'name'} || 'arr_sum');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		foreach (@_) { $_ = "&to_array($_)" unless (/^@/); }
		return ("&{sub { my \$sum; foreach (@{[ join(',', @_) ]}) { \$sum += \$_; } return \$sum; }}");
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::ArrAvg;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'arr_avg');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		foreach (@_) { $_ = "&to_array($_)" unless (/^@/); }
		return ("&{sub { my \$sum; my \$count; foreach (@{[ join(',', @_) ]}) { \$count++; \$sum += \$_; } \$count ? \$sum / \$count : 0; }}");
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::ArrMean;
	use base qw(ETL::Pequel::Type::Macro::ArrAvg);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'mean');
		bless($self, $class);
		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::ArrShift;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'arr_shift');
		bless($self, $class);
		return $self;
	}

	sub codeInit : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		$c->add("my  \%_ARR_SHIFT; # Macro:@{[ $self->name ]}()");
		return $c;
	}

	sub compile : method
	{
		my $self = shift;
		return ("(@{[ join(',', @_) ]})[\$_ARR_SHIFT{_@{[ $_[0] =~ /^@/ ? substr($_[0], 1) : $_[0] ]}}++]");
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::ArrPop;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'arr_pop');
		bless($self, $class);
		return $self;
	}

	sub codeInit : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		$c->add("my  \%_ARR_POP; # Macro:@{[ $self->name ]}()");
		return $c;
	}

	sub compile : method
	{
		my $self = shift;
		return ("(reverse (@{[ join(',', @_) ]}))[\$_ARR_POP{_@{[ $_[0] =~ /^@/ ? substr($_[0], 1) : $_[0] ]}}++]");
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::ArrPush;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'arr_push');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		return ("push($_[0])");
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::ArrFirst;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'arr_first');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		foreach (@_) { $_ = "&to_array($_)" unless (/^@/); }
		return ("(@{[ join(',', @_) ]})[0]");
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::ArrLast;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'arr_first');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		foreach (@_) { $_ = "&to_array($_)" unless (/^@/); }
		return ("(reverse(@{[ join(',', @_) ]}))[0]");
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::ArrMin;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'arr_min');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		foreach (@_) { $_ = "&to_array($_)" unless (/^@/); }
		return "(sort { \$a <=> \$b }(@{[ join(',', @_) ]}))[0]";
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::ArrMax;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'arr_max');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		foreach (@_) { $_ = "&to_array($_)" unless (/^@/); }
		return "(sort { \$b <=> \$a }(@{[ join(',', @_) ]}))[0]";
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::ArrLookup;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'arr_lookup');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		$_[0] =~ s/['"]//g;
		return "( grep(\$_ =~ /^$_[0]\$/, $_[1]) ? 1 : 0 )";
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::ArrUnpack;
	use base qw(ETL::Pequel::Type::Macro::Element);
	
	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'arr_unpack');
		bless($self, $class);
		return $self;
	}
	
	sub compile : method
	{
		my $self = shift;
		my $delim = $self->PARAM->properties('default_list_delimiter');
		foreach (@_[1..$#_]) { $_ = "&to_array($_)" unless (/^@/); }
		return $#_ > 1 
			? ("join('$delim', unpack($_[0], join('', @{[ join(',', @_[1..$#_]) ]}) ) )")
			: ("join('$delim', unpack($_[0], $_[1] ) )");
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::ArrPack;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'arr_pack');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		foreach (@_[1..$#_]) { $_ = "&to_array($_)" unless (/^@/); }
#> What about repeat fmt's?
		return ("pack($_[0], @{[ join(',', @_[1..$#_]) ]})");
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::Unpack;
	use base qw(ETL::Pequel::Type::Macro::Element);
	
	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'unpack');
		bless($self, $class);
		return $self;
	}
	
	sub compile : method
	{
		my $self = shift;
		my $delim = $self->PARAM->properties('default_list_delimiter');
		return "join('$delim', unpack($_[0], $_[1]))";
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::Pack;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'pack');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		return ("pack($_[0], @{[ join(',', @_[1..$#_]) ]})");
	}
}
# ----------------------------------------------------------------------------------------------------
#>	
#>	  package ETL::Pequel::Type::Macro::InputFieldCount;
#>	  use base qw(ETL::Pequel::Type::Macro::Element);
#>	
#>	  sub new : method
#>	  {
#>	  	my $self = shift;
#>	  	my $class = ref($self) || $self;
#>	  	my %params = @_;
#>	  	$self = $class->SUPER::new(@_, name => $params{'name'} || 'input_field_count');
#>	  	bless($self, $class);
#>	  	return $self;
#>	  }
#>	
#>	  sub compile : method
#>	  {
#>	  	my $self = shift;
#>	  	return ("int(\@I_VAL)"); 
#>	  }
#>	
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::InputRecordCount;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'input_record_count');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		return ("int(@{[ $self->PARAM->properties('use_inline') ? '$i' : '$.' ]})");
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::Lookup;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'lookup');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		return ("%$_[0]($_[1])");
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::Period;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'period');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		return ("%_PERIOD($_[0])");
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::Month;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'month');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		return ("%_MONTH($_[0])");
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::Length;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'length');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		return ("length($_[0])");
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::Substr;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'substr');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		return ("substr($_[0], $_[1] @{[ $_[2] ? ', ' . $_[2] : '' ]})");
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::Index;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'index');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		return ("index($_[0], $_[1] @{[ $_[2] ? ', ' . $_[2] : '' ]})");
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::Sprintf;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'sprintf');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		return ("sprintf(@{[ join(',', @_) ]})");
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::RightIndex;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'rindex');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		return ("rindex($_[0], $_[1] @{[ $_[2] ? ', ' . $_[2] : '' ]})");
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::Lc;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'lc');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		return ("lc($_[0])");
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::Uc;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'uc');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		return ("uc($_[0])");
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::LcFirst;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'lc_first');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		return ("lcfirst($_[0])");
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::UcFirst;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'uc_first');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		return ("ucfirst($_[0])");
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::Commify;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'commify');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		$c->add("&{sub");
		$c->add("{");
		$c->over;
			$c->add("my \$idec = index($_[0], '.');");
			$c->add("my \$dec = \$idec > 0 ? substr($_[0], \$idec) : '';");
			$c->add("my \$txt = reverse(\$idec > 0 ? substr($_[0], 0, \$idec) : $_[0]);");
			$c->add("\$txt =~ s/(\\d\\d\\d)(?=\\d)(?!\\d*\\.)/\$1,/g;");
			$c->add("return (scalar reverse \$txt) . \$dec;");
		$c->back;
		$c->add("}}");
		return $c->sprint;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::Env;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'env');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		return ("$ENV{$_[0]}");
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::Option;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'option');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		return ("@{[ $self->PARAM->properties($_[0]) ]}");
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::MatchAny;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'match_any');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		return "$_[0] =~ m/^(@{[ join('|', @_[1..$#_]) ]})\$/ ? 1 : 0";
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::Match;
	use base qw(ETL::Pequel::Type::Macro::MatchAny);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'match');
		bless($self, $class);
		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::Select;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'select');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		my @args = @_;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);

		my $i;
		for ($i=0; $i < @args -1; $i += 2)
		{
			$c->over;
			$c->add($args[$i]);
			$c->add("? $args[$i+1]");
			$c->addNonl(": ");
		}
		$c->add($args[$i]);
		$c->prepare;
		return $c->text;
	}

	sub format : method
	{
		my $self = shift;
		my @args = @_;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);

		$c->add("&select \\");
		$c->add("( \\");
		$c->over;	
		my $i;
		for ($i=0; $i < @args -1; $i += 2)
		{
			if (length($args[$i]) + length($args[$i+1]) > 85)
			{
				$c->add("@{[ $args[$i] ]}, \\");
				$c->over;
				$c->add("@{[ $args[$i+1] ]}, \\");
				$c->back;
			}
			else
			{
				$c->add("$args[$i], $args[$i+1], \\");
			}
		}
		$c->add("@{[ $args[$i] ]} \\");
		$c->back;
		$c->add(")");
		$c->add;
		return $c;
	}

	sub inWords : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		my $clause = $self->SUPER::compile(shift);
		$clause =~ s/__OPEN__(.*?)__CLOSE__/__SELECT__/;
		my $select = $1;
		my @select = split(/\s*,\s*/, $select, -1);

		my $i;
		my $if = 'If';
		for ($i=0; $i < @select -1; $i += 2)
		{
#>			ETL::Pequel::Type::Macro->lookup->inWords...
			my $cond = $select[$i];
			$c->add("$if $select[$i] then set to $select[$i+1],");
			$if = 'else if';
		}
		$c->add("else set to $select[$i]");
		return $c;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::Generic;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_,
			name => $params{'name'},
			eval => $params{'eval'},
		);
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		return ("@{[ $self->eval ]}") if ($self->eval);
		return ("@{[ $self->name ]}($_[0])");
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::Ord;
	use base qw(ETL::Pequel::Type::Macro::Generic);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new (@_, name => $params{'name'} || 'ord');
		bless($self, $class);
		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::Sqrt;
	use base qw(ETL::Pequel::Type::Macro::Generic);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new (@_, name => $params{'name'} || 'sqrt');
		bless($self, $class);
		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::Rand;
	use base qw(ETL::Pequel::Type::Macro::Generic);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new (@_, name => $params{'name'} || 'rand');
		bless($self, $class);
		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::Sin;
	use base qw(ETL::Pequel::Type::Macro::Generic);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new (@_, name => $params{'name'} || 'sin');
		bless($self, $class);
		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::Exp;
	use base qw(ETL::Pequel::Type::Macro::Generic);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new (@_, name => $params{'name'} || 'exp');
		bless($self, $class);
		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::Cos;
	use base qw(ETL::Pequel::Type::Macro::Generic);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new (@_, name => $params{'name'} || 'cos');
		bless($self, $class);
		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::Log;
	use base qw(ETL::Pequel::Type::Macro::Generic);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new (@_, name => $params{'name'} || 'log');
		bless($self, $class);
		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::Chr;
	use base qw(ETL::Pequel::Type::Macro::Generic);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new (@_, name => $params{'name'} || 'chr');
		bless($self, $class);
		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::Abs;
	use base qw(ETL::Pequel::Type::Macro::Generic);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new (@_, name => $params{'name'} || 'abs');
		bless($self, $class);
		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::Int;
	use base qw(ETL::Pequel::Type::Macro::Generic);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new (@_, name => $params{'name'} || 'int');
		bless($self, $class);
		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::Atan2;
	use base qw(ETL::Pequel::Type::Macro::Generic);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new (@_, name => $params{'name'} || 'atan2');
		bless($self, $class);
		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::Sign;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new (@_, name => $params{'name'} || 'sign');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		return ("($_[0] < 0 ? -1 : $_[0] == 0 ? 0 : 1)");
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::Trunc;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'trunc');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;

#>		Need to validate 2'nd arg.

		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		$c->add("index($_[0], '.') == -1");
		$c->over;
		$c->over;
		$c->add("? sprintf(\"%ld\", ($_[1]>=0 ? $_[0] : substr($_[0], 0, &length($_[0]) + $_[1]) . ('0' x ($_[1] * -1))) )");
		$c->add(": sprintf(\"%ld%s%.*s\", ");
		$c->add("($_[1]>=0 ? $_[0] : substr($_[0], 0, index($_[0], '.') + $_[1]) . ('0' x ($_[1] * -1))), ");
		$c->over;
		$c->add("($_[1]>0  ? '.' : ''), ");
		$c->add("($_[1]<0 ? 0 : $_[1]), ");
		$c->add("substr($_[0], index($_[0], '.')+1), ");
		$c->back;
		$c->add(")");
		$c->back;
		$c->back;
		return $c->sprint;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::Trim;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'trim');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		my $trim_chars = $_[1] || '\s';
		$trim_chars =~ s/['"]//g;
		$c->addNonl("&{sub{");
		$c->addNonl("my \$tmp=$_[0]; ");
		$c->addNonl("while (\$tmp ne '' && substr(\$tmp, 0, 1) =~ /[$trim_chars]/) ");
		$c->addNonl("{ \$tmp = substr(\$tmp, 1); }");
		$c->addNonl(" while (\$tmp ne '' && substr(reverse(\$tmp), 0, 1) =~ /[$trim_chars]/) ");
		$c->addNonl("{ \$tmp = reverse(substr(reverse(\$tmp), 1)); }");
		$c->addNonl(" \$tmp; ");
		$c->addNonl("}}");
		return $c->sprint;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::Spaceout;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'spaceout');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		return "&{sub{ my \$tmp=$_[0]; \$tmp =~ s/\\s+\/ /g; \$tmp; }}";
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::TrimTrailing;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'trim_trailing');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		my $trim_chars = $_[1];
		$trim_chars =~ s/['"]//g;	# only remove quotes at start and end
		#return "&{sub{ my \$tmp=$_[0]; \$tmp =~ s/[!@#\$\%^*()+{}\[\]\\:;<>?/]+//g; \$tmp; }}"
		$c->addNonl("&{sub{");
		$c->addNonl("my \$tmp=$_[0]; ");
		$c->addNonl("while (\$tmp ne '' && substr(reverse(\$tmp), 0, 1) =~ /[$trim_chars]/) ");
		$c->addNonl("{ \$tmp = reverse(substr(reverse(\$tmp), 1)); }");
		$c->addNonl(" \$tmp; ");
		$c->addNonl("}}");
		return $c->sprint;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::TrimLeading;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'trim_leading');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		my $trim_chars = $_[1];
		$trim_chars =~ s/['"]//g;
		$c->addNonl("&{sub{");
		$c->addNonl("my \$tmp=$_[0]; ");
		$c->addNonl("while (\$tmp ne '' && substr(\$tmp, 0, 1) =~ /[$trim_chars]/) ");
		$c->addNonl("{ \$tmp = substr(\$tmp, 1); }");
		$c->addNonl(" \$tmp; ");
		$c->addNonl("}}");
		return $c->sprint;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::ClipStr;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'clip_str');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		return "&{sub{ my \$tmp=$_[0]; \$tmp =~ s/^\\s*(.*?)\\s*\$/\$1/; \$tmp; }}"
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::LeftClipStr;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'left_clip_str');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		return "&{sub{ my \$tmp=$_[0]; \$tmp =~ s/^\\s*//; \$tmp; }}"
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::RightClipStr;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'right_clip_str');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		return "&{sub{ my \$tmp=$_[0]; \$tmp =~ s/\\s*\$//; \$tmp; }}"
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::LeftPadStr;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'left_pad_str');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		return "\"\@{[ $_[1] x ($_[2] - length($_[0])) ]}\@{[ $_[0] ]}\"";
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::RightPadStr;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'right_pad_str');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		return "\"\@{[ $_[0] ]}\@{[ $_[1] x ($_[2] - length($_[0])) ]}\"";
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::RemoveSpaces;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'remove_spaces');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		return "&{sub{ my \$tmp=$_[0]; \$tmp =~ s/\\s+//g; \$tmp; }}"
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::ToNumber;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'to_number');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		return "&{sub{ my \$tmp=$_[0]; \$tmp =~ s/.(?<![0-9])//g; \$tmp; }}"
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::ExtractNumeric;
	use base qw(ETL::Pequel::Type::Macro::ToNumber);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'extract_numeric');
		bless($self, $class);
		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::RemoveNonNumeric;
	use base qw(ETL::Pequel::Type::Macro::ToNumber);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'remove_non_numeric');
		bless($self, $class);
		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::RemoveNumeric;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'remove_numeric');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		return "&{sub{ my \$tmp=$_[0]; \$tmp =~ s/\\d+//g; \$tmp; }}"
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::RemoveSpecial;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'remove_special');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		return "&{sub{ my \$tmp=$_[0]; \$tmp =~ s/[!@#\$\%^*()+{}\\\[\\\]\\\\:;<>?\\/\.]+//g; \$tmp; }}"
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::Translate;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'translate');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		return "&{sub{ my \$tmp=$_[0]; \$tmp =~ tr/$_[1]/$_[2]/@{[ $_[3] ? $_[3] : '' ]}; \$tmp; }}"
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::Initcap;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'initcap');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		$c->addNonl("&{sub{");
		$c->addNonl("my \$tmp=$_[0]; ");
		$c->addNonl("\$tmp=~s/((^\\w)|(\\s\\w))/\\U\$1/g; ");
    	$c->addNonl("\$tmp=~s/([\\w']+)/\\u\\L\$1/g;  ");
		$c->addNonl("\$tmp; ");
		$c->addNonl("}}");
		return $c->sprint;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::ExtractInit;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'extract_init');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		$c->addNonl("join(' ', map(substr(\$_, 0, 1), split(/\\s+/, $_[0])))");
		return $c->sprint;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::Soundex;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_, name => $params{'name'} || 'soundex');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		# Retain the first letter of the string and remove all other occurrences of the 
		# following letters: a, e, h, i, o, u, w, y. 
		# Assign numbers to the remaining letters (after the first) as follows: 
		# b, f, p, v = 1
		# c, g, j, k, q, s, x, z = 2
		# d, t = 3
		# l = 4
		# m, n = 5
		# r = 6
		# 
		# If two or more letters with the same number were adjacent in the original 
		# name (before step 1), or adjacent except for any intervening h and w, 
		# then omit all but the first. 
		# Return the first four bytes padded with 0. 

		my $self = shift;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		$c->addNonl("&{sub{");
		$c->addNonl("my \$f = substr($_[0], 0, 1); ");
		$c->addNonl("my \$rest = substr($_[0], 1); ");
		$c->addNonl("\$rest =~ y/bfpvcgjkqsxzdtlmnraehiouwy/111122222222334556        /d; ");
		$c->addNonl("\$rest =~ y///cs; ");	# remove consecutive duplicate chars.
		$c->addNonl("\$rest =~ s/\\s*//g; ");
		$c->addNonl("\$f = substr(\$f . \$rest, 0, 4); ");
		$c->addNonl("\$f . '0' x ( 4 - length( \$f ) );  ");
		$c->addNonl("}}");
		return $c->sprint;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::Banding;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new (@_, name => $params{'name'} || 'banding');
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		$self->PARAM->error->fatalError("[4012] Invalid band divisor -- zero not allowed") 
			if (!$_[1] || $_[1] == 0);
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
#		$c->addNonl("$_[0] eq '' || $_[0] =~ /^\\s+\$/ ? ''");
#		$c->addNonl(" : $_[0] == 0 ? 'U'");
#		$c->addNonl(" : int(($_[0] - 1) / $_[1]) + 1");
		$c->add("$_[0] eq '' || $_[0] =~ /^\\s+\$/ ? ''");
		$c->over;
		$c->add(": $_[0] == 0 ? 'U'");
		$c->over;
		$c->add(": int(($_[0] - 1) / $_[1]) + 1");
		$c->back;
		$c->back;
		return $c->sprint;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::UserMacro;
	use base qw(ETL::Pequel::Type::Macro::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_,
			name => $params{'name'},
			eval => $params{'eval'},
			external => 1,
		);
		bless($self, $class);
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		my $eval = $self->eval;
		$eval =~ s/_ARGS/@{[ join(',', @_) ]}/g;
		$eval =~ s/_ARG(\d+)/@{[ $_[$1-1] ]}/g;
		return ("$eval");
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::Group::Basic;
	use ETL::Pequel::Collection;	#+++++
	use base qw(ETL::Pequel::Collection::Hierarchy);

	sub new : method
	{
		my $self = shift;
		my $param = shift;
		my $class = ref($self) || $self;
		$self = $class->SUPER::new(@_);
		bless($self, $class);
		$self->addAll
		(
			ETL::Pequel::Type::Macro::Select->new(PARAM => $param),
			ETL::Pequel::Type::Macro::Lookup->new(PARAM => $param),
			ETL::Pequel::Type::Macro::Map->new(PARAM => $param),
			ETL::Pequel::Type::Macro::MatchAny->new(PARAM => $param),		# &match_any(field, val1, val2, ...)
			ETL::Pequel::Type::Macro::Match->new(PARAM => $param),			# same as &match_any()
			ETL::Pequel::Type::Macro::Env->new(PARAM => $param),
			ETL::Pequel::Type::Macro::Option->new(PARAM => $param),
			ETL::Pequel::Type::Macro::Banding->new(PARAM => $param),
#>			ETL::Pequel::Type::Macro::InputFieldCount->new(PARAM => $param),
			ETL::Pequel::Type::Macro::InputRecordCount->new(PARAM => $param),
		);
		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::Group::Date;
	use ETL::Pequel::Collection;	#+++++
	use base qw(ETL::Pequel::Collection::Hierarchy);

	sub new : method
	{
		my $self = shift;
		my $param = shift;
		my $class = ref($self) || $self;
		$self = $class->SUPER::new(@_);
		bless($self, $class);
		$self->addAll
		(
#>			ETL::Pequel::Type::Macro::IsBetween->new,		# &is_between(field, from, to)
#>			ETL::Pequel::Type::Macro::IsIn->new,			# &is_in(field, v1, ...)
			ETL::Pequel::Type::Macro::Date->new(PARAM => $param),
			ETL::Pequel::Type::Macro::Period->new(PARAM => $param),
			ETL::Pequel::Type::Macro::Month->new(PARAM => $param),
			ETL::Pequel::Type::Macro::MonthsSince->new(PARAM => $param),
			ETL::Pequel::Type::Macro::Today->new(PARAM => $param),
#>			ETL::Pequel::Type::Macro::Now->new,
#>			ETL::Pequel::Type::Macro::Time->new,

#>			ETL::Pequel::Type::Macro::DaysBetween->new,		# return num days between 2 dates
			ETL::Pequel::Type::Macro::MonthsBetween->new(PARAM => $param),	# return num months between 2 dates
#>			ETL::Pequel::Type::Macro::DateIsBetween->new,	# &date_is_between(dt1, dt2)
			ETL::Pequel::Type::Macro::D->new(PARAM => $param),				# return day part of date
			ETL::Pequel::Type::Macro::M->new(PARAM => $param),				# return month part of date
			ETL::Pequel::Type::Macro::Y->new(PARAM => $param),				# return year part of date
			ETL::Pequel::Type::Macro::DayNumber->new(PARAM => $param),		# return day number in year
#>			ETL::Pequel::Type::Macro::DayNumberToDate->new,	# return date for day number in year
#>			ETL::Pequel::Type::Macro::AddDays->new,			# return date plus days
#>			ETL::Pequel::Type::Macro::AddMonths->new,		# return date plus months
			ETL::Pequel::Type::Macro::DateLastDay->new(PARAM => $param),		# return date for last day in month/year
			ETL::Pequel::Type::Macro::LastDay->new(PARAM => $param),			# return day for last day in month/year
			ETL::Pequel::Type::Macro::DateNextDay->new(PARAM => $param),		# return date for next day
#>			ETL::Pequel::Type::Macro::ToDate->new,			# 
		);
		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::Group::String;
	use ETL::Pequel::Collection;	#+++++
	use base qw(ETL::Pequel::Collection::Hierarchy);

	sub new : method
	{
		my $self = shift;
		my $param = shift;
		my $class = ref($self) || $self;
		$self = $class->SUPER::new(@_);
		bless($self, $class);
		$self->addAll
		(
			ETL::Pequel::Type::Macro::Length->new(PARAM => $param),
			ETL::Pequel::Type::Macro::Substr->new(PARAM => $param),
			ETL::Pequel::Type::Macro::Index->new(PARAM => $param),
			ETL::Pequel::Type::Macro::RightIndex->new(PARAM => $param),
			ETL::Pequel::Type::Macro::Lc->new(PARAM => $param),
			ETL::Pequel::Type::Macro::Uc->new(PARAM => $param),
			ETL::Pequel::Type::Macro::LcFirst->new(PARAM => $param),
			ETL::Pequel::Type::Macro::UcFirst->new(PARAM => $param),
			ETL::Pequel::Type::Macro::Commify->new(PARAM => $param),

			ETL::Pequel::Type::Macro::Trim->new(PARAM => $param),			# &trim(field, trim-char) -- remove leading & trailing trim-char
			ETL::Pequel::Type::Macro::TrimLeading->new(PARAM => $param),		# or &ltrim()
			ETL::Pequel::Type::Macro::TrimTrailing->new(PARAM => $param),	# or &rtrim()
#?			ETL::Pequel::Type::Macro::Remove->new,			# &remove(field, char-set) -- remove all char-set characters from field
			ETL::Pequel::Type::Macro::ClipStr->new(PARAM => $param),
			ETL::Pequel::Type::Macro::LeftClipStr->new(PARAM => $param),
			ETL::Pequel::Type::Macro::RightClipStr->new(PARAM => $param),
			ETL::Pequel::Type::Macro::LeftPadStr->new(PARAM => $param),
			ETL::Pequel::Type::Macro::RightPadStr->new(PARAM => $param),
			ETL::Pequel::Type::Macro::Translate->new(PARAM => $param),		# translate characters
#>			ETL::Pequel::Type::Macro::Replace->new,			# change words
			ETL::Pequel::Type::Macro::Initcap->new(PARAM => $param),			# return 1st letter of each word uppercase, all othe lowercase
			ETL::Pequel::Type::Macro::ExtractInit->new(PARAM => $param),		# return 1st letter of each word 
			ETL::Pequel::Type::Macro::Soundex->new(PARAM => $param),
			ETL::Pequel::Type::Macro::Spaceout->new(PARAM => $param),		# replace 2 or more successive spaces by one space

			ETL::Pequel::Type::Macro::ToNumber->new(PARAM => $param),
			ETL::Pequel::Type::Macro::ExtractNumeric->new(PARAM => $param),	# same as ToNumber
			ETL::Pequel::Type::Macro::RemoveNonNumeric->new(PARAM => $param),# same as ToNumber
			ETL::Pequel::Type::Macro::RemoveSpecial->new(PARAM => $param),
			ETL::Pequel::Type::Macro::RemoveNumeric->new(PARAM => $param),
			ETL::Pequel::Type::Macro::RemoveSpaces->new(PARAM => $param),
			ETL::Pequel::Type::Macro::Sprintf->new(PARAM => $param),
			ETL::Pequel::Type::Macro::Pack->new(PARAM => $param),
			ETL::Pequel::Type::Macro::Unpack->new(PARAM => $param),
		);
		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::Group::Array;
	use ETL::Pequel::Collection;	#+++++
	use base qw(ETL::Pequel::Collection::Hierarchy);

	sub new : method
	{
		my $self = shift;
		my $param = shift;
		my $class = ref($self) || $self;
		$self = $class->SUPER::new(@_);
		bless($self, $class);
		$self->addAll
		(
			ETL::Pequel::Type::Macro::ToArray->new(PARAM => $param),
			ETL::Pequel::Type::Macro::ArrSize->new(PARAM => $param),
			ETL::Pequel::Type::Macro::ArrSort->new(PARAM => $param),
			ETL::Pequel::Type::Macro::ArrReverse->new(PARAM => $param),
			ETL::Pequel::Type::Macro::ArrShift->new(PARAM => $param),		# shift and remove from source field
			ETL::Pequel::Type::Macro::ArrPop->new(PARAM => $param),			# pop and remove from source field
#>			ETL::Pequel::Type::Macro::ArrPush->new,			# &push(field, value [, ...])
#<			ETL::Pequel::Type::Macro::ArrPeek->new,			# return last element in array
			ETL::Pequel::Type::Macro::ArrFirst->new(PARAM => $param),		# return first element in array
			ETL::Pequel::Type::Macro::ArrLast->new(PARAM => $param),			# return last element in array
			ETL::Pequel::Type::Macro::ArrMin->new(PARAM => $param),			# return lowest value in array
			ETL::Pequel::Type::Macro::ArrMax->new(PARAM => $param),			# return highest value in array
			ETL::Pequel::Type::Macro::ArrAvg->new(PARAM => $param),			# return average value in array
#>			ETL::Pequel::Type::Macro::ArrAvgDistinct->new,	# return average for distinct values in array
			ETL::Pequel::Type::Macro::ArrMean->new(PARAM => $param),			# return average value in array
			ETL::Pequel::Type::Macro::ArrSum->new(PARAM => $param),			# return sum value in array
#>			ETL::Pequel::Type::Macro::ArrSumDistinct->new,	# return sum for distinct values in array
			ETL::Pequel::Type::Macro::ArrLookup->new(PARAM => $param),		# return 1 if 1st par exists in array else 0
#>			ETL::Pequel::Type::Macro::ArrPack->new(PARAM => $param),
#>			ETL::Pequel::Type::Macro::ArrUnpack->new(PARAM => $param),

#>v3		ETL::Pequel::Type::Macro::ArrSelect->new,	# or ArrFilter: &arr_select(<field>, <value|/regex/>)
#>v3		ETL::Pequel::Type::Macro::ArrDistinct->new,		# return count of distinct values
#>v3		ETL::Pequel::Type::Macro::ArrMedian->new,
#>v3		ETL::Pequel::Type::Macro::ArrVariance->new,
#>v3		ETL::Pequel::Type::Macro::ArrStddev->new,
#>v3		ETL::Pequel::Type::Macro::ArrRange->new,
#>v3		ETL::Pequel::Type::Macro::ArrMode->new,
			ETL::Pequel::Type::Macro::ArrValuesUniq->new(PARAM => $param),

#>			ETL::Pequel::Type::Macro::ArrLongest->new,		# return longest string in array
#>			ETL::Pequel::Type::Macro::ArrShortest->new,		# return longest string in array
#>			ETL::Pequel::Type::Macro::ArrString->new,		# combine elements into string
#>			ETL::Pequel::Type::Macro::Split->new,

#>			ETL::Pequel::Type::Macro::ArrSetAnd->new,
#>			ETL::Pequel::Type::Macro::ArrSetOr->new,
#>			ETL::Pequel::Type::Macro::ArrSetXOr->new,
		);
		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::Group::Arithmetic;
	use ETL::Pequel::Collection;	#+++++
	use base qw(ETL::Pequel::Collection::Hierarchy);

	sub new : method
	{
		my $self = shift;
		my $param = shift;
		my $class = ref($self) || $self;
		$self = $class->SUPER::new(@_);
		bless($self, $class);
		$self->addAll
		(
			ETL::Pequel::Type::Macro::Sqrt->new(PARAM => $param),
			ETL::Pequel::Type::Macro::Rand->new(PARAM => $param),
			ETL::Pequel::Type::Macro::Ord->new(PARAM => $param),
			ETL::Pequel::Type::Macro::Log->new(PARAM => $param),
			ETL::Pequel::Type::Macro::Sin->new(PARAM => $param),
			ETL::Pequel::Type::Macro::Exp->new(PARAM => $param),
			ETL::Pequel::Type::Macro::Cos->new(PARAM => $param),
			ETL::Pequel::Type::Macro::Chr->new(PARAM => $param),
			ETL::Pequel::Type::Macro::Abs->new(PARAM => $param),
			ETL::Pequel::Type::Macro::Int->new(PARAM => $param),
			ETL::Pequel::Type::Macro::Atan2->new(PARAM => $param),
			ETL::Pequel::Type::Macro::Sign->new(PARAM => $param),
			ETL::Pequel::Type::Macro::Trunc->new(PARAM => $param),

#>v3		ETL::Pequel::Type::Macro::LeftShift,			# numeric shift
#>v3		ETL::Pequel::Type::Macro::RightShift,			# numeric shift
		);
		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro::Group::Other;
	use ETL::Pequel::Collection;	#+++++
	use base qw(ETL::Pequel::Collection::Hierarchy);

	sub new : method
	{
		my $self = shift;
		my $param = shift;
		my $class = ref($self) || $self;
		$self = $class->SUPER::new(@_);
		bless($self, $class);
		$self->addAll
		(
#>v3		ETL::Pequel::Type::Macro::Pequel->new,
#>v3		ETL::Pequel::Type::Macro::Switch->new,	#--> same as ? :

#>v3		ETL::Pequel::Type::Macro::Lag,				# &lag(value-exp, offset, default)
#>v3		#It provides access to more than one row of a table at the same time without a 
#>v3		#self join. Given a series of rows returned from a query and a position of the 
#>v3		#cursor, LAG provides access to a row at a given physical offset prior to that 
#>v3		#position.
#>v3		#If you do not specify offset, then its default is 1. The optional default value 
#>v3		#is returned if the offset goes beyond the scope of the window. If you do not 
#>v3		#specify default, then its default value is null.
#>v3
#>v3		ETL::Pequel::Type::Macro::Lead,				# &lead(value-exp, offset, default)
#>v3		#It provides access to more than one row of a table at the same time without a 
#>v3		#self join. Given a series of rows returned from a query and a position of the 
#>v3		#cursor, LEAD provides access to a row at a given physical offset beyond that 
#>v3		#position.
#>v3		#If you do not specify offset, then its default is 1. The optional default value 
#>v3		#is returned if the offset goes beyond the scope of the table. If you do not 
#>v3		#specify default, then its default value is null.
#>v3
#>v3		ETL::Pequel::Type::Macro::ArrWidthBucket,
#>v3		ETL::Pequel::Type::Macro::ArrNtile,
#>v3		ETL::Pequel::Type::Macro::ArrRank,
#>v3		ETL::Pequel::Type::Macro::ArrCorr,
#>v3		ETL::Pequel::Type::Macro::ArrCumeDist,
#>v3		ETL::Pequel::Type::Macro::ArrCovarPop,
#>v3		ETL::Pequel::Type::Macro::ArrCovarSamp,
#>v3		ETL::Pequel::Type::Macro::ArrPercentRank,
#>v3		ETL::Pequel::Type::Macro::ArrPercentileCont,
#>v3		ETL::Pequel::Type::Macro::ArrPercentileDisc,
		);
		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Macro;
	use ETL::Pequel::Collection;	#+++++
	use base qw(ETL::Pequel::Collection::Hierarchy);

	our $this = __PACKAGE__;

	sub BEGIN
	{
		our @attr =
		qw(
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

		$self->addAll
		(
			ETL::Pequel::Type::Macro::Group::Basic->new($self->PARAM),
			ETL::Pequel::Type::Macro::Group::Date->new($self->PARAM),
			ETL::Pequel::Type::Macro::Group::String->new($self->PARAM),
			ETL::Pequel::Type::Macro::Group::Array->new($self->PARAM),
			ETL::Pequel::Type::Macro::Group::Arithmetic->new($self->PARAM),
#>			ETL::Pequel::Type::Macro::Group::Other->new($self->PARAM),
		);

		return $self;
	}

	sub codeInit : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		if 
		(
			$self->exists('day_number')->useList->size 
			|| $self->exists('last_day')->useList->size 
			|| $self->exists('date_last_day')->useList->size 
			|| $self->exists('date_next_day')->useList->size 
		)
		{
			$c->add("my \@_LAST_DAY_LEAP = (0,31,29,31,30,31,30,31,31,30,31,30,31);");
			$c->add("my \@_LAST_DAY = (0,31,28,31,30,31,30,31,31,30,31,30,31);");
		}

		if 
		(
			$self->exists('day_number')->useList->size 
		)
		{
			$c->add("my \@_FIRST_DAYNUMBER_LEAP = (0,1,32,61,92,122,153,183,214,245,275,306,336);");
			$c->add("my \@_FIRST_DAYNUMBER = (0,1,32,60,91,121,152,182,213,244,274,305,335);");

			$c->add("my \%_DAYNUMBER_TO_MONTH;");
			$c->add("my \$d=0; foreach my \$m (1..12) { foreach (1..\$_LAST_DAY[\$m]) { \$_DAYNUMBER_TO_MONTH{++\$d} = \$m; } }");
			$c->add("my \%_DAYNUMBER_TO_MONTH_LEAP;");
			$c->add("my \$d=0; foreach my \$m (1..12) { foreach (1..\$_LAST_DAY_LEAP[\$m]) { \$_DAYNUMBER_TO_MONTH_LEAP{++\$d} = \$m; } }");
		}

		if (grep($_->useList->size && $_->m->len > 2, $self->PARAM->datetypes->toArray))
		{
			$c->add("my \%MONTH_NUM = ");
			$c->add("(");
			$c->over;
			map
			(
				$c->add("@{[ $_->name ]} => '@{[ $_->value ]}',"),
				$self->PARAM->monthtypes->toArray
			);
			$c->endList;
			$c->back;
			$c->add(");");
			$c->add("my \%MONTH_NAME;  foreach my \$m (keys \%MONTH_NUM) { \$MONTH_NAME{\$MONTH_NUM{\$m}} = \$m; }");
		}
		return $c;
	}
}
# ----------------------------------------------------------------------------------------------------
1;
