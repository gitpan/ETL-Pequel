#!/usr/bin/perl
# ----------------------------------------------------------------------------------------------------
#  Name		: ETL::Pequel::Type::Date.pm
#  Created	: 5 February 2005
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
# 01/12/2005	2.4-6		gaffie	fixed addUserType -- required PARAM.
# ----------------------------------------------------------------------------------------------------
# TO DO:
# ----------------------------------------------------------------------------------------------------
require 5.005_62;
use strict;
use attributes qw(get reftype);
use warnings;
use vars qw($VERSION $BUILD);
$VERSION = "2.4-6";
$BUILD = 'Thursday December  1 22:33:01 GMT 2005';
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Date::Part;
	use ETL::Pequel::Type;	#+++++
	use base qw(ETL::Pequel::Type::Element);

	our $this = __PACKAGE__;

	sub BEGIN
	{
		our @attr =
		qw(
			pos
			len
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

		$self->pos($params{'pos'});
		$self->len($params{'len'});

		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Date::Element;
	use ETL::Pequel::Type;	#+++++
	use base qw(ETL::Pequel::Type::Element);
	use ETL::Pequel::Code;	#+++++

	our $this = __PACKAGE__;

	sub BEGIN
	{
		our @attr =
		qw(
			d
			m
			y
			regEx
			fmt
			delimiter
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

		$self->fmt($params{'fmt'} || $params{'name'});
		$self->regEx($params{'regex'});
	
		$self->d(ETL::Pequel::Type::Date::Part->new
		(
			name => 'day', 
			pos => index($self->fmt, 'D'), 
			len => rindex($self->fmt, 'D') - index($self->fmt, 'D') + 1,
			PARAM => $self->PARAM
		));

		$self->m(ETL::Pequel::Type::Date::Part->new
		(
			name => 'month', 
			pos => index($self->fmt, 'M'), 
			len => rindex($self->fmt, 'M') - index($self->fmt, 'M') + 1,
			PARAM => $self->PARAM
		));

		$self->y(ETL::Pequel::Type::Date::Part->new
		(
			name => 'year', 
			pos => index($self->fmt, 'Y'), 
			len => rindex($self->fmt, 'Y') - index($self->fmt, 'Y') + 1,
			PARAM => $self->PARAM
		));

		$self->delimiter((grep(!/[DMY]/i, split(//, $self->name)))[0]);

		return $self;
	}

    sub codeCmpDate : method 
	{ 
		my $self = shift; 
		my $d1 = shift;
		my $d2 = shift;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);

		if ($self->fmt eq 'YYYYMMDD')
		{
			$c->add("my \$cmp = $d1 <=> $d2;");
			return $c;
		}
	
		if ($self->fmt eq 'YYMMDD')
		{
			$c->add("my \$yr1 = substr($d1, 0, 2) < 20 ? '20' : '19'");
			$c->add("my \$yr2 = substr($d2, 0, 2) < 20 ? '20' : '19'");
			$c->add("my \$cmp = scalar(\$yr1 . $d1) <=> scalar(\$yr2 . $d2);");
			return $c;
		}

		$c->add('my $cmp;');
		$c->add("if ($d1 eq $d2) { \$cmp = 0; }");
		$c->add("else {");
		$c->over;
			if ($self->y->len == 2)
			{
				$c->add("my \$yr1 = (int(substr(qq{$d1}, @{[ $self->y->pos ]}, 2)) < 20 ? '20' : '19') . substr(qq{$d1}, @{[ $self->y->pos ]}, 2)");
				$c->add("my \$yr2 = (int(substr(qq{$d2}, @{[ $self->y->pos ]}, 2)) < 20 ? '20' : '19') . substr(qq{$d2}, @{[ $self->y->pos ]}, 2)");
				$c->add("if ((\$cmp = ((\$yr1 . substr($d1, @{[ $self->y->pos ]}, 2)) <=> (\$yr2 . substr($d2, @{[ $self->y->pos ]}, 2)))) == 0) {");
			}
			else
			{
				$c->add("if ((\$cmp = (substr($d1, @{[ $self->y->pos ]}, 4) <=> substr($d2, @{[ $self->y->pos ]}, 4))) == 0) {");
			}
			$c->over;
				if ($self->m->len == 2)
				{
					$c->addNonl("if ((\$cmp = (substr($d1, @{[ $self->m->pos ]}, @{[ $self->m->len ]}) ");
					$c->add("<=> substr($d2, @{[ $self->m->pos ]}, @{[ $self->m->len ]}))) == 0) {");
				}
				else
				{
					$c->addNonl("if ((\$cmp = (\$MONTH_NUM{substr($d1, @{[ $self->m->pos ]}, @{[ $self->m->len ]})} ");
					$c->add("<=> \$MONTH_NUM{substr($d2, @{[ $self->m->pos ]}, @{[ $self->m->len ]})})) == 0) {");
				}
				$c->over;
					$c->addNonl("\$cmp = (substr($d1, @{[ $self->d->pos ]}, @{[ $self->d->len ]}) ");
					$c->add("<=> substr($d2, @{[ $self->d->pos ]}, @{[ $self->d->len ]}));");
				$c->back;
				$c->add("}");
			$c->back;
			$c->add("}");
		$c->back;
		$c->add("}");
		return $c;
	}

	sub codeToCCYYMMDD : method
	{
		my $self = shift;
		my $dt = shift;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);

		$c->addNonl("scalar(");
		if ($self->fmt eq 'YYYYMMDD')
		{
			$c->addNonl("qq{$dt}");
		}
		else
		{
			($self->y->len == 4)
				? $c->addNonl("substr(qq{$dt}, @{[ $self->y->pos ]}, 4)")
				: $c->addNonl("(int(substr(qq{$dt}, @{[ $self->y->pos ]}, 2)) < 20 ? '20' : '19') . substr(qq{$dt}, @{[ $self->y->pos ]}, 2)");

			($self->m->len == 2)
				? $c->addNonl(". substr(qq{$dt}, @{[ $self->m->pos ]}, 2)")
				: $c->addNonl(". \$MONTH_NUM{substr(qq{$dt}, @{[ $self->m->pos ]}, @{[ $self->m->len ]})}");

			$c->addNonl(". substr(qq{$dt}, @{[ $self->d->pos ]}, 2)");
		}
		$c->addNonl(")");
		return $c;
	}

	sub codeFmtDate : method
	{
		my $self = shift;
		my $d = shift;
		my $m = shift;
		my $y = shift;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);

		foreach my $i (0..2)
		{
			if ($i)
			{
				$c->add(".");
				$c->add("'@{[ $self->delimiter ]}' .") if (defined($self->delimiter));
			}

			if (($self->dmyOrder)[$i] eq 'D')
			{
				$c->add("(");
				$c->over;
					ref($d) ? $c->addAll($d) : $c->add($d);
				$c->back;
				$c->add(")");
			}
			elsif (($self->dmyOrder)[$i] eq 'M')
			{
				$c->add("(");
				$c->over;
					ref($d) ? $c->addAll($m) : $c->add($m);
				$c->back;
				$c->add(")");
			}
			elsif (($self->dmyOrder)[$i] eq 'Y')
			{
				$c->add("(");
				$c->over;
					ref($d) ? $c->addAll($y) : $c->add($y);
				$c->back;
				$c->add(")");
			}
		}
		return $c;
	}

	sub dmyOrder : method
	{
		my $self = shift;
		my $order = $self->name;
		$order =~ y///cs;
		return defined($self->delimiter) 
			? map(uc, split(/@{[ $self->delimiter ]}/, $order))
			: map(uc, split(//, $order));
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Date;
	use ETL::Pequel::Collection;	#+++++
	use base qw(ETL::Pequel::Collection::Vector);

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
			ETL::Pequel::Type::Date::Element->new(name => 'DD/MM/YYYY', PARAM => $param,
					regex => '\d{2}\/\d{2}\/\d{4}'),
			ETL::Pequel::Type::Date::Element->new(name => 'DD/MM/YY',  	PARAM => $param,
				regex => '\d{2}\/\d{2}\/\d{2}'),
			ETL::Pequel::Type::Date::Element->new(name => 'DDMMYY',		PARAM => $param,
				regex => '\d{2}\d{2}\d{2}'),
			ETL::Pequel::Type::Date::Element->new(name => 'DDMMYYYY',	PARAM => $param,
				regex => '\d{2}\d{2}\d{4}'),
			ETL::Pequel::Type::Date::Element->new(name => 'DDMMMYY',	PARAM => $param,
				regex => '\d{2}\w{3}\d{2}'),
			ETL::Pequel::Type::Date::Element->new(name => 'YYYYMMDD',	PARAM => $param,
				regex => '\d{4}\d{2}\d{2}'),
			ETL::Pequel::Type::Date::Element->new(name => 'YYMMDD',		PARAM => $param,
				regex => '\d{2}\d{2}\d{2}'),
			ETL::Pequel::Type::Date::Element->new(name => 'MM/DD/YYYY',	PARAM => $param,
				regex => '\d{2}\/\d{2}\/\d{2}'),
			ETL::Pequel::Type::Date::Element->new(name => 'MM/DD/YY',  	PARAM => $param,
				regex => '\d{2}\/\d{2}\/\d{2}'),
			ETL::Pequel::Type::Date::Element->new(name => 'MMDDYY',		PARAM => $param,
				regex => '\d{2}\d{2}\d{2}'),
			ETL::Pequel::Type::Date::Element->new(name => 'MMDDYYYY',	PARAM => $param,
				regex => '\d{2}\d{2}\d{4}'),
			ETL::Pequel::Type::Date::Element->new(name => 'YYYY-MM-DD',	PARAM => $param,
				regex => '\d{4}\-\d{2}\-\d{2}'),
			ETL::Pequel::Type::Date::Element->new(name => 'YY-MM-DD',	PARAM => $param,
				regex => '\d{2}\-\d{2}\-\d{2}'),
		);
		return $self;
	}

	sub addUserType : method
	{
		my $self = shift;
		my $user_type = shift;
		if 
		(
			$user_type =~ /D{2}/
			&& $user_type =~ /M{2,3}/
			&& ($user_type =~ /Y{2}/ || $user_type =~ /Y{4}/)
		)
		{
			my $regex = $user_type;
			$regex =~ s/(?![D|M|Y])/\\/g;
			$regex =~ s/\\$//;
			$regex =~ s/(D+)/\\d{@{[ length($1) ]}}/;
			$regex =~ s/MMM/\\w{3}/;
			$regex =~ s/MM/\\d{2}/;
			$regex =~ s/(Y+)/\\d{@{[ length($1) ]}}/;
			$self->add(ETL::Pequel::Type::Date::Element->new
			(
				name => $user_type, 
				regex => $regex, 
				PARAM => $self->PARAM
			));
			return $self->last;
		}
		return 0;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Month;
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
		$self->addAll
		(
			ETL::Pequel::Type::Element->new(name => 'JAN',	number => 1,	value => '01', PARAM => $param),
			ETL::Pequel::Type::Element->new(name => 'FEB',	number => 2,	value => '02', PARAM => $param),
			ETL::Pequel::Type::Element->new(name => 'MAR',	number => 3,	value => '03', PARAM => $param),
			ETL::Pequel::Type::Element->new(name => 'APR',	number => 4,	value => '04', PARAM => $param),
			ETL::Pequel::Type::Element->new(name => 'MAY',	number => 5,	value => '05', PARAM => $param),
			ETL::Pequel::Type::Element->new(name => 'JUN',	number => 6,	value => '06', PARAM => $param),
			ETL::Pequel::Type::Element->new(name => 'JUL',	number => 7,	value => '07', PARAM => $param),
			ETL::Pequel::Type::Element->new(name => 'AUG',	number => 8,	value => '08', PARAM => $param),
			ETL::Pequel::Type::Element->new(name => 'SEP',	number => 9,	value => '09', PARAM => $param),
			ETL::Pequel::Type::Element->new(name => 'OCT',	number => 10,	value => '10', PARAM => $param),
			ETL::Pequel::Type::Element->new(name => 'NOV',	number => 11,	value => '11', PARAM => $param),
			ETL::Pequel::Type::Element->new(name => 'DEC',	number => 12,	value => '12', PARAM => $param),
		);
		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
1;
