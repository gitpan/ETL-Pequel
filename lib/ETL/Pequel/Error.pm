#!/usr/bin/perl
# ----------------------------------------------------------------------------------------------------
#  Name		: ETL::Pequel::Error.pm
#  Created	: 21 April 2005
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
$VERSION = "2.4-3";
$BUILD = 'Tuesday November  1 08:45:13 GMT 2005';
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Error;
#<	use Pequel::Base;

	use constant ERROR_FATAL => int 1;
	use constant ERROR_WARNING => int 2;

	our $this = __PACKAGE__;
	sub BEGIN
	{
		our @attr =
		qw(
			PARAM
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
		my %params = @_;
		my $class = ref($proto) || $proto;
		my $self = {};
		bless($self, $class);
		$self->PARAM($params{'PARAM'});
		return $self;
	}

	sub error : method
	{
		my $self = shift;
		my $error_type = shift;
		my $descript = shift;

		print STDERR ($error_type == ETL::Pequel::Error::ERROR_WARNING ? "Warning" : "Error");
		print STDERR ": $descript\n";
		exit if ($error_type == ETL::Pequel::Error::ERROR_FATAL);
	}

	sub warning : method
	{
		my $self = shift;
		$self->error(ETL::Pequel::Error::ERROR_WARNING, shift);
	}

	sub fatalError : method
	{
		my $self = shift;
		print STDERR "Called from:", caller(), "\n" if ($self->PARAM->properties('debug_show_caller'));
		$self->error(ETL::Pequel::Error::ERROR_FATAL, shift);
	}
		
	sub msgStderr : method
	{
		my $self = shift;
		my $msg = shift;
		return if ($self->PARAM->properties('noverbose'));
		print STDERR $msg, "\n";
	}

	sub msgStderrNonl : method
	{
		my $self = shift;
		my $msg = shift;
		return if ($self->PARAM->properties('noverbose'));
		print STDERR $msg;
	}
}
# ----------------------------------------------------------------------------------------------------
1;
