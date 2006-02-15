#!/usr/bin/perl
# ----------------------------------------------------------------------------------------------------
#  Name		: ETL::Pequel.pm
#  Created	: 27 October 2005
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
# 30/10/2005	2.4-1		gaffie	Initial version.
# ----------------------------------------------------------------------------------------------------
require 5.005_62;
use strict;
use attributes qw(get reftype);
use warnings;
use vars qw($VERSION $BUILD);
$VERSION = "2.4-6";
$BUILD = 'Wednesday November 23 10:17:15 GMT 2005';
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel;
	use UNIVERSAL qw( isa can );

	use constant OPTIONS 							=> 'options';
	use constant PROPERTIES 						=> 'options'; # alternate name
	use constant DESCRIPTION 						=> 'description section';
	use constant INPUT_SECTION	 					=> 'input section';
	use constant GROUP_BY 							=> 'group by';
	use constant SORT_BY 							=> 'sort by';
	use constant SORT_INPUT							=> 'sort by'; # alternate name
	use constant SORT_OUTPUT 						=> 'sort output';
	use constant LOAD_TABLE 						=> 'load table';
	use constant LOAD_TABLE_PEQUEL 					=> 'load table pequel';
	use constant INIT_TABLE 						=> 'init table';
	use constant FILTER		 						=> 'filter';
	use constant FILTER_INPUT		 				=> 'filter'; # alternate name
	use constant REJECT		 						=> 'reject';
	use constant OUTPUT_SECTION						=> 'output section';
	use constant HAVING		 						=> 'having';
	use constant FILTER_OUTPUT		 				=> 'having'; # alternate name
	use constant DEDUP_ON		 					=> 'dedup on';
	use constant USE_PACKAGE 						=> 'use package';
	use constant FIELD_PREPROCESS 					=> 'field preprocess';
	use constant FIELD_POSTPROCESS 					=> 'field postprocess';
	use constant DIVERT_INPUT_RECORD 				=> 'divert input record';
	use constant COPY_INPUT_RECORD 					=> 'copy input record';
	use constant DIVERT_OUTPUT_RECORD 				=> 'divert output record';
	use constant COPY_OUTPUT_RECORD 				=> 'copy output record';
	use constant DISPLAY_MESSAGE_ON_INPUT			=> 'display message on input';
	use constant DISPLAY_MESSAGE_ON_INPUT_ABORT		=> 'display message on input abort';
	use constant DISPLAY_MESSAGE_ON_OUTPUT			=> 'display message on output';
	use constant DISPLAY_MESSAGE_ON_OUTPUT_ABORT	=> 'display message on output abort';

	sub new : method
	{
		my $proto = shift;
		my $parent = shift || undef;
		my $class = ref($proto) || $proto;
		my $self = {};
		bless($self, $class);

		use lib qw(./lib);
		use ETL::Pequel::Param;
		$self->PARAM(ETL::Pequel::Param->new());
		defined($parent) 
			? $self->PARAM->root($parent->PARAM->root) 
			: $self->PARAM->root($self); # Not sure
		$self->initRc();
		$self->section('options')->addItem(name => 'script_name', value => $0);

		return $self;
	}

	sub prepare : method
	{
		my $self = shift;

		$self->PARAM->SCRIPT->compile();
		$self->PARAM->SCRIPT->check();
#?		$self->PARAM->options->cmd_prep(); #??
#?		$self->PARAM->options->cmd_exec(); #??
	}

	sub generate : method
	{
		my $self = shift;

		$self->PARAM->ENGINE->generate();
		$self->PARAM->ENGINE->prepare();
#?		$self->PARAM->options->cmd_postgen(); #??
	}

	sub execute : method
	{
		my $self = shift;
		$self->PARAM->ENGINE->execute();
	}

	sub check : method
	{
		my $self = shift;
		return $self->PARAM->ENGINE->check();
	}

	sub section : method
	{
		my $self = shift;
		my $name = shift;
		my $derive = shift || undef;

		return $self->PARAM->sections->find($name)
			|| $self->PARAM->error->fatalError("[101] Unknown section name '$name'.");
	}

	sub engine : method
	{
		my $self = shift;
		return $self->PARAM->ENGINE();
	}

	sub initRc : method
	{
		my $self = shift;
		# Open the pequel init file ~/.pequelrc and set any options
		if (-e "$ENV{HOME}/.pequelrc")
		{
			$self->PARAM->error->msgStderr("reading ~/.pequelrc...") if ($self->PARAM->properties('debug'));
			open(PequelRC, "$ENV{HOME}/.pequelrc");
			while (<PequelRC>)
			{
				chomp;
				last if (/__END__/);
				s/#.*//;		# remove comments
				s/\/\/.*//g;	# remove c style comment lines if not cpp'd
				s/^\s*//;
				s/\s*$//;
				s/\s*,$//;
				next if ($_ eq '');

				if (/^&|^macro/)	# its a macro
				{
					s/^&\s*|^macro\s*//;
					my $name = substr($_, 0, index($_, '=')-1);
					my $exp = substr($_, index($_, '=')+1, length($_)-index($_, '='));
					$self->PARAM->macros->add
					(
						ETL::Pequel::Type::UserMacro->new
						(
							name => $name, 
							eval => $exp,
							PARAM => $self->PARAM
						)
					);
				}
				elsif (/^aggregate/)	# its an aggregate
				{
				}
				elsif (/^datetype/)	# its a datetype
				{
				}
				else # assume its an option
				{
					my ($name, $value) = split(/\s*=\s*/);
					my $o;
					if 
					(
						($o = $self->PARAM->options->exists($name))
						|| ($o = $self->PARAM->options->getAlias($name))
					)
					{
						$self->PARAM->error->msgStderr("->$name=$value;") if ($self->PARAM->properties('debug'));
						$self->PARAM->sections->exists('options')->items->add(ETL::Pequel::Field::Element->new
						(
							name => $o->name, 
							value => $value,
							comment => '_PequelRC',
							ref => $o,
							PARAM => $self->PARAM
						));
					}
				}
			}	
		}
	}

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
		#
		# Refer to this table for argument names and requirements for the addItem()
		# function per section type.
		# prefix '_' indicates optional parameter.
		# When calling the addItem() function omit the prefix '_'.
		# 
		our %section_defs = 
		(
			options 							=> [ qw(name _value) ],
			properties 							=> [ qw(name _value) ],
			field_preprocess 					=> [ qw(name type _operator _calc) ],
			field_postprocess 					=> [ qw(name type _operator _calc) ],
			description							=> [ qw(value) ],
			use_package							=> [ qw(value) ],
			input_section	 					=> [ qw(name _type _operator _calc) ],
			output_section						=> [ qw(type field _clause) ],
			filter		 						=> [ qw(value) ],
			reject		 						=> [ qw(value) ],
			sort_by 							=> [ qw(fld _type _sort) ],
			sort_input 							=> [ qw(fld _type _sort) ],
			group_by 							=> [ qw(fld _type _sort) ],
			sort_output 						=> [ qw(fld _type) ],
			dedup_on		 					=> [ qw(fld _type) ],
			having		 						=> [ qw(value) ],
			divert_input_record 				=> [ qw(value) ],
			copy_input_record 					=> [ qw(value) ],
			divert_output_record 				=> [ qw(value) ],
			copy_output_record 					=> [ qw(value) ],
			display_message_on_input			=> [ qw(value) ],
			display_message_on_input_abort		=> [ qw(value) ],
			display_message_on_output			=> [ qw(value) ],
			display_message_on_output_abort		=> [ qw(value) ],
			init_table 							=> [ qw(name key values) ],
			load_table 							=> [ qw(name filename keycol keytype field_list) ],
			load_table_pequel 					=> [ qw(name scriptname keyfield _keytype) ],
#TBD		load_table_merge 					=> [ qw(table_name filename keycol keytype field_list) ],
#TBD		load_table_pequel_merge 			=> [ qw(table_name script_name keyfield keytype) ],
#TBD		load_table_sqlite 					=> [ qw() ],
#TBD		load_table_sqlite_merge 			=> [ qw() ],
#TBD		load_table_oracle 					=> [ qw() ],
#TBD		load_table_oracle_merge 			=> [ qw() ],
#TBD		load_table_mysql 					=> [ qw() ],
#TBD		load_table_mysql_merge 				=> [ qw() ],
#TBD		load_table_sybase 					=> [ qw() ],
#TBD		load_table_sybase_merge 			=> [ qw() ],
		);
	}
}
1;
# ----------------------------------------------------------------------------------------------------
