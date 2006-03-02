#!/usr/bin/perl
# ----------------------------------------------------------------------------------------------------
#  Name		: ETL::Pequel::Main.pm
#  Created	: 14 January 2005
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
# 02/03/2006	2.4-6b		gaffie	Fixed tests.
# 15/02/2006	2.4-6		gaffie	Fixed module name in ETL::Pequel::Pod2Pdf, scripts/pequelpod2pdf.
# 26/01/2006	2.4-6		gaffie	vim-syntax: fix description section.
# 01/12/2005	2.4-6		gaffie	fixed date.addUserType -- required PARAM.
# 30/11/2005	2.4-6		gaffie	exec_min_lines -- use exec() if code > lines.
# 16/11/2005	2.4-5		gaffie	Bug fix -- input record line counter.
# 15/11/2005	2.4-5		gaffie	Bug fix in sort-output section codeOpen() extra pipe.
# 11/11/2005	2.4-5		gaffie	new option show_synonyms -- 
# 04/11/2005	2.4-5		gaffie	Bug fix numeric/decimal type checking for nulls/nonulls options.
# 04/11/2005	2.4-5		gaffie	Bug fix numeric/decimal type comparison in dedup-on.
# 03/11/2005	2.4-4		gaffie	Bug fix field-process sections.
# 03/11/2005	2.4-4		gaffie	Bug fix in reject-section.
# 01/11/2005	2.4-2		gaffie	Fixed test failures -- caused by CPP parsing '#' comment.
# 26/10/2005	2.4-1		gaffie	ETL::Pequel.pm
# 26/10/2005	2.3-6		gaffie	display message section types.
# 26/10/2005	2.3-6		gaffie	&input_record_count() macros.
# 26/10/2005	2.3-6		gaffie	&arr_pack(), &arr_unpack() macros.
# 26/10/2005	2.3-6		gaffie	&pack(), &unpack() macros.
# 24/09/2005	2.3-6		gaffie	unpack_input/pack_output implementation.
# 19/10/2005	2.3-5		gaffie	Added gzcat_cmd, gzcat_args, cat_cmd, cat_args options.
# 19/10/2005	2.3-5		gaffie	Added sort_cmd, sort_args, cpp_cmd, cpp_args options.
# 13/10/2005	2.3-5		gaffie	New 'copy input record', 'divert input record' sections replace 'copy/divert record'.
# 13/10/2005	2.3-5		gaffie	New 'copy output record', 'divert output record' sections.
# 05/10/2005	2.3-4		gaffie	New 'copy record' section.
# 05/10/2005	2.3-4		gaffie	New 'divert record' section.
# 03/10/2005	2.3-3		gaffie	Allow sort-by when input-file option is pequel script.
# 03/10/2005	2.3-3		gaffie	New section type 'sort output'.
# 30/09/2005	2.3-2		gaffie	New option 'prefix' for prefixing input_file and sub pequel script names.
# 29/09/2005	2.3-2		gaffie	cmdType attribute to distinguish between cmdline and script only options.
# 27/09/2005	2.3-2		gaffie	Removed Pequel::Base usage and all refs to Pequel::root.
# 22/09/2005	2.3-2		gaffie	Parse.pm:Removed Pequel::Base usage and all refs to Pequel::root.
# 22/09/2005	2.3-2		gaffie	Begin removal of 'root'
# 21/09/2005	2.3-2		gaffie	Added -option option to view option values for script.
# 20/09/2005	2.3-2		gaffie	Added pequel script chaining functionality.
# 19/09/2005	2.3-2		gaffie	Added viewraw option for use by Pequel embedded tables.
# 15/09/2005	2.3-2		gaffie	Fixed code generation when using 'use_inline' with input_file specified.
# 14/09/2005	2.3-2		gaffie	Fixed code generated for external tables with single data column.
# 14/09/2005	2.3-2		gaffie	Added Pequel tables.
# 13/09/2005	2.2-9		gaffie	Docgen:Added About Pequel Chapter includes copyright notice;
# 13/09/2005	2.2-9		gaffie	Type/Table:PEQUEL_TABLE_PATH env for runtime external table path.
# 12/09/2005	2.2-9		gaffie	Parse/Parse:Revamped the table compiler
# 09/09/2005	2.2-9		gaffie	Type/Macro:Array field macros -- now assuming all args to be array field using &to_array().
# 09/09/2005	2.2-9		gaffie	Parse/Parse:Revamped the macro compiler -- now handles nested macros and complex statements.
# 05/09/2005	2.2-9		gaffie	Type/Section:Must escape litereal quote argument in Option with '\'.
# 05/09/2005	2.2-9		gaffie	Type/Section:Allow quoted (single/double) Option arguments
# 05/09/2005	2.2-9		gaffie	Engine/Engin:Prevent repeated calc of input field when they appear in group-by.
# 05/09/2005	2.2-9		gaffie	Engine/Engin:In codeBreak Fixed calc of derived group-by fields.
# 01/09/2005	2.2-9		gaffie	Bug fix in compile() for arr_avg macro.
# 01/09/2005	2.2-9		gaffie	All array macros now will parse any param as an array field.
# 01/09/2005	2.2-9		gaffie	Fixed arr_size macro.
# 01/09/2005	2.2-9		gaffie	Fixed example and test scripts containing old macro formats.
# 31/08/2005	2.2-8		gaffie	Added o_inline_lddlflags
# 31/08/2005	2.2-8		gaffie	Added o_inline_make
# 31/08/2005	2.2-8		gaffie	Added o_inline_libs, o_inline_inc.
# 31/08/2005	2.2-8		gaffie	Removed input_parse_input_qoutes option (replace by input_delimiter_extra).
# 31/08/2005	2.2-8		gaffie	Call cmdPrep method for all used options.
# 31/08/2005	2.2-8		gaffie	Type/Oprions:added cmdPrep method to Option::Element class.
# 31/08/2005	2.2-8		gaffie	Type/Oprions:added: input_delimiter_extra
# 30/08/2005	2.2-8		gaffie	Engine/Inline:added: inline_ccflags, inline_optimize
# 26/08/2005	2.2-8		gaffie	Engine/Inline:Fixed Config=>NAME -- subst '/' by '::'.
# 26/08/2005	2.2-8		gaffie	Engine/Inline:Added Apache Log file paring w/o_inline_parse_input_quotes via readsplit()
# 26/08/2005	2.2-8		gaffie	Engine/Inline:Added inline_cc option.
# 25/08/2005	2.2-7		gaffie	Code/Code.pm:updated sprintRaw so as to wrap code line > 110 chars.
# 25/08/2005	2.2-7		gaffie	Engine/Engine.pm:Added vim syntax perl setting in generated script.
# 25/08/2005	2.2-7		gaffie	Engine/Engine.pm:Removed 'use warnings' because eval in execute (might) complain.
# 25/08/2005	2.2-7		gaffie	Script/Script.pm:Bugfix with 'no cpp' misspelling.
# 25/08/2005	2.2-7		gaffie	Code/Code.pm:supress addCommentBegin()/End() unless --debug option specified.
# ----------------------------------------------------------------------------------------------------
# TODO:
# Qt GUI IDE for Pequel development/execution.
# PDF: pequel quick reference.
# PequelDTP: Web Data Transformation Portal.
# Combine common table loads across scripts.
# XML layer/interface: pequel-language -> XML -> pequel-engine.
# output_file(pequel:...) -- use this for output combiner, inherit attr so all divert/copy scripts will use.
# ETL::Pequel::Lang; ETL::Pequel::Lang::InputSection, ...; to contain parse(), anything pequel-language related.
# Sys::Syslog;
#
# DS1  DS2	 -- multi-datasources
# |    |
# PQL1 PQL2
#  \  /
#   \/
#  PQL3		-- combiner
#
# Need to use read/sysread for fixed length input when no eol character exists.
# Buffered output -- push output record to stack and output as batch -- enhance shared (locked) output.
# 	-- test output fd for lock; if locked continue processing next record.
# Option: input_record_length for use when input file is fixed length -- set $\ = record length;
# use pipe with syswrite to ensure that buffering does not deadlock the processes waiting for each other's message,
# use do-block instead of anonymous sub for macros that eval to anon-sub call.
# Replace 'use constant' in generated code with real-constants because use-constant replaces value with subroutine call() which degrades performance!
# apache: prefix for input/output_file spec.
# Socket pipe-fitting -- input_file(socket:...); output_file(socket:...); same with divert/copy section types.
# Use pack/unpack instead of substr in date manip code.
# input-section(file:, pequel:, oracle:, ...) for enabling multiple (successive) input stream
#	-- use input-merge-section(data-source, key-field) for multi simultaneous merge.
# dedup-input, dedup-output to replace dedup-on
# If packed input && sort-by then generate temp pequel srcipt and fit as input-file. The tmp script will do
# Combine Script.pm, Section->parse() functions, Parser.pm because they all relate to pequel-language parsing 
#	-- Parser/Pequel.pm; then future: Parser/XML.pm, etc
# Macro: &output_record_count()
# Macros: &input_fields_count(), &record_number(), 
# Pequel tables pack/unpack pipe interface.
# frequency analysis aggregate: &freq() -- ouput result in array field.
# Lazy pipe open; use fifo list for close() ordering -- only open copy/divert pipe if data matches condition.
# Replace pod2pdf with pdf package and rewrite pequeldoc stuff.
# merge/table cleanup:
# 	table(local: | file: | pequel: | oracle:connect_str | sqlite: | other:, static/dynamic, ...)
# 	merge(local: | file: | pequel: | oracle:connect_str | sqlite: | other:, ...)
#	input-merge section
# rename 'sort by' --> 'sort input'; output/input_file --> output/input_stream; 'input section'-->'input record'
# Perl module interface.
#   my $pequel = Pequel->new();
#	use constant GROUP_BY => 'group by';
#	$pequel->section(Pequel::GROUP_BY)->add(field => $fld, type => $type); -- will call parse() for Pequel::Type::Section::GroupBy;
#	or $pequel->group_by->add(...);
#	$pequel->section(Pequel::COPY_INPUT_RECORD, 'copy_ir_NSW.pql')->add(value => 'LOCATION =~ /NSW/') -- create derived section type;
#	or $pequel->copy_input_record('copy_ir_NSW.pql')->add(...);
#	or $pequel->copy_input_record($sub_pequel)->...;
#	$pequel->check();
#	$pequel->run();
# sort-by/sort-output with packing.
# pack/unpack interface between pequel scripts.
#		print map(pack("A3/Z*", $_), @VALS), "\n";
#		print join('|', unpack('A3/Z*' x 9, $_)), "\n";
# output_file(pequel:...)
# Fix divert output record when diverting to file.
# 'copy input record', 'copy output record', 'divert input record', ..., output_file(pequel:...);
# combiner: use named pipe
# 'distribute(thread_num)' -- split input based on condition and pipe to n processes.
#	how to avoid double splitting input record?
# 	implement as divert; remove input_file(), output_file(),
# &input_record() special macro acts on raw input line (ie before split) -- use in filter, etc sections.
# &num_fields() -- return number of fields in input record;
# input_format option: input_format(pequel:script.pql); input_format(delim:file.txt); 
#	apachelog:, excell:, cvs:, fixed:, ...
# 'reject record(file:outfile.dat)' section. Divert input record if matches condition.
# 'reject record(pequel:outfile.pql)' -- reject and divert are identical.
# Chain Pequel scripts -- 
#	input section not required -- get fields names from output_format of input_file script.
# Bug:Input derived fields referenced in output calc or where clause not being calculated.
# &to_array(..., delim);
# ----------------------------------------------------------------------------------------------------
require 5.005_62;
use strict;
use attributes qw(get reftype);
use warnings;
use vars qw($VERSION $BUILD);
$VERSION = "2.4-6b";
$BUILD = 'Thursday March 2 21:41:45 GMT 2006';
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Main;
	use UNIVERSAL qw( isa can );

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
		my $class = ref($proto) || $proto;
		my $self = {};
		bless($self, $class);

		use ETL::Pequel::Param;
		$self->PARAM(ETL::Pequel::Param->new());
		$self->initRc();
	
		if (my $scriptname = shift) 
		{
			$self->PARAM->properties('script_name', $scriptname); 
		
			if (my $parent_PARAM = shift)
			{
				$self->PARAM->root($parent_PARAM->root());
				foreach my $o (grep($_->inherit, $self->PARAM->options->toArray()))
				{
					$self->PARAM->properties($o->name(), $parent_PARAM->properties($o->name()));
				}
				$self->PARAM->depth($parent_PARAM->depth() +1);
			}
			$self->prepare();
		}
		else
		{
			$self->PARAM->root($self);
			$self->parseCmdLine();
			$self->prepare();
			$self->generate();
		}
		return $self;
	}

	sub prepare : method
	{
		my $self = shift;

		$self->PARAM->SCRIPT->prepare();
		$self->PARAM->options->cmd_prep();
		$self->PARAM->options->cmd_exec();
	}

	sub generate : method
	{
		my $self = shift;

		$self->PARAM->ENGINE->generate();
		$self->PARAM->ENGINE->prepare();
		$self->PARAM->options->cmd_postgen();
	}

	sub execute : method
	{
		my $self = shift;
		if ($self->PARAM->properties('script_name') eq '')
		{
			$self->PARAM->SCRIPT->usage();
			return;
		}
		$self->PARAM->error->msgStderr("");
		if (!$self->PARAM->ENGINE->size())
		{
			$self->prepare();
			$self->generate();
#??			$self->execute();
		}
		$self->PARAM->ENGINE->execute();
	}

	sub check : method
	{
		my $self = shift;
		if ($self->PARAM->properties('script_name') eq '')
		{
			$self->PARAM->SCRIPT->usage();
			return;
		}
		if (!$self->PARAM->ENGINE->size())
		{
			$self->prepare();
			$self->execute();
		}
		return $self->PARAM->ENGINE->check();
	}

#>	Move to ETL::Pequel::Parser::CmdLine
	sub parseCmdLine : method 
	{
		# Cmdline option settings override:
		#	Script option settings, which override:
		#		Pequelrc option settings,

		my $self = shift;
		use Getopt::Long;

		my $sub = sub 
		{ 
			my $name = shift;
			my $arg = shift || undef;
			my $o;

			$self->PARAM->error->fatalError("Invalid option '$name'")
				if 
				(
					!($o = $self->PARAM->options->exists($name))
					&& !($o = $self->PARAM->options->getAlias($name))
				);

			$self->PARAM->sections->exists('options')->items->add(ETL::Pequel::Field::Element->new
			(
				name => $o->name(), 
				value => defined($arg) ? $arg : $o->cmdFormat() eq '!' ? 1 : $o->value(),
				comment => '_CMDLINE',
				ref => $o,
				PARAM => $self->PARAM
			));
		};

		my %Options;
		foreach (grep($_->cmdFormat, $self->PARAM->options->toArray()))
#>		foreach (grep($_->can("cmdFormat"), $self->root->t_option->toArray))
		{
			$Options{$_->name . $_->cmdFormat} = \&{$sub};
			next unless ($_->cmdAlias->size);
			foreach my $alias ($_->cmdAlias->toArray())
			{
				$Options{$alias->name . $_->cmdFormat} = \&{$sub};
			}
		}

		$self->PARAM->error->fatalError("Use --help option for info (@{[ join(',', @ARGV) ]}).")
			if (!Getopt::Long::GetOptions(%Options));

		if (!$self->PARAM->sections->exists('options')->items->exists('script_name') && grep(/.pql$/, @ARGV))
		{
			$self->PARAM->sections->exists('options')->items->add(ETL::Pequel::Field::Element->new
			(
				name => 'script_name',
				value => grep(/.pql$/, @ARGV),
				comment => '_CMDLINE',
				ref => $self->PARAM->options->exists('script_name'),
				PARAM => $self->PARAM
			));
		}
	}

	sub initRc : method
	{
		my $self = shift;
		# Open the pequel init file ~/.pequelrc and set any options
		if (-e "$ENV{HOME}/.pequelrc")
		{
			$self->PARAM->error->msgStderr("reading ~/.pequelrc...") if ($self->PARAM->properties('debug'));
			open(PequelRC, "$ENV{HOME}/.pequelrc");
# put <> into slurp mode
#undef $/;
## read configuration file supplied on command line into string
#my $configuration = <>;
#my %config;
## read all configuration options from config string
#while ($configuration =~ /^\s*(\w+)\s* = \s*(.+?)\s*$/mg) {
#  $config{$1} = $2;
#}
#print "Got: $_ => '$config{$_}'\n" foreach (sort keys %config);
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
}
1;
# ----------------------------------------------------------------------------------------------------
