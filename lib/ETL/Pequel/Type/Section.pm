#!/usr/bin/perl
# ----------------------------------------------------------------------------------------------------
#  Name		: ETL::Pequel::Type::Section.pm
#  Created	: 10 February 2005
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
# 26/10/2005	2.3-6		gaffie	display message section types.
# 21/09/2005	2.3-6		gaffie	add() function.
# 20/09/2005	2.3-6		gaffie	unpack_input/pack_output implementation.
# 05/10/2005	2.3-4		gaffie	New 'copy record' section.
# 05/10/2005	2.3-4		gaffie	New 'divert record' section.
# 03/10/2005	2.3-3		gaffie	Allow sort-by when input-file option is pequel script.
# 03/10/2005	2.3-3		gaffie	New section type 'sort output'.
# 20/09/2005	2.3-2		gaffie	Added pequel script chaining functionality.
# 14/09/2005	2.3-2		gaffie	New section type for 'load table pequel'.
# 05/09/2005	2.2-9		gaffie	Must escape litereal quote argument in Option with '\'.
# 05/09/2005	2.2-9		gaffie	Allow quoted (single/double) Option arguments.
# ----------------------------------------------------------------------------------------------------
# TO DO:
# ----------------------------------------------------------------------------------------------------
require 5.005_62;
use strict;
use attributes qw(get reftype);
use warnings;
use lib './lib';
use vars qw($VERSION $BUILD);
$VERSION = "2.4-3";
$BUILD = 'Tuesday November  1 08:45:13 GMT 2005';
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Section;
	use ETL::Pequel::Collection;
	use base qw(ETL::Pequel::Collection::Element);

	our $this = __PACKAGE__;

	sub BEGIN
	{
		our @attr =
		qw(
			lines
			items
			present
			required
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

		$self->required($params{'required'} || 0);

		$self->lines(ETL::Pequel::Collection::Vector->new);	#collection of ETL::Pequel::Script::Line::Element
		$self->items(ETL::Pequel::Collection::Vector->new);	#collection of ETL::Pequel::Field::Element
#>		$self->code(ETL::Pequel::Type::Code::Section->new);

		return $self;
	}

#> 	TODO: use $engine parameter instead of $c:
    sub codeInit : method 		# ETL::Pequel::Type::Code::Section->init
	{ 
		my $self = shift; 
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		return $c;
	}

    sub codeOpen : method 		# ETL::Pequel::Type::Code::Section->init
	{ 
		my $self = shift; 
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		return $c;
	}

    sub codeReset : method 		# ETL::Pequel::Type::Code::Section->reset
	{ 
		my $self = shift; 
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		return $c;
	}

    sub codeBreakBefore : method 		# ETL::Pequel::Type::Code::Section->breakBefore
	{ 
		my $self = shift; 
		my $engine = shift;
	}

    sub codeBreakAfter : method 		# ETL::Pequel::Type::Code::Section->breakAfter
	{ 
		my $self = shift; 
		my $engine = shift;
	}

    sub codePrintBefore : method 		# ETL::Pequel::Type::Code::Section->breakBefore
	{ 
		my $self = shift; 
		my $engine = shift;
	}

    sub codePrintAfter : method 		# ETL::Pequel::Type::Code::Section->breakAfter
	{ 
		my $self = shift; 
		my $engine = shift;
	}

    sub codeMainBefore : method 		# ETL::Pequel::Type::Code::Section->mainBefore
	{ 
		my $self = shift; 
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		return $c;
	}

    sub codeMainAfter : method 		# ETL::Pequel::Type::Code::Section->mainAfter
	{ 
		my $self = shift; 
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		return $c;
	}

    sub codeClose : method 
	{ 
		my $self = shift; 
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		return $c;
	}

    sub codePackages : method 
	{ 
		my $self = shift; 
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		return $c;
	}

	sub parseAll : method
	{
		my $self = shift;
		map($self->parse($_->value), $self->lines->toArray);
	}

	sub parse : method
	{
		my $self = shift;
		my $value = shift || $self->lines->last->value();
		$self->addItem(value => $value);
	}

	sub addItem : method
	{
		my $self = shift;
		my %params = @_;
		my $value = $params{'value'} || $self->add_error(ref($self), 'value');
		$self->items->add(ETL::Pequel::Collection::Element->new
		(
			value => $value,
			PARAM => $self->PARAM
		));
	}

	sub compile : method
	{
		my $self = shift;
		map($_->compile, $self->items->toArray);
	}

	sub regEx : method
	{
		my $self = shift;
		my $name = $self->name;
		while (index($name, ' ') != -1) { substr($name, index($name, ' '), 1) = '\s+'; }
		return "\^$name\$";
	}

	sub add_error : method
	{
		my $self = shift;
		my $caller = shift;
		my $param_name = shift;

		$self->PARAM->error->fatalError("[10100] Missing parameter '$param_name' for $caller.") 
	}

}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Section::Options;
	use base qw(ETL::Pequel::Type::Section);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_, 
			name => $params{'name'} || 'options', 
			required => 1,
		);
		bless($self, $class);

		return $self;
	}

	sub parse : method
	{
		my $self = shift;
		my $code_line = shift || $self->lines->last->value;

		my $name;
		my $val;
		$code_line =~ s/\\"/__QUOTE__/g;
		$code_line =~ s/\s$//;

#> 		Also need to allow for embedded brackets with quotes!
		if ($code_line =~ /(.*?)\s*?\(['"]?(.*?)['"]?\)$/)
		{
			$name = $1;
			$val = $2;
		}
		else
		{
			$name = $code_line;
			$val = '1';
		}
		$val =~ s/__QUOTE__/"/g;
		$self->addItem(name => $name, value => $val);
	}

	sub addItem : method
	{
		my $self = shift;
		my %params = @_;
		my $name = $params{'name'} || $self->add_error(ref($self), 'name');
		my $value = $params{'value'};
		my $o = $self->PARAM->options->find($name) || $self->PARAM->options->getAlias($name);
		$self->PARAM->error->fatalError("[10102] Invalid option '$name'") if (!$o);

		return if ($self->items->find($name)); # already set from cmdline option;

		$self->PARAM->error->fatalError("[10103] Invalid option argument '$name ($value)'") 
			if (defined($o->format) && $value !~ /^@{[ $o->format ]}$/);

		if ($o->cmdType() == 2)
		{
			$self->PARAM->error->warning("[10104] This option '$name' may only be set via command line.");
			return;
		}

		$self->items->add(ETL::Pequel::Field::Element->new
		(
			name => $o->name, 
			value => $value,
			comment => '_SCRIPT',
			ref => $o,
			PARAM => $self->PARAM,
		));
	}

	sub compile : method
	{
		my $self = shift;

		# Check conflicting property settings...

		if ($self->PARAM->properties('unpack_input') && $self->PARAM->properties('use_inline'))
		{
			$self->PARAM->error->warning("[10105] 'unpack_input' not allowed when 'use_inline' used -- ignored.");
			$self->PARAM->properties('unpack_input', '');
		}
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Section::FieldProcess;
	use base qw(ETL::Pequel::Type::Section);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		$self = $class->SUPER::new (@_); 
		bless($self, $class);

		return $self;
	}

	sub parse : method
	{
		my $self = shift;
		my $code_line = shift || $self->lines->last->value;

		my $name;
		my $operator='';
		my $calc='';

		my $type = ($code_line =~ s/^@//) ? 'array' : 'string';

		if ($code_line =~ /^[\w|_|\d]+\s*\=\>/)
		{
			$operator = '=>';
			$calc = $code_line;
			$calc =~ s/^([\w|_|\d]+)\s*=>\s*(.*)/$2/;
			$name = $1;
		}
		elsif ($code_line =~ /^[\w|_|\d]+\s*\=\~/)
		{
			$operator = '=~';
			$calc = $code_line;
			$calc =~ s/^([\w|_|\d]+)\s*=~\s*(.*)/$2/;
			$name = $1;
		}
		else
		{
			$name = $code_line;
		}
		$self-addItem(name => $name, operator => $operator, type => $type, calc => $calc);
	}

	sub addItem : method
	{
		my $self = shift;
		my %params = @_;
		my $name = 		$params{'name'} 	|| $self->add_error(ref($self), 'name');
		my $type = 		$params{'type'} 	|| $self->add_error(ref($self), 'type');
		my $operator = 	$params{'operator'};
		my $calc = 		$params{'calc'};

		$type = ($type eq 'array')
			? $self->PARAM->datatypes->exists('array')
			: $self->PARAM->datatypes->exists('string');

		$self->items->add(ETL::Pequel::Field::Element->new
		(
			name => $name, 
			calc => $self->PARAM->parser->compile($calc, $self->name, $name), # MUST BE DONE EARLY
			calc_orig => $calc,
			operator => $operator, 
			type => $type,
			input_field => $self->PARAM->sections->exists('input section')->items->exists($name),
			output_field => $self->PARAM->sections->exists('output section')->items->exists($name),
			PARAM => $self->PARAM,
		));
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Section::FieldProcess::Pre;
	use base qw(ETL::Pequel::Type::Section::FieldProcess);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_, 
			name => $params{'name'} || 'field preprocess', 
			required => 0,
		);
		bless($self, $class);

		return $self;
	}

    sub codeBreakBefore : method 
	{ 
		my $self = shift; 
		my $engine = shift;
		foreach ($self->items->toArray)
		{
			$engine->addNonl("@{[ $_->inputField->codeVar ]}");
			$engine->addNonl("@{[ $_->operator eq '=~' ? ' =~ ' : ' = ' ]}");
			$engine->add("@{[ $self->PARAM->parser->compile($_->calc) ]};");
		}
	}

	sub addItem : method
	{
		my $self = shift;
		$self->SUPER::addItem(@_);

		$self->PARAM->error->fatalError
		("[10106] In section '@{[ $self->name() ]}': Input field '@{[ $self->items->last->name ]}' is not defined.")
			if (!$self->PARAM->sections->exists('input section')->items->exists($self->items->last->name));
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Section::FieldProcess::Post;
	use base qw(ETL::Pequel::Type::Section::FieldProcess);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_, 
			name => $params{'name'} || 'field postprocess', 
			required => 0,
		);
		bless($self, $class);

		return $self;
	}

#?	Should be in codeOutputFinal so as to appear just before the Print()
    sub codeMainAfter : method 
	{ 
		my $self = shift; 
		my $c = $self->SUPER::codeMainAfter;
		foreach ($self->items->toArray)
		{
			$c->addNonl("@{[ $_->outputField->codeVar ]}");
			$c->addNonl("@{[ $_->operator eq '=~' ? ' =~ ' : ' = ' ]}");
			$c->add("@{[ $self->PARAM->parser->compile($_->calc) ]};");
		}
		return $c;
	}

	sub addItem : method
	{
		my $self = shift;
		$self->SUPER::addItem(@_);

		$self->PARAM->error->fatalError
		("[10107] In section '@{[ $self->name() ]}': Output field '@{[ $self->items->last->name ]}' is not defined.")
			if (!$self->PARAM->sections->exists('output section')->items->exists($self->items->last->name));
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Section::Description;
	use base qw(ETL::Pequel::Type::Section);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_, 
			name => $params{'name'} || 'description section', 
		);
		bless($self, $class);
		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Section::UsePackage;
	use base qw(ETL::Pequel::Type::Section);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_, 
			name => $params{'name'} || 'use package', 
		);
		bless($self, $class);

		return $self;
	}

    sub codeInit : method 
	{ 
		my $self = shift; 
		my $c = $self->SUPER::codeInit;
		map($c->add("use @{[ $_->value ]};"), $self->items->toArray);
		return $c;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Section::Input;
	use base qw(ETL::Pequel::Type::Section);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_, 
#			name => $params{'name'} || 'input section', 
			name => 'input section', 
			required => 1,
		);
		bless($self, $class);
		return $self;
	}

	sub parse : method
	{
		my $self = shift;
		my $code_line = shift || $self->lines->last->value;

		my $name;
		my $operator='';
		my $calc='';

		my $type = ($code_line =~ s/^@//) ? 'array' : 'string';

		if ($code_line =~ /^[\w|_|\d]+\s*\=\>/)
		{
			$operator = '=>';
			$calc = $code_line;
			$calc =~ s/^([\w|_|\d]+)\s*=>\s*(.*)/$2/;
			$name = $1;
		}
		elsif ($code_line =~ /^[\w|_|\d]+\s*\=\~/)
		{
			$operator = '=~';
			$calc = $code_line;
			$calc =~ s/^([\w|_|\d]+)\s*=~\s*(.*)/$2/;
			$name = $1;
		}
		else
		{
			$name = $code_line;
		}
		$self->addItem(name => $name, operator => $operator, calc => $calc, type => $type);
	}

	sub addItem : method
	{
		my $self = shift;
		my %params = @_;
		my $name = 		$params{'name'} 	|| $self->add_error(ref($self), 'name');
		my $type = 		$params{'type'} 	|| $self->add_error(ref($self), 'type');
		my $operator = 	$params{'operator'};
		my $calc = 		$params{'calc'};

		$type = ($type eq 'array')
			? $self->PARAM->datatypes->exists('array')
			: $self->PARAM->datatypes->exists('string');

		$self->PARAM->error->fatalError("[10108] Invalid input field name:$name") 
			unless ($name =~ /^[a-zA-Z0-9_]*$/);

		$self->PARAM->error->fatalError("[10109] Duplicate input field name '$name' invalid.") 
			if ($self->PARAM->ifields->find($name));
#<			if ($self->PARAM->sections->exists('input section')->items->exists($name));

		$self->PARAM->error->fatalError("[10110] input field name '$name' may not be same as a table name.") 
			if ($self->PARAM->tables->exists($name));

		$self->items->add(ETL::Pequel::Field::Input::Element->new
		(
			name => $name, 
			calc_orig => $calc,
			operator => $operator, 
			type => $type,
			PARAM => $self->PARAM,
		));
		$self->items->last->calc($self->PARAM->parser->compile($calc, $self->name, $name)); 

#>		Use ifields instead of above:
		$self->PARAM->ifields->add(ETL::Pequel::Field::Input::Element->new
		(
			name => $name, 
			calc_orig => $calc,
			operator => $operator, 
			type => $type,
			PARAM => $self->PARAM,
		));
		$self->PARAM->ifields->last->calc($self->PARAM->parser->compile($calc, $self->name, $name)); 
	}

	sub compile : method
	{
		my $self = shift;
		return unless ($self->PARAM->properties('input_file') =~ /([\w|_|\d]+\.pql)/);
		my $scriptname = $self->PARAM->getscriptname($self->PARAM->properties('input_file'));
	
		my $filename = $self->PARAM->getfilepath($self->PARAM->properties('input_file'));

		$self->PARAM->error->fatalError("[10111] Script $filename does not exist.")
			unless (-e $filename);

		$self->PARAM->error->fatalError("[10112] Recursive Pequel script call '$filename' prohibited.")
			if ($self->PARAM->pequel_script_disallow->find($scriptname)
				|| $scriptname eq $self->PARAM->getscriptname($self->PARAM->properties('script_name')));

		if (!$self->PARAM->root->PARAM->pequel_script->find($scriptname))
		{
			$self->PARAM->root->PARAM->pequel_script->add(ETL::Pequel::Collection::Element->new
			(
				name => $self->PARAM->properties('input_file'), 
				value => ETL::Pequel::Main->new($filename, $self->PARAM),
			));
		}
		my $sub_pql = $self->PARAM->root->PARAM->pequel_script->find($self->PARAM->properties('input_file'))->value();

		# Move this.sort_by to sub.sort_output, unless this.hash:
		if ($self->PARAM->sections->find('sort by')->items->size() && !$self->PARAM->properties('hash'))
		{
			$sub_pql->PARAM->sections->find('sort output')->items->clear();
			foreach my $f ($self->PARAM->sections->find('sort by')->items->toArray())
			{
				my $if;
				if (($if = $sub_pql->PARAM->sections->find('output section')->items->exists($f->name())) == 0)
				{
					$if = $sub_pql->PARAM->sections->find('input section')->items->exists($f->name())
						if ($sub_pql->PARAM->properties('transfer'));
				}
				$self->PARAM->error->fatalError("[10113] Field '@{[ $f->name() ]}' is not available in the output format.")
					if ($if == 0);
	
				$sub_pql->PARAM->sections->find('sort output')->items->add(ETL::Pequel::Field::Element->new
				(	
					name => $f->name(), 	
					type => $f->type(),
					input_field => $if,
					direction => $f->direction(),
					PARAM => $sub_pql->PARAM,
				));
			}
			$self->PARAM->sections->find('sort by')->items->clear();
		}
		# Pipe fitting; Use packed interface between this and sub where possible:
		$sub_pql->PARAM->properties('output_delimiter', $self->PARAM->properties('input_delimiter'));
		$sub_pql->PARAM->properties('output_file', '');
		if 
		(
			(
				$self->PARAM->sections->find('sort by')->items->size() == 0 
				|| $self->PARAM->properties('hash')
			)
			&&
			(
				$sub_pql->PARAM->sections->find('sort output')->items->size() == 0 
			)
		)
		{
			$self->PARAM->properties('unpack_input', '1');
			$sub_pql->PARAM->properties('pack_output', '1');
			$sub_pql->PARAM->properties('output_pack_fmt', $self->PARAM->properties('input_pack_fmt'));
		}
		$sub_pql->PARAM->pequel_script_disallow->addAll(
			ETL::Pequel::Collection::Element->new(name => $self->PARAM->getscriptname()),
			$self->PARAM->pequel_script_disallow->toArray()
		);
		$sub_pql->generate();

		$self->PARAM->error->fatalError("[10114] The Pequel script '$filename' failed syntax check.")
			unless ($sub_pql->check =~ /Syntax\s+OK/i);

		# ... or an arg for input_section
		$self->PARAM->error->fatalError("[10115] The Pequel script '$filename' must have an 'input_file' option specified when used as a input_file.")
			unless ($sub_pql->PARAM->properties('input_file') ne '');

#>		Should verify that $sub_pql.input_section == this.input_section.

		if (!$self->PARAM->root->PARAM->packages->find(lc($self->get_fd_name())))
		{
			$self->PARAM->root->PARAM->packages->add(ETL::Pequel::Collection::Element->new
			(
				name => lc($self->get_fd_name()),
				value => $sub_pql
			));
		}
		return;
	}
	
	sub get_fd_name
	{
		my $self = shift; 
		if ($self->PARAM->properties('input_file') || $self->PARAM->sections->find('sort by')->items->size())
		{
			return 'DATA' unless ($self->PARAM->properties('input_file') =~ /([\w|_|\d]+\.pql)/);
			my $fdname = $self->PARAM->properties('input_file');
			$fdname =~ s/^.*://;
			$fdname =~ s/\..*$//;
			$fdname = "INPUT_$fdname";
			return uc($fdname);
		}
		else
		{
			return 'STDIN';
		}
	}

	sub codeOpen : method
	{
		my $self = shift;
		my $c = $self->SUPER::codeOpen;
		my $sortstr='';
		my $sortcomment='';
		if ($self->PARAM->sections->exists('sort by')->items->size)
		{
			if ($self->PARAM->properties('input_file') =~ /\.gz$|\.z$|\.Z$|\.zip$/)
			{
				$sortstr = "@{[ $self->PARAM->properties('gzcat_cmd') ]}";
				$sortstr .= " @{[ $self->PARAM->properties('gzcat_args') ]}";
				$sortstr .= " @{[ $self->PARAM->getfilepath($self->PARAM->properties('input_file')) ]} |";
			}

			$sortstr .= "@{[ $self->PARAM->properties('sort_cmd') ]}";
			$sortstr .= " @{[ $self->PARAM->properties('sort_args') ]}";
			$sortstr .= " -t'@{[ $self->PARAM->properties('input_delimiter') ]}' -y";
			$sortstr .= " -T @{[ $self->PARAM->properties('sort_tmp_dir') ]}" 
				if ($self->PARAM->properties('sort_tmp_dir'));

			foreach ($self->PARAM->sections->exists('sort by')->items->toArray)
			{
				$sortcomment .= "@{[ $_->inputField->name ]}(@{[ $_->direction ==$self->PARAM->SORT_DES ? 'des' : 'asc' ]}:@{[ $_->type->name ]}) ";
				$sortstr .= ' -k ';
				foreach my $comma ('', ',')
				{
					$sortstr .= $comma . "@{[ $_->inputField->number ]}";
					$sortstr .= 'n' if ($_->type->name eq 'numeric' || $_->type->name eq 'decimal');
					$sortstr .= 'r' if ($_->direction == $self->PARAM->SORT_DES);
				}
			}
			$sortstr .= " " . $self->PARAM->getfilepath($self->PARAM->properties('input_file'))
				if ($self->PARAM->properties('input_file') && $self->PARAM->properties('input_file') !~ /\.gz$|\.z$|\.Z$|\.zip$/);
			$sortstr .= " 2>/dev/null";
			$c->addComment(" Sort:$sortcomment");
		}
		
		my $fdname = $self->get_fd_name();
		if ($self->PARAM->properties('input_file') =~ /([\w|_|\d]+\.pql)/)
		{
			$c->add("if (open(@{[ $fdname ]}, '-|') == 0) # Fork -- read from child");
			$c->openBlock("{");
			$c->add("&p_@{[ lc($fdname) ]}::@{[ lc($fdname) ]};");
			$c->add("exit(0);");
			$c->closeBlock;
		}
		elsif ($self->PARAM->properties('input_file'))
		{
			$c->addNonl("open($fdname, q{");

			if ($self->PARAM->sections->exists('sort by')->items->size)
			{
				$c->addNonl("@{[ $sortstr ]} |});");
			}
			else
			{
				my $filename = $self->PARAM->getfilepath($self->PARAM->properties('input_file'));
				if ($self->PARAM->properties('input_file') ne '' 
					&& $self->PARAM->properties('input_file') =~ /\.gz$|\.z$|\.Z$|\.zip$/)
				{
					$c->addNonl("@{[ $self->PARAM->properties('gzcat_cmd') ]} @{[ $self->PARAM->properties('gzcat_args') ]} @{[ $filename ]} |})");
					$c->addNonl("|| die \"Cannot open @{[ $filename ]}: \$!\";");
				}
				else
				{
					$c->addNonl("@{[ $filename ]}})");
					$c->addNonl("|| die \"Cannot open @{[ $filename ]}: \$!\";");
				}
			}
			$c->add;
		}
		elsif ($self->PARAM->sections->exists('sort by')->items->size())
		{
			$c->addNonl("open($fdname, q{@{[ $self->PARAM->properties('cat_cmd') ]} @{[ $self->PARAM->properties('cat_args') ]} -");
			$c->addNonl(" | @{[ $sortstr ]}")
				if ($self->PARAM->sections->exists('sort by')->items->size);
			$c->addNonl(" |}) || die \"Cannot open input: \$!\";");
			$c->add;
		}
		if ($self->PARAM->properties('use_inline'))
		{
			$c->add("my \$fd = fileno(@{[ $fdname ]});");
			$c->add("OpenSortStream(\$fd);") 
				if ($self->PARAM->sections->exists('sort by')->items->size()
					|| $self->PARAM->properties('input_file') ne '');
		}
		return $c;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Section::SortBy;
	use base qw(ETL::Pequel::Type::Section);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_, 
			name => $params{'name'} || 'sort by', 
#>			syntax => 'name(&inputField->name) [type(&type) [direction(des|asc)]]'
		);
		bless($self, $class);

		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		# This is correct even if input_file is a pequel script because pack_output not allowed with sort-output.
		$self->PARAM->error->warning("[10116] 'unpack_input' not allowed when 'sort by' used -- ignored.")
			if ($self->PARAM->properties('unpack_input') && !$self->PARAM->properties('hash'));
		$self->PARAM->properties('unpack_input', '')
			unless ($self->PARAM->properties('hash'));
#?		$self->SUPER::compile();
		$self->PARAM->sections->find('sort by')->items->clear()
			if ($self->PARAM->properties('hash'));
		map($_->compile, $self->items->toArray);
	}

	sub parse : method
	{
		# format-1: fld [ numeric | string ] [ asc | ascending | desc | descending ]
		# format-2: fld (type => numeric | string, sort => asc | desc)
		my $self = shift;
		my $code_line = shift || $self->lines->last->value;

		# format-1:
		my ($fld, $type, $sort) = split(/\s+/, $code_line, -1);
		$self->addItem(fld => $fld, type => $type, sort => $sort);
	}

	sub addItem : method
	{
		my $self = shift;
		my %params = @_;
		my $fld = $params{'fld'} || $self->add_error(ref($self), 'fld');
		my $type = $params{'type'};
		my $sort = $params{'sort'};

		my $iFld;
		$self->PARAM->error->fatalError("[10117] Field '@{[ $fld ]}' is not defined in the input section.")
			if (($iFld = $self->PARAM->sections->exists('input section')->items->exists($fld)) == 0);

		$type = 'string' if (!defined($type) || $type eq '');
		my $o_type;
		$self->PARAM->error->fatalError("[10118] Invalid type '@{[ $type ]}' specified in 'Sort By' section.")
			if (($o_type = $self->PARAM->datatypes->exists($type)) == 0);

		$self->items->add(ETL::Pequel::Field::Element->new
		(	
			name => $fld, 	
			type => $o_type,
			input_field => $iFld,
			direction => "@{[ (defined($sort) && $sort =~ /des/ ? $self->PARAM->SORT_DES : $self->PARAM->SORT_ASC) ]}",
			PARAM => $self->PARAM,
		));
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Section::DedupOn;
	use base qw(ETL::Pequel::Type::Section);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_, 
			name => $params{'name'} || 'dedup on', 
		);
		bless($self, $class);

		return $self;
	}

    sub codeInit : method 
	{ 
		my $self = shift; 
		my $c = $self->SUPER::codeInit;
		map
		(
			$c->add("my \$previous_dedup_@{[ $_->inputField->id ]} = undef;"),
			($self->items->toArray)
		);
		return $c;
	}

    sub codeBreakAfter : method 
	{ 
		my $self = shift; 
		my $engine = shift;
		$engine->addNonl("next if (");
		$engine->addNonl
		(
			join
			(
				" && ", 
				map
				(
					"defined(\$previous_dedup_@{[ $_->inputField->id ]})"
					. " && \$previous_dedup_@{[ $_->inputField->id ]}"
					. "@{[ $_->type =~ /numeric|decimal/ ? ' == ' : ' eq ' ]}"
					. "@{[ $_->inputField->codeVar ]}", 
					$self->items->toArray
				)
			)
		);
		$engine->add(");");

		map
		(
			$engine->add("\$previous_dedup_@{[ $_->inputField->id ]} = @{[ $_->inputField->codeVar ]};"),
			$self->items->toArray
		);
	}

	sub parse : method
	{
		# format-1: fld [ numeric | string ] [ asc | ascending | desc | descending ]
		# format-2: fld (type => numeric | string, sort => asc | desc)
		my $self = shift;
		my $code_line = shift || $self->lines->last->value;

		# format-1:
		my ($fld, $type) = split(/\s+/, $code_line, -1);
		$self->addItem(fld => $fld, type => $type);
	}

	sub addItem : method
	{
		my $self = shift;
		my %params = @_;
		my $fld = $params{'fld'} || $self->add_error(ref($self), 'fld');
		my $type = $params{'type'};

		$self->PARAM->error->fatalError("[10119] Field '@{[ $fld ]}' is not defined in the input section.")
			unless ($self->PARAM->sections->exists('input section')->items->exists($fld));

		$type = 'string' if (!defined($type) || $type eq '');
		my $o_type;
		$self->PARAM->error->fatalError("[10120] Invalid type '@{[ $type ]}' specified in 'Dedup On' section.")
			if (($o_type = $self->PARAM->datatypes->exists($type)) == 0);

		$self->items->add(ETL::Pequel::Field::Element->new
		(	
			name => $fld, 	
			input_field => $self->PARAM->sections->exists('input section')->items->exists($fld),
			type => $o_type,
			PARAM => $self->PARAM,
		));
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Section::GroupBy;
	use base qw(ETL::Pequel::Type::Section);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_, 
			name => $params{'name'} || 'group by', 
		);
		bless($self, $class);

		return $self;
	}

    sub codeInit : method 
	{ 
		my $self = shift; 
		my $c = $self->SUPER::codeInit;
		return $c if ($self->PARAM->properties('hash'));
		foreach ($self->items->toArray)
		{
			$c->add("my \$key_@{[ $_->inputField->id ]};");
			$c->add("my \$previous_key_@{[ $_->inputField->id ]} = undef;");
		}
		return $c;
	}

	sub parse : method
	{
		# format-1: fld [ numeric | string ]
		# format-2: fld (type => numeric | string)
		my $self = shift;
		my $code_line = shift || $self->lines->last->value;
		# format-1:
		my ($fld, $type) = split(/\s+/, $code_line, -1);
		$self->addItem(fld => $fld, type => $type);
	}

	sub addItem : method
	{
		my $self = shift;
		my %params = @_;
		my $fld = $params{'fld'} || $self->add_error(ref($self), 'fld');
		my $type = $params{'type'};

		$self->PARAM->error->fatalError("[10121] Field '@{[ $fld ]}' is not defined in the input section.")
			unless ($self->PARAM->sections->exists('input section')->items->exists($fld));

		#$self->root->error->fatalError("[10121] The 'hash' option must be used when group-by field refers to a derinved input field '@{[ $fld ]}'.")
			#unless ($self->root->s_input_section->items->exists($fld));

		$type = 'string' if (!defined($type) || $type eq '');
		my $o_type;
		$self->PARAM->error->fatalError("[10113] Invalid type '@{[ $type ]}' specified in 'Group By' section.")
			if (($o_type = $self->PARAM->datatypes->exists($type)) == 0);

		$self->items->add(ETL::Pequel::Field::Element->new
		(	
			name => $fld, 	
			input_field => $self->PARAM->sections->exists('input section')->items->exists($fld),
#>			input_field => $self->root->ifield($fld),
			type => $o_type,
			comment => "added by group-by section",
			PARAM => $self->PARAM,
		));	
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Section::Filter;
	use base qw(ETL::Pequel::Type::Section);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_, 
			name => $params{'name'} || 'filter', 
		);
		bless($self, $class);

		return $self;
	}

    sub codeBreakBefore : method 
	{ 
		my $self = shift; 
		my $engine = shift;
		foreach ($self->items->toArray)
		{
			$engine->add("next unless (@{[ $self->PARAM->parser->compile($_->value) ]});");
		}
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Section::ReplicateRecord;
	use base qw(ETL::Pequel::Type::Section);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_, 
			name => $params{'name'} || 'divert record', 
		);
		bless($self, $class);

		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		$self->PARAM->properties('lock_output', 1);
#?		$self->SUPER::compile();
		map($_->compile, $self->items->toArray); # ??
		return unless ($self->args() =~ /^pequel:|\.pql$/i);
		my $scriptname = $self->PARAM->getscriptname($self->args());
	
		my $filename = $self->PARAM->getfilepath($self->args());

		$self->PARAM->error->fatalError("[10124] Script $filename does not exist.")
			unless (-e $filename);

		$self->PARAM->error->fatalError("[10125] Recursive Pequel script call '$filename' prohibited.")
			if ($self->PARAM->pequel_script_disallow->find($scriptname));

		return if ($self->PARAM->root->PARAM->pequel_script->find($scriptname));
	
		$self->PARAM->root->PARAM->pequel_script->add(ETL::Pequel::Collection::Element->new
		(
			name => $scriptname, 
			value => ETL::Pequel::Main->new($filename, $self->PARAM),
		));
	
		my $sub_pql = $self->PARAM->root->PARAM->pequel_script->find($scriptname)->value;
		$sub_pql->PARAM->properties('input_file', '');
		$sub_pql->PARAM->properties('input_delimiter', $self->PARAM->properties('output_delimiter'));
		$sub_pql->PARAM->properties('lock_output', 1);
		if ($sub_pql->PARAM->properties('output_file') eq ''
			|| $sub_pql->PARAM->properties('output_file') eq $self->PARAM->properties('output_file'))
		{
			$sub_pql->PARAM->properties('output_file', '');
			$sub_pql->PARAM->properties('output_delimiter', $self->PARAM->properties('output_delimiter'));
			$sub_pql->PARAM->properties('pack_output', $self->PARAM->properties('pack_output'));
			$sub_pql->PARAM->properties('output_pack_fmt', $self->PARAM->properties('output_pack_fmt'));
		}
		# What about sub_pql->use_inline??
		if ($self->PARAM->properties('pack_output') && $self->name =~ /^copy output|^divert output/)
		{
			$sub_pql->PARAM->properties('unpack_input', '1');
			$sub_pql->PARAM->properties('input_pack_fmt', $self->PARAM->properties('output_pack_fmt'));
		}
		elsif ($self->PARAM->properties('unpack_input') && $self->name =~ /^copy input|^divert input/)
		{
			$sub_pql->PARAM->properties('unpack_input', '1');
			$sub_pql->PARAM->properties('input_pack_fmt', $self->PARAM->properties('input_pack_fmt'));
		}
		else
		{
			$sub_pql->PARAM->properties('unpack_input', '');
		}
		$sub_pql->PARAM->pequel_script_disallow->addAll(
			ETL::Pequel::Collection::Element->new(name => $self->PARAM->getscriptname()),
			$self->PARAM->pequel_script_disallow->toArray()
		);
		$sub_pql->generate();
		$self->PARAM->error->fatalError("[10126] The Pequel script '$filename' failed syntax check.")
			unless ($sub_pql->check =~ /Syntax\s+OK/i);

		if (!$self->PARAM->root->PARAM->packages->find(lc($self->get_fd_name())))
		{
			$self->PARAM->root->PARAM->packages->add(ETL::Pequel::Collection::Element->new
			(
				name => lc($self->get_fd_name()),
				value => $sub_pql
			));
		}

#>		Should verify that $sub_pql.input_section == this.input_section.
		return;
	}

    sub codeOpen : method 
	{ 
		my $self = shift; 
		my $c = $self->SUPER::codeOpen;
		my $fdname = $self->get_fd_name();
		if ($self->args() =~ /^pequel:|\.pql$/i)
		{
			$c->add("if (open($fdname, '|-') == 0) # Fork -- write to child");# child reads STDIN
			$c->openBlock("{");
				$c->add("&p_@{[ lc($fdname) ]}::@{[ lc($fdname) ]};");
				$c->add("exit(0);");
			$c->closeBlock;
		}
		else
		{
			my $ofile = $self->args();
			$ofile =~ s/^.*://;
			$c->add("open($fdname, '>@{[ $ofile ]}');");
		}
		return $c;
	}

    sub codeBreakBefore : method 	#--> sub printBefore()
	{ 
		my $self = shift; 
		my $engine = shift;

		# Optimise by combining into if-elsif block...
		$engine->addNonl("if ((");
		$engine->addNonl(join(") || (", map($self->PARAM->parser->compile($_->value), $self->items->toArray)));
		$engine->add    ("))");
		$engine->openBlock("{");
			$engine->addNonl("print @{[ $self->get_fd_name() ]} ");
#			Packing not allowed when use_inline used:
			($self->PARAM->properties('use_inline'))
				? $engine->addNonl("\@I_VAL[0..LAST_ICELL]") # NOTE: derived input fields not yet calculated!
				: $engine->addNonl("\$_");
			$engine->add(";");
			$engine->add("next;") unless ($self->name =~ /^copy/i);
		$engine->closeBlock;
	}

    sub codeClose : method 
	{ 
		my $self = shift; 
		my $c = $self->SUPER::codeClose;
		$c->add("close(@{[ $self->get_fd_name() ]});");
		return $c;
	}

	sub get_fd_name
	{
		my $self = shift; 
		my $fdname = $self->args();
		$fdname =~ s/^.*://;
		$fdname =~ s/\..*$//;
		$fdname = "@{[ uc((split(/\s+/, $self->name))[0]) ]}_@{[ uc((split(/\s+/, $self->name))[1]) ]}_@{[ uc($fdname) ]}"; 
		return $fdname;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Section::DivertRecord;
	use base qw(ETL::Pequel::Type::Section::ReplicateRecord);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_, 
			name => $params{'name'} || 'divert record', 
		);
		bless($self, $class);

		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Section::DivertInputRecord;
	use base qw(ETL::Pequel::Type::Section::ReplicateRecord);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_, 
			name => $params{'name'} || 'divert input record', 
		);
		bless($self, $class);

		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Section::CopyRecord;
	use base qw(ETL::Pequel::Type::Section::ReplicateRecord);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_, 
			name => $params{'name'} || 'copy record', 
		);
		bless($self, $class);

		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Section::CopyInputRecord;
	use base qw(ETL::Pequel::Type::Section::ReplicateRecord);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_, 
			name => $params{'name'} || 'copy input record', 
		);
		bless($self, $class);

		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Section::DivertOutputRecord;
	use base qw(ETL::Pequel::Type::Section::ReplicateRecord);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_, 
			name => $params{'name'} || 'divert output record', 
		);
		bless($self, $class);

		return $self;
	}

    sub codeBreakBefore : method 
	{ 
		my $self = shift; 
		my $engine = shift;
	}

	sub codePrintAfter : method
	{
		my $self = shift;
		my $engine = shift;
		$engine->addNonl("if (");
		$engine->addNonl(join(" || ", map($self->PARAM->parser->compileOutput($_->value), $self->items->toArray)));
		$engine->add    (")");
		$engine->openBlock("{");
			$engine->codePrint($self->get_fd_name);
			$engine->add("next;");
		$engine->closeBlock;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Section::CopyOutputRecord;
	use base qw(ETL::Pequel::Type::Section::ReplicateRecord);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_, 
			name => $params{'name'} || 'copy output record', 
		);
		bless($self, $class);

		return $self;
	}

    sub codeBreakBefore : method 
	{ 
		my $self = shift; 
		my $engine = shift;
	}

	sub codePrintAfter : method
	{
		my $self = shift;
		my $engine = shift;
		$engine->addNonl("if (");
		$engine->addNonl(join(" || ", map($self->PARAM->parser->compileOutput($_->value), $self->items->toArray)));
		$engine->add    (")");
		$engine->openBlock("{");
			$engine->codePrint($self->get_fd_name);
		$engine->closeBlock;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Section::Output;
	use base qw(ETL::Pequel::Type::Section);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_, 
			name => $params{'name'} || 'output section',
#>			syntax => 'type(&type) name([_|\w|\d]+) [&inputField | aggregate(&aggregate) [serial_start(\d+)|*|&inputField] [<where> condition(.*)] | aggregate(=) calc(.*)]
		);
		bless($self, $class);

		return $self;
	}

    sub codeInit : method 
	{ 
		my $self = shift; 
		my $c = $self->SUPER::codeInit;
		return $c if ($self->PARAM->properties('hash')); #???
		$c->add("foreach my \$f (1..@{[ $self->items->size ]}) { \$O_VAL[\$f] = undef; }");
		return $c;
	}

    sub codeOpen : method 
	{ 
		my $self = shift; 
		my $c = $self->SUPER::codeOpen;
		return $c if ($self->PARAM->sections->find('sort output')->items->size());
		return $c unless ($self->PARAM->properties('output_file'));
		my $ofl = $self->PARAM->getfilepath($self->PARAM->properties('output_file'));
		$c->add("open(STDOUT, '@{[ $self->PARAM->properties('output_file_append') ? q{>>} : q{>} ]}$ofl')");
		$c->over;
		$c->add("|| die(\"Cannot open $ofl: \$!\");");
		$c->back;
		return $c;
	}

	sub codeClose : method
	{
		my $self = shift;
		my $c = $self->SUPER::codeClose;
		$c->add("close(STDOUT);") if ($self->PARAM->properties('output_file'));
		return $c;
	}

	sub parse : method
	{
		# format-1:   < numeric | decimal | string | date | time> <output field name> <aggregate expression> [, ...]
		# format-2: fld(type => < numeric | decimal | string | date | time >, aggregate = < sum | min | max | ... | = | <input-field> >, exp = ...
		my $self = shift;
		my $code_line = shift || $self->lines->last->value;

		$code_line =~ s/date\s+\(/date\(/g;
		my ($type, $field, @clause) = split(/\s+/, $code_line, -1);
		$self->addItem(type => $type, field => $field, clause => join(" ", @clause));
	}

	sub addItem : method
	{
		my $self = shift;
		my %params = @_;
		my $type = $params{'type'} || $self->add_error(ref($self), 'type');
		my $field = $params{'field'} || $self->add_error(ref($self), 'field');
		my $clause = $params{'clause'};

		my $date_type;
		if ($type =~ s/^date\(['"]*(.*?)['"]*\)/date/i) { $date_type = $1; }

		my $o_type;
		$self->PARAM->error->fatalError("[10127] Invalid type '@{[ $type ]}' for output field '@{[ $field ]}'") 
			if (($o_type = $self->PARAM->datatypes->exists($type)) == 0);

		my $o_date_type = $self->PARAM->datatypes->exists($self->PARAM->properties('default_datetype')) 
			unless ($date_type);
		$self->PARAM->error->fatalError("[10128] Invalid date type '@{[ $date_type ]}' used for output field '@{[ $field ]}'") 
			if ($type eq 'date' && $date_type && ($o_date_type = $self->PARAM->datatypes->exists($date_type)) == 0);

		$self->PARAM->error->fatalError("[10129] Duplicate output field name '$field' invalid.") 
			if ($self->PARAM->sections->exists('output section')->items->exists($field));

		$self->items->add(ETL::Pequel::Field::Output::Element->new
		(
			name => $field,
			type => $o_type,
			date_type => $o_date_type,
			clause => $clause,
			PARAM => $self->PARAM,
		));
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Section::SortOutput;
	use base qw(ETL::Pequel::Type::Section);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_, 
			name => $params{'name'} || 'sort output', 
#>			syntax => 'name(&inputField->name) [type(&type) [direction(des|asc)]]'
		);
		bless($self, $class);

		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		$self->PARAM->error->warning("[10130] 'pack_output' not allowed when 'sort output' used -- ignored.")
			if ($self->PARAM->properties('pack_output'));
		$self->PARAM->properties('pack_output', 0);
#?		$self->SUPER::compile();
		map($_->compile, $self->items->toArray);
	}

    sub codeOpen : method 
	{ 
		my $self = shift; 
		my $c = $self->SUPER::codeOpen;
		my $sortstr;
		$sortstr = "@{[ $self->PARAM->properties('sort_cmd') ]}";
		$sortstr .= " @{[ $self->PARAM->properties('sort_args') ]}";
		$sortstr .= " -t'@{[ $self->PARAM->properties('input_delimiter') ]}' -y";
		$sortstr .= " -T @{[ $self->PARAM->properties('sort_tmp_dir') ]}" 
			if ($self->PARAM->properties('sort_tmp_dir'));
		foreach ($self->PARAM->sections->find('sort output')->items->toArray)
		{
			$sortstr .= ' -k ';
			foreach my $comma ('', ',')
			{
				$sortstr .= $comma . "@{[ $_->inputField->number ]}";
				$sortstr .= 'n' if ($_->type->name eq 'numeric' || $_->type->name eq 'decimal');
				$sortstr .= 'r' if ($_->direction == $self->PARAM->SORT_DES);
			}
		}
		$sortstr .= " 2>/dev/null";

		$c->addNonl("open(STDOUT, '|-', ");
		if ($self->PARAM->properties('output_file'))
		{
			$c->add("q{$sortstr >@{[ $self->PARAM->getfilepath($self->PARAM->properties('output_file')) ]}});");
		}
		else
		{
			$c->add("q{$sortstr |});");
		}
		return $c;
	}

	sub codeClose : method
	{
		my $self = shift;
		my $c = $self->SUPER::codeClose;
		$c->add("close(STDOUT);") if (!$self->PARAM->properties('output_file'));
		return $c;
	}

	sub parse : method
	{
		# format-1: fld [ numeric | string ] [ asc | ascending | desc | descending ]
		# format-2: fld (type => numeric | string, sort => asc | desc)
		my $self = shift;
		my $code_line = shift || $self->lines->last->value;

		# format-1:
		my ($fld, $type, $sort) = split(/\s+/, $code_line, -1);
		$self->addItem(type => $type, fld => $fld, sort => $sort);
	}

	sub addItem : method
	{
		my $self = shift;
		my %params = @_;
		my $fld = $params{'fld'} || $self->add_error(ref($self), 'fld');
		my $type = $params{'type'};
		my $sort = $params{'sort'};

		my $iFld;
		if (($iFld = $self->PARAM->sections->find('output section')->items->exists($fld)) == 0)
		{
			$iFld = $self->PARAM->sections->find('input section')->items->exists($fld)
				if ($self->PARAM->properties('transfer'));
		}
		$self->PARAM->error->fatalError("[10131] Field '@{[ $fld ]}' is not available in the output format.")
			if ($iFld == 0);

		$type = 'string' if (!defined($type) || $type eq '');
		my $o_type;
		$self->PARAM->error->fatalError("[10132] Invalid type '@{[ $type ]}' specified in 'Sort Output' section.")
			if (($o_type = $self->PARAM->datatypes->exists($type)) == 0);

		$self->items->add(ETL::Pequel::Field::Element->new
		(	
			name => $fld, 	
			type => $o_type,
			input_field => $iFld,
			direction => "@{[ (defined($sort) && $sort =~ /des/ ? $self->PARAM->SORT_DES : $self->PARAM->SORT_ASC) ]}",
			PARAM => $self->PARAM,
		));
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Section::DisplayMessageOnInput;
	use base qw(ETL::Pequel::Type::Section);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_, 
			name => $params{'name'} || 'display message on input', 
		);
		bless($self, $class);

		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		$self->PARAM->error->fatalError("[10133] Section '@{[ $self->name() ]}' requires an argument.")
			unless (defined($self->args()));
		map($_->compile, $self->items->toArray);
	}

	sub codeBreakBefore : method # !! wrong event
	{
		my $self = shift;
		my $engine = shift;
		$engine->add("if (( @{[ join(') || (', map($self->PARAM->parser->compile($_->value), $self->items->toArray)) ]} ))");
		$engine->openBlock("{");
		my $msg = $self->args();
		$msg =~ s/^[\'\"]//;
		$msg =~ s/[\'\"]$//;
		$engine->add(qq{print STDERR "@{[ $self->PARAM->parser->compile($msg) ]}";});
		$engine->closeBlock;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Section::DisplayMessageOnInputAbort;
	use base qw(ETL::Pequel::Type::Section);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_, 
			name => $params{'name'} || 'display message on input abort', 
		);
		bless($self, $class);

		return $self;
	}

	sub codeBreakBefore : method
	{
		my $self = shift;
		my $engine = shift;
		$engine->add("if (( @{[ join(') || (', map($self->PARAM->parser->compile($_->value), $self->items->toArray)) ]} ))");
		$engine->openBlock("{");
		if (defined($self->args()))
		{
			my $msg = $self->args();
			$msg =~ s/^[\'\"]//;
			$msg =~ s/[\'\"]$//;
			$engine->add(qq{print STDERR "@{[ $self->PARAM->parser->compile($msg) ]}";});
		}
		$engine->add(qq{print STDERR "Process aborted at record " . @{[ $self->PARAM->parser->compile('&input_record_count()') ]};});
		$engine->add("last;");
		$engine->closeBlock;
	}
}
# ----------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Section::DisplayMessageOnOutput;
	use base qw(ETL::Pequel::Type::Section);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_, 
			name => $params{'name'} || 'display message on output', 
		);
		bless($self, $class);

		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		$self->PARAM->error->fatalError("[10134] Section '@{[ $self->name() ]}' requires an argument.")
			unless (defined($self->args()));
		map($_->compile, $self->items->toArray);
	}

	sub codePrintAfter : method # !! wrong event
	{
		my $self = shift;
		my $engine = shift;
		$engine->add("if (( @{[ join(') || (', map($self->PARAM->parser->compileOutput($_->value), $self->items->toArray)) ]} ))");
		$engine->openBlock("{");
		my $msg = $self->args();
		$msg =~ s/^[\'\"]//;
		$msg =~ s/[\'\"]$//;
		$engine->add(qq{print STDERR "@{[ $self->PARAM->parser->compileOutput($msg) ]}";});
		$engine->closeBlock;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Section::DisplayMessageOnOutputAbort;
	use base qw(ETL::Pequel::Type::Section);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_, 
			name => $params{'name'} || 'display message on output abort', 
		);
		bless($self, $class);

		return $self;
	}

	sub codePrintAfter : method
	{
		my $self = shift;
		my $engine = shift;
		$engine->add("if (( @{[ join(') || (', map($self->PARAM->parser->compileOutput($_->value), $self->items->toArray)) ]} ))");
		$engine->openBlock("{");
		if (defined($self->args()))
		{
			my $msg = $self->args();
			$msg =~ s/^[\'\"]//;
			$msg =~ s/[\'\"]$//;
			$engine->add(qq{print STDERR "@{[ $self->PARAM->parser->compileOutput($msg) ]}";});
		}
		$engine->add(qq{print STDERR "Process aborted at record " . @{[ $self->PARAM->parser->compile('&input_record_count()') ]};});
		$engine->add("last;");
		$engine->closeBlock;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Section::Reject;
	use base qw(ETL::Pequel::Type::Section);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_, 
			name => $params{'name'} || 'reject', 
		);
		bless($self, $class);

		return $self;
	}

    sub codeInit : method 
	{ 
		my $self = shift; 
		my $c = $self->SUPER::codeInit;
		$c->add("open(REJECT, \">@{[ $self->PARAM->properties('reject_file') ]}\");");
		return $c;
	}

    sub codeBreakBefore : method 
	{ 
		my $self = shift; 
		my $c = $self->SUPER::codeBreakBefore;
		$c->add("if (( @{[ join(') || (', map($self->PARAM->parser->compile($_->value), $self->items->toArray)) ]} ))");
		$c->openBlock("{");
		$c->add("local \$\\=\"\\n\";");
		$c->add("print REJECT \$_;");
		$c->add("next;");
		$c->closeBlock;
		return $c;
	}

	sub addItem : method	# !! TODO: check this
	{
		my $self = shift;
		$self->SUPER::addItem(@_);

		$self->PARAM->properties('reject_file', "@{[ $self->PARAM->properties('script_name') ]}.reject")
			if ($self->PARAM->properties('reject_file') eq '');
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Section::Having;
	use base qw(ETL::Pequel::Type::Section);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_, 
			name => $params{'name'} || 'having', 
		);
		bless($self, $class);

		return $self;
	}

	sub addItem : method	# !! TODO: check this
	{
		my $self = shift;

		$self->PARAM->error->fatalError("[10135] Group By section must be defined before 'having' section can be used.")
			if ($self->PARAM->sections->exists('group by')->lines == 0);

		$self->PARAM->error->fatalError("[10136] Output section must be defined before 'having' section can be used.")
			if ($self->PARAM->sections->exists('output section')->lines == 0);

		$self->SUPER::addItem(@_);
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Section::Summary;
	use base qw(ETL::Pequel::Type::Section);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_, 
			name => $params{'name'} || 'summary section', 
		);
		bless($self, $class);

		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Section::InitMONTH; # TODO
	use base qw(ETL::Pequel::Type::Section);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_, 
			name => $params{'name'} || 'init _MONTH', 
		);
		bless($self, $class);

		return $self;
	}

	sub parse : method
	{
		my $self = shift;
		my $code_line = shift || $self->lines->last->value;

		$code_line = $self->PARAM->parser->saveSpaces($code_line);
		$code_line =~ s/'|"//g;
		my ($key, @values) = split(/\s+/, $code_line, -1);
		$key = $self->PARAM->parser->restoreSpaces($key);
		map($_ = $self->PARAM->parser->restoreSpaces($_), @values);

		my $t;
		if (($t = $self->PARAM->tables->exists('_MONTH')) == 0)
		{
			$self->items->add(ETL::Pequel::Type::Table::Local->new
			(
				name => '_MONTH',
				PARAM => $self->PARAM,
#>				type => ETL::Pequel::Type::Table::TYPE_MONTH
			));
			$t = $self->PARAM->tables->last;

			foreach (1..@values)
			{
				$t->fields->add(ETL::Pequel::Table::Field::Element->new(
					name => $_,
					PARAM => $self->PARAM,
				));
			}
		}
		# add the new key value
		$t->data->add
		(
			ETL::Pequel::Table::Data::Element->new
			(
				name => $key, 
				value => ETL::Pequel::Collection::Vector->new
				(
					map(ETL::Pequel::Table::Data::Element->new(name => $key, value => $_), @values)
					# May not handle properly if @values is zero
				)
			)
		);
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Section::InitPERIOD; # TODO
	use base qw(ETL::Pequel::Type::Section);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_, 
			name => $params{'name'} || 'init _PERIOD', 
		);
		bless($self, $class);

		return $self;
	}

	sub parse : method
	{
		my $self = shift;
		my $code_line = shift || $self->lines->last->value;

		$code_line = $self->PARAM->parser->saveSpaces($code_line);
		$code_line =~ s/'|"//g;
		my ($key, @values) = split(/\s+/, $code_line, -1);
		$key = $self->PARAM->parser->restoreSpaces($key);
		map($_ = $self->PARAM->parser->restoreSpaces($_), @values);

		my $t;
		if (($t = $self->PARAM->tables->exists('_PERIOD')) == 0)
		{
			$self->items->add(ETL::Pequel::Type::Table::Local->new
			(
				name => '_PERIOD',
#>				type => ETL::Pequel::Type::Table::TYPE_PERIOD
			));
			$t = $self->PARAM->tables->last;

			foreach (1..@values)
			{
				$t->fields->add(ETL::Pequel::Table::Field::Element->new(name => $_));
			}
		}
		# add the new key value
		$t->data->add
		(
			ETL::Pequel::Table::Data::Element->new
			(
				name => $key, 
				value => ETL::Pequel::Collection::Vector->new
				(
					map(ETL::Pequel::Table::Data::Element->new(name => $key, value => $_), @values)
					# May not handle properly if @values is zero
				)
			)
		);
	}
}
# ----------------------------------------------------------------------------------------------------
# table(local: | file: | pequel: | oracle:connect_str | sqlite: | other, static/dynamic, ...)
# merge(local: | file: | pequel: | oracle:connect_str | sqlite: | other, ...)
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Section::InitTable;
	use base qw(ETL::Pequel::Type::Section);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_, 
			name => $params{'name'} || 'init table', 
		);
		bless($self, $class);

		return $self;
	}

	sub parse : method
	{
		my $self = shift;
		my $code_line = shift || $self->lines->last->value;

		$code_line = $self->PARAM->parser->saveSpaces($code_line);
		$code_line =~ s/'|"//g;
		my ($table, $key, @values) = split(/\s+/, $code_line, -1);
		$key = $self->PARAM->parser->restoreSpaces($key);
		map($_ = $self->PARAM->parser->restoreSpaces($_), @values);
		$self->addItem(name => $table, key => $key, values => \@values );
	}

	sub addItem : method
	{
		my $self = shift;
		my %params = @_;
		my $name = $params{'name'} || $self->add_error(ref($self), 'name');
		my $key = $params{'key'} || $self->add_error(ref($self), 'key');
		my $values = $params{'values'} || $self->add_error(ref($self), 'values');

		my $load_at_runtime = ($name =~ s/^_//);

		my $t;
		if (($t = $self->PARAM->tables->find($name)) == 0)
		{
			$self->items->add(ETL::Pequel::Type::Table::Local->new
			(
				name => $name,
				PARAM => $self->PARAM
#>				type => ETL::Pequel::Type::Table::TYPE_LOCAL
			));
			$t = $self->PARAM->tables->last();

			foreach (1..@$values)
			{
				$t->fields->add(ETL::Pequel::Table::Field::Element->new(
					name => $_,
					PARAM => $self->PARAM
				));
			}
		}
		# add the new key value
		$t->data->add
		(
			ETL::Pequel::Table::Data::Element->new
			(
				name => $key, 
				value => ETL::Pequel::Collection::Vector->new
				(
					map
					(
						ETL::Pequel::Table::Data::Element->new
						(
							name => $key, 
							value => $_, 
							PARAM => $self->PARAM
						),
						@$values
					)
					# May not handle properly if @values is zero
				),
				PARAM => $self->PARAM,
			)
		);
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Section::LoadTable;
	use base qw(ETL::Pequel::Type::Section);

	our $this = __PACKAGE__;

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_, 
			name => $params{'name'} || 'load table', 
		);
		bless($self, $class);

		$self->merge($params{'merge'} || 0);
		$self->persistent($params{'persistent'} || 0);

		return $self;
	}

	sub persistent : method 
	{ 
		my $self = shift; 
		$self->{$this}->{PERSISTENT} = shift if (@_); 
		return $self->{$this}->{PERSISTENT}; 
	}
	
	sub merge : method 
	{ 
		my $self = shift; 
		$self->{$this}->{MERGE} = shift if (@_); 
		return $self->{$this}->{MERGE}; 
	}

	sub parse : method
	{
		# format-1: <table> [ <filename> [ <keycol> [ <valfld>=<valcol> [...] ] ] ]
		my $self = shift;
		my $code_line = shift || $self->lines->last->value;

		$code_line =~ s/\s*=\s*/=/g;
		my ($table, $filename, $keycol, $keytype, @fields) = split(/\s+/, $code_line, -1);
		$self->addItem(name => $table, filename => $filename, keycol => $keycol, keytype => $keytype, field_list => \@fields );
	}

	sub addItem : method
	{
		my $self = shift;
		my %params = @_;
		my $name = $params{'name'} || $self->add_error(ref($self), 'name');
		my $filename = $params{'filename'} || $self->add_error(ref($self), 'filename');
		my $keycol = $params{'keycol'} || $self->add_error(ref($self), 'keycol');
		my $keytype = $params{'keytype'} || $self->add_error(ref($self), 'keytype');
		my $field_list = $params{'field_list'} || $self->add_error(ref($self), 'field_list');

		my $load_at_runtime = ($name =~ s/^_//);

		$self->PARAM->error->fatalError("[10137] Invalid table statement -- keycol (3rd arg) should be a number.")
			unless ($keycol =~ /\d+/);

		$self->PARAM->error->fatalError("[10138] Invalid table statement -- keytype (4th arg) should be 'NUMERIC' or 'STRING'.")
			unless ($keytype =~ /^(NUMERIC|STRING)$/i);

		if ($load_at_runtime)
		{
			$self->items->add(ETL::Pequel::Type::Table::External->new
			(
				name => $name,
				data_source_filename => $filename,
				key_column => $keycol,
				key_type => $keytype,
				persistent => 0,
				PARAM => $self->PARAM,
			));
		}
		else	# should be better way of doing this...
		{
			$self->items->add(ETL::Pequel::Type::Table::Local->new
			(
				name => $name,
				data_source_filename => $filename,
				key_column => $keycol,
				persistent => 0,
				PARAM => $self->PARAM,
			));
		}
		my $t = $self->PARAM->tables->last;
		foreach (@$field_list)
		{
			if (m/(.*)=(\d+)/ || m/(.*)\s*\((\d+)\)/)
			{
				$t->fields->add(ETL::Pequel::Table::Field::Element->new(
					name => $1, 
					column => $2,
					PARAM => $self->PARAM,
				));
			}
			else
			{
				my $name = $1;
				my $column = $2;
				$self->PARAM->error->fatalError("[10139] Invalid table field column specification for field '$name'")
					unless (defined($name) && defined($column) && $column =~ /\d+/);
			}
		}
		return if ($load_at_runtime);
		
		$filename = $self->PARAM->getfilepath($filename);

		$self->PARAM->error->fatalError("[10140] Table $name datasource $filename does not exist.")
			unless (-e $filename);
		$self->PARAM->error->fatalError("[10141] Table $name datasource $filename is unreadable.")
			unless (-r $filename);

		($filename =~ /\.gz$|\.GZ$|\.z$|\.Z$|\.zip$/)
			? open(LOAD_TABLE, "@{[ $self->PARAM->properties('gzcat_cmd') ]} @{[ $self->PARAM->properties('gzcat_args') ]} $filename |")
			: open(LOAD_TABLE, "$filename");

		while (<LOAD_TABLE>)
		{
			chomp;
			my (@flds) = split("[|]");
			next if ($t->data->exists($flds[$keycol-1]));
			$t->data->add
			(
				ETL::Pequel::Table::Data::Element->new
				(
					name => $flds[$keycol-1], 
					value => ETL::Pequel::Collection::Vector->new
					(
						map
						(
							ETL::Pequel::Table::Data::Element->new
							(
								name => $flds[$keycol-1], 
								value => $_,
								PARAM => $self->PARAM,
							), 
							@flds[map($_->column -1, $t->fields->toArray)]
						)
					),
					PARAM => $self->PARAM,
				)
			);
		}
		close(LOAD_TABLE);
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Section::LoadTable::Pequel;
	use base qw(ETL::Pequel::Type::Section::LoadTable);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_, 
			name => $params{'name'} || 'load table pequel', 
			persistent => 0,
		);
		bless($self, $class);

		return $self;
	}

	sub parse : method
	{
		# format: <table> <pequel-script-name> [ <keyfield-name> ]
		my $self = shift;
		my $code_line = shift || $self->lines->last->value;

		$code_line =~ s/\s*=\s*/=/g;
		my ($table, $scriptname, $keyfield, $keytype) = split(/\s+/, $code_line, -1);
		$self->addItem(name => $table, scriptname => $scriptname, keyfield => $keyfield, keytype => $keytype);
	}

	sub addItem : method
	{
		my $self = shift;
		my %params = @_;
		my $name = $params{'name'} || $self->add_error(ref($self), 'name');
		my $scriptname = $params{'scriptname'} || $self->add_error(ref($self), 'scriptname');
		my $keyfield = $params{'keyfield'} || $self->add_error(ref($self), 'keyfield');
		my $keytype = $params{'keytype'} || 'STRING';

		my $filename = $self->PARAM->getfilepath($scriptname);

		$self->PARAM->error->fatalError("[10142] Table $name datasource script $filename does not exist.")
			unless (-e $filename);
		$self->PARAM->error->fatalError("[10143] Table $name datasource script $filename is unreadable.")
			unless (-r $filename);

		$self->PARAM->pequel_script->add(ETL::Pequel::Collection::Element->new
		(
			name => $filename, 
			value => ETL::Pequel::Main->new($filename, $self->PARAM),
			PARAM => $self->PARAM
		));
		my $tpql = $self->PARAM->pequel_script->find($filename)->value;
		$tpql->generate();

		$self->PARAM->error->fatalError("[10144] The Pequel script '$filename' failed syntax check.")
			unless ($tpql->check =~ /Syntax\s+OK/i);

		$self->PARAM->error->fatalError("[10145] The Pequel script '$filename' must have an 'input_file' option specified when used as a table data loader.")
			unless ($tpql->PARAM->properties('input_file') ne '');

		$self->PARAM->error->fatalError("[10146] The Pequel script '$filename' must not have an 'output_file' option specified when used as a input_file.")
			if ($tpql->PARAM->properties('output_file'));

#>		How to prevent circular pequel table calls???

		my @pequel_table_fields;
		push(@pequel_table_fields, map($_->name, grep($_->name !~ /^_/, $tpql->PARAM->sections->exists('input section')->items->toArray)))
			if ($tpql->PARAM->properties('transfer'));
		push(@pequel_table_fields, map($_->name, grep($_->name !~ /^_/, $tpql->PARAM->sections->exists('output section')->items->toArray))); 
		$keyfield = $pequel_table_fields[0] unless (defined($keyfield));
		my $keycol=0;
		foreach my $f (0..$#pequel_table_fields) 
		{ 
			$keycol = $f+1 if ($keyfield eq $pequel_table_fields[$f]); 
		}
		$self->PARAM->error->fatalError("[10147] Field '$keyfield' does not exist in table '$name'.")
			if ($keycol == 0);

		$self->PARAM->error->fatalError("[10148] Invalid table statement -- keytype (4th arg) should be 'NUMERIC' or 'STRING'.")
			if ($keytype !~ /^(NUMERIC|STRING)$/i);

#>		what about static (embedded) tables?
		$self->items->add(ETL::Pequel::Type::Table::External::Pequel->new
		(
			name => $name,
			data_source_filename => $filename,
			key_column => $keycol,
			key_type => $keytype,
			persistent => 0,
			PARAM => $self->PARAM,
		));
		my $t = $self->PARAM->tables->last;

		# Duplicate field names -- keep only last occurance...
		my %check_fields;
		foreach (reverse(@pequel_table_fields))
		{
			$_ = '__IGNORE__$_' if ($check_fields{$_}++ > 0);
		}
		foreach my $fnum (0..$#pequel_table_fields)
		{
			next if ($pequel_table_fields[$fnum] =~ /^__IGNORE__/);
			next if ($fnum+1 == $keycol);
			$t->fields->add(ETL::Pequel::Table::Field::Element->new(
				name => $pequel_table_fields[$fnum], 
				column => $fnum+1,
				PARAM => $self->PARAM,
			));
		}
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Section::LoadTable::Persistent;
	use base qw(ETL::Pequel::Type::Section::LoadTable);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_, 
			name => $params{'name'} || 'load table persistent', 
			persistent => 1,
		);
		bless($self, $class);

		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Section::LoadTable::Sqlite;
	use ETL::Pequel::Type::Table::Sqlite;
	use base qw(ETL::Pequel::Type::Section::LoadTable);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_, 
			name => $params{'name'} || 'load table sqlite', 
		);
		bless($self, $class);

		return $self;
	}

	sub parse : method
	{
		# format-1: <table> [ <filename> [ <keycol> [ <valfld>=<valcol> [...] ] ] ]
		my $self = shift;
		my $code_line = shift || $self->lines->last->value;

		$code_line =~ s/\s*=\s*/=/g;
		my ($table, $filename, $keycol, $keytype, @fields) = split(/\s+/, $code_line, -1);
		$table =~ s/^_//;

		$self->PARAM->error->fatalError("[10149] Invalid table statement ($code_line).")
			unless (defined($table) && defined($filename) && defined($keycol) && defined($keytype));

		$self->PARAM->error->fatalError("[10150] Invalid table statement -- keycol (3rd arg) should be a number ($code_line).")
			unless ($keycol =~ /\d+/);

		$self->PARAM->error->fatalError("[10151] Invalid table statement -- keytype (4th arg) should be 'INTEGER' or 'VARCHAR' ($code_line).")
			unless ($keytype =~ /^(INTEGER|VARCHAR)$/i);

		$self->merge
			? $self->items->add(ETL::Pequel::Type::Table::Sqlite::Merge->new
				(
					name => $table,
					data_source_filename => $filename,
					key_column => $keycol,
					key_type => $keytype,
					PARAM => $self->PARAM,
				))
			: $self->items->add(ETL::Pequel::Type::Table::Sqlite->new
				(
					name => $table,
					data_source_filename => $filename,
					key_column => $keycol,
					key_type => $keytype,
					PARAM => $self->PARAM,
				));
		foreach (@fields)
		{
			if (m/(.*)=(\d+)/ || m/(.*)\s*\((\d+)\)/)
			{
				$self->items->last->fields->add(ETL::Pequel::Table::Field::Element->new(
					name => $1, 
					column => $2,
					PARAM => $self->PARAM,
				));
			}
			else
			{
				my $name = $1;
				my $column = $2;
				$self->PARAM->error->fatalError("[10152] Invalid table field column specification for field '$name' ($code_line)")
					unless (defined($name) && defined($column) && $column =~ /\d+/);
			}
		}
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Section::LoadTable::Sqlite::Merge;	# --> ETL::Pequel::Table::Sqlite.pm
	use base qw(ETL::Pequel::Type::Section::LoadTable::Sqlite);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_, 
			name => $params{'name'} || 'load table sqlite merge', 
			merge => 1,
		);
		bless($self, $class);

		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Section::LoadTable::Oracle;
	use ETL::Pequel::Type::Table::Oracle;
	use base qw(ETL::Pequel::Type::Section::LoadTable);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_, 
			name => $params{'name'} || 'load table oracle', 
		);
		bless($self, $class);

		return $self;
	}

	sub parse : method
	{
		my $self = shift;
		my $code_line = shift || $self->lines->last->value;

		$code_line =~ s/\s*=\s*/=/g;
		my ($table, $filename, $connect, $keycol, $keytype, @fields) = split(/\s+/, $code_line, -1);

		$self->PARAM->error->fatalError("[10153] Invalid oracle table statement ($code_line).")
			unless (defined($table) && defined($filename) && defined($connect) && defined($keycol) && defined($keytype));

		$self->PARAM->error->fatalError("[10154] Invalid oracle table statement -- keytype (4th arg) should be in 'type(length)' format ($code_line).")
			unless ($keytype =~ /^\w+\(\d+\)$/);

		$self->PARAM->error->fatalError("[10155] Invalid oracle table statement -- keycol (3rd arg) should be a number ($code_line).")
			unless ($keycol =~ /\d+/);

		$table =~ s/^_//;
		$connect =~ s/["']//g;
#<		my ($username, $password, $db_name) = split("[/@]", $connect, -1);
		my ($username, $password, $db_name) = $connect =~ /\// ? split("[/@]", $connect, -1) : ( (split("[@]", $connect, -1))[0], '', (split("[@]", $connect, -1))[1] );
		$self->PARAM->error->fatalError("[10156] Invalid oracle table statement -- connect should be in the format 'user/passwd\@db' ($code_line).")
			if ($db_name eq '');

		$self->merge
			? $self->items->add(ETL::Pequel::Type::Table::Oracle::Merge->new
				(
					name => $table,
					data_source_filename => $filename,
					key_column => $keycol,
					key_type => $keytype,
					username => $username,
					password => $password,
					db_name => $db_name,
					PARAM => $self->PARAM,
				))
			: $self->items->add(ETL::Pequel::Type::Table::Oracle->new
				(
					name => $table,
					data_source_filename => $filename,
					key_column => $keycol,
					key_type => $keytype,
					username => $username,
					password => $password,
					db_name => $db_name,
					PARAM => $self->PARAM,
				));
		foreach (@fields)
		{
			if (m/(.*)=(\d+)/ || m/(.*)\s*\((\d+)\)/)
			{
#> column may not be same as keyColumn...
				$self->PARAM->error->fatalError("[10157] Invalid table field name (too long:max is 34) for field '$1'")
					if (length($1) > 34);
				$self->items->last->fields->add(ETL::Pequel::Table::Field::Element->new(
					name => $1, 
					column => $2,
					PARAM => $self->PARAM,
				));
			}
			else
			{
				my $name = $1;
				my $column = $2;
				$self->PARAM->error->fatalError("[10158] Invalid table field column specification for field '$_' ($code_line)")
					unless (defined($name) && defined($column) && $column =~ /\d+/);
			}
		}
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Section::LoadTable::Oracle::Merge;
	use base qw(ETL::Pequel::Type::Section::LoadTable::Oracle);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_, 
			name => $params{'name'} || 'load table oracle merge', 
			merge => 1,
		);
		bless($self, $class);

		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Sections;
	use ETL::Pequel::Collection;	#+++++
	use base qw(ETL::Pequel::Collection::Vector);

	sub new : method
	{
		my $self = shift;
		my $param = shift;
		my $class = ref($self) || $self;
		$self = $class->SUPER::new(@_);
		bless($self, $class);

		$self->addAll
		(
			ETL::Pequel::Type::Section::Options->new(PARAM => $param),
			ETL::Pequel::Type::Section::Description->new(PARAM => $param),
			ETL::Pequel::Type::Section::UsePackage->new(PARAM => $param),
#TODO		ETL::Pequel::Type::Section::InitMONTH->new(PARAM => $param),
#TODO		ETL::Pequel::Type::Section::InitPERIOD->new(PARAM => $param),
			ETL::Pequel::Type::Section::InitTable->new(PARAM => $param),
			ETL::Pequel::Type::Section::LoadTable->new(PARAM => $param),
			ETL::Pequel::Type::Section::LoadTable::Pequel->new(PARAM => $param),
			ETL::Pequel::Type::Section::LoadTable::Sqlite::Merge->new(PARAM => $param),
			ETL::Pequel::Type::Section::LoadTable::Sqlite->new(PARAM => $param),
			ETL::Pequel::Type::Section::LoadTable::Oracle::Merge->new(PARAM => $param),
			ETL::Pequel::Type::Section::LoadTable::Oracle->new(PARAM => $param),
			ETL::Pequel::Type::Section::Input->new(PARAM => $param),
#TODO		ETL::Pequel::Type::Section::InputMerge->new(PARAM => $param),
			ETL::Pequel::Type::Section::FieldProcess::Pre->new(PARAM => $param),
			ETL::Pequel::Type::Section::DisplayMessageOnInput->new(PARAM => $param),
			ETL::Pequel::Type::Section::DisplayMessageOnInputAbort->new(PARAM => $param),
			ETL::Pequel::Type::Section::SortBy->new(PARAM => $param),
			ETL::Pequel::Type::Section::GroupBy->new(PARAM => $param),
			ETL::Pequel::Type::Section::DedupOn->new(PARAM => $param),
			ETL::Pequel::Type::Section::Filter->new(PARAM => $param),
			ETL::Pequel::Type::Section::DivertRecord->new(PARAM => $param),
			ETL::Pequel::Type::Section::CopyRecord->new(PARAM => $param),
			ETL::Pequel::Type::Section::DivertInputRecord->new(PARAM => $param),
			ETL::Pequel::Type::Section::CopyInputRecord->new(PARAM => $param),
			ETL::Pequel::Type::Section::Output->new(PARAM => $param),
			ETL::Pequel::Type::Section::SortOutput->new(PARAM => $param),
			ETL::Pequel::Type::Section::FieldProcess::Post->new(PARAM => $param),
			ETL::Pequel::Type::Section::Reject->new(PARAM => $param),
			ETL::Pequel::Type::Section::Having->new(PARAM => $param),
			ETL::Pequel::Type::Section::DivertOutputRecord->new(PARAM => $param),
			ETL::Pequel::Type::Section::CopyOutputRecord->new(PARAM => $param),
			ETL::Pequel::Type::Section::Summary->new(PARAM => $param),
			ETL::Pequel::Type::Section::DisplayMessageOnOutput->new(PARAM => $param),
			ETL::Pequel::Type::Section::DisplayMessageOnOutputAbort->new(PARAM => $param),


#TODO		ETL::Pequel::Type::Section::LoadTable::MySql::Merge->new(PARAM => $param),
#TODO		ETL::Pequel::Type::Section::LoadTable::MySql->new(PARAM => $param),
#TODO		ETL::Pequel::Type::Section::LoadTable::Sybase::Merge->new(PARAM => $param),
#TODO		ETL::Pequel::Type::Section::LoadTable::Sybase->new(PARAM => $param),


#>v3		ETL::Pequel::Type::Section::Pequel->new,	# Another pequel script follows:
#>v3		ETL::Pequel::Type::Section::LoadTable::Pequel::Merge->new,

#>v3		ETL::Pequel::Type::Section::Report->new,
#>v3		ETL::Pequel::Type::Section::Report::Stats->new,
#>v3		ETL::Pequel::Type::Section::Report::CPL->new,
		); 

		return $self;
	}
}
1;
# ----------------------------------------------------------------------------------------------------
