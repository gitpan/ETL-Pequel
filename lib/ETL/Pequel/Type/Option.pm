#!/usr/bin/perl
# ----------------------------------------------------------------------------------------------------
#  Name		: ETL::Pequel::Type::Option.pm
#  Created	: 25 February 2005
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
# 11/11/2005	2.4-5		gaffie	new option show_synonyms -- 
# 09/11/2005	2.4-5		gaffie	new option use_piped_chain -- use pipe() to connect input from pequel script.
# 20/09/2005	2.3-6		gaffie	unpack_input/pack_output implementation.
# 19/10/2005	2.3-5		gaffie	Added gzcat_cmd, gzcat_args, cat_cmd, cat_args options.
# 19/10/2005	2.3-5		gaffie	Added sort_cmd, sort_args, cpp_cmd, cpp_args options.
# 30/09/2005	2.3-2		gaffie	inherit for inheriting options in sub-pequel-scripts.
# 29/09/2005	2.3-2		gaffie	cmdType attribute to distinguish between cmdline and script only options.
# 21/09/2005	2.3-2		gaffie	Added -option option to view option values for script.
# 19/09/2005	2.3-2		gaffie	Added viewraw option for use by Pequel embedded tables.
# 14/09/2005	2.3-2		gaffie	Added output_format option for use by pequel-tables.
# 14/09/2005	2.3-2		gaffie	'--list option' option will now display only options used by script.
# 31/08/2005	2.2-8		gaffie	Added o_inline_libs, o_inline_inc.
# 31/08/2005	2.2-8		gaffie	added cmdPrep method to Option::Element class.
# 31/08/2005	2.2-8		gaffie	added: input_delimiter_extra
# 30/08/2005	2.2-8		gaffie	added: inline_ccflags, inline_optimize
# 25/08/2005	1.1-2		gaffie	Remove exit from DumpCode cmdExec.
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
	package ETL::Pequel::Type::Option::Element;
	use ETL::Pequel::Type;	#+++++
	use base qw(ETL::Pequel::Type::Element);

	use constant CMDTYPE_SCRIPT_ONLY => int 0;
	use constant CMDTYPE_CMDLINE_ONLY => int 2;
	use constant CMDTYPE_ANY => 1;

	our $this = __PACKAGE__;

	sub BEGIN
	{
		our @attr =
		qw(
			cmdAlias
			cmdFormat
			format
			allow
			usage
			description
			required
			cmdType
			inherit
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

		$self->inherit($params{'inherit'} || 0);
		$self->cmdType($params{'cmd_type'} || 0);	# 0=script option only; 1=both; 2=cmd line only; 
		$self->cmdFormat($params{'cmd_format'} || undef);
		$self->cmdAlias($params{'cmd_alias'} || ETL::Pequel::Collection::Vector->new);
		$self->format($params{'format'} || undef);
		$self->allow($params{'allow'});
		$self->usage($params{'usage'});
		$self->description($params{'description'});
		$self->required($params{'required'});
		$self->PARAM($params{'PARAM'});

		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Option::Version;
	use base qw(ETL::Pequel::Type::Option::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_);
		bless($self, $class);
		return $self;
	}

	sub cmdExec : method
	{
		my $self = shift;
		print "@{[ $self->PARAM->VERSION ]}\n";
		exit;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Option::Usage;
	use base qw(ETL::Pequel::Type::Option::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_);
		bless($self, $class);
		return $self;
	}

	sub cmdExec : method
	{
		my $self = shift;

		$self->PARAM->SCRIPT->usage();
		exit;
	}

}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Option::SrcList;
	use base qw(ETL::Pequel::Type::Option::Element);
	use ETL::Pequel::Lister;

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_);
		bless($self, $class);
		return $self;
	}

	sub cmdExec : method
	{
		my $self = shift;
		my $arg = shift || 'all';

$self->PARAM->error->msgStderr("This option is not currently available.");
#>		$self->PARAM->error->msgStderr("generating source listing...");
#>		my $pod = ETL::Pequel::Lister::SrcList->new;
#>		$pod->generate;
#>		$pod->prepare;
#>		$pod->podToPdf;
#>		$self->PARAM->error->msgStderr("->@{[ $pod->pdfName ]}");
		exit;
	}

}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Option::ProgRef;
	use base qw(ETL::Pequel::Type::Option::Element);
	use ETL::Pequel::Lister;

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_);
		bless($self, $class);
		return $self;
	}

	sub cmdExec : method
	{
		my $self = shift;
		my $arg = shift || 'pod';

$self->PARAM->error->msgStderr("This option is not currently available.");
#>		$self->PARAM->error->msgStderr("generating reference...");
#>		my $pod = ETL::Pequel::Lister::ProgRef->new;
#>		$pod->generate;
#>		$pod->prepare;
#>		$pod->podToPdf;
#>		$self->PARAM->error->msgStderr("->@{[ $pod->pdfName ]}");
		exit;
	}

}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Option::Pequeldoc;
	use base qw(ETL::Pequel::Type::Option::Element);
	use ETL::Pequel::Docgen;

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_);
		bless($self, $class);
		return $self;
	}

	sub cmdExec : method
	{
		my $self = shift;
		my $arg = shift || 'pod';

		$self->PARAM->error->fatalError("[5001] Cannot open Pequel script file '@{[ $self->PARAM->o_script_name ]}'")
			if (!-e $self->PARAM->properties('script_name'));

		if ($arg eq 'pod')
		{
			$self->PARAM->error->msgStderr("pequeldoc...");
			my $pod = ETL::Pequel::Docgen->new(PARAM => $self->PARAM, doc_type => 'pod');;
			$pod->genFull;
			$pod->prepare;
#?			$pod->displayPod;
			$self->PARAM->error->msgStderr("->@{[ $self->PARAM->properties('script_name') ]}.pod");
		}
		else
		{
			$self->PARAM->error->msgStderr("pod2pdf...");
			my $pod = ETL::Pequel::Docgen->new(PARAM => $self->PARAM, doc_type => 'pdf');;
			$pod->genFull;
			$pod->prepare;
			$pod->podToPdf;
			$self->PARAM->error->msgStderr("->@{[ $self->PARAM->properties('script_name') ]}.pdf");
		}
		exit;
	}

}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Option::ViewCode;
	use base qw(ETL::Pequel::Type::Option::Element);
	use ETL::Pequel::Engine; #+++++

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_);
		bless($self, $class);
		return $self;
	}

	sub cmdPostgen : method
	{
		my $self = shift;
		$self->PARAM->error->fatalError("[5002] Cannot open Pequel script file '@{[ $self->PARAM->properties('script_name') ]}'")
			if (!-e $self->PARAM->properties('script_name'));
		$self->PARAM->ENGINE->showCode();
		exit;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Option::ViewRaw;
	use base qw(ETL::Pequel::Type::Option::Element);
	use ETL::Pequel::Engine; #+++++

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_);
		bless($self, $class);
		return $self;
	}

	sub cmdPostgen : method
	{
		my $self = shift;
		$self->PARAM->error->fatalError("[50021] Cannot open Pequel script file '@{[ $self->PARAM->properties('script_name') ]}'")
			if (!-e $self->PARAM->properties('script_name'));
		$self->PARAM->ENGINE->print();
		exit;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Option::DumpCode;
	use base qw(ETL::Pequel::Type::Option::Element);
	use ETL::Pequel::Engine; #+++++

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_);
		bless($self, $class);
		return $self;
	}

	sub cmdPostgen : method
	{
		my $self = shift;
		$self->PARAM->error->fatalError("[5003] A Pequel script file was not specified -- file-name must have a '.pql' suffix.")
			if (!-e $self->PARAM->properties('script_name'));
		$self->PARAM->ENGINE->printToFile("@{[ $self->PARAM->properties('script_name') ]}.2.code");
		$self->PARAM->error->msgStderr("->@{[ $self->PARAM->properties('script_name') ]}.2.code");
		exit;
	}

}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Option::Diag;
	use base qw(ETL::Pequel::Type::Option::Element);
	use ETL::Pequel::Engine; #+++++

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_);
		bless($self, $class);
		return $self;
	}

	sub cmdPostgen : method
	{
		my $self = shift;
		$self->PARAM->error->fatalError("[50031] A Pequel script file was not specified -- file-name must have a '.pql' suffix.")
			if (!-e $self->PARAM->properties('script_name'));
		$self->PARAM->ENGINE->printToFile("@{[ $self->PARAM->properties('script_name') ]}.2.code");
		$self->PARAM->error->msgStderr("->@{[ $self->PARAM->properties('script_name') ]}.2.code");
	}

}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Option::NoVerbose;
	use base qw(ETL::Pequel::Type::Option::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_);
		bless($self, $class);
		return $self;
	}

	sub cmdExec : method
	{
		my $self = shift;
		$self->PARAM->error->fatalError("[5004] Cannot open Pequel script file '@{[ $self->PARAM->properties('script_name') ]}'")
			if (!-e $self->PARAM->properties('script_name'));
		$self->PARAM->properties('verbose', 0);
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Option::Check;
	use base qw(ETL::Pequel::Type::Option::Element);
	use ETL::Pequel::Engine; #+++++

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_);
		bless($self, $class);
		return $self;
	}

	sub cmdPostgen : method
	{
		my $self = shift;

		$self->PARAM->error->fatalError("[50051] This does not appear to be a Pequel script, or Pequel '.pql' script name not specified on command line.")
			if (!$self->PARAM->properties('script_name'));
		$self->PARAM->error->fatalError("[5005] Cannot open Pequel script file '@{[ $self->PARAM->properties('script_name') ]}'")
			if (!-e $self->PARAM->properties('script_name'));

		$self->PARAM->error->fatalError("[50031] A Pequel script file was not specified -- file-name must have a '.pql' suffix.")
			if (!-e $self->PARAM->properties('script_name'));
		my $check = $self->PARAM->ENGINE->check();

		if ($check !~ /syntax OK/)
		{
			$self->PARAM->error->msgStderr("");
			$self->PARAM->error->msgStderr("Errors in script");
			$self->PARAM->error->msgStderr("For debugging please check intermediate Perl code in @{[ $self->PARAM->properties('script_name') ]}.DEBUG");
			$self->PARAM->error->msgStderr("$check");
			exit;
		}
		$self->PARAM->error->msgStderr("");
		$self->PARAM->error->msgStderr("@{[ $self->PARAM->properties('script_name') ]} syntax OK");
		exit;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Option::TableInfo;
	use base qw(ETL::Pequel::Type::Option::Element);
	use ETL::Pequel::Code; 	#+++++

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_);
		bless($self, $class);
		return $self;
	}

	sub cmdExec : method
	{
		my $self = shift;
		my $arg = shift || undef;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);

		$c->add;
		$c->add("Table Information for Pequel Script @{[ $self->PARAM->properties('script_name') ]}");
		foreach ($self->PARAM->tables->toArray)
		{
			$_->dataSourceFilename
				? $c->add("@{[ $_->name ]}: (ds=@{[ $_->dataSourceFilename ]} -- @{[ -e $_->dataSourceFilename ? 'OK' : '** Not found **']})")
				: $c->add("@{[ $_->name ]}:");
			if ($arg && $arg eq 'fields')
			{
				$c->over;
				foreach my $f ($_->fields->toArray)
				{
					$c->add("@{[ $f->name ]},");
				}
				$c->endList;
				$c->add;
				$c->back;
			}
			$c->over;
			$c->add("Table usage info:");
			foreach my $use ($_->useList->toArray)
			{
				$c->add("@{[ $_->name ]}(@{[ $use->value ]}) --> @{[ $use->sourceSectionName ]}\::@{[ $use->sourceFieldName ]}");
			}
			$c->back;
		}
		$c->prepare;
		$c->print;
		exit;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Option::MacroInfo;
	use base qw(ETL::Pequel::Type::Option::Element);
	use ETL::Pequel::Code; 	#+++++

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_);
		bless($self, $class);
		return $self;
	}

	sub cmdExec : method
	{
		my $self = shift;
		my $arg = shift || undef;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);

		$c->add;
		$c->add("Macro Information for Pequel Script @{[ $self->PARAM->properties('script_name') ]}");

		foreach my $m ($self->PARAM->macros->toArray)
		{
			foreach my $use ($m->useList->toArray)
			{
				$c->add("&@{[ $m->name ]}(@{[ $use->value ]}) --> @{[ $use->sourceSectionName ]}\::@{[ $use->sourceFieldName ]}");
			}
		}
		$c->prepare;
		$c->print;
		exit;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Option::Option;
	use base qw(ETL::Pequel::Type::Option::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_);
		bless($self, $class);
		return $self;
	}

	sub cmdExec : method
	{
		my $self = shift;
		my $arg = shift || undef;

		if ($arg eq 'all')
		{
			foreach ($self->PARAM->options->toArray)
			{
				print $_->name, " = '", eval(qq/\$self->PARAM->properties('@{[ $_->name ]}')/), "'\n";
			}
		}
		elsif ($self->PARAM->options->exists($arg))
		{
			print "$arg = '@{[ eval(qq/\$self->PARAM->properties('$arg')/) ]}'\n";
		}
		else
		{
			print "Unknown option '$arg'\n";
		}
		
		exit;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Option::List;
	use base qw(ETL::Pequel::Type::Option::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_);
		bless($self, $class);
		return $self;
	}

	sub cmdExec : method
	{
		my $self = shift;
		my $arg = shift || undef;

#>		$self->root->lister->exists('option')->toArray; #--> ETL::Pequel::Type::Lister
#>or	$self->root->lister->option->raw; #--> ETL::Pequel::Type::Lister
#>or	$self->root->lister->option->prefixed; #--> ETL::Pequel::Type::Lister

		print join("\n", map("@{[ $arg eq 'all' ? 'option.' : '' ]}@{[ $_->name ]}", 
#<			$self->root->s_options->items->toArray)) . "\n"
			$self->PARAM->sections->exists('options')->items->toArray)) . "\n"
				if ($arg eq 'all' || $arg eq 'option');
		
		print join("\n", map("@{[ $arg eq 'all' ? 'options.' : '' ]}@{[ $_->name ]}", 
			$self->PARAM->options->toArray)) . "\n"
				if ($arg eq 'all' || $arg eq 'options');
		
		print join("\n", map("@{[ $arg eq 'all' ? 'type.' : '' ]}@{[ $_->name ]}", 
			$self->PARAM->datatypes->toArray)) . "\n"
				if ($arg eq 'all' || $arg eq 'type');

		print join("\n", map("@{[ $arg eq 'all' ? 'date.' : '' ]}@{[ $_->name ]}", 
			$self->PARAM->datetypes->toArray)) . "\n"
				if ($arg eq 'all' || $arg eq 'date');

		print join("\n", map("@{[ $arg eq 'all' ? 'month.' : '' ]}@{[ $_->name ]}", 
			$self->PARAM->monthtypes->toArray)) . "\n"
				if ($arg eq 'all' || $arg eq 'month');

		print join("\n", map("@{[ $arg eq 'all' ? 'macro.' : '' ]}@{[ $_->name ]}", 
			$self->PARAM->macros->toArray)) . "\n"
				if ($arg eq 'all' || $arg eq 'macro');

		print join("\n", map("@{[ $arg eq 'all' ? 'section.' : '' ]}@{[ $_->name ]}", 
			$self->PARAM->sections->toArray)) . "\n"
				if ($arg eq 'all' || $arg eq 'section');

		print join("\n", map("@{[ $arg eq 'all' ? 'input_field.' : '' ]}@{[ $_->name ]}", 
			$self->PARAM->sections->exists('input section')->items->toArray)) . "\n"
				if ($arg eq 'all' || $arg eq 'input_field' || ($arg eq 'output_format' && $self->PARAM->properties('transfer')));

		print join("\n", map("@{[ $arg eq 'all' ? 'output_field.' : '' ]}@{[ $_->name ]}", 
			$self->PARAM->sections->exists('output section')->items->toArray)) . "\n"
				if ($arg eq 'all' || $arg eq 'output_field' || $arg eq 'output_format');

		print join("\n", map("@{[ $arg eq 'all' ? 'table.' : '' ]}@{[ $_->name ]}", 
			$self->PARAM->tables->toArray)) . "\n"
				if ($arg eq 'all' || $arg eq 'table');

#>		if ($arg eq 'all' || $arg eq 'error')
#>		if ($arg eq 'all' || $arg eq 'table_field')
		
		print join("\n", map("@{[ $arg eq 'all' ? 'aggregate.' : '' ]}@{[ $_->name ]}", 
			$self->PARAM->aggregates->toArray)) . "\n"
				if ($arg eq 'all' || $arg eq 'aggregate');

		if ($arg eq 'all' || $arg eq 'db')
		{
			foreach my $dbtype ($self->PARAM->dbtypes->toArray)
			{
				print "db.@{[ $dbtype->name ]}";
				foreach my $db ($dbtype->toArray)
				{
					print ".@{[ $db->name ]}";
				}
				print "\n";
			}
		}

		exit;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Option::Group::Docgen;
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
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'doc_title',		
				value => '', 	
				format => '[-|_|\w|\s|\(|\)]+', 
				description => 'document title.',
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'doc_version',		
				value => '', 	
				format => '[\w|\d|\.]+', 
				description => 'document version for pequel script.',
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'doc_email',		
				value => '', 	
				format => '[\w|\@|\.|\/|:]+', 
				description => 'document email entry.',
				PARAM => $param,
			),
		);
		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Option::Group::Table;
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
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'display_table_stats',		
				value => '1',	
				cmd_format => '!',
				cmd_type => 2,
				description => 'Display table statistics',
				inherit => 1,
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'reload_tables',		
				cmd_format => '!',
				cmd_type => 1,
				description => 'Drop existing table(s) and reload data',
				inherit => 1,
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'load_tables_only',		
				cmd_format => '!',
				cmd_type => 2,
				description => 'Run pequel script to load tables only',
				cmd_alias => ETL::Pequel::Collection::Vector->new
				(
					ETL::Pequel::Type::Element->new(name => 'load'),
					ETL::Pequel::Type::Element->new(name => 'load_only'),
				),
				inherit => 1,
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'table_drop_unused_fields',		
				value => 1,
				cmd_format => '!',
				description => "Table load will drop unused fields from input datasource",
				inherit => 1,
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'table_dir',		
				value => '',	
				format => '[/|_|\.|\w]+', 	
				cmd_format => ':s',
				cmd_type => 1,
				description => 'directory pathname for tables.',
				inherit => 1,
				PARAM => $param,
			),
		);
		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Option::Group::Oracle;
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
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'oracle_prefetch_count',		
				value => '100',	
				cmd_format => ':s',
				description => 'Oracle(merge): number of rows to prefetch',
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'oracle_home',		
				value => $ENV{ORACLE_HOME},	
				cmd_format => ':s',
				cmd_type => 1,
				description => 'Oracle Home Path',
				inherit => 1,
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'oracle_sqlldr_rows',		
				value => 100000,
				cmd_format => ':s',
				description => "Oracle loader 'rows' setting",
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'oracle_use_merge_fetch_macro',		
				value => 1,
				cmd_format => '!',
				description => "Oracle use merge fetch macro",
				PARAM => $param,
			),
#>	Future implementation: use this user definable to replace inside Oracle/Oracle.pm
#>			ETL::Pequel::Type::Option::Element->new
#>			(
#>				name => 'oracle_get_password_cmd',		
#>				value => 'grep -iw _DB_ $HOME/.password | grep -iw _USER_ | awk "{ print $3 }"',
#>				cmd_format => ':s',
#>				description => "Oracle shell command to get db password",
#>			),
		);
		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Option::Group::Sqlite;
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
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'sqlite_dir',		
				value => '',	
				cmd_type => 1,
				inherit => 1,
				format => '[/|_|\w]+', 	
				description => 'directory pathname for sqlite installation.',
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'sqlite_merge_optimize',		
				cmd_format => '!',
				description => 'Optimize Sqlite merge tables',
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'sqlite_merge_optimize_count',		
				cmd_format => ':s',
				value => '20',
				description => 'Optimize Sqlite merge tables',
				PARAM => $param,
			),
		);
		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Option::Group::Inline;
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
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'use_inline',		
				cmd_format => '!',
				description => 'Generate C Inline Code',
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'inline_cc',		
				value => 'CC',
				cmd_format => ':s',
				description => 'Inline: CC',
				inherit => 1,
				cmd_alias => ETL::Pequel::Collection::Vector->new
				(
					ETL::Pequel::Type::Element->new(name => 'inline_CC'),
				),
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'inline_libs',		
				value => '',
				cmd_format => ':s',
				description => 'Inline: LIBS',
				inherit => 1,
				cmd_alias => ETL::Pequel::Collection::Vector->new
				(
					ETL::Pequel::Type::Element->new(name => 'inline_LIBS'),
				),
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'inline_inc',		
				value => '',
				cmd_format => ':s',
				description => 'Inline: INC',
				inherit => 1,
				cmd_alias => ETL::Pequel::Collection::Vector->new
				(
					ETL::Pequel::Type::Element->new(name => 'inline_INC'),
				),
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'inline_ccflags',		
				value => '',
				cmd_format => ':s',
				description => 'Inline: CCFLAGS',
				inherit => 1,
				cmd_alias => ETL::Pequel::Collection::Vector->new
				(
					ETL::Pequel::Type::Element->new(name => 'inline_CCFLAGS'),
				),
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'inline_optimize',		
				value => '',
				cmd_format => ':s',
				description => 'Inline: OPTIMIZE',
				inherit => 1,
				cmd_alias => ETL::Pequel::Collection::Vector->new
				(
					ETL::Pequel::Type::Element->new(name => 'inline_OPTIMIZE'),
				),
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'inline_lddlflags',		
				value => '',
				cmd_format => ':s',
				description => 'Inline: LDDLFLAGS',
				inherit => 1,
				cmd_alias => ETL::Pequel::Collection::Vector->new
				(
					ETL::Pequel::Type::Element->new(name => 'inline_LDDLFLAGS'),
				),
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'inline_make',		
				value => '',
				cmd_format => ':s',
				description => 'Inline: MAKE',
				inherit => 1,
				cmd_alias => ETL::Pequel::Collection::Vector->new
				(
					ETL::Pequel::Type::Element->new(name => 'inline_MAKE'),
				),
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'inline_clean_after_build',		
				value => 1,
				cmd_format => '!',
				description => "Inline: clean_after_build",
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'inline_clean_build_area',		
				value => 1,
				cmd_format => '!',
				description => "Inline: clean_build_area",
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'inline_build_noisy',		
				value => 0,
				cmd_format => '!',
				description => "Inline: build_noisy",
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'inline_build_timers',		
				value => 0,
				cmd_format => '!',
				description => "Inline: build_timers",
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'inline_force_build',		
				value => 0,
				cmd_format => '!',
				cmd_type => 1,
				description => "Inline: force_build",
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'inline_print_info',		
				value => 0,
				cmd_format => '!',
				cmd_type => 1,
				description => "Inline: print_info",
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'inline_directory',		
				value => '',
				cmd_format => ':s',
				description => "Inline: directory",
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'inline_cache_recs',		
				value => '0',	
				format => '\d+', 
				description => 'Number of records to cache in inline input read.',
				cmd_alias => ETL::Pequel::Collection::Vector->new
				(
					ETL::Pequel::Type::Element->new(name => 'cache_recs'),
				),
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'use_av_store_macro',		
				value => 1,
				cmd_format => '!',
				description => "Use _av_store() macro",
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'inline_merge_optimize',		
				cmd_format => '!',
				description => 'Optimize merge tables',
				cmd_alias => ETL::Pequel::Collection::Vector->new
				(
					ETL::Pequel::Type::Element->new(name => 'merge_optimize'),
				),
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'inline_merge_optimize_count',		
				cmd_format => ':s',
				value => '20',
				description => 'Optimize merge tables record count',
				cmd_alias => ETL::Pequel::Collection::Vector->new
				(
					ETL::Pequel::Type::Element->new(name => 'merge_optimize_count'),
				),
				PARAM => $param,
			),
		);
		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Option::Group::Developer;
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
			ETL::Pequel::Type::Option::DumpCode->new
			(
				name => 'dumpcode',		
				cmd_format => '!',
				cmd_type => 2,
				description => 'Dump the generated Perl code for pequel script',
				cmd_alias => ETL::Pequel::Collection::Vector->new
				(
					ETL::Pequel::Type::Element->new(name => 'dc'),
					ETL::Pequel::Type::Element->new(name => 'dump_code'),
#?					ETL::Pequel::Type::Element->new(name => 'diag'),
				),
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'debug_show_caller',		
				value => 0,
				cmd_format => '!',
				cmd_type => 1,
				inherit => 1,
				description => "Debugging - turn on show caller() info",
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'debug',		
				value => 0,
				cmd_format => '!',
				cmd_type => 1,
				inherit => 1,
				description => "Debug Pequel Script mode",
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'debug_generate',		
				value => 0,
				cmd_format => '!',
				cmd_type => 1,
				inherit => 1,
				description => "Debug Pequel Script code generation",
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'debug_parser',		
				value => 0,
				cmd_format => '!',
				cmd_type => 2,
				inherit => 1,
				description => "Debug Pequel Script parser phase",
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Diag->new
			(
				name => 'diagnostics',		
				value => '', 	
				cmd_format => '!',
				cmd_type => 1,
				description => 'Save generated code in script.2.code.',
				cmd_alias => ETL::Pequel::Collection::Vector->new
				(
					ETL::Pequel::Type::Element->new(name => 'diagnostic'),
					ETL::Pequel::Type::Element->new(name => 'diag'),
				),
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::TableInfo->new
			(
				name => 'tinfo',		
				cmd_format => ':s',
				cmd_type => 2,
				description => 'Display Table information',
				cmd_alias => ETL::Pequel::Collection::Vector->new
				(
					ETL::Pequel::Type::Element->new(name => 'ti'),
					ETL::Pequel::Type::Element->new(name => 'table_info'),
				),
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::MacroInfo->new
			(
				name => 'minfo',		
				cmd_format => ':s',
				cmd_type => 2,
				description => 'Display Macro information',
				cmd_alias => ETL::Pequel::Collection::Vector->new
				(
					ETL::Pequel::Type::Element->new(name => 'macro_info'),
				),
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::SrcList->new
			(
				name => 'pequelsrclist',		
				value => 'all',
				cmd_format => ':s',
#>				cmd_type => 2,
				description => "Generate Pequel source listing document (pdf)",
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::ProgRef->new
			(
				name => 'pequelprogref',		
				value => 'all',
				cmd_format => ':s',
#>				cmd_type => 2,
				description => "Generate Pequel Programmer's Reference Manual (pdf)",
				PARAM => $param,
			),
		);
		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Option::InputDelimiterExtra;
	use base qw(ETL::Pequel::Type::Option::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_);
		bless($self, $class);
		return $self;
	}

	sub cmdPrep : method
	{
		my $self = shift;
		$self->PARAM->properties('use_inline', 1);
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Option::Logging;
	use base qw(ETL::Pequel::Type::Option::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_);
		bless($self, $class);
		return $self;
	}

	sub cmdPrep : method
	{
		my $self = shift;
		$self->PARAM->properties('logfilename', "@{[ $self->PARAM->properties('script_name') ]}.log")
			if ($self->PARAM->properties('logging') && $self->PARAM->properties('logfilename') eq '');
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Option::Group::Basic;
	use ETL::Pequel::Collection;	#+++++
	use base qw(ETL::Pequel::Collection::Hierarchy);
	use ETL::Pequel::Type::Date;

	sub new : method
	{
		my $self = shift;
		my $param = shift;
		my $class = ref($self) || $self;
		$self = $class->SUPER::new(@_);
		bless($self, $class);
		$self->addAll
		(
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'verbose',			
				value => 10000, 
				format => '\d*', 
				cmd_format => ':s',
				cmd_type => 1,
				description => 'display progress counter',
				cmd_alias => ETL::Pequel::Collection::Vector->new
				(
					ETL::Pequel::Type::Element->new(name => 'ver'),
				),
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::NoVerbose->new
			(
				name => 'noverbose',			
				cmd_format => '!',
				cmd_type => 1,
				inherit => 1,
				description => 'do not progress counter',
				cmd_alias => ETL::Pequel::Collection::Vector->new
				(
					ETL::Pequel::Type::Element->new(name => 'silent'),
					ETL::Pequel::Type::Element->new(name => 'quite'),
				),
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::InputDelimiterExtra->new
			(
				name => 'input_delimiter_extra',			
				value => '', 	
				format => '\"|\[|\{|\(', 
				cmd_format => ':s',
				description => 'Extra input field delimiter(s)',
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'input_delimiter', 	
				value => '|', 	
				format => '\||\w+|\s+|\t+|[\';:~^&"-_=,]', 
				cmd_format => ':s',
				description => 'input file field delimiter',
				cmd_alias => ETL::Pequel::Collection::Vector->new
				(
					ETL::Pequel::Type::Element->new(name => 'id'),
				),
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'output_delimiter', 
				value => '|', 	
				format => '\||\w+|\s+|\t+|[\';:~^&"-_=,]', 
				cmd_format => ':s',
				description => 'output file field delimiter',
				cmd_alias => ETL::Pequel::Collection::Vector->new
				(
					ETL::Pequel::Type::Element->new(name => 'od'),
				),
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'input_file',		
				value => '', 	
				format => '[/|\w|.]+', 
				cmd_type => 1,
				description => 'input data filename',
				cmd_format => ':s',
				cmd_alias => ETL::Pequel::Collection::Vector->new
				(
					ETL::Pequel::Type::Element->new(name => 'is'),
					ETL::Pequel::Type::Element->new(name => 'if'),
					ETL::Pequel::Type::Element->new(name => 'i'),
				),
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'output_file',		
				value => '', 	
				format => '/|\w+', 
				cmd_type => 1,
				cmd_format => ':s',
				description => 'output data filename',
				cmd_alias => ETL::Pequel::Collection::Vector->new
				(
					ETL::Pequel::Type::Element->new(name => 'os'),
					ETL::Pequel::Type::Element->new(name => 'of'),
					ETL::Pequel::Type::Element->new(name => 'o'),
				),
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'script_name',		
				value => '', 	
				format => '/|\w+', 
				cmd_format => ':s',
				cmd_type => 1,
				description => 'script filename',
				cmd_alias => ETL::Pequel::Collection::Vector->new
				(
					ETL::Pequel::Type::Element->new(name => 'script'),
					ETL::Pequel::Type::Element->new(name => 's'),
				),
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'discard_header',	
				value => '', 	
				description => 'Input file has header record - must be discarded.',
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'header',			
				value => '', 	
				cmd_format => '!',
				cmd_type => 1,
				description => 'write header record to output.',
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'noheader',			
				value => '', 	
				description => 'do not write header record to output.',
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'addpipe',			
				value => '', 	
				description => 'add extra delimiter to output lines.',
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'noaddpipe',		
				value => '', 	
				description => 'do not add extra pipe delim at end of each record.',
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'optimize',			
				value => '', 	
				description => 'optimize generated code.',
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'nooptimize',		
				value => '', 	
				description => 'do not optimize generated code.',
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'nulls',			
				value => '', 	
				description => 'print zero for null numeric/decimal.',
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'nonulls',			
				value => '', 	
				description => 'do not print zero for null numeric/decimal.',
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'reject_file',		
				value => '', 	
				format => '/|\w+', 
				description => 'specify filename for rejected records (default=<scriptname>.reject).',
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'default_datetype',	
				value => 'YYYYMMDD', 	
				format => join('|', $param->datetypes->toArrayName),
				description => "default date format",
				cmd_alias => ETL::Pequel::Collection::Vector->new
				(
					ETL::Pequel::Type::Element->new(name => 'default_date_type'),
					ETL::Pequel::Type::Element->new(name => 'default_date_format'),
				),
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'default_list_delimiter', 
				value => ',', 	
				format => '\,|\-|\+\+_|\~|\||\w+\\',
				description => 'default delimiter for list created by values_all and values_uniq aggregates.',
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'hash',				
				value => '', 	
				format => '\w+', 
				description => 'Generate in memory. Input data can be unsorted.',
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'transfer',			
				value => '', 	
				description => 'Copy input (including calculated fields) to output.',
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'suppress_output',	
				value => '', 	
				description => 'Use with \'summary section\' to print only summary.',
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'num_threads',		
				value => '', 	
				format => '\d+', 
				cmd_type => 1,
				description => 'specify number of threads to use.',
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'sort_tmp_dir',		
				cmd_format => ':s',
				cmd_type => 1,
				description => 'Sort temporary directory',
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'logfilename',		
#				value => "@{[ $self->root->o_script_name ]}.log",
				cmd_format => ':s',
				cmd_type => 1,
				description => 'Log filename',
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Logging->new
			(
				name => 'logging',		
				value => '',
				cmd_format => '!',
				cmd_type => 1,
				description => 'Logging On/Off',
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'prefix',		
				value => '',	
				format => '[/|_|\.|\w]+', 	
				cmd_format => ':s',
				cmd_type => 1,
				description => 'directory pathname prefix.',
				inherit => 1,
				cmd_alias => ETL::Pequel::Collection::Vector->new
				(
					ETL::Pequel::Type::Element->new(name => 'prefix_path'),
				),
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'lock_output',		
				value => '',
				cmd_format => '!',
				cmd_type => 0,
				inherit => 0,
				description => 'Lock output data stream',
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'output_file_append',		
				value => '',
				cmd_format => '!',
				cmd_type => 0,
				inherit => 0,
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'sort_cmd',		
				value => 'sort',
				cmd_format => ':s',
				cmd_type => 0,
				inherit => 1,
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'sort_args',		
				value => '',
				cmd_format => ':s',
				cmd_type => 0,
				inherit => 1,
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'cpp_cmd',		
				value => 'cpp',
				cmd_format => ':s',
				cmd_type => 2,
				inherit => 1,
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'cpp_args',		
				value => '',
				cmd_format => ':s',
				cmd_type => 0,
				inherit => 1,
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'gzcat_cmd',		
				value => 'gzcat',
				cmd_format => ':s',
				cmd_type => 0,
				inherit => 1,
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'gzcat_args',		
				value => '',
				cmd_format => ':s',
				cmd_type => 0,
				inherit => 1,
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'cat_cmd',		
				value => 'cat',
				cmd_format => ':s',
				cmd_type => 0,
				inherit => 1,
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'cat_args',		
				value => '',
				cmd_format => ':s',
				cmd_type => 0,
				inherit => 1,
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'pack_output',		
				value => '',
				cmd_format => '!',
				cmd_type => 0,
				inherit => 0,
				description => 'Pack output data stream',
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'output_pack_fmt',		
				value => '[A3/Z*]',		# Enclose format in brackets to indicate repeate per field.
				cmd_format => ':s',
				cmd_type => 0,
				inherit => 0,
				description => 'Pack format for output data stream',
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'unpack_input',		
				value => '',
				cmd_format => '!',
				cmd_type => 0,
				inherit => 0,
				description => 'Unpack input data stream',
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'input_pack_fmt',		
				value => '[A3/Z*]',		# Enclose format in brackets to indicate repeate per field.
				cmd_format => ':s',
				cmd_type => 0,
				inherit => 0,
				description => 'Pack format for input data stream',
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'input_record_limit',		
				cmd_format => ':s',
				cmd_type => 1,
				inherit => 0,
				description => 'Input record process limit count -- process only initial <limit> records.',
				cmd_alias => ETL::Pequel::Collection::Vector->new
				(
					ETL::Pequel::Type::Element->new(name => 'irl'),
				),
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'rmctrlm',		
				cmd_format => '!',
				cmd_type => 0,
				inherit => 0,
				description => 'Remove extra ctrl-m from end-of-record on input stream',
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Element->new
			(
				name => 'show_synonyms',		
				value => 1,
				cmd_format => '!',
				cmd_type => 1,
				inherit => 1,
				description => 'Show field names in generated code.',
				PARAM => $param,
			),
#>			ETL::Pequel::Type::Option::Element->new
#>			(
#>				name => 'use_piped_chain',		
#>				cmd_format => '!',
#>				cmd_type => 0,
#>				inherit => 1,
#>				description => 'Use pipe to connect input stream to external pequel script output',
#>				PARAM => $param,
#>			),
		);
		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Option::Group::CommandLineOnly;
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
			ETL::Pequel::Type::Option::Version->new
			(
				name => 'version',		
				cmd_format => '!',
				cmd_type => 2,
				description => 'Display Pequel Version information',
				cmd_alias => ETL::Pequel::Collection::Vector->new
				(
					ETL::Pequel::Type::Element->new(name => 'v'),
				),
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Usage->new
			(
				name => 'usage',		
				value => '', 	
				cmd_format => ':s',
				cmd_type => 2,
				description => 'display command usage description',
				cmd_alias => ETL::Pequel::Collection::Vector->new
				(
					ETL::Pequel::Type::Element->new(name => 'help'),
				),
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::ViewCode->new
			(
				name => 'viewcode',		
				cmd_format => '!',
				cmd_type => 2,
				description => 'Display the generated Perl code for pequel script',
				cmd_alias => ETL::Pequel::Collection::Vector->new
				(
					ETL::Pequel::Type::Element->new(name => 'vc'),
				),
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::ViewRaw->new
			(
				name => 'viewraw',		
				cmd_format => '!',
				cmd_type => 2,
				description => 'Display the generated Perl code for pequel script',
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Check->new
			(
				name => 'syntax_check',		
				cmd_format => '!',
				cmd_type => 2,
				description => 'check the pequel script for syntax errors',
				cmd_alias => ETL::Pequel::Collection::Vector->new
				(
					ETL::Pequel::Type::Element->new(name => 'c'),
					ETL::Pequel::Type::Element->new(name => 'check'),
				),
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::List->new
			(
				name => 'list',		
				value => 'all',
				cmd_format => ':s',
				cmd_type => 2,
				description => 'Pequel type lister',
				cmd_alias => ETL::Pequel::Collection::Vector->new
				(
					ETL::Pequel::Type::Element->new(name => 'l'),
				),
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Option->new
			(
				name => 'option',		
				value => 'all',
				cmd_format => ':s',
				cmd_type => 2,
				description => 'Pequel option values lister',
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Pequeldoc->new
			(
				name => 'pequeldoc',		
				cmd_format => ':s',
				cmd_type => 2,
#>				cmd_arg_allow => [ 'pod', 'pdf' ],
				description => 'generate pod / pdf pequel script Reference Guide.',
				cmd_alias => ETL::Pequel::Collection::Vector->new
				(
#<					ETL::Pequel::Type::Element->new(name => 'doc'),
#>					ETL::Pequel::Type::Element->new(name => 'pdf', value => 'pdf'), # value becomes the arg.
#>					ETL::Pequel::Type::Element->new(name => 'pod', value => 'pod'), # value becomes the arg.
				),
				PARAM => $param,
			),
			ETL::Pequel::Type::Option::Usage->new
			(
				name => 'detail',		
				value => '', 	
				cmd_format => '!',
				cmd_type => 2,
				description => 'Include Pequel Generated Program chapter in Pequeldoc',
				PARAM => $param,
			),
		);
		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Option;
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
		$self->PARAM($param);
		bless($self, $class);

		# also: input_stream, show_comments, gzcat_program_name,
		# sort_program_name

		$self->addAll
		(
			ETL::Pequel::Type::Option::Group::Basic->new($param),
			ETL::Pequel::Type::Option::Group::Table->new($param),
			ETL::Pequel::Type::Option::Group::Oracle->new($param),
			ETL::Pequel::Type::Option::Group::Sqlite->new($param),
			ETL::Pequel::Type::Option::Group::Inline->new($param),
			ETL::Pequel::Type::Option::Group::Docgen->new($param),
			ETL::Pequel::Type::Option::Group::Developer->new($param),
			ETL::Pequel::Type::Option::Group::CommandLineOnly->new($param),
		);
		return $self;
	}

	sub getAlias : method
	{
		my $self = shift;
		my $alias_name = shift;

		foreach my $o (grep($_->cmdAlias->size, $self->toArray))
		{
			return $o if ($o->cmdAlias->exists($alias_name));
		}
		return 0;
	}

	sub cmd_prep : method
	{
		my $self = shift;
		foreach (grep($_->ref->can("cmdPrep"), $self->PARAM->sections->find('options')->items->toArray))
		{
			$self->PARAM->error->msgStderrNonl($self->PARAM->properties('debug') ? $_->name : '.');
			$_->ref->cmdPrep($_->value);
		}
	}

	sub cmd_exec : method
	{
		my $self = shift;
		foreach (grep($_->ref->can("cmdExec"), $self->PARAM->sections->find('options')->items->toArray))
		{
			$self->PARAM->error->msgStderrNonl($self->PARAM->properties('debug') ? $_->name : '.');
			$_->ref->cmdExec($_->value);
		}
	}

	sub cmd_postgen : method
	{
		my $self = shift;
		foreach (grep($_->ref->can("cmdPostgen"), $self->PARAM->sections->find('options')->items->toArray))
		{
			$self->PARAM->error->msgStderrNonl($self->PARAM->properties('debug') ? $_->name : '.');
			$_->ref->cmdPostgen($_->value);
		}
	}
}
# ----------------------------------------------------------------------------------------------------
1;
