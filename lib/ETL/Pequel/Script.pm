#!/usr/bin/perl
# ----------------------------------------------------------------------------------------------------
#  Name		: ETL::Pequel::Script.pm
#  Created	: 14 January 2005
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
# 05/10/2005	2.3-4		gaffie	Handle section arguments -- dynamically create new derived section.
# 25/8/2005		1.1-2		gaffie	Bugfix with 'no cpp' misspelling.
# ----------------------------------------------------------------------------------------------------
# TO DO:
# ----------------------------------------------------------------------------------------------------
require 5.005_62;
use strict;
use attributes qw(get reftype);
use warnings;
use vars qw($VERSION $BUILD);
$VERSION = "1.1-2";
$BUILD = 'Thursday August 25 09:45:06 BST 2005';
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Script;			# contains vector of Pequel::Section::Element objects

	our $this = __PACKAGE__;

	sub BEGIN
	{
		# Create the class attributes
		our @attr =
		qw(
			scriptOrig
			scriptCppProcessed
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
		my $proto = shift;
		my %params = @_;
		my $class = ref($proto) || $proto;
		my $self = {};
		bless($self, $class);
		$self->PARAM($params{'PARAM'});
		return $self;
	}

	sub usage : method
	{
		my $self = shift;

		print "@{[ $self->PARAM->VERSION ]}\n";
		print "Usage: @{[ $self->PARAM->PEQUEL_EXEC_NAME ]} < script-name.pql > < options... >\n";
		foreach my $o (grep($_->cmdType, $self->PARAM->options->toArray))
		{
			my $args = (defined($o->cmdFormat) && $o->cmdFormat eq ':s') ? ' < args >' : '' ;
			print join(', ', "--@{[ $o->name ]}${args}", map("--@{[ $_->name ]}${args}", $o->cmdAlias->toArray)), "\n";
			print '   ', $o->description, "\n";
		}
	}

	sub prepare : method
	{
		my $self = shift;
		return unless ($self->PARAM->properties('script_name'));
		$self->PARAM->error->msgStderrNonl("Processing pequel script '@{[ $self->PARAM->properties('script_name') ]}'...");
		$self->read;
		$self->preProcess;
		$self->process;
		$self->parse;
		$self->compile;
		$self->check;
	}

	sub read : method
	{
		# Step #1 -- read...
		my $self = shift;
		# read pequel-script into $self->SciptOrig;
		$self->PARAM->error->msgStderrNonl($self->PARAM->properties('debug') ? 'read...' : '.');
		local $/;
		undef $/;

		$self->PARAM->error->fatalError
			("[2006] ETL::Pequel script file '@{[ $self->PARAM->properties('script_name') ]}' does not exist")
			unless (-e $self->PARAM->properties('script_name'));

		open(SCRIPT, $self->PARAM->properties('script_name')) 
			|| $self->PARAM->error->fatalError
			("[2001] $0: cannot open @{[ $self->PARAM->properties('script_name') ]}");
		$self->scriptOrig(<SCRIPT>);
		close(SCRIPT);
		$self->{$this}->{SCRIPTORIG} =~ s/\\\s*\n\s*//g;	
			# remove line continuation and join the lines.
	}

	sub preProcess : method
	{
		# Step #2 -- preProcess...
		my $self = shift;
		$self->PARAM->error->msgStderrNonl($self->PARAM->properties('debug') ? 'pre-process...' : '.');
		local $/;
		undef $/;

		# process with cpp and store in $self->SciptCppProcessed;
		my $have_cpp = `which @{[ $self->PARAM->properties('cpp_cmd') ]} 2>&1`;
		chomp($have_cpp);
		if ($have_cpp =~ /no cpp/i || $have_cpp =~ /not found/i)
		{
			open(SCRIPT, "@{[ $self->PARAM->properties('script_name') ]}") 
				|| $self->PARAM->error->fatalError
					("[2002] $0: cannot open @{[ $self->PARAM->properties('script_name') ]}");
		}
		else
		{
			open(SCRIPT, "@{[ $self->PARAM->properties('cpp_cmd') ]} @{[ $self->PARAM->properties('cpp_args') ]} @{[ $self->PARAM->properties('script_name') ]} |") 
				|| $self->PARAM->error->fatalError
					("[2003] $0: cannot open @{[ $self->PARAM->properties('script_name') ]}");
		}
		$self->scriptCppProcessed(<SCRIPT>);
		$self->{$this}->{SCRIPTCPPPROCESSED} =~ s/\\\s*\n\s*//g;	
			# remove line continuation and join the lines.
	}

	sub find_section : method
	{
		my $self = shift;
		my $tofind = shift;
		my $args = undef;
		my $s;
		if ($tofind =~ s/\s*\((.*)\).*$//) { $args = $1; }
		return 0 if (($s = $self->PARAM->sections->regEx($tofind)) == 0);
		if (defined($args))
		{
			# Create a new section by appending the args to section name:
			my $new_section_name = $args; $new_section_name =~ s/[\.\:\-\'\"\$\%\#\=]|\s+/_/g;
			my $new_section_packname = ref($s) . '::' . uc($new_section_name);
			$new_section_name = $s->name . ' ' . $new_section_name;
			my $snew;
			if (($snew = $self->PARAM->sections->regEx($new_section_name)) == 0)
			{
				# Create the new package:
				if (!$self->PARAM->root->PARAM->pequel_script->find($self->PARAM->getscriptname($args)))
				{
					my $package = "
{
    package $new_section_packname;
    use base qw(@{[ ref($s) ]});
    sub new
    {
        my \$self = shift;
        my \$class = ref(\$self) || \$self;
        my \%params = \@_;
        \$self = \$class->SUPER::new(\@_,name=>\$params{'name'}||'@{[ $new_section_name ]}');
        bless(\$self, \$class);
        return \$self;
    }
}
					";
					eval($package);
				}
#print "\nAdd Section:", $self->PARAM->properties('script_name'), "->", $new_section_packname, "\n";
				eval("\$self->PARAM->sections->add($new_section_packname\->new(PARAM => \$self->PARAM));");
			}
			$s = $self->PARAM->sections->regEx($new_section_name);
		}
		$s->args($args);
		return $s;
	}

	sub process : method
	{
		# Step #3 -- process
		my $self = shift;
		$self->PARAM->error->msgStderrNonl($self->PARAM->properties('debug') ? 'process...' : '.');

		my $line_number=0;		# use an object to contain line_number stuff
		my $current_section=0;
		foreach (split("[\n]", $self->scriptCppProcessed, -1))
		{
			chomp;
			last if (/__END__/);
			s/#.*//;		# remove comments

			# WARNING: Next line will also remove s/...// regex exp:
			s/\/\/.*//g;	# remove c style comment lines if not cpp'd
			s/^\s*//;
			s/\s*$//;
			s/\s*,$//;
			next if ($_ eq '');

			if ((my $s = $self->find_section($_)) != 0)
			{
				$s->present(1);
				$current_section = $s;
				next;
			}
			next unless ($current_section);

			# if the line contains more than one item then split on comma:
			foreach my $e ($self->PARAM->parser->splitEntries($_))
			{
				$current_section->lines->add
				(
					ETL::Pequel::Collection::Element->new
					(
						name => $current_section->name, 	
						value => $e,
						PARAM => $self->PARAM,
#>						number => ...	# script line-number
					)
				);	
			}
		}
	}

	sub parse : method
	{
		# Step #4 -- parse
		my $self = shift;
		$self->PARAM->error->msgStderrNonl($self->PARAM->properties('debug') ? 'parse...' : '.');
		my $valid_script=0;
		foreach (grep($_->lines->size, $self->PARAM->sections->toArray))
		{
			$self->PARAM->properties('debug')
				? $self->PARAM->error->msgStderrNonl("parse:@{[ $_->name ]}...")
				: $self->PARAM->error->msgStderrNonl(".");
			$_->parseAll;
			$valid_script++;
		}
		$self->PARAM->error->errorfatalError("This does not appear to be a Pequel script")
			unless $valid_script;
	}

	sub compile : method
	{
		# Step #5 -- compile
		my $self = shift;
		$self->PARAM->error->msgStderrNonl($self->PARAM->properties('debug') ? 'compile...' : '.');
		
		foreach (grep($_->items->size, $self->PARAM->sections->toArray))
		{
			$_->compile;
		}
	}

	sub check : method
	{
		# Step #6 -- check
		my $self = shift;
		$self->PARAM->error->msgStderrNonl($self->PARAM->properties('debug') ? 'check...' : '.');

		$self->PARAM->error->fatalError("[2007] Not an Pequel script!")
			if 
			(
				!$self->PARAM->sections->exists('options')
				&& !$self->PARAM->sections->exists('input section')->items->size
				&& !$self->PARAM->tables->size
			);

		$self->PARAM->error->warning
			("[2008]'load_tables_only' option ignored -- no tables defined in script")
			if ($self->PARAM->properties('load_tables_only') && !$self->PARAM->tables->size);
				
		$self->PARAM->error->warning
			("[2010]'reload_tables' option ignored -- no tables defined in script")
			if ($self->PARAM->properties('load_tables_only') && !$self->PARAM->tables->size);
				
		$self->PARAM->error->fatalError
			("[2009] Minimal Pequel script requires 'input section' and/or 'load table' section")
			if (!$self->PARAM->tables->size && !$self->PARAM->sections->exists('input section'));

#		foreach ($self->root->t_section->toArray)
#		{
#			if ($_->required && $_->lines->size == 0)
#			{
#				$self->PARAM->error->fatalError("Required section '@{[ $_->name ]}' is missing from script.");
#			}
#		}
	}
}
1;
# ----------------------------------------------------------------------------------------------------
