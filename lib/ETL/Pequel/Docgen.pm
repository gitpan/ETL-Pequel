#!/usr/bin/perl
# ----------------------------------------------------------------------------------------------------
#  Name		: ETL::Pequel::Docgen.pm
#  Created	: 20 February 2005
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
# 13/09/2005	2.2-9		gaffie	Added About Pequel Chapter includes copyright notice;
# ----------------------------------------------------------------------------------------------------
# TO DO:
# ----------------------------------------------------------------------------------------------------
require 5.005_62;
use strict;
use UNIVERSAL qw( isa can );
use attributes qw(get reftype);
use warnings;
use ETL::Pequel::Type;
use vars qw($VERSION $BUILD);
$VERSION = "2.4-3";
$BUILD = 'Tuesday November  1 08:45:13 GMT 2005';
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Docgen;
	use ETL::Pequel::Code;	#+++++
	use base qw(ETL::Pequel::Code);	#--> Pequel::Code::Pod;

	our $this = __PACKAGE__;

	use constant PAPER_USLETTER	=> 'usletter';
	use constant PAPER_A4 => 'a4';

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_);
		bless($self, $class);

		$self->has_page_tag($params{'has_page_tag'});
		$self->paper($params{'paper'} || PAPER_A4);
		$self->docType($params{'doc_type'} || 'pod');

		$self->sections(ETL::Pequel::Collection::Vector->new);

		return $self;
    }

	sub docType : method
	{ 
		my $self = shift; 
		$self->{$this}->{DOC_TYPE} = shift if (@_); 
		return $self->{$this}->{DOC_TYPE}; 
	}

	sub generate : method
	{
		my $self = shift; 

		$self->add("=pod");		#--> $self->pod;
		$self->add;
		$self->add("=head1 SCRIPT NAME");	#--> $self->head1("...");
		$self->add;
		$self->add("@{[ $self->PARAM->properties('script_name') ]}");
		$self->add;
		$self->add("=head1 DESCRIPTION");
		$self->add;
		map($self->add($_->value), $self->PARAM->sections->find('description section')->items->toArray);
		$self->add;

		foreach ($self->sections->toArray)
		{
			$self->add("=head1 @{[ $_->number ]}. @{[ $_->heading ]}");
			$self->add;
			$self->addAll($_->generate);
			$self->add("=page") if ($self->docType eq 'pdf');
			$self->add;
		}
	}

	sub genFull : method
	{
		my $self = shift; 

		$self->sections->add(ETL::Pequel::Docgen::Chapter::Overview->new(PARAM => $self->PARAM));
		$self->sections->add(ETL::Pequel::Docgen::Chapter::Config->new(PARAM => $self->PARAM));
#		$self->sections->add(ETL::Pequel::Docgen::Chapter::InputFields->new(PARAM => $self->PARAM));
		$self->sections->add(ETL::Pequel::Docgen::Chapter::Tables->new(PARAM => $self->PARAM));
		$self->sections->add(ETL::Pequel::Docgen::Chapter::TablesSummary->new(PARAM => $self->PARAM));
		$self->sections->add(ETL::Pequel::Docgen::Chapter::PequelScript->new(PARAM => $self->PARAM));
		$self->sections->add(ETL::Pequel::Docgen::Chapter::PequelGenerated->new(PARAM => $self->PARAM))
			if ($self->PARAM->properties('detail'));
		$self->sections->add(ETL::Pequel::Docgen::Chapter::Copyright->new(PARAM => $self->PARAM));

		$self->generate;
	}

	sub genBrief : method
	{
		my $self = shift; 
		$self->sections->add(ETL::Pequel::Docgen::Chapter::Overview->new(PARAM => $self->PARAM));
		$self->sections->add(ETL::Pequel::Docgen::Chapter::PequelScript->new(PARAM => $self->PARAM));

		$self->generate;
	}

	sub sections : method
	{ 
		my $self = shift; 
		$self->{$this}->{SECTIONS} = shift if (@_); 
		return $self->{$this}->{SECTIONS}; 
	}

	sub displayPod : method
	{
		my $self = shift;
		$self->printToFile("@{[ $self->PARAM->properties('script_name') ]}.pod");
		system("perldoc @{[ $self->PARAM->properties('script_name') ]}.pod");
	}

	sub podToPdf : method
	{
		my $self = shift;
		my $have_pod2pdf = `which pequelpod2pdf`;
		chomp($have_pod2pdf);

		if ($have_pod2pdf)
		{
			my $cmd;
			$self->printToFile("@{[ $self->PARAM->properties('script_name') ]}.pod");
			$cmd = "pequelpod2pdf ";
			$cmd .= "--title \"@{[ $self->PARAM->properties('doc_title') ]}\" ";
			$cmd .= "--version \"@{[ $self->PARAM->properties('doc_version') ]}\" ";
			$cmd .= "--type \"@{[ lc($self->PARAM->properties('script_name')) ]}\" ";
			$cmd .= "--email \"@{[ $self->PARAM->properties('doc_email') ]}\" @{[ $self->PARAM->properties('script_name') ]}.pod";
			system($cmd);
		}
	}

    sub has_page_tag : method 
	{ 
		my $self = shift; 
		$self->{HAS_PAGE_TAG} = shift if (@_); 
		return $self->{HAS_PAGE_TAG}; 
	}
    sub paper : method 
	{ 
		my $self = shift; 
		$self->{PAPER} = shift if (@_); 
		return $self->{PAPER}; 
	}

#?	sub FileExtension : method
#?	{
#?		# Overide this function in subclass; return the file extension, eg 'pod', 'txt';
#?	}

#?	sub DocFileName : method
#?	{
#?		my $self = shift;
#?		# May need to remove '.pql' from ScriptFileName:
#<	return "@{[Pequel::Base::ScriptFileName]}.@{[$self->FileExtension]}";
#?	}

#>	sub calcDescription : method
#>	{
#>		my $self = shift;
#>		my $f = shift;
#>	
#>		my $calcdesc;
#>		if ($self->{ORIG_CALC}->{$field_number} =~ /_SELECT/)
#>		{
#>			$calcdesc = $self->_select_clause_in_words($self->{ORIG_CALC}->{$field_number});
#>			$calcdesc =~ s/,\s*/,\n\n/g;
#>		}
#>		elsif ($self->{ORIG_CALC}->{$field_number} =~ /\?.*\:/)
#>		{
#>			$calcdesc = $self->_switch_clause_in_words($self->{ORIG_CALC}->{$field_number});
#>			$calcdesc =~ s/,\s*/,\n\n/g;
#>		}
#>		elsif ($self->{ORIG_CALC}->{$field_number} =~ /\b_L(?:OOKUP)?_(\w+?)\s*\(['|"]?(\w+?)['|"]?\)/)
#>		{
#>			$calcdesc = $self->{ORIG_CALC}->{$field_number};
#>			my ($table, $keyfield, $count);
#>			($table, $keyfield) = ($1, $2) if ($count = ($calcdesc =~ s/\b_L(?:OOKUP)?_(\w+?)\s*\(['|"]?(\w+?)['|"]?\)->(\w+)/Set to C<$1.$3>/g) != 0);
#>			($table, $keyfield) = ($1, $2) if ($count = ($calcdesc =~ s/\b_L(?:OOKUP)?_(\w+?)\s*\(['|"]?(\w+?)['|"]?\)\[(\w+)\]/Set to C<$1.$3>/g) != 0);
#>			$calcdesc =~ s/\b_L(?:OOKUP)?_(\w+?)\s*\(['|"]?(\w+?)['|"]?\)/if C<$2> exists in C<$1>/g;		# Is this one required?
#>			$calcdesc .= ";\n\nMatch on C<input.$keyfield> equals C<$table.$keyfield>" if ($keyfield ne '');
#>		}
#>		else
#>		{
#>			$calcdesc = "Set to @{[$self->{ORIG_CALC}->{$field_number}]}";
#>		}
#>		return $calcdesc;
#>	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Docgen::Chapter::Element;
	use ETL::Pequel::Type; #++++
	use base qw(ETL::Pequel::Type::Element);

	our $this = __PACKAGE__;

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_);
		bless($self, $class);

		$self->heading($params{'heading'});

		return $self;
	}

	sub generate : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		return $c;
	}

	sub heading : method
	{ 
		my $self = shift; 
		$self->{$this}->{HEADING} = shift if (@_); 
		return $self->{$this}->{HEADING}; 
	}

	sub formatSwitch : method
	{
		my $self = shift;
		my $switch = shift;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
	
		return $c unless ($switch =~ /\?.*\:/);
		my ($condition, $true, $false) = split(/\?|\:/, $switch, -1);
		$c->add("$condition");
		$c->over;
		$c->add("? $true");
		$c->add(": $false");
		$c->back;
		return $c;
	}

	sub formatIf : method
	{
		my $self = shift;
		my $line = shift;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
	
		return $c unless ($line =~ /\s+if\s+/);
		my ($statement, $condition) = split(/\s+if\s+/, $line, -1);
		$c->add("$statement");
		$c->over;
		$c->add("if $condition");
		$c->back;
		return $c;
	}
#>	
#>	sub manualType : method 	#--> compact / full
#>	{
#>	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Docgen::Chapter::Overview;
	use base qw(ETL::Pequel::Docgen::Chapter::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		$self = $class->SUPER::new(@_, heading => 'PROCESS DETAILS');
		bless($self, $class);
		return $self;
	}

	sub generate : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		
		$c->addNonl("Input records are read from @{[ $self->PARAM->properties('input_file') ? $self->PARAM->properties('input_file') : 'standard input' ]}. ");
		$c->addNonl("The input record contains F<@{[ $self->PARAM->c_numInputFields ]}> fields. ");
		$c->add("Fields are delimited by the 'C<@{[ $self->PARAM->properties('input_delimiter') ]}>' character.");
		$c->add;
		$c->add;
		$c->addNonl("Output records are written to @{[ $self->PARAM->properties('output_file') ? $self->PARAM->properties('output_file') : 'standard output' ]}. ");
		$c->addNonl("The output record contains F<@{[ $self->PARAM->c_numOutputFields ]}> fields. ");
		$c->addNonl("Fields are delimited by the 'C<@{[ $self->PARAM->properties('output_delimiter') ]}>' character.");
		$c->add;
		$c->add;
		$c->add;

		if ($self->PARAM->sections->find('sort by')->items->size)
		{
			$c->addNonl("Input stream is B<sorted> by the ");
			$c->addNonl("input field@{[ $self->PARAM->sections->find('sort by')->items->size > 1 ? 's' : '' ]} ");
			$c->addNonl(join(" and ", map("F<@{[ $_->name ]}> (I<@{[ $_->type->name ]}>)", $self->PARAM->sections->find('sort by')->items->toArray)));
			$c->add(".");
			$c->add;
			$c->add;
		}

		if ($self->PARAM->sections->find('filter')->items->size)
		{
			$c->addNonl("Input records are eliminated (B<filtered>) unless ");
			$c->addNonl(join(" I<and> ", map("F<@{[ $_->value ]}>", $self->PARAM->sections->find('filter')->items->toArray)));
			$c->add(".");
			$c->add;
			$c->add;
		}

		if ($self->PARAM->sections->find('group by')->items->size)
		{
			$c->addNonl("Input records are B<grouped> by the ");
			$c->addNonl("input field@{[ $self->PARAM->sections->find('group by')->items->size > 1 ? 's' : '' ]} ");
			$c->addNonl(join(" and ", map("F<@{[ $_->name ]}> (I<@{[ $_->type->name ]}>)", $self->PARAM->sections->find('group by')->items->toArray)));
			$c->add(".");
			$c->add;
			$c->add;
		}

		if ($self->PARAM->sections->find('having')->items->size)
		{
			$c->addNonl("Output aggregated records are eliminated unless B<having> ");
			$c->addNonl(join(" I<and> ", map("F<@{[ $_->value ]}>", $self->PARAM->sections->find('having')->items->toArray)));
			$c->add(".");
			$c->add;
			$c->add;
		}

		if ($self->PARAM->sections->find('dedup on')->items->size)
		{
			$c->addNonl("Input records are eliminated (B<deduped>) if they contain the same values, within the group, ");
			$c->addNonl("for the input field@{[ $self->PARAM->sections->find('dedup on')->items->size > 1 ? 's' : '' ]} ");
			$c->addNonl(join(" and ", map("F<@{[ $_->name ]}> (I<@{[ $_->type->name ]}>)", $self->PARAM->sections->find('dedup on')->items->toArray)));
			$c->add(".");
			$c->add;
			$c->add;
		}
	
		if ($self->PARAM->sections->find('reject')->items->size)
		{
			$c->addNonl("Input records are eliminated (B<rejected>) if ");
			$c->addNonl(join(" I<and> ", map("F<@{[ $_->value ]}>", $self->PARAM->sections->find('reject')->items->toArray)));
			$c->addNonl(".");
			$c->addNonl(" Rejected input records are written to the file @{[ $self->PARAM->properties('reject_file') ]}");
			$c->add(".");
			$c->add;
			$c->add;
		}

		if ($self->PARAM->tables->size)
		{
		}
	
		if ($self->PARAM->properties('transfer'))
		{
			foreach my $f (grep($_->name !~ /^_/, $self->PARAM->sections->find('input section')->items->toArray))
			{
				$c->add("=head2 @{[ $self->number ]}.@{[ $f->number ]} F<@{[ $f->name ]}>");
				$c->add;
				$c->add("Input @{[ $f->operator ne '' ? 'Derived' : '' ]} Field");
				$c->add;

				if (grep($_->inputField->name eq $f->name, $self->PARAM->sections->find('field preprocess')->items->toArray))
				{
					$c->add("=item F<Field Pre-Process>");
					$c->add;
					$c->add("=begin text");
					$c->add;
					$c->over;
					foreach my $pp (grep($_->inputField->name eq $f->name, $self->PARAM->sections->find('field preprocess')->items->toArray))
					{
#<						$c->addNonl("@{[ $pp->name ]}");
						if ($pp->operator ne '')
						{
							$c->addNonl(" @{[ $pp->operator ]} ");
							if ($pp->calcOrig =~ /&select/)
							{
								my ($m, $idx, $clause, @args) 
									= $self->PARAM->parser->extractNextMacro(index($pp->calcOrig, "&select"), $pp->calcOrig);
								$c->addAll($m->format(@args));
							}
							elsif ($pp->calcOrig =~ /\?.*\:/)
							{
								$c->addAll($self->formatSwitch($pp->calcOrig));
							}
							else
							{
								$c->addFmt($pp->calcOrig);
							}
						}
					}
					$c->add;
					$c->back;
					$c->add("=end");
					$c->add;
				}

				if ($f->operator ne '')
				{
					$c->add("=item F<Derived Field Evaluation>");
					$c->add;
					$c->add("=begin text");
					$c->add;
					$c->over;
					$c->addNonl("@{[ $f->operator ]} ");
#					if ($self->root->t_macro->exists('select')->can('format'))
					if ($f->calcOrig =~ /&select/)
					{
						my ($m, $idx, $clause, @args) 
							= $self->PARAM->parser->extractNextMacro(index($f->calcOrig, "&select"), $f->calcOrig);
						$c->addAll($m->format(@args));
					}
#<					if ($f->calcOrig =~ /&select/)
#<					{
#<						$c->addAll($self->root->t_macro->exists('select')->format($f->calcOrig));
#<					}
					elsif ($f->calcOrig =~ /\?.*\:/)
					{
						$c->addAll($self->formatSwitch($f->calcOrig));
					}
					elsif ($f->calcOrig =~ /\s+if\s+/)
					{
						$c->addAll($self->formatIf($f->calcOrig));
					}
					else
					{
						$c->add($f->calcOrig);
					}
					$c->add;
					$c->back;
					$c->add("=end");
					$c->add;
#>					$c->add("=item F<Description>");
#>					$c->add;
				}
			}
		}

		foreach (grep($_->name !~ /^_/, $self->PARAM->sections->find('output section')->items->toArray))
		{
			$c->add("=head2 @{[ $self->number ]}.@{[ $_->number ]} F<@{[ $_->name ]}>");
			$c->add;
			$c->add("Output Field");
			$c->add;
			$c->add;
			$c->add("=item F<Description>");
			$c->add;
			if (!$_->aggregate)
			{
				$c->add("Set to input field F<@{[ $_->inputField->name ]}>");
				$c->add;
				if ($_->inputField->operator ne '')
				{
					$c->add;
					$c->add("=item F<Derived Input Field Evaluation>");
					$c->add;
					$c->add("=begin text");
					$c->add;
					$c->over;
					$c->addNonl("@{[ $_->inputField->operator ]} ");

#>					my ($macro_name, @args);
#>					my $clause = $_->inputField->calcOrig;
#>					while (($macro_name, @args) = $self->extractMacro(1, \$clause))
#>					{
#>						next unless ($macro_name =~ /select/);
#>						my $m;
#>						next if (($m = $self->root->t_macro->exists($macro_name)) == 0);
#>						$c->addAll($m->format(@args));
#>					}

					if ($_->inputField->calcOrig =~ /&select/)
					{
						my ($m, $idx, $clause, @args) 
							= $self->PARAM->parser->extractNextMacro(index($_->inputField->calcOrig, "&select"), $_->inputField->calcOrig);
						$c->addAll($m->format(@args));
					}
#<					if ($_->inputField->calcOrig =~ /&select/)
#<					{
#<						$c->addAll($self->root->t_macro->exists('select')->format($_->inputField->calcOrig));
#<					}
					elsif ($_->inputField->calcOrig =~ /\?.*\:/)
					{
						$c->addAll($self->formatSwitch($_->inputField->calcOrig));
					}
					elsif ($_->inputField->calcOrig =~ /\s+if\s+/)
					{
						$c->addAll($self->formatIf($_->inputField->calcOrig));
					}
					else
					{
						$c->add($_->inputField->calcOrig);
					}
					$c->add;
					$c->back;
					$c->add("=end");
					$c->add;
				}
			}
			elsif ($_->aggregate->name eq '=')
			{
				$c->add("Derived (calculated) field.");
				$c->add;
				$c->add;
				$c->add("=item F<Derived Field Evaluation>");
				$c->add;
				$c->add("=begin text");
				$c->add;
				$c->over;
				if ($_->clause =~ /&select/)
				{
#? Not quite -- what about nested / multiple macros in clause??
					my ($m, $idx, $clause, @args) 
						= $self->PARAM->parser->extractNextMacro(index($_->clause, "&select"), $_->clause);
					$c->addAll($m->format(@args));
				}
#<				$c->add("@{[ $_->clause ]}");
				$c->add;
				$c->back;
				$c->add("=end");
				$c->add;
			}
			elsif ($_->inputField)
			{
				$c->add("F<@{[ ucfirst($_->aggregate->name) ]}> aggregation on input field F<@{[ $_->inputField->name ]}>.");
				$c->add;
			}
			elsif ($_->aggregate->name eq 'serial')
			{
				$c->add("F<@{[ ucfirst($_->aggregate->name) ]}> number starting at B<@{[ $_->serialStart ]}>.");
				$c->add;
			}
			else
			{
				$c->add("F<@{[ ucfirst($_->aggregate->name) ]}> aggregation.");
				$c->add;
			}
		
			if ($_->condition)
			{
				$c->add;
				$c->add("=item F<Aggregation condition>");
				$c->add;
				$c->add("@{[ $_->condition ]};");
				$c->add;
			}
		}
#>		Show Intermediate (transperent) fields info...
		return $c;
	}

# Summarize the output result of the process...
#		field-number, field-name, 'input-transfer' / 'input-derived' / 'output' / 'output aggregated',
#			calc-description / aggregation description
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Docgen::Chapter::Config;
	use base qw(ETL::Pequel::Docgen::Chapter::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		$self = $class->SUPER::new(@_, heading => 'CONFIGURATION SETTINGS');
		bless($self, $class);
		return $self;
	}

	sub generate : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		foreach ($self->PARAM->sections->find('options')->items->toArray)
		{
			$c->add("=head2 @{[ $self->number ]}.@{[ $_->number ]} F<@{[ $_->name ]}>");
			$c->add;
			$c->add("@{[ $_->ref->description ]}: @{[ $_->value ]}");
			$c->add;
		}
		return $c;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Docgen::Chapter::InputFields;
	use base qw(ETL::Pequel::Docgen::Chapter::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		$self = $class->SUPER::new(@_);
		bless($self, $class);
		return $self;
	}

#	Input fields
#	Input derived fields
#	Cross-references
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Docgen::Chapter::Tables;
	use base qw(ETL::Pequel::Docgen::Chapter::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		$self = $class->SUPER::new(@_, heading => 'TABLES');
		bless($self, $class);
		return $self;
	}

	sub generate : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		foreach (sort { $a->sequence <=> $b->sequence } $self->PARAM->tables->toArray)
		{
			$c->add("=head2 @{[ $self->number ]}.@{[ $_->sequence ]} F<@{[ $_->name ]}>");
			$c->add;
			$c->add("Table Type: F<@{[ $_->type ]}@{[ $_->merge ? ' merge' : '' ]}>");
			$c->add;
			if ($_->type eq 'sqlite' || $_->type eq 'oracle')
			{
				$c->add("Data Source Filename: F<@{[ $_->dataSourceFilename ]}>");
				$c->add;
				$c->add("Key Field Number: F<@{[ $_->keyColumn ]}>");
				$c->add;
				$c->add("Key Field Type: F<@{[ $_->keyType ]}>");
				$c->add;
				$c->add("Database Filename: F<@{[ $_->dbFilename ]}>");
				$c->add;
				$c->add("=begin");
				$c->add;
				$c->add("=end");
				$c->add;
				foreach my $f ($_->fields->toArray)
				{
					$c->addNonl("B<@{[ $self->number ]}.@{[ $_->sequence ]}.@{[ $f->number ]}> ");
					$c->addNonl("F<@{[ $f->name ]} = @{[ $f->column ]}>");
					$c->add;
					$c->add;
				}
			}
			elsif ($_->type eq 'external' || ($_->type eq 'local' && $_->persistent))
			{
				$c->add("Data Source Filename: F<@{[ $_->dataSourceFilename ]}>");
				$c->add;
				$c->add("Key Field Number: F<@{[ $_->keyColumn ]}>");
				$c->add;
#>				$c->add("Key Field Type: F<@{[ $_->keyType ]}>");
#>				$c->add;
				$c->add("=begin");
				$c->add;
				$c->add("=end");
				$c->add;
				foreach my $f ($_->fields->toArray)
				{
					$c->addNonl("B<@{[ $self->number ]}.@{[ $_->sequence ]}.@{[ $f->number ]}> ");
					$c->addNonl("F<@{[ $f->name ]} = @{[ $f->column ]}>");
					$c->add;
					$c->add;
				}
			}
			elsif ($_->type eq 'local')
			{
				$c->add("=item F<Data>");
				$c->add;
				$c->add;
				foreach my $d ($_->data->toArray)
				{
					$c->add("@{[ $d->name ]} --> @{[ join(' ', map($_->value, $d->value->toArray)) ]}");
					$c->add;
				}
				$c->add;
				$c->add;
			}

		}
		return $c;
	}
}
# ----------------------------------------------------------------------------------------------------
#?	
#?	   package Pequel::Docgen::Chapter::InputSummary;		#--> maybe (partly) done in overview
#?	   use base qw(Pequel::Docgen::Chapter::Element);
#?	
#?	   sub new : method
#?	   {
#?	       my $self = shift;
#?	       my $class = ref($self) || $self;
#?	       $self = $class->SUPER::new(@_);
#?	       bless($self, $class);
#?	   	return $self;
#?	   }
#?	
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Docgen::Chapter::TablesSummary;		#--> maybe (partly) done in overview
	use base qw(ETL::Pequel::Docgen::Chapter::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		$self = $class->SUPER::new(@_, heading => 'TABLE INFORMATION SUMMARY');
		bless($self, $class);
		return $self;
	}

	sub generate : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		$c->add("=head2 @{[ $self->number ]}.1 Table List Sorted By Table Name");
		$c->add;
		foreach (sort { $a->name cmp $b->name } $self->PARAM->tables->toArray)
		{
			$c->add("@{[ $_->name ]} -- F<@{[ $_->sequence ]}> (I<@{[ $_->type ]}@{[ $_->merge ? ' merge' : '' ]}>)");
			$c->add;
		}
		return $c;
	}
}
# ----------------------------------------------------------------------------------------------------
#?	
#?	   package Pequel::Docgen::Chapter::OutputFields;		#--> maybe (partly) done in overview
#?	   use base qw(Pequel::Docgen::Chapter::Element);
#?	
#?	   sub new : method
#?	   {
#?	       my $self = shift;
#?	       my $class = ref($self) || $self;
#?	       $self = $class->SUPER::new(@_);
#?	       bless($self, $class);
#?	   	return $self;
#?	   }
#?	
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Docgen::Chapter::PequelScript;
	use base qw(ETL::Pequel::Docgen::Chapter::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		$self = $class->SUPER::new(@_);
		$self->heading(uc($self->PARAM->properties('script_name')));
		bless($self, $class);
		return $self;
	}

	sub generate : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);

		$c->add("=head2 F<options>");
		$c->add;
		$c->add("=begin text");
		$c->add;
		$c->over;
		foreach ($self->PARAM->sections->find('options')->items->toArray)
		{
			$c->add("@{[ $_->name ]}(@{[ $_->value ]})");
		}
		$c->back;
		$c->add("=end");
		$c->add;

		if ($self->PARAM->sections->find('description section')->items->size)
		{
			$c->add("=head2 F<description>");
			$c->add;
			$c->add("=begin text");
			$c->add;
			$c->over;
			map($c->add($_->value), $self->PARAM->sections->find('description section')->items->toArray);
			$c->back;
			$c->add;
			$c->add("=end");
			$c->add;
		}

		if (grep($_->type eq 'local' && !$_->persistent, $self->PARAM->tables->toArray))
		{
			$c->add("=head2 F<init table>");
			$c->add;
			$c->add("=begin text");
			$c->add;
			$c->over;
			foreach my $t (grep($_->type eq 'local' && !$_->persistent, $self->PARAM->tables->toArray))
			{
				foreach my $d ($t->data->toArray)
				{
					$c->add("@{[ $t->name ]} @{[ $d->name ]} @{[ join(' ', map($_->value, $d->value->toArray)) ]}");
				}
				$c->add;
			}
			$c->back;
			$c->add;
			$c->add("=end");
			$c->add;
		}

		if (grep($_->type eq 'external' || ($_->type eq 'local' && $_->persistent), $self->PARAM->tables->toArray))
		{
			$c->add("=head2 F<load table>");
			$c->add;
			$c->add("=begin text");
			$c->add;
			$c->over;
			foreach my $t (grep($_->type eq 'external' || ($_->type eq 'local' && $_->dataSourceFilename), $self->PARAM->tables->toArray))
			{
				$c->add("@{[ $t->name ]} /* Table Name */ \\");
				$c->over;
				$c->add("@{[ $t->dataSourceFilename ]} /* Data Source Filename */ \\");
				$c->add("@{[ $t->keyColumn ]} /* Key Column Number */ \\");
#>				$c->addNonl("@{[ $t->keyType ]} /* Key Type */");
				foreach my $f ($t->fields->toArray)
				{
					$c->add(" \\");
					$c->addNonl("@{[ $f->name ]} = @{[ $f->column ]}");
				}
				$c->add;
				$c->back;
				$c->add;
			}
			$c->back;
			$c->add;
			$c->add("=end");
			$c->add;
		}

		foreach my $merge (0, 1)
		{
			if (grep($_->type eq 'sqlite' && $_->merge == $merge, $self->PARAM->tables->toArray))
			{
				$c->add("=head2 F<load table sqlite @{[ $merge ? 'merge' : '' ]}>");
				$c->add;
				$c->add("=begin text");
				$c->add;
				$c->over;
				#>	ETL::Pequel::Type::Table::...->docInfo
				foreach (grep($_->type eq 'sqlite' && $_->merge == $merge, $self->PARAM->tables->toArray))
				{
					$c->add("@{[ $_->name ]} /* Table Name */ \\");
					$c->over;
					$c->add("@{[ $_->dataSourceFilename ]} /* Data Source Filename */ \\");
					$c->add("@{[ $_->keyColumn ]} /* Key Column Number */ \\");
					$c->addNonl("@{[ $_->keyType ]} /* Key Type */");
					foreach my $f ($_->fields->toArray)
					{
						$c->add(" \\");
						$c->addNonl("@{[ $f->name ]} = @{[ $f->column ]}");
					}
					$c->add;
					$c->back;
					$c->add;
				}
				$c->back;
				$c->add;
				$c->add("=end");
				$c->add;
			}
		}

		foreach my $merge (0, 1)
		{
			if (grep($_->type eq 'oracle' && $_->merge == $merge, $self->PARAM->tables->toArray))
			{
				$c->add("=head2 F<load table oracle @{[ $merge ? 'merge' : '' ]}>");
				$c->add;
				$c->add("=begin text");
				$c->add;
				$c->over;
				#>	ETL::Pequel::Type::Table::...->docInfo
				foreach (grep($_->type eq 'oracle' && $_->merge == $merge, $self->PARAM->tables->toArray))
				{
					$c->add("@{[ $_->name ]} /* Table Name */ \\");
					$c->over;
					$c->add("@{[ $_->dataSourceFilename ]} /* Data Source Filename */ \\");
					$c->add("@{[ $_->keyColumn ]} /* Key Column Number */ \\");
					$c->addNonl("@{[ $_->keyType ]} /* Key Type */");
					foreach my $f ($_->fields->toArray)
					{
						$c->add(" \\");
						$c->addNonl("@{[ $f->name ]} = @{[ $f->column ]}");
					}
					$c->add;
					$c->back;
					$c->add;
				}
				$c->back;
				$c->add;
				$c->add("=end");
				$c->add;
			}
		}

		if ($self->PARAM->sections->find('input section')->items->size)
		{
			$c->add("=head2 F<input section>");
			$c->add;
			$c->add("=begin text");
			$c->add;
			$c->over;
			foreach ($self->PARAM->sections->find('input section')->items->toArray)
			{
				$c->addNonl("@{[ $_->name ]}");
				if ($_->operator ne '')
				{
					$c->addNonl(" @{[ $_->operator ]} ");
					if ($_->calcOrig =~ /&select/)
					{
						my ($m, $idx, $clause, @args) 
							= $self->PARAM->parser->extractNextMacro(index($_->calcOrig, "&select"), $_->calcOrig);
						$c->addAll($m->format(@args));
					}
#<					if ($_->calcOrig =~ /&select/)
#<					{
#<						$c->addAll($self->root->t_macro->exists('select')->format($_->calcOrig));
#<					}
					elsif ($_->calcOrig =~ /\?.*\:/)
					{
						$c->addAll($self->formatSwitch($_->calcOrig));
					}
					elsif ($_->calcOrig =~ /\s+if\s+/)
					{
						$c->addAll($self->formatIf($_->calcOrig));
					}
					else
					{
						$c->addFmt($_->calcOrig);
					}
				}
				$c->add;
			}
			$c->back;
			$c->add("=end");
			$c->add;
		}

		foreach my $s ($self->PARAM->sections->toArray())
		{
			next unless ($s->items->size());
			next unless ($s->name =~ /^copy|^divert/);
			$c->add("=head2 F<@{[ $s->SUPER::name =~ /^copy/ ? 'copy record' : 'divert record' ]}(@{[ $s->args() ]})>");
			$c->add;
			$c->add("=begin text");
			$c->add;
			$c->over;
			map($c->add($_->value), $s->items->toArray);
			$c->back;
			$c->add;
			$c->add("=end");
			$c->add;
		}

		if ($self->PARAM->sections->find('filter')->items->size)
		{
			$c->add("=head2 F<filter>");
			$c->add;
			$c->add("=begin text");
			$c->add;
			$c->over;
			map($c->add($_->value), $self->PARAM->sections->find('filter')->items->toArray);
			$c->back;
			$c->add;
			$c->add("=end");
			$c->add;
		}

#>		ETL::Pequel::Type::Section::SortBy->docPequelScript
		if ($self->PARAM->sections->find('sort by')->items->size)
		{
			$c->add("=head2 F<sort by>");
			$c->add;
			$c->add("=begin text");
			$c->add;
			$c->over;
			map
			(
				$c->add("@{[ $_->name ]} @{[ $_->type->name ]} @{[ $_->direction == $self->PARAM->SORT_DES ? 'des' : '' ]}"), 
				$self->PARAM->sections->find('sort by')->items->toArray
			);
			$c->back;
			$c->add;
			$c->add("=end");
			$c->add;
		}

		if ($self->PARAM->sections->find('group by')->items->size)
		{
			$c->add("=head2 F<group by>");
			$c->add;
			$c->add("=begin text");
			$c->add;
			$c->over;
			map($c->add("@{[ $_->name ]} @{[ $_->type->name ]}"), $self->PARAM->sections->find('group by')->items->toArray);
			$c->back;
			$c->add;
			$c->add("=end");
			$c->add;
		}

		if ($self->PARAM->sections->find('dedup on')->items->size)
		{
			$c->add("=head2 F<dedup on>");
			$c->add;
			$c->add("=begin text");
			$c->add;
			$c->over;
			map($c->add("@{[ $_->name ]} @{[ $_->type->name ]}"), $self->PARAM->sections->find('dedup on')->items->toArray);
			$c->back;
			$c->add;
			$c->add("=end");
			$c->add;
		}

		if ($self->PARAM->sections->find('reject')->items->size)
		{
			$c->add("=head2 F<reject>");
			$c->add;
			$c->add("=begin text");
			$c->add;
			$c->over;
			map($c->add($_->value), $self->PARAM->sections->find('reject')->items->toArray);
			$c->back;
			$c->add;
			$c->add("=end");
			$c->add;
		}

		if ($self->PARAM->sections->find('field preprocess')->items->size)
		{
			$c->add("=head2 F<field preprocess>");
			$c->add;
			$c->add("=begin text");
			$c->add;
			$c->over;
			foreach ($self->PARAM->sections->find('field preprocess')->items->toArray)
			{
				$c->addNonl("@{[ $_->name ]}");
				if ($_->operator ne '')
				{
					$c->addNonl(" @{[ $_->operator ]} ");
					if ($_->calcOrig =~ /&select/)
					{
						my ($m, $idx, $clause, @args) 
							= $self->PARAM->parser->extractNextMacro(index($_->calcOrig, "&select"), $_->calcOrig);
						$c->addAll($m->format(@args));
					}
#<					if ($_->calcOrig =~ /&select/)
#<					{
#<						$c->addAll($self->root->t_macro->exists('select')->format($_->calcOrig));
#<					}
					elsif ($_->calcOrig =~ /\?.*\:/)
					{
						$c->addAll($self->formatSwitch($_->calcOrig));
					}
					else
					{
#>	Not quite because have to include name + operator from above.
						$c->addFmt($_->calcOrig);
					}
				}
				$c->add;
			}
			$c->back;
			$c->add("=end");
			$c->add;
		}

		if ($self->PARAM->sections->find('output section')->items->size)
		{
			$c->add("=head2 F<output section>");
			$c->add;
			$c->add("=begin text");
			$c->add;
			$c->over;
			my $maxlen=0;
			foreach ($self->PARAM->sections->find('output section')->items->toArray)
			{
				$maxlen = length($_->name) if ($maxlen < length($_->name));
			}
			foreach ($self->PARAM->sections->find('output section')->items->toArray)
			{
				$c->addNonl(sprintf("%-9s ", $_->type->name));
				$c->addNonl(sprintf("%-${maxlen}s ", $_->name));
				$c->addNonl("@{[ $_->clause ]}");
				$c->add;
			}
			$c->back;
			$c->add("=end");
			$c->add;
		}

		if ($self->PARAM->sections->find('field postprocess')->items->size)
		{
			$c->add("=head2 F<field postprocess>");
			$c->add;
			$c->add("=begin text");
			$c->add;
			$c->over;
			foreach ($self->PARAM->sections->find('field postprocess')->items->toArray)
			{
				$c->addNonl("@{[ $_->name ]}");
				if ($_->operator ne '')
				{
					$c->addNonl(" @{[ $_->operator ]} ");
					if ($_->calcOrig =~ /&select/)
					{
						my ($m, $idx, $clause, @args) 
							= $self->PARAM->parser->extractNextMacro(index($_->calcOrig, "&select"), $_->calcOrig);
						$c->addAll($m->format(@args));
					}
#<					if ($_->calcOrig =~ /&select/)
#<					{
#<						$c->addAll($self->root->t_macro->exists('select')->format($_->calcOrig));
#<					}
					elsif ($_->calcOrig =~ /\?.*\:/)
					{
						$c->addAll($self->formatSwitch($_->calcOrig));
					}
					else
					{
						$c->addFmt($_->calcOrig);
					}
				}
				$c->add;
			}
			$c->back;
			$c->add("=end");
			$c->add;
		}

		if ($self->PARAM->sections->find('having')->items->size)
		{
			$c->add("=head2 F<having>");
			$c->add;
			$c->add("=begin text");
			$c->add;
			$c->over;
			map($c->add($_->value), $self->PARAM->sections->find('having')->items->toArray);
			$c->back;
			$c->add;
			$c->add("=end");
			$c->add;
		}

		if ($self->PARAM->sections->find('sort output')->items->size)
		{
			$c->add("=head2 F<sort output>");
			$c->add;
			$c->add("=begin text");
			$c->add;
			$c->over;
			map
			(
				$c->add("@{[ $_->name ]} @{[ $_->type->name ]} @{[ $_->direction == $self->PARAM->SORT_DES ? 'des' : '' ]}"), 
				$self->PARAM->sections->find('sort output')->items->toArray
			);
			$c->back;
			$c->add;
			$c->add("=end");
			$c->add;
		}

		return $c;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Docgen::Chapter::PequelGenerated;
	use base qw(ETL::Pequel::Docgen::Chapter::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		$self = $class->SUPER::new(@_, heading => 'PEQUEL GENERATED PROGRAM');
		bless($self, $class);
		return $self;
	}

	sub generate : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code::Pod->new(PARAM => $self->PARAM);

		my $engine = ETL::Pequel::Engine->new(PARAM => $self->PARAM);
		$engine->generate;
		$c->begin;
		$c->add($engine->sprintRaw);
		$c->end;

		return $c;
	}
}
#----------------------------------------------------------------------------------------------------
	
	   package ETL::Pequel::Docgen::Chapter::Copyright;
	   use base qw(ETL::Pequel::Docgen::Chapter::Element);
	
	   sub new : method
	   {
	       my $self = shift;
	       my $class = ref($self) || $self;
		   $self = $class->SUPER::new(@_, heading => 'ABOUT PEQUEL');
	       bless($self, $class);
	   	return $self;
	   }
	
		sub generate : method
		{
			my $self = shift;
			my $c = ETL::Pequel::Code::Pod->new(PARAM => $self->PARAM);

			$c->add("This document was generated by Pequel.");
			$c->add;
			$c->add("F<https://sourceforge.net/projects/pequel/>");
			$c->add;
			$c->add("=head2 COPYRIGHT");
			$c->add;
			$c->add("Copyright E<copy>1999-2005, Mario Gaffiero. All Rights Reserved.");
			$c->add("'Pequel' TM Copyright E<copy>1999-2005, Mario Gaffiero. All Rights Reserved.");
			$c->add;
			$c->add("This program and all its component contents is copyrighted free software by Mario Gaffiero and is released under the GNU General Public License (GPL), Version 2, a copy of which may be found at http://www.opensource.org/licenses/gpl-license.html");
			$c->add;
			$c->add("Pequel is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation; either version 2 of the License, or (at your option) any later version.");
			$c->add;
			$c->add("Pequel is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.");
			$c->add;
			$c->add("You should have received a copy of the GNU General Public License along with Pequel; if not, write to the Free Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA");
			return $c;
		}
# ----------------------------------------------------------------------------------------------------
#?	
#?	   package ETL::Pequel::Docgen::Chapter::Credits;
#?	   use base qw(ETL::Pequel::Docgen::Chapter::Element);
#?	
#?	   sub new : method
#?	   {
#?	       my $self = shift;
#?	       my $class = ref($self) || $self;
#?	       $self = $class->SUPER::new(@_);
#?	       bless($self, $class);
#?	   	return $self;
#?	   }
#?	
# ----------------------------------------------------------------------------------------------------
1;
