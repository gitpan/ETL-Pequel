#!/usr/bin/perl
# ----------------------------------------------------------------------------------------------------
#  Name		: ETL::Pequel::Engine.pm
#  Created	: 29 January 2005
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
# 20/09/2005	2.3-6		gaffie	unpack_input/pack_output implementation.
# 04/09/2005	2.3-3		gaffie	Support input_file(pequel-script) with sort-by.
# 04/09/2005	2.3-3		gaffie	Support sort-output with 'close(out)' code.
# 20/09/2005	2.3-2		gaffie	Added pequel script chaining functionality.
# 05/09/2005	2.2-9		gaffie	Prevent repeated calc of input field when they appear in group-by.
# 05/09/2005	2.2-9		gaffie	In codeBreak Fixed calc of derived group-by fields.
# 26/08/2005	2.2-8		gaffie	Added o_inline_parse_input_quotes implementation via readsplit()
# 25/8/2005		2.2-7		gaffie	Added vim syntax perl setting in generated script.
# 25/8/2005		2.2-7		gaffie	Removed 'use warnings' because eval in execute (might) complain.
# 16/6/2004		1.1-2		gaffie	Check getlogin() for undefined return (ie. during backgrnd runs).
# ----------------------------------------------------------------------------------------------------
# TO DO:
# ----------------------------------------------------------------------------------------------------
require 5.005_62;
use strict;
use attributes qw(get reftype);
#use warnings; --- NOT HERE because the eval in execute will complain!

# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Engine;
	use ETL::Pequel::Code;	#++++
	use base qw(ETL::Pequel::Code);
	use ETL::Pequel::Engine::Inline;
	use UNIVERSAL qw( isa can );

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_);
		bless($self, $class);

		return $self;
	}

	sub check : method
	{
		my $self = shift;

		$self->printToFile("@{[ $self->PARAM->properties('script_name') ]}.DEBUG");
		my $check = `perl -cw @{[ $self->PARAM->properties('script_name') ]}.DEBUG 2>&1`;
		chomp($check);
		if ($check !~ /syntax OK/)
		{
			$self->add("__END__");
			$self->add("Syntax check summary follows:");
			$self->add("$check");
			$self->printToFile("@{[ $self->PARAM->properties('script_name') ]}.DEBUG");
		}
		else
		{
			unlink "@{[ $self->PARAM->properties('script_name') ]}.DEBUG";
		}
		return $check;
	}

	sub execute : method
	{
		my $self = shift;
		eval ($self->text);
		$self->PARAM->error->msgStderr("$@");
	}

	sub generate : method
	{
		my $self = shift;

		$self->PARAM->error->msgStderrNonl($self->PARAM->properties('debug') ? 'generate...' : '.');
		$self->codeHeaderInfo;
		$self->codeInit;

		if ($self->PARAM->properties('load_tables_only'))
		{
			map
			(
				$self->addAll($_->codeLoadTable),	# -> codePackages
				(sort { $a->name cmp $b->name } $self->PARAM->tables->toArray)
			);
			return;
		}
		return unless ($self->PARAM->sections->exists('input section')->items->size);
	
		map($self->addAll($_->codeOpen()), grep($_->items->size, $self->PARAM->sections->toArray));

		$self->add("\&PrintHeader();") 
			if 
			(
				$self->PARAM->properties('header') #< || $self->PARAM->properties('print_header')) 
				&& !$self->PARAM->properties('suppress_output')
			);

#<		$self->add("exit;") if ($self->PARAM->properties('print_header'));

		map($self->addAll($_->codePrepare), sort { $a->name cmp $b->name } $self->PARAM->tables->toArray);

		$self->verboseMessage("Start");
		$self->codeBenchmarkInit;
		$self->codeBenchmarkStart;

		if ($self->PARAM->properties('use_inline'))
		{
			$self->addAll($self->PARAM->dbtypes->codeConnect);
			$self->verboseMessage("Tables opened.") if ($self->PARAM->tables->size);
		}
		$self->codeWhileLoop();

#>		$self->codeSummarySection;
		# Important -- first close last opened to prevent deadlock:
		map($self->addAll($_->codeClose), grep($_->items->size, reverse($self->PARAM->sections->toArray)));

		$self->addAll($self->PARAM->dbtypes->codeDisconnect)
			if ($self->PARAM->properties('use_inline')); # wrong -- relevant to oracle/sqlite tables

		map($self->addAll($_->codeClose), sort { $a->name cmp $b->name } $self->PARAM->tables->toArray);
		$self->codeRecordCounterMessageFinal;
		$self->codeBenchmarkEnd;
		$self->addBar;
		map($self->addAll($_->codeLoadTable), (sort { $a->name cmp $b->name } $self->PARAM->tables->toArray));
		map($self->addAll($_->codePackages), grep($_->items->size, $self->PARAM->sections->toArray));
		$self->codePackages();

		$self->codeSubPrintHeader();

		if ($self->PARAM->properties('use_inline'))
		{
			my $inline = ETL::Pequel::Engine::Inline->new(PARAM => $self->PARAM);
			$inline->generate();
			$self->addAll($inline);
		}
	}

	sub codePackages : method
	{
		my $self = shift;
		foreach ($self->PARAM->packages->toArrayUniq())
		{
			$self->openBlock("{");
				$self->add("package p_@{[ lc($_->name) ]};");
				$self->add("sub @{[ lc($_->name) ]}");
				$self->openBlock("{");
				$self->addAll($_->value->PARAM->ENGINE);
				$self->closeBlock;
			$self->closeBlock;
		}
	}
	
	sub codeWhileLoop : method
	{
		my $self = shift;

		my $fdname = $self->PARAM->sections->find('input section')->get_fd_name();
		$self->add("my \$discard_header = <$fdname>;") if ($self->PARAM->properties('discard_header'));

		if ($self->PARAM->properties('use_inline'))
		{
			$self->add("my \$i;");
			$self->add( "while (readsplit(\\\@I_VAL))");
		}
		else
		{
			$self->add("while (<$fdname>)");
		}

		$self->openBlock("{");	# Begin of main WHILE LOOP

			$self->codeIncRecordCounter;
			$self->codeRecordCounterMessage;
			$self->codeInputRecordLimitBreak;

			if (!$self->PARAM->properties('use_inline'))
			{
				$self->add("chomp;");
				$self->add("chop;") if ($self->PARAM->properties('rmctrlm'));;
				if ($self->PARAM->properties('unpack_input'))
				{
					$self->add("\@I_VAL = unpack(INPUT_PACK_FMT, \$_);");
				}
				else
				{
					$self->addNonl("\@I_VAL = split(");
					$self->addNonl($self->PARAM->properties('input_delimiter') =~ /\\/ ? '/' : '"[');
					$self->addNonl($self->PARAM->properties('input_delimiter'));
					$self->addNonl($self->PARAM->properties('input_delimiter') =~ /\\/ ? '/' : ']"');
					$self->addNonl(", \$_);");	
					$self->add;
				}
			}

			map
			(
				$self->addAll($_->codeFetchRow),
				sort { $a->name cmp $b->name } $self->PARAM->tables->toArray
			);
#?				$runcode .= '    @{$O_VAL{$key}{TRANSFER}} = @' . "${I};" . "\n" if ($self->{HASH} && $self->{TRANSFER});

			$self->codeBreak;

			$self->codeMainBody;
		
			$self->codeZeroNulls 
				if 
				(
					$self->PARAM->properties('nonulls')
					&& !$self->PARAM->sections->exists('group by')->items->size
#>					&& !$self->PARAM->properties('group_all')
					&& !$self->PARAM->properties('suppress_output')
				);

			if 
			(
				!$self->PARAM->sections->exists('group by')->items->size
#>				&& !$self->PARAM->properties('group_all')
				&& !$self->PARAM->properties('suppress_output')
			)
			{
				$self->codePrintBefore();
				$self->codePrint();
				$self->codePrintAfter();
			}

		$self->closeBlock;	# End of main WHILE LOOP
		return if ($self->PARAM->properties('suppress_output'));
	
		if ($self->PARAM->properties('hash'))
		{
			$self->add("foreach \$key (sort @{[ $self->PARAM->properties('hash') eq 'numeric' ? '{$a <=> $b}' : '' ]} keys \%O_VAL)");
			$self->openBlock("{");
		}
		$self->codeZeroNulls 
			if 
			(
				$self->PARAM->properties('nonulls')
				&& 
				(
					$self->PARAM->sections->exists('group by')->items->size
#>					|| $self->PARAM->properties('group_all')
				)
			);
		if 
		(
			$self->PARAM->sections->exists('group by')->items->size
#>			|| $self->PARAM->properties('group_all')
		)
		{
			$self->codePrintBefore();
			$self->codePrint();
			$self->codePrintAfter();
		}
		if ($self->PARAM->properties('hash'))
		{
			$self->closeBlock;
		}
	}

	sub codeDecl : method
	{
		my $self = shift;
		$self->codeInputVarNames;
		$self->codeOutputVarNames;
		$self->codeTableFieldNames;
		$self->codeInlineTableFieldNames;
	}

	sub codeInit : method
	{
		my $self = shift;

		$self->codeUseStd;
		$self->codeDecl;

		$self->add("local \$\\=\"\\n\";");
		$self->PARAM->properties('pack_output')
			? $self->add("local \$,=\"\";")
			: $self->add("local \$,=\"@{[ $self->PARAM->properties('output_delimiter') ]}\";");
		$self->codeOpenLog if ($self->PARAM->properties('logging'));
		$self->verboseMessage("Init");

		$self->add("use constant VERBOSE => int @{[ $self->PARAM->properties('verbose') ]};")  
			if ($self->PARAM->properties('verbose') != 0);
		$self->add("use constant LAST_ICELL => int @{[ $self->PARAM->sections->exists('input section')->items->size -1 ]};");

		if ($self->PARAM->properties('unpack_input'))
		{
			$self->addNonl("use constant INPUT_PACK_FMT => ");
			if ($self->PARAM->properties('input_pack_fmt')  =~ /^\[(.*)\]$/)
			{
				$self->addNonl("'$1' x (LAST_ICELL+1)");
			}
			else
			{
				$self->addNonl("'@{[ $self->PARAM->properties('input_pack_fmt') ]}'");
			}
			$self->add(';');
		}
		if ($self->PARAM->properties('pack_output'))
		{
			$self->addNonl("use constant OUTPUT_PACK_FMT => ");
			if ($self->PARAM->properties('output_pack_fmt')  =~ /^\[(.*)\]$/)
			{
				$self->addNonl("'$1' x (");
				$self->addNonl("@{[ $self->PARAM->properties('transfer') ? 'LAST_ICELL+1+' : '' ]}");
				$self->addNonl("@{[ int(grep($_->name !~ /^_/, $self->PARAM->section('output section')->items->toArray())) ]}");
				$self->addNonl(")");
			}
			else
			{
				$self->addNonl("'@{[ $self->PARAM->properties('output_pack_fmt') ]}'");
			}
			$self->add(';');
		}

		$self->add("my \@I_VAL;");		#? unless ($self-PARAM->properties('use_default_var'));
		$self->add("my \@O_VAL;") unless ($self->PARAM->properties('hash'));
		$self->add("my \%O_VAL;") if ($self->PARAM->properties('hash'));
		$self->add("my \$key;") if ($self->PARAM->properties('hash'));

#>		# types:
#>		map
#>		(
#>			$self->addAll($_->codeInit),
#>			grep($_->useList->size, $self->PARAM->datatypes->toArray)
#>		);

		# macros:
		$self->addAll($self->PARAM->macros->codeInit) if ($self->PARAM->macros->can("codeInit"));
		map
		(
			$self->addAll($_->codeInit),
			grep($_->useList->size, $self->PARAM->macros->toArray)
		);

		# aggregates:
		$self->addAll($self->PARAM->aggregates->codeInit) if ($self->PARAM->aggregates->can("codeInit"));
		map
		(
			$self->addAll($_->codeInit),
			grep($_->useList->size, $self->PARAM->aggregates->toArray)
		);

		# sections:
		$self->addAll($self->PARAM->sections->codeInit) if ($self->PARAM->sections->can("codeInit"));
		map
		(
			$self->addAll($_->codeInit),
			grep($_->items->size, $self->PARAM->sections->toArray)
		);

		# tables:
		$self->addAll($self->PARAM->tables->codeInit) if ($self->PARAM->tables->can("codeInit"));
		map
		(
			$self->addAll($_->codeInit),
			$self->PARAM->tables->toArray
		);

		$self->codeNumericFieldsInit if ($self->PARAM->properties('nonulls'));
	}

	sub codeBreak : method
	{
		my $self = shift;

		$self->addCommentBegin("codeBreak");
#<		map($self->addAll($_->codeBreakBefore), grep($_->items->size, $self->PARAM->sections->toArray));
		map($_->codeBreakBefore($self), grep($_->items->size, $self->PARAM->sections->toArray));
		return unless ($self->PARAM->sections->exists('group by')->items->size);
		foreach (grep($_->inputField->calc, $self->PARAM->sections->exists('group by')->items->toArray))
		{
			$self->addNonl("@{[ $_->inputField->codeVar ]}");
			$self->addNonl("@{[ $_->inputField->operator eq '=~' ? ' =~ ' : ' = ' ]}");
#<			$self->add("@{[ $self->PARAM->parser->compile($_->inputField->calc) ]};");
			$self->add("@{[ $_->inputField->calc ]};");
		}

		if ($self->PARAM->properties('hash'))
		{
			$self->addNonl("\$key = ");
			$self->addNonl
			(
				join
				(
					" . '|' . ", 
					map
					(
						"( @{[ $_->inputField->codeVar ]}@{[ $self->PARAM->properties('hash') eq 'numeric' ? '+0' : '' ]} )",
						$self->PARAM->sections->exists('group by')->items->toArray
					)
				)
			);
			$self->add(";");
			map($_->codeBreakAfter($self), grep($_->items->size, $self->PARAM->sections->toArray));
			$self->addCommentEnd("codeBreak");
			return;
		}

		# These are not needed, can refer to $_->inputField->codeVar directly...
		map($self->add("\$key_@{[ $_->inputField->id ]} = @{[ $_->inputField->codeVar ]};"),
			$self->PARAM->sections->exists('group by')->items->toArray);

		$self->addNonl("if (");
		$self->addNonl
		(
			join
			(
				" || ", 
				map
				(
					"!defined(\$previous_key_@{[ $_->inputField->id ]})", 
					$self->PARAM->sections->exists('group by')->items->toArray
				)
			)
		);
		$self->add(")");
		$self->openBlock('{');
			map($self->add("\$previous_key_@{[ $_->inputField->id ]} = \$key_@{[ $_->inputField->id ]};"),
				$self->PARAM->sections->exists('group by')->items->toArray);
		$self->closeBlock;

		$self->addNonl("elsif (");
		$self->addNonl
		(
			join
			(
				" || ", 
				map
				(
					"\$previous_key_@{[ $_->inputField->id ]} @{[ $_->inputField->type =~ /numeric|decimal/ ? '!=' : 'ne' ]} \$key_@{[ $_->inputField->id ]}", 
					$self->PARAM->sections->exists('group by')->items->toArray
				)
			)
		);
		$self->add(")");
		$self->openBlock("{");
			$self->codeZeroNulls if ($self->PARAM->properties('nonulls'));
			$self->codePrintBefore();
			$self->codePrint();
			$self->codePrintAfter();

			map($self->add("\$previous_key_@{[ $_->inputField->id ]} = \$key_@{[ $_->inputField->id ]};"),
				$self->PARAM->sections->exists('group by')->items->toArray);

			$self->add("\@O_VAL = undef;");
			map
			(
				$self->addAll($_->codeReset),
				grep($_->useList->size, $self->PARAM->aggregates->toArray)
			);

			map($self->add("\$previous_dedup_@{[ $_->inputField->id ]} = undef;"),
				$self->PARAM->sections->exists('dedup on')->items->toArray);

		$self->closeBlock;
		map($_->codeBreakAfter($self), grep($_->items->size, $self->PARAM->sections->toArray));
		$self->addCommentEnd("codeBreak");
	}

	sub codePrintBefore : method
	{
		my $self = shift;

		$self->addCommentBegin("codePrintBefore");
		# Alternatevly list all input fields (except '_' fields) and / or calulations.
		if ($self->PARAM->properties('transfer'))
		{
			foreach (grep($_->calc, $self->PARAM->sections->exists('input section')->items->toArray))
			{
				$self->addNonl("@{[ $_->codeVar ]}");
				$self->addNonl("@{[ $_->operator eq '=~' ? ' =~ ' : ' = ' ]}");
#<				$self->add("@{[ $self->PARAM->parser->compile($_->calc) ]};");
				$self->add("@{[ $_->calc ]};");
			}
		}
		$self->codeOutputFinal;
		map($_->codePrintBefore($self), grep($_->items->size, $self->PARAM->sections->toArray));
#>		map($_->printBefore($self), grep($_->items->size, $self->PARAM->sections->toArray));
		$self->addCommentEnd("codePrintBefore");
	}

	sub codePrint : method
	{
		my $self = shift;
		my $ofl = shift || 'STDOUT';

		$self->addCommentBegin("codePrint");
		$self->add("flock($ofl, LOCK_EX);") if ($self->PARAM->properties('lock_output'));
		$self->add("print $ofl");
		$self->over;

#> Maybe exclude input fields with /^_/
		if ($self->PARAM->properties('pack_output'))
		{
			$self->add("pack(OUTPUT_PACK_FMT, ");
			$self->over();
		}
		$self->add("\@I_VAL[0..LAST_ICELL],") if ($self->PARAM->properties('transfer'));

		foreach (grep($_->name !~ /^_/, $self->PARAM->sections->exists('output section')->items->toArray))
		{
			$self->add("@{[ $_->codeVar ]},");
		}
		$self->add('""') if ($self->PARAM->properties('addpipe'));
		$self->endList;
		if ($self->PARAM->properties('pack_output'))
		{
			$self->back();
			$self->add(")"); # close pack
		}
		$self->back;

#> This should not be here but in codeBreak()
		if ($self->PARAM->sections->exists('having')->items->size)
		{
			$self->add("if");
			$self->add("(");
			$self->over;
			$self->add(join(" && ", map($self->PARAM->parser->compileOutput($_->value), $self->PARAM->sections->exists('having')->items->toArray)));
			$self->back;
			$self->addNonl(")");
		}
		$self->add(";");
		$self->add("flock($ofl, LOCK_UN);") if ($self->PARAM->properties('lock_output'));
		$self->addCommentEnd("codePrint");
	}

	sub codePrintAfter : method
	{
		my $self = shift;
		map($_->codePrintAfter($self), grep($_->items->size, $self->PARAM->sections->toArray));
#>		map($_->printAfter($self), grep($_->items->size, $self->PARAM->sections->toArray));
	}

	sub codeMainBody : method
	{
		my $self = shift;
		$self->addCommentBegin("codeMainBody");
		map($self->addAll($_->codeMainBefore), grep($_->items->size, $self->PARAM->sections->toArray));

		my @C3;
		my %C1;
		my %C2;
		foreach ($self->PARAM->sections->exists('output section')->items->toArray)
		{
#! this will disrupt output setting sequence...
#<			if ($self->root->o_optimize && defined($_->condition) && $_->condition =~ /\s+&&\s+/)
#<			{
#<				my ($c1, @therest) = split(/&&/, $_->condition);
#<				my $c2 = join('&&', @therest);
#<				$c1 =~ s/^\s+//;
#<				$c1 =~ s/\s+$//;
#<				$c2 =~ s/^\s+//;
#<				$c2 =~ s/\s+$//;
#<				push(@{$C1{$c1}{FLDS}}, $_);
#<				push(@{$C1{$c1}{$c2}{FLDS}}, $_);
#<			}
#<			elsif ($_->calculated)
			if ($_->calculated)
			{
				push(@C3, $_);
			}
			else
			{
				push(@{$C2{"@{[ defined($_->condition) ? $_->condition : '' ]}"}}, $_);
			}
		}	

#?		$self->{LEVEL} = 0;
		my $prev_f1 = undef;
		foreach my $c (sort keys %C2)
		{
			if ($self->PARAM->properties('optimize'))
			{
				if ($c eq '')
				{
					$prev_f1 = undef;
				}
				else
				{
					my $ff = ${$C2{$c}}[0];	#@{$self->{C2}->{$c}}[0];
					my $f1 = join('|', sort map($_->name, $ff->CFlds->toArray)) 
						if ($ff->CFlds->size);
					if (defined($prev_f1) && $prev_f1 eq $f1)
					{
						$self->add("elsif (@{[ $self->PARAM->parser->compileOutput($c, 1) ]}) {");
					}
					else
					{
						$self->add;
						$self->add("if (@{[ $self->PARAM->parser->compileOutput($c, 1) ]}) {");
					}
					$self->over;
					$prev_f1 = $f1;
#?					$self->{LEVEL}++;
				}
			} 
			elsif ($c ne '')
			{
				$self->add;
				$self->add("if (@{[ $self->PARAM->parser->compileOutput($c, 1) ]}) {");
				$self->over;
#?				$self->{LEVEL}++;
			}

			foreach (@{$C2{$c}}) { $self->codeOutput($_); }
			if ($c ne '')
			{
				$self->back;
				$self->add("}");
#?				%{$self->{OUT_CALC_LIST}->{$self->{LEVEL}}} = undef;
#?				$self->{LEVEL}--;
			}
		}

		if ($self->PARAM->properties('optimize'))
		{
			my $prev_f1 = undef;
			foreach my $c (sort keys %C1)
			{
				my $ff = @{$C1{$c}{FLDS}}[0];
				my $f1 = join('|', sort map($_->name, $ff->CFlds->toArray)) 
					if ($ff->CFlds->size);
				if (defined($prev_f1) && $prev_f1 eq $f1)
				{
					$self->add("elsif (@{[ $self->PARAM->parser->compileOutput($c, 1) ]}) {");
				}
				else
				{
					$self->add;
					$self->add("if (@{[ $self->PARAM->parser->compileOutput($c, 1) ]}) {");
				}
				$self->over;
#?				$self->{LEVEL}++;
				$prev_f1 = $f1;

				my $prev_f2 = undef;
				foreach my $c2 (sort keys %{$C1{$c}})
				{
					next if ($c2 eq 'FLDS');

					my $ff = @{$C1{$c}{$c2}{FLDS}}[0];
					my $f2 = join('|', sort map($_->name, $ff->CFlds->toArray)) 
						if ($ff->CFlds->size);
					if (defined($prev_f2) && $prev_f2 eq $f2)
					{
						$self->add("elsif (@{[ $self->PARAM->parser->compileOutput($c2, 1) ]}) {");
					}
					else
					{
						$self->add;
						$self->add("if (@{[ $self->PARAM->parser->compileOutput($c2, 1) ]}) {");
					}	
					$self->over;
#?					$self->{LEVEL}++;
					$prev_f2 = $f2;
					foreach (@{$C1{$c}{$c2}{FLDS}}) { $self->codeOutput($_); }

					$self->back;
					$self->add("}");
#?					%{$self->{OUT_CALC_LIST}->{$self->{LEVEL}}} = undef;
#?					$self->{LEVEL}--;
				}
				$self->back;
				$self->add("}");
#?				%{$self->{OUT_CALC_LIST}->{$self->{LEVEL}}} = undef;
#?				$self->{LEVEL}--;
			}
		}	
		foreach (@C3) { $self->codeOutput($_); }    	
		map($self->addAll($_->codeMainAfter), grep($_->items->size, $self->PARAM->sections->toArray));
		$self->addCommentEnd("codeMainBody");
	}        	
             	
	sub codeOutput : method
	{        	
		my $self = shift;
		my $ofld = shift;

		if ($ofld->aggregate)
		{
			$self->addAll($ofld->aggregate->codeOutput($ofld));
		}
		else
		{
			if ($ofld->inputField->calc && !$self->PARAM->sections->exists('group by')->items->exists($ofld->inputField->name))
			{
				$self->addNonl("@{[ $ofld->inputField->codeVar ]}");
				$self->addNonl("@{[ $ofld->inputField->operator eq '=~' ? ' =~ ' : ' = ' ]}");
#<				$self->add("@{[ $self->root->parser->compile($ofld->inputField->calc) ]};");
				$self->add("@{[ $ofld->inputField->calc ]};");
			}
			$self->addNonl("@{[ $ofld->codeVar ]}");
			$self->addNonl("@{[ $ofld->inputField->operator eq '=~' ? ' =~ ' : ' = ' ]}");
			$self->add("@{[ $self->PARAM->parser->compile($ofld->inputField->name) ]};");
		}
	}

	sub codeOutputFinal : method
	{
		my $self = shift;

		$self->addCommentBegin("codeOutputFinal");
		map
		(
			$self->addAll($_->aggregate->codeOutputFinal($_)),
			grep($_->aggregate && $_->aggregate->level == 2, $self->PARAM->sections->exists('output section')->items->toArray)
		);
		$self->addCommentEnd("codeOutputFinal");
	}

	sub codeHeaderInfo : method
	{
		my $self = shift;

		$self->addComment("!$^X");
		$self->addBar;
		$self->addComment(" vim: syntax=perl ts=4 sw=4");
		$self->addBar;
		$self->addComment("Generated By: @{[ $self->PARAM->VERSION ]}");
		$self->addComment("            : http://sourceforge.net/projects/pequel/");
		$self->addComment("Script Name : @{[ $self->PARAM->getscriptname() ]}");
		$self->addComment("Created On  : @{[ '' . localtime() ]}");
		$self->addComment("Perl Version: @{[ $^X . ' ' . sprintf(qq{%vd}, $^V) . ' on ' . $^O ]}");
		$self->addComment("For         : @{[ getlogin() || '' ]}");
		$self->addBar;
		$self->addComment("Options:");
		$self->over;
		foreach ($self->PARAM->sections->exists('options')->items->toArray)
		{
			$self->addComment("@{[ $_->name ]}(@{[ $_->value ]}) @{[ $_->ref->description ]}");
		}

		$self->back;
		$self->addBar;
	}

	sub codeUseStd : method
	{
		my $self = shift;

		$self->add("use strict;");
		$self->add("use Fcntl ':flock';") if ($self->PARAM->properties('lock_output'));
		foreach ($self->PARAM->tables->toArray)
		{
			$self->add("use Fcntl;") if ($_->persistent);
			last if ($_->persistent);
		}
	}

	sub codeInputRecordLimitBreak : method
	{
		my $self = shift;
		return unless ($self->PARAM->properties('input_record_limit'));
		$self->add("break if ( @{[ $self->lineCounterVar ]} == @{[ $self->PARAM->properties('input_record_limit')+1 ]} );");
	}

	sub lineCounterVar : method
	{
		my $self = shift;
#<		return $self->root->t_db->tableList->size ? '$i' : '$.';
		return $self->PARAM->properties('use_inline') ? '$i' : '$.';
	}
	
	sub codeIncRecordCounter : method
	{
		my $self = shift;
#<		return unless ($self->root->t_db->tableList->size);
		return unless ($self->PARAM->properties('use_inline'));
		$self->add("++@{[ $self->lineCounterVar ]};");
	}
	
	sub codeRecordCounterMessage : method
	{
		my $self = shift;
#? don't do this here but in verboseMessage()
		return if ($self->PARAM->properties('noverbose') || !$self->PARAM->properties('verbose'));
#<		my $inc = $self->root->t_db->tableList->size ? '++' : '';
		$self->verboseMessage
		(
			"@{[ $self->lineCounterVar ]} records.", 
			"@{[ $self->lineCounterVar ]} % VERBOSE == 0"
		);
	}

	sub codeRecordCounterMessageFinal : method
	{
		my $self = shift;
		return if ($self->PARAM->properties('noverbose') || !$self->PARAM->properties('verbose'));
		$self->verboseMessage("@{[ $self->lineCounterVar ]} records.");
	}

	sub codeBenchmarkInit : method
	{
		my $self = shift;
		return if ($self->PARAM->properties('noverbose') || !$self->PARAM->properties('verbose'));
		$self->add("use Benchmark;");
	}
	
	
	sub codeBenchmarkStart : method
	{
		my $self = shift;
		return if ($self->PARAM->properties('noverbose') || !$self->PARAM->properties('verbose'));
		$self->add("my \$benchmark_start = new Benchmark;");
	}
	
	sub codeBenchmarkEnd : method
	{
		my $self = shift;
		return if ($self->PARAM->properties('noverbose') || !$self->PARAM->properties('verbose'));
		$self->add("my \$benchmark_end = new Benchmark;");
		$self->add("my \$benchmark_timediff = timediff(\$benchmark_start, \$benchmark_end);");
		$self->verboseMessage("Code statistics: \@{[timestr(\$benchmark_timediff)]}");
	}

	sub codeSubPrintHeader : method
	{
		my $self = shift;
		return unless ($self->PARAM->properties('header')); #< || $self->PARAM->properties('print_header'));

		$self->add('sub PrintHeader');
		$self->openBlock("{");

			$self->add('local $\="\n";');
			$self->add("local \$,=\"@{[ $self->PARAM->properties('output_delimiter') ]}\";");
			$self->add("print @{[ $self->PARAM->properties('output_file') ? 'OUTPUT_FILE' : '']}");

			$self->over;
				map($self->add("'@{[ $_->name ]}',"), grep($_->name !~ /^_/, $self->PARAM->sections->exists('input section')->items->toArray))
					if ($self->PARAM->properties('transfer'));

				map($self->add("'@{[ $_->name ]}',"), grep($_->name !~ /^_/, $self->PARAM->sections->exists('output section')->items->toArray)) 
					if ($self->PARAM->sections->exists('output section')->items->size);

				$self->add('""') if ($self->PARAM->properties('addpipe'));
				$self->endList;
				$self->add(";");
			$self->back;

		$self->closeBlock;
	}

	sub codeNumericFieldsInit : method
	{
		my $self = shift;
		
		if (grep($_->type =~ /numeric|decimal/, $self->PARAM->sections->exists('output section')->items->toArray))
		{
			$self->addNonl("my \@numeric_fields = (");
			$self->addNonl("@{[ join(',', map($_->number, grep($_->type =~ /numeric|decimal/, $self->PARAM->sections->exists('output section')->items->toArray))) ]}");
			$self->add(");");	
		}
	}

	sub codeZeroNulls : method	#--> codeNumericFieldsZeroNulls
	{
		my $self = shift;

		if ($self->PARAM->sections->exists('group by')->items->size == 0)
		{
			foreach ($self->PARAM->sections->exists('output section')->items->toArray)
			{ 
				next unless ($_->type =~ /numeric|decimal/);
				next unless (!$_->aggregate && $_->inputField);	# same as __INPUT__
				$self->add("@{[ $_->inputField->codeVar ]} = 0 if (@{[ $_->inputField->codeVar ]} == 0);");
			}
		}
		if (grep($_->type =~ /numeric|decimal/, $self->PARAM->sections->exists('output section')->items->toArray))
		{ 
			next unless ($_->type =~ /numeric|decimal/);
			$self->add("foreach (\@numeric_fields)");
			$self->openBlock("{");
			$self->add("\$O_VAL@{[ $self->PARAM->properties('hash') ? '{$key}{$f}' : '[$f]' ]} = 0");
			$self->over;
			$self->add("if (\$O_VAL@{[ $self->PARAM->properties('hash') ? '{$key}{$f}' : '[$f]' ]} == 0);");
			$self->back;
			$self->closeBlock;
		}
	}


	sub maxHeaderLen : method
	{
		my $self = shift;
		my $maxheader=0;
		foreach ($self->PARAM->sections->exists('input section')->items->toArray, $self->PARAM->sections->exists('output section')->items->toArray)
		{
			$maxheader = length($_->name) if ($maxheader < length($_->name));
		}
		return $maxheader +5;
	}

	sub codeInputVarNames : method
	{
		my $self = shift;

		foreach ($self->PARAM->sections->exists('input section')->items->toArray)
		{
			$self->addNonl(sprintf("use constant %-@{[ $self->maxHeaderLen+1 ]}s => int %4d;", 
				$_->id, $_->number-1));
			$self->add;
		}
	}

	sub codeOutputVarNames : method
	{
		my $self = shift;

		foreach ($self->PARAM->sections->exists('output section')->items->toArray)
		{
			$self->addNonl(sprintf("use constant %-@{[ $self->maxHeaderLen+1 ]}s => int %4d;", 
				$_->id, $_->number));
			$self->add;
		}
	}

	sub codeTableFieldNames : method
	{
		my $self = shift;

		foreach my $t ($self->PARAM->tables->toArray)
		{
			foreach my $f ($t->fields->toArray)
			{
				$self->add(sprintf("use constant %-@{[ $self->maxTableHeaderLen ]}s => int %4d;", 
					"_T_@{[ $t->name ]}_FLD_@{[ $f->name ]}",
					$f->number-1
				));
			}
		}
	}

	sub codeInlineTableFieldNames : method	# These are for inline '_I_'
	{
		my $self = shift;

		my $next_fldnum = $self->PARAM->sections->exists('input section')->items->size;
		foreach my $t (sort { $a->sequence <=> $b->sequence } $self->PARAM->tables->toArray)	# <<< sequence is IMPORTANT here
		{
			foreach my $k ($t->refKeyList->toArray)
			{
				$self->add(sprintf("use constant %-@{[ $self->maxTableHeaderLen(1) ]}s => int %4d;", 
					"_I_@{[ $t->name ]}_@{[ $k->name ]}_FLD_KEY", $next_fldnum++));

				foreach my $f ($t->fields->toArray)
				{
					$self->add(sprintf("use constant %-@{[ $self->maxTableHeaderLen(1) ]}s => int %4d;", 
						"_I_@{[ $t->name ]}_@{[ $k->name ]}_FLD_@{[ $f->name ]}",
						$next_fldnum++));
				}
			}
		}
	}

	sub maxTableHeaderLen : method
	{
		my $self = shift;
		my $with_refkey = shift || 0;

		# need to increase:
		my $maxheader=0;
		foreach my $t ($self->PARAM->tables->toArray)
		{
			if ($with_refkey)
			{
				foreach my $k ($t->refKeyList->toArray)
				{
					foreach my $f ($t->fields->toArray)
					{
						$maxheader = length($t->name) + length($k->name) + length($f->name) 
							if ($maxheader < length($t->name) + length($k->name) +  length($f->name));
					}
				}
			}
			else
			{
				foreach my $f ($t->fields->toArray)
				{
					$maxheader = length($t->name) + length($f->name) 
						if ($maxheader < length($t->name) +  length($f->name));
				}
			}
		}
		return $maxheader+10;
	}
}
1;
# ----------------------------------------------------------------------------------------------------
