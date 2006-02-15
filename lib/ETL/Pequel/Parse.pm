#!/usr/bin/perl
# ----------------------------------------------------------------------------------------------------
#  Name		: ETL::Pequel::Parser.pm
#  Created	: 30 January 2005
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
# 22/09/2005	2.3-2		gaffie	Removed Pequel::Base usage and all refs to Pequel::root.
# 12/09/2005	2.2-9		gaffie	Revamped the table compiler
# 09/09/2005	2.2-9		gaffie	Revamped the macro compiler -- now handles nested macros and complex statements.
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
	package ETL::Pequel::Parse;

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
		my %params = @_;
		my $self = {};
		bless($self, $class);
		$self->PARAM($params{'PARAM'});
		return $self;
	}

	sub compile : method
	{
		my $self = shift;
		my $clause = shift;
		my $section_name = shift || undef;
		my $field_name = shift || undef;

		return $clause if (!defined($clause));

		$clause = $self->compileArray($clause);
		$clause = $self->compileMacro($clause, $section_name, $field_name);
		$clause = $self->compileToArray($clause);
		$clause = $self->compileTable($clause, $section_name, $field_name);
		$clause = $self->compileInputFields($clause, $section_name, $field_name);
		return $clause;
	}

	sub compileInputFields : method
	{
		my $self = shift;
		my $clause = shift;
		my $section_name = shift || undef;
		my $field_name = shift || undef;

		return $clause unless (defined($clause));

		$clause = $self->saveQuotes($clause);
		foreach ($self->PARAM->sections->exists('input section')->items->toArray)
		{
			if ($clause =~ s/\b@{[ $_->name ]}\b/@{[ $_->codeVar ]}/g)
			{
				$_->useList->add(ETL::Pequel::Type::Element->new
				(
					sourceSectionName => $section_name,
					sourceFieldName => $field_name,
					PARAM => $self->PARAM,
				));
			}
		}
		$clause = $self->restoreQuotes($clause);
		return $clause;
	}

	sub compileOutput : method
	{
		my $self = shift;
		my $clause = shift;
		my $input_pref = shift;
		return $clause if (!defined($clause));

#?		Need this here, not sure why though...
		$clause = $self->compileToArray($clause);
		$clause = $self->compileTable($clause);

		$clause = $self->saveQuotes($clause);

		foreach ($self->PARAM->sections->exists('output section')->items->toArray)
		{
			# Input field name has preference over Output field name.
			next if ($input_pref && $self->PARAM->sections->exists('input section')->items->exists($_->name));
			$clause =~ s/\b@{[ $_->name ]}\b/@{[ $_->codeVar ]}/g;
		}
		$clause = $self->restoreQuotes($clause);
		return $self->compile($clause);
	}

	sub compileMacro : method
	{
		my $self = shift;
		my $clause = shift;
		my $section_name = shift || undef;
		my $field_name = shift || undef;
		return $clause if (!defined($clause) || !length($clause));

		my ($macro_name, @args);
		my $i=0; my $MAXDEPTH=100;
		my $m;
		while (($macro_name, @args) = $self->extractMacro(1, \$clause))
		{
			ETL::Pequel::Error::fatalError("[6001] Macro '$macro_name' does not exist")
				if (($m = $self->PARAM->macros->exists($macro_name)) == 0);
			if ($self->PARAM->properties('debug_parser')) 
			{
				print STDERR "MACRO:@{[ $m->name ]}=@{[ $m->useList->size ]}\n";
				foreach my $arg (0..$#args) { print STDERR "ARG[$arg]:", $args[$arg], "\n"; }
			}
			$clause =~ s/__MACRO__${macro_name}.*__MACRO_END__/@{[ $m->compile(@args) ]}/;
			$m->useList->add(ETL::Pequel::Type::Element->new
			(
				sourceSectionName => $section_name,
				sourceFieldName => $field_name,
				value => join(',', @args),
				PARAM => $self->PARAM,
			));
			last if (++$i>$MAXDEPTH);
		}
		return $clause;
	}

	sub extractMacro : method
	{
		my $self = shift;
		my $level = shift;
		my $clause = shift;

		return () if ($$clause !~ /(?<!&)&\w*\(/);
		$$clause =~ s/(?<!&)&(\w*)\s*\(/__MACRO__$1(/;
		my $macro_name = $1;
		my $inside_quotes=0;
		my $inside_quotes_char;
		my $inside_nested_brackets=0;
		my $arg_start_pos = index($$clause, "(", index($$clause, "__MACRO__${macro_name}("))+1;
		my @args;

		my $pos;
		for ($pos=$arg_start_pos; ; $pos++)
		{
			last if ($pos > length($$clause));
			my $p = substr($$clause, $pos, 1);
#>			$text =~ /('|")([^\1]*)\1/; print "Found quote:$1$2$1\n"; # find matching quotes
			if ($p eq '"' || $p eq "'")
			{
				if (!$inside_quotes || ($inside_quotes && $p eq $inside_quotes_char)) 
				{
					$inside_quotes = $inside_quotes ? 0 : $pos;
					$inside_quotes_char = $inside_quotes ? $p : '';
				}
			}
			elsif ($inside_quotes) { next; }
			elsif (!$inside_nested_brackets && ($p eq ')' || $p eq ']' || $p eq ','))
			{
				push(@args, $self->trim(substr($$clause, $arg_start_pos, $pos-($arg_start_pos))));
				$arg_start_pos = $pos+1 if ($p eq ',');
				if ($p eq ')' || $p eq ']') # End of ARG list
				{
					substr($$clause, $pos, 1) = ')__MACRO_END__';
					return ($macro_name, @args);
				}
			}
			elsif (($p eq '(' || $p eq '[')) { $inside_nested_brackets++; }
			elsif (($p eq ')' || $p eq ']')) { $inside_nested_brackets--; }
		}

		print STDERR "$$clause\n";
		if ($inside_quotes)
		{
			print STDERR "@{[ ' ' x $inside_quotes ]}^";
			print STDERR "@{[ '-' x ($pos-1-$inside_quotes-1) ]}^\n";
		}
		else
		{
			print STDERR "@{[ ' ' x $arg_start_pos ]}^";
			print STDERR "@{[ '-' x ($pos-1-$arg_start_pos-1) ]}^\n";
		}
		ETL::Pequel::Error::fatalError(
		qq/[60011] Invalid Clause -- unexpected end of clause encountered -- missing close '@{[ $inside_quotes ? '"' : ')' ]}'/);
		return ();
	}
	sub trim { my $self = shift; my $st = shift; $st =~ s/^\s*//; $st =~ s/\s$$//; return $st; }

	sub extractNextMacro : method # still used by Docgen.pm
	{
		my $self = shift;
		my $offset = shift;
		my $clause = shift;
		my $section_name = shift || undef;
		my $field_name = shift || undef;

		$clause = $self->saveQuotedPc($clause);
		$clause = $self->saveAnd($clause);
		$clause = $self->saveAmp($clause);
		$clause = $self->saveQuotedCommas($clause);
		my $idx_from;
		my $idx_to;
		if 
		(
			($idx_from = index($clause, "&", $offset)) != -1 
			&& ($idx_to = index($clause, "(", $idx_from)) != -1
		)
		{
			my $macro_name = substr($clause, $idx_from+1, $idx_to - $idx_from);
			$macro_name =~ s/(.*?)\s*\(/$1/;
			my $m;
			ETL::Pequel::Error::fatalError("[6001] Macro '$macro_name' does not exist")
				if (($m = $self->PARAM->macros->exists($macro_name)) == 0);

#>			Not quite: should store ref to input/output field...
			print STDERR "MACRO:@{[ $m->name ]}=@{[ $m->useList->size ]}\n" 
				if ($self->PARAM->properties('debug_parser'));

			substr($clause, $idx_from+1, $idx_to - $idx_from) = "__MACRO__$macro_name(";

			my $open=0;
			for (my $i = $idx_to; $i < length($clause); $i++)
			{
				if (substr($clause, $i, 1) eq '(')
				{
					substr($clause, $i, 1) = '__OPEN__' if (!$open);
					$open++;
				}
				if (substr($clause, $i, 1) eq ')')
				{
					$open--;
					substr($clause, $i, 1) = '__CLOSE__' if (!$open);
					last if (!$open);
				}
				if (substr($clause, $i, 1) eq ',' && $open == 1)
				{
					substr($clause, $i, 1) = '__ARG__';
				}
			}

			$clause = $self->restoreQuotedPc($clause);
			$clause = $self->restoreAnd($clause);
			$clause = $self->restoreAmp($clause);
			$clause = $self->restoreQuotedCommas($clause);
			my $args = $clause;
			$args =~ /__OPEN__(.*)__CLOSE__/;
			$args = $1;
			my @args = split("__ARG__", $args);
			$clause =~ s/&__MACRO__${macro_name}.*__CLOSE__/@{[ $m->compile(@args) ]}/;
			$m->useList->add(ETL::Pequel::Type::Element->new
			(
				sourceSectionName => $section_name,
				sourceFieldName => $field_name,
				value => join(',', @args)
			));

			return ($m, $idx_from+1, $clause, @args);
		}
		$clause = $self->restoreQuotedPc($clause);
		$clause = $self->restoreAnd($clause);
		$clause = $self->restoreAmp($clause);
		$clause = $self->restoreQuotedCommas($clause);
		return (0, length($clause), $clause);
	}

	sub compileTable
	{
		my $self = shift;
		my $clause = shift;
		my $section_name = shift || undef;
		my $field_name = shift || undef;
		return $clause if (!defined($clause) || !length($clause));

		my ($table_name, $key, $column);
		my $i=0; my $MAXDEPTH=100;
		my $t;
		while (($table_name, $key, $column) = $self->extractTable(1, \$clause))
		{
			ETL::Pequel::Error::fatalError("[6002] Table '$table_name' does not exist")
				if (($t = $self->PARAM->tables->exists($table_name)) == 0);
				ETL::Pequel::Error::fatalError("[6003] Field '$column' does not exist in Table '$table_name'")
				unless ($column eq '' || ($column =~ /\d+/ && $column <= $t->fields->size) || $t->fields->exists($column));
			if ($self->PARAM->properties('debug_parser')) {
				print STDERR "TABLE:@{[ $t->name ]}; KEY=$key; COLUMN=$column; (@{[ $t->useList->size ]})\n";
			}
			$column ne ''
				? $clause =~ s/__TABLE__${table_name}\(.*__TABLE_END__->${column}/@{[ $t->fields->size <= 1 ? $t->codeVar($key) : $t->codeVar($key,$column) ]}/
				: $clause =~ s/__TABLE__${table_name}\(.*__TABLE_END__/@{[ $t->codeVar($key) ]}/;
#>			Not quite: should store ref to input/output field...
			$t->useList->add(ETL::Pequel::Type::Element->new
			(
				sourceSectionName => $section_name,
				sourceFieldName => $field_name,
				value => $column ? "$key,$column" : $key,
				PARAM => $self->PARAM,
			));
			last if (++$i>$MAXDEPTH);
		}
		return $clause;
	}

	sub extractTable
	{
		my $self = shift;
		my $level = shift;
		my $clause = shift;

		return () if ($$clause !~ /%\w*\s*\(/);

		$$clause =~ s/%(\w*)\s*\(/__TABLE__$1(/;
		my $table_name = $1;
		my $inside_quotes=0;
		my $inside_quotes_char;
		my $inside_nested_brackets=0;
		my $arg_start_pos = index($$clause, "(", index($$clause, "__TABLE__${table_name}("))+1;

		my $pos;
		for ($pos=$arg_start_pos; ; $pos++)
		{
			last if ($pos > length($$clause));
			my $p = substr($$clause, $pos, 1);
			if ($p eq '"' || $p eq "'")
			{
				if (!$inside_quotes || ($inside_quotes && $p eq $inside_quotes_char)) 
				{
					$inside_quotes = $inside_quotes ? 0 : $pos;
					$inside_quotes_char = $inside_quotes ? $p : '';
				}
			}
			elsif ($inside_quotes) { next; }
			elsif (!$inside_nested_brackets && $p eq ')')
			{
				my $key = $self->trim(substr($$clause, $arg_start_pos, $pos-($arg_start_pos)));
				# error if ($key eq '');
				if ($p eq ')') # End of ARG list
				{
					substr($$clause, $pos, 1) = ')__TABLE_END__';
					my $column='';
					if ($$clause =~ /__TABLE__${table_name}\(.*__TABLE_END__->([\w|_]+)\s+/) { $column = $1; }
					elsif ($$clause =~ /__TABLE__${table_name}\(.*__TABLE_END__->([\w|_]+)\b/) { $column = $1; }
					elsif ($$clause =~ /__TABLE__${table_name}\(.*__TABLE_END__->([\w|_]+)$/) { $column = $1; }
					return ($table_name, $key, $column);
				}
			}
			elsif (($p eq '(')) { $inside_nested_brackets++; }
			elsif (($p eq ')')) { $inside_nested_brackets--; }
		}

		print STDERR "$$clause\n";
		if ($inside_quotes)
		{
			print STDERR "@{[ ' ' x $inside_quotes ]}^";
			print STDERR "@{[ '-' x ($pos-1-$inside_quotes-1) ]}^\n";
		}
		else
		{
			print STDERR "@{[ ' ' x $arg_start_pos ]}^";
			print STDERR "@{[ '-' x ($pos-1-$arg_start_pos-1) ]}^\n";
		}
		ETL::Pequel::Error::fatalError(
		qq/[60012] Invalid Clause -- unexpected end of clause encountered -- missing close '@{[ $inside_quotes ? '"' : ')' ]}'/);
		return ();
	}

#> Prevent / handle &to_array(@arr) -- ie @ inside &to_array
	sub compileArray : method
	{
		my $self = shift;
		my $clause = shift;
		return $clause if (!defined($clause) || !length($clause));
#?		$clause =~ s/&to_array\s*\(\s*@/&to_array(/g;
		$clause =~ s
			/
				@(\w+)\s*\(?\)?->([\w|_]+)
			/
				"&arr_$2(\@$1)"
			/gxe;
		$clause =~ s
			/
				@(\w+)\s*\(([\@|\\|\w|$|_|,|.]+)?\)?->([\w|_]+)
			/
				"&arr_$3((\@$1)[$2])"
			/gxe;
		$clause =~ s
			/
				@(\w+)\s*\(?\)?->([\w|_]+)
			/
				"&arr_$2(\@$1)"
			/gxe;
		$clause =~ s
			/
				@(\w+)\s*\(([\w|$|_|.]+)\)
			/
				"((\@$1)[$2])"
			/gxe;
		return $clause;
	}

	sub compileToArray : method
	{
		my $self = shift;
		my $clause = shift;
		return $clause if (!defined($clause) || !length($clause));
		$clause = $self->saveAt($clause);
		$clause =~ s/@(\w+)/split(\/\\s*@{[ $self->PARAM->properties('default_list_delimiter') ]}\\s*\/,$1,-1)/g;
		$clause = $self->restoreAt($clause);
		return $clause;
	}

	sub splitEntries : method
	{
		my $self = shift;
		my $line = shift;
		$line = $self->saveCommas($line);
		my @entries = split('\s*,\s*', $line, -1);
		return map($self->restoreCommas($_), @entries);
	}

	sub saveAnd : method #Del
	{
		my $self = shift;
		my $line = shift;
		$line =~ s/&&/_AND_/g;
		return $line;
	}

	sub restoreAnd : method #Del
	{
		my $self = shift;
		my $line = shift;
		$line =~ s/_AND_/&&/g;
		return $line;
	}

	sub saveAmp : method #Del
	{
		my $self = shift;
		my $line = shift;
		$line =~ s/&\s*\$/__AMP1__/g;
		$line =~ s/&\s*{/__AMP2__/g;
		return $line;
	}

	sub restoreAmp : method #Del
	{
		my $self = shift;
		my $line = shift;
		$line =~ s/__AMP1__/&\$/g;
		$line =~ s/__AMP2__/&{/g;
		return $line;
	}

	sub saveQuotes : method 
	{
		my $self = shift;
		my $line = shift;
		$line =~ s/'/__Q__/g;
		$line =~ s/"/__QQ__/g;
		return $line;
	}

	sub restoreQuotes : method 
	{
		my $self = shift;
		my $line = shift;
		$line =~ s/__Q__/'/g;
		$line =~ s/__QQ__/"/g;
		return $line;
	}

	sub saveCommas : method
	{
		my $self = shift;
		my $line = shift;
		$line =~ s/,$//;
		$line =~ s/\s*,\s*/,/g;
		my $open_b = 0;
		my $open_q = 0;
		my $open_sq = 0;
		my $i;
		for ($i = 0; $i < length($line); $i++)
		{
			my $c = substr($line, $i, 1);
			$open_b++ if ($c eq '(');
			$open_b-- if ($c eq ')');
			if ($c eq '"') { $open_q = ($open_q) ? 0 : 1 };
			if ($c eq "'") { $open_sq = ($open_sq) ? 0 : 1 };

			if ($c eq ',' && $open_b) { substr($line, $i, 1) = '__C__' ; }
			elsif ($c eq ',' && $open_q) { substr($line, $i, 1) = '__CQ__'; }
			elsif ($c eq ',' && $open_sq) { substr($line, $i, 1) = '__CSQ__'; }
		}
		return $line;
	}

	sub restoreCommas : method
	{
		my $self = shift;
		my $line = shift;
		$line =~ s/__C__/,/g;
		$line =~ s/__CQ__/,/g;
		$line =~ s/__CSQ__/,/g;
		return $line;
	}

	sub saveQuotedCommas : method#Del
	{
		my $self = shift;
		my $line = shift;

		my @qc;
		my $single = 'closed';
		my $double = 'closed';
		foreach my $i (0..length($line))
		{
			if (substr($line, $i, 1) eq "'") 
			{ 
				$single = ($single eq 'closed' && $double eq 'closed') ? 'open' : 'closed'; 
			}
			elsif (substr($line, $i, 1) eq '"') 
			{ 
				$double = ($double eq 'closed' && $single eq 'closed') ? 'open' : 'closed'; 
			}
			elsif (substr($line, $i, 1) eq "," && ($single eq 'open' || $double eq 'open')) 
			{ 
				push(@qc, $i); 
			}
		}
		foreach my $i (reverse(@qc)) { substr($line, $i, 1) = '__QC__'; }
		return $line;
	}

	sub restoreQuotedCommas : method#Del
	{
		my $self = shift;
		my $line = shift;
		$line =~ s/__QC__/,/g;
		return $line;
	}

	sub saveQuotedPc : method # Del
	{
		# Need to preserve quoted percentage character so as not to confuse with table access char.
		my $self = shift;
		my $line = shift;

		my @pc;
		my $single = 'closed';
		my $double = 'closed';
		foreach my $i (0..length($line))
		{
			if (substr($line, $i, 1) eq "'") 
			{ 
				$single = ($single eq 'closed' && $double eq 'closed') ? 'open' : 'closed'; 
			}
			elsif (substr($line, $i, 1) eq '"') 
			{ 
				$double = ($double eq 'closed' && $single eq 'closed') ? 'open' : 'closed'; 
			}
			elsif (substr($line, $i, 1) eq "%" && ($single eq 'open' || $double eq 'open')) 
			{ 
				push(@pc, $i); 
			}
		}
		foreach my $i (reverse(@pc)) { substr($line, $i, 1) = '__PC__'; }
		return $line;
	}

	sub restoreQuotedPc : method # Del
	{
		my $self = shift;
		my $line = shift;
		$line =~ s/__PC__/%/g;
		return $line;
	}

	sub saveEscPc : method # Del
	{
		# Need to preserve quoted percentage character so as not to confuse with table access char.
		my $self = shift;
		my $line = shift;
		$line =~ s/\\%/__EPC__/g;
		return $line;
	}

	sub restoreEscPc : method # Del
	{
		my $self = shift;
		my $line = shift;
		$line =~ s/__EPC__/\\%/g;
		return $line;
	}

	sub saveSpaces : method # Del
	{
		my $self = shift;
		my $line = shift;

		my @spaces;
		my $single = 'closed';
		my $double = 'closed';
		foreach my $i (0..length($line))
		{
			if (substr($line, $i, 1) eq "'") 
			{ 
				$single = ($single eq 'closed' && $double eq 'closed') ? 'open' : 'closed'; 
			}
			elsif (substr($line, $i, 1) eq '"') 
			{ 
				$double = ($double eq 'closed' && $single eq 'closed') ? 'open' : 'closed'; 
			}
			elsif (substr($line, $i, 1) eq " " && ($single eq 'open' || $double eq 'open')) 
			{ 
				push(@spaces, $i); 
			}
		}
		foreach my $i (reverse(@spaces)) { substr($line, $i, 1) = '__S__'; }
		return $line;
	}

	sub restoreSpaces : method # Del
	{
		my $self = shift;
		my $line = shift;
		$line =~ s/__S__/ /g;
		return $line;
	}

	sub saveAt : method
	{
		my $self = shift;
		my $line = shift;

		my @at;
		my $single = 'closed';
		my $double = 'closed';
		foreach my $i (0..length($line))
		{
			if (substr($line, $i, 1) eq "'") 
			{ 
				$single = ($single eq 'closed' && $double eq 'closed') ? 'open' : 'closed'; 
			}
			elsif (substr($line, $i, 1) eq '"') 
			{ 
				$double = ($double eq 'closed' && $single eq 'closed') ? 'open' : 'closed'; 
			}
			elsif (substr($line, $i, 1) eq "@" && ($single eq 'open' || $double eq 'open')) 
			{ 
				push(@at, $i); 
			}
		}
		foreach my $i (reverse(@at)) { substr($line, $i, 1) = '__AT__'; }
		return $line;
	}

	sub restoreAt : method
	{
		my $self = shift;
		my $line = shift;
		$line =~ s/__AT__/@/g;
		return $line;
	}
}
1;
# ----------------------------------------------------------------------------------------------------
