#!/usr/bin/perl
# ----------------------------------------------------------------------------------------------------
#  Name		: ETL::Pequel::Lister.pm
#  Created	: 3 May 2005
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
$VERSION = "1.1-1";
$BUILD = 'Mon May  3 14:52:20 EST 2005';
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Lister;
	use ETL::Pequel::Code;
	use ETL::Pequel::Collection;
	use base qw(ETL::Pequel::Code::Pod);

	our $this = __PACKAGE__;

	sub BEGIN
	{
		# Create the class attributes
		our @attr =
		qw(
			pmList
			classHierarchy
			classReference
		);
		eval ("sub attr { my \$self = shift; return (qw(@{[ join(' ', @attr) ]})); } ");
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

		$self->pmList
		(
			ETL::Pequel::Collection::Hierarchy->new
			(
				name => 'all',
				ETL::Pequel::Collection::Hierarchy->new
				(
					name => 'l1',
					ETL::Pequel::Collection::Hierarchy->new
					(
						name => 'base',
						ETL::Pequel::Collection::Element->new(name => 'Base/Base.pm'),
					),
					ETL::Pequel::Collection::Hierarchy->new
					(
						name => 'script',
						ETL::Pequel::Collection::Element->new(name => 'Script/Script.pm'),
					),
					ETL::Pequel::Collection::Hierarchy->new
					(
						name => 'main',
						ETL::Pequel::Collection::Element->new(name => 'Main/Main.pm'),
					),
					ETL::Pequel::Collection::Hierarchy->new
					(
						name => 'error',
						ETL::Pequel::Collection::Element->new(name => 'Error/Error.pm'),
					),
					ETL::Pequel::Collection::Hierarchy->new
					(
						name => 'parse',
						ETL::Pequel::Collection::Element->new(name => 'Parse/Parse.pm'),
					),
					ETL::Pequel::Collection::Hierarchy->new
					(
						name => 'collection',
						ETL::Pequel::Collection::Element->new(name => 'Collection/Collection.pm'),
					),
					ETL::Pequel::Collection::Hierarchy->new
					(
						name => 'code',
						ETL::Pequel::Collection::Element->new(name => 'Code/Code.pm'),
					),
				),
				ETL::Pequel::Collection::Hierarchy->new
				(
					name => 'type',
					ETL::Pequel::Collection::Hierarchy->new
					(
						name => 'data',
						ETL::Pequel::Collection::Element->new(name => 'Type/Type.pm'),
					),
					ETL::Pequel::Collection::Hierarchy->new
					(
						name => 'date',
						ETL::Pequel::Collection::Element->new(name => 'Type/Date/Date.pm'),
					),
					ETL::Pequel::Collection::Hierarchy->new
					(
						name => 'option',
						ETL::Pequel::Collection::Element->new(name => 'Type/Option/Option.pm'),
					),
					ETL::Pequel::Collection::Hierarchy->new
					(
						name => 'section',
						ETL::Pequel::Collection::Element->new(name => 'Type/Section/Section.pm'),
					),
					ETL::Pequel::Collection::Hierarchy->new
					(
						name => 'aggregate',
						ETL::Pequel::Collection::Element->new(name => 'Type/Aggregate/Aggregate.pm'),
					),
					ETL::Pequel::Collection::Hierarchy->new
					(
						name => 'macro',
						ETL::Pequel::Collection::Element->new(name => 'Type/Macro/Macro.pm'),
					),
					ETL::Pequel::Collection::Hierarchy->new
					(
						name => 'db',
						ETL::Pequel::Collection::Element->new(name => 'Type/Db/Db.pm'),
						ETL::Pequel::Collection::Hierarchy->new
						(
							name => 'db.oracle',
							ETL::Pequel::Collection::Element->new(name => 'Type/Db/Oracle/Oracle.pm'),
						),
						ETL::Pequel::Collection::Hierarchy->new
						(
							name => 'db.sqlite',
							ETL::Pequel::Collection::Element->new(name => 'Type/Db/Sqlite/Sqlite.pm'),	
						),
					),
					ETL::Pequel::Collection::Hierarchy->new
					(
						name => 'type.table',
						ETL::Pequel::Collection::Element->new(name => 'Type/Table/Table.pm'),
						ETL::Pequel::Collection::Hierarchy->new
						(
							name => 'type.table.oracle',
							ETL::Pequel::Collection::Element->new(name => 'Type/Table/Oracle/Oracle.pm'),
						),
						ETL::Pequel::Collection::Hierarchy->new
						(
							name => 'type.table.sqlite',
							ETL::Pequel::Collection::Element->new(name => 'Type/Table/Sqlite/Sqlite.pm'),
						),
					),
				),
				ETL::Pequel::Collection::Hierarchy->new
				(
					name => 'field',
					ETL::Pequel::Collection::Element->new(name => 'Field/Field.pm'),
				),
				ETL::Pequel::Collection::Hierarchy->new
				(
					name => 'table',
					ETL::Pequel::Collection::Element->new(name => 'Table/Table.pm'),
				),
				ETL::Pequel::Collection::Hierarchy->new
				(
					name => 'docgen',
					ETL::Pequel::Collection::Element->new(name => 'Docgen/Docgen.pm'),
				),
				ETL::Pequel::Collection::Hierarchy->new
				(
					name => 'lister',
					ETL::Pequel::Collection::Element->new(name => 'Lister/Lister.pm'),
				),
				ETL::Pequel::Collection::Hierarchy->new
				(
					name => 'engine',
					ETL::Pequel::Collection::Element->new(name => 'Engine/Engine.pm'),
					ETL::Pequel::Collection::Element->new(name => 'Engine/Inline/Inline.pm'),
				),
			)
		);

		$self->classHierarchy(ETL::Pequel::Collection::Hierarchy->new(name => 'root'));
		$self->classReference(ETL::Pequel::Collection::Hierarchy->new(name => 'root'));
        return $self;
    }
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Lister::SrcList;
	use base qw(ETL::Pequel::Lister);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_);
		bless($self, $class);

		$self->docTitle("Pequel -- Pequel Query Language");
		$self->name("pequelsrclist");
		$self->podName("@{[ $self->name ]}.pod");
		$self->pdfName("@{[ $self->name ]}.pdf");
		$self->docType("Source Listing");
		$self->docVersion("version 2.0");

        return $self;
    }

	sub generate : method
	{
		my $self = shift; 

		$self->root->error->fatalError
			("[1201] Invalid pequelsrclist argument '@{[ $self->root->o_pequelsrclist ]}'")
			if (!$self->pmList->branch($self->root->o_pequelsrclist));

		my $count=0;
		$self->pod;
		foreach my $p ($self->pmList->branch($self->root->o_pequelsrclist)->toArray)
		{
			$self->root->error->msgStderr("Package:@{[ $p->name ]}");
			$self->page if (++$count > 1);
			$self->packageFileName($p->name);
			$self->packageClasses($p->name);
		}
	}
	
	sub packageFileName : method
	{
		my $self = shift;
		my $filename = shift;

		$self->head1($filename);

		open(PROG, $filename);
		while (<PROG>)
		{
			last if (/^__END__/);
			last if (/^{/);
			last if (/^\s*package/);
			next if (/^#/);
			next if (/^\/\//);
			chomp;
			s/^\t/ /;
			s/\t/    /g;
			s/#.*//;
			s/\/\/.*//;
			$self->add("$_\n");
		}
	}

	sub packageClasses : method
	{
		my $self = shift;
		my $filename = shift;

		my $class='';
		my $count=0;
		my $sub='';
		my $open_sub = 0;
		my $method;

		open(PROG, $filename);
		while (<PROG>)
		{
			last if (/^__END__/);
			next if (/^{/);
			next if (/^}/);
			next if (/^1;/);
			next if (/^#/);
			next if (/^\/\//);
			chomp;
			s/^\t//;
			s/^    // unless $open_sub;
			s/^        /\t\t/;
			s/\t/    /g;
#			s/\s?#.*//;
			s/^\/\/.*//;
			s/\s+$//;
			if (/^\s*package\s+/)
			{
				$self->end if ($sub =~ /^sub/);
				$self->add("}\n") if ($sub =~ /^sub/);
				s/^\s*//;
				s/package/class/;
				s/#.*//;
				$class = $_;
				$method = $class;
				$method =~ s/^\s*class\s*//;
				$method =~ s/;\s*$//;
				$self->root->error->msgStderr("  $class");
#?				$self->page if (++$count > 1);
				$self->head2($_);
				$sub='';
				next;
			}
			next unless ($class =~ /^\s*class\s+/);

			if (/^sub\s+/)
			{
				$self->end if ($sub =~ /^sub/);
				$self->add("}\n") if ($sub =~ /^sub/);
				s/^\s*//;
				s/#.*//;
				$sub = $_;
				$sub =~ s/\{\s*\}//;
				$self->root->error->msgStderr("    $sub");
				my $subname = $sub;
				$subname =~ s/^\s*sub\s*//;
				$subname =~ s/\s+\:\s*method//;
				$self->add("I<# $method\::$subname>\n");
				$self->itemBold("$sub");
#<				$self->itemBold("$sub    I<# $method\::$subname>");
				$self->add("{\n");
				$self->begin;
				$open_sub = 1;
				next;
			}
			next if (/^{/);
			next if (/^}/);
			if (length($_) > 85)
			{
				my $line = $_;
				$line =~ s/(^\s+)//;
				my $pre_space = length($1);
				while (length($line)+$pre_space > 85)
				{
					$self->add(' ' x $pre_space . substr($line, 0, 85-$pre_space) . ' \\');
					$line = substr($line, 85-$pre_space);
				}
				$self->add(' ' x $pre_space . $line) if (length($line));
			}
			else
			{
				$sub eq '' ? $self->add("$_\n") : $self->add($_);
			}
		}
		close(PROG);
		$self->add("\n=end\n") if ($sub =~ /^sub/);
		$self->add("}\n") if ($sub =~ /^sub/);
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Lister::ProgRef;
	use base qw(ETL::Pequel::Lister);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_);
		bless($self, $class);

		$self->docTitle("Pequel -- Pequel Query Language");
		$self->name("pequelprogref");
		$self->podName("@{[ $self->name ]}.pod");
		$self->pdfName("@{[ $self->name ]}.pdf");
		$self->docType("Programmer's Reference");
		$self->docVersion("version 2.0");

        return $self;
    }

	sub generate : method
	{
		my $self = shift;

		$self->generateClassHierarchy;
		$self->generateReference;

		$self->sectionClassHierarchy;
		$self->sectionPackageList;
		$self->sectionTypeList;
		$self->sectionClassReference;
	}

	sub sectionClassHierarchy : method
	{
		my $self = shift;

		$self->pod;
		$self->head1("Pequel Class Hierarchy");
		$self->begin;
		my $c = $self->classHierarchy->tree;
		$c->rightIndentBlock;
		$self->addAll($c);
		$self->end;
	}
	
	sub sectionPackageList : method
	{
		my $self = shift;

		$self->page;
		$self->head1("Pequel Packages");

		my $curr_package='';
		my $curr_class='';
		foreach (map($_->name, $self->classReference->toArray))
		{
			next if (/_EXTENDS_/);
			my ($package, $class, $method) = split("[|]");
			if ($curr_package ne $package)
			{
				$self->head2("$package");
				$curr_package = $package;
				$curr_class = '';
			}	
			if ($curr_class ne $class)
			{
				$curr_class = $class;
				$class =~ s/__/::/g;
				$self->add($class);
			}
		}
	}

	sub sectionTypeList : method
	{
		my $self = shift;

		$self->page;
		$self->head1("Pequel Types");

		$self->head2("Data Type");
		map($self->add("type.@{[ $_->name ]}"), $self->root->t_data->toArray);

		$self->head2("Date Type");
		map($self->add("date.@{[ $_->name ]}"), $self->root->t_date->toArray);

		$self->head2("Month Type");
		map($self->add("month.@{[ $_->name ]}"), $self->root->t_month->toArray);

		$self->head2("Option Type");
		map($self->add("option.@{[ $_->name ]}"), $self->root->t_option->toArray);
		
		$self->head2("Macro Type");
		map($self->add("macro.@{[ $_->name ]}"), $self->root->t_macro->toArray);

		$self->head2("Section Type");
		map($self->add("section.@{[ $_->name ]}"), $self->root->t_section->toArray);

		$self->head2("Aggregate Type");
		map($self->add("aggregate.@{[ $_->name ]}"), $self->root->t_aggregate->toArray);

		$self->head2("Db Type");
		map($self->add("db.@{[ $_->name ]}"), $self->root->t_db->toArray);
	}
	
	sub sectionClassReference : method
	{
		my $self = shift;

		$self->page;
		$self->head1("Pequel Class Reference");

		my $curr_package='';
		my $curr_class='';

		foreach (map($_->name, $self->classReference->toArray))
		{
			next if (/_EXTENDS_/);
			my ($package, $class, $method, $attr) = split("[|]");
			if ($curr_package ne $package)
			{
				$self->page if ($curr_package ne '');
				$curr_package = $package;
			}
			if ($curr_class ne $class)
			{
				$curr_class = $class;
				$class =~ s/__/::/g;
				$self->head1($class);	# Class Name

				if 
				(
					$self->classReference->branch($curr_class)->exists("_EXTENDS_")
					&& $self->classReference->branch($curr_class)->exists("_EXTENDS_")->value ne $curr_class
				)
				{
					my $super_class = $self->classReference->branch($curr_class)->exists("_EXTENDS_")->value;
					$super_class =~ s/__/::/g;
					$self->add("Extends I<$super_class>");
				}
				else
				{
					$self->add("I<-- Base Class -->");
				}

				$self->add("Package I<$package>");

				my $count=0;
				foreach my $cl (map($_->name, $self->classReference->toArray))
				{
					next if ($cl =~ /_EXTENDS_/);
					next unless ($cl =~ /$package\|$curr_class\|BEGIN/);
					next unless ((split("[|]", $cl))[3]);
					$self->head2("Class Attributes") if (!$count);
					$self->item(qq{F<@{[ (split("[|]", $cl))[3] ]}>});
					$count++;
				}
#				$self->add("I<-- N/A -->") if (!$count);

				$count=0;
				foreach my $cl (map($_->name, $self->classReference->toArray))
				{
					next if ($cl =~ /_EXTENDS_/);
					next unless ($cl =~ /$package\|$curr_class\|/);
					next if ($cl =~ /BEGIN|DESTROY/);
					$self->head2("Class Methods") if (!$count);
					$self->item(qq{F<@{[ (split("[|]", $cl))[2] ]}>});
					$count++;
				}
#				$self->add("I<-- N/A -->") if (!$count);
			}
		}
	}

	sub generateClassHierarchy : method
	{
		my $self = shift;

		my $class='';
		my $sub='';
		my $open_sub = 0;
		my $class_name;

		foreach my $p ($self->pmList->branch('all')->toArray)
		{
			$self->root->error->msgStderr("Package:@{[ $p->name ]}") if ($self->root->o_debug);

			open(PROG, $p->name);
			while (<PROG>)
			{
				last if (/^__END__/);
				next if (/^{/);
				next if (/^}/);
				next if (/^1;/);
				next if (/^#/);
				next if (/^\/\//);
				chomp;
				s/^\t//;
				s/^    // unless $open_sub;
				s/^        /\t\t/;
				s/\t/    /g;
#				s/\s?#.*//;
				s/^\/\/.*//;
				s/\s+$//;
				if (/^\s*package\s+/)
				{
					s/^\s*//;
					s/package/class/;
					s/#.*//;
					$class = $_;
					$class_name = $class;
					$class_name =~ s/^\s*class\s*//;
					$class_name =~ s/;\s*$//;
					$class_name =~ s/::/__/g;
					$self->root->error->msgStderr("  $class") if ($self->root->o_debug);
					$sub='';
					next;
				}
				next unless ($class =~ /^\s*class\s+/);
				if (/\s*use\s+base\s+qw\(\s*([\w|:]+)\s*\);/)
				{
					my $super_class = $1;
					$super_class =~ s/::/__/g;
					$self->classHierarchy->branch($super_class)->addAll
					(
						ETL::Pequel::Collection::Element->new(name => "@{[ $p->name ]}|$class_name"),
						ETL::Pequel::Collection::Hierarchy->new(name => "$class_name"),
					);
					$self->classHierarchy->branch($class_name)->add(ETL::Pequel::Collection::Element->new(
					(
						name => '_EXTENDS_', 
						value => $super_class
					)));
				}
				if (/^sub\s+/)
				{
					if (!$self->classHierarchy->branch($class_name))
					{
						$self->classHierarchy->addAll
						(
							ETL::Pequel::Collection::Element->new(name => "@{[ $p->name ]}|$class_name"),
							ETL::Pequel::Collection::Hierarchy->new(name => "$class_name"),
						);
					}
					s/^\s*//;
					s/#.*//;
					$sub = $_;
					$sub =~ s/\{\s*\}//;
					$self->root->error->msgStderr("    $sub") if ($self->root->o_debug);
					my $subname = $sub;
					$subname =~ s/^\s*sub\s*//;
					$subname =~ s/\s+\:\s*method//;
					if ($self->classHierarchy->branch($class_name))
					{
						$self->classHierarchy->branch($class_name)->add
						(
							ETL::Pequel::Collection::Element->new(name => "@{[ $p->name ]}|$class_name|$subname"),
						);
					}
					if ($subname eq 'BEGIN')	# Build the attribute list
					{
						while (<PROG>)
						{
							next if (/\@attr/);	# Start of begin sub
							next if (/{/);	# Start of begin sub
							next if (/\(/);	# Start of @attr list
							last if (/\);/); # End of @attr list
							s/['",]//g;
							s/\s+//g;
							s/#.*/#/;
							next if (/^#/);
							next if ($_ eq '');
							$self->classHierarchy->branch($class_name)->add
							(
								ETL::Pequel::Collection::Element->new(name => "@{[ $p->name ]}|$class_name|$subname|$_"),
							);
						}
					}
					next;
				}
			}
			close(PROG);
		}
	}

	sub generateReference : method
	{
		my $self = shift;

		my $class='';
		my $sub='';
		my $open_sub = 0;
		my $class_name;
		my $super_class;

		foreach my $p ($self->pmList->branch('all')->toArray)
		{
			$self->root->error->msgStderr("Package:@{[ $p->name ]}") if ($self->root->o_debug);

			open(PROG, $p->name);
			while (<PROG>)
			{
				last if (/^__END__/);
				next if (/^{/);
				next if (/^}/);
				next if (/^1;/);
				next if (/^#/);
				next if (/^\/\//);
				chomp;
				s/^\t//;
				s/^    // unless $open_sub;
				s/^        /\t\t/;
				s/\t/    /g;
#				s/\s?#.*//;
				s/^\/\/.*//;
				s/\s+$//;
				if (/^\s*package\s+/)
				{
					s/^\s*//;
					s/package/class/;
					s/#.*//;
					$class = $_;
					$class_name = $class;
					$class_name =~ s/^\s*class\s*//;
					$class_name =~ s/;\s*$//;
					$class_name =~ s/::/__/g;
					$self->root->error->msgStderr("  $class") if ($self->root->o_debug);
					$sub='';
					$super_class = '';
					next;
				}
				next unless ($class =~ /^\s*class\s+/);
				if (/\s*use\s+base\s+qw\(\s*([\w|:]+)\s*\);/)
				{
					$super_class = $1;
					$super_class =~ s/::/__/g;
#					$self->classReference->branch($super_class)->addAll
#					(
#						Pequel::Collection::Element->new(name => "@{[ $p->name ]}|$class_name"),
#						Pequel::Collection::Hierarchy->new(name => "$class_name"),
#					);
#					$self->classReference->branch($class_name)->add(Pequel::Collection::Element->new(
#					(
#						name => '_EXTENDS_', 
#						value => $super_class
#					)));
				}
				if (/^sub\s+/)
				{
					if (!$self->classReference->branch($class_name))
					{
						$self->classReference->addAll
						(
							ETL::Pequel::Collection::Element->new(name => "@{[ $p->name ]}|$class_name"),
							ETL::Pequel::Collection::Hierarchy->new(name => "$class_name"),
						);
						$self->classReference->branch($class_name)->add(ETL::Pequel::Collection::Element->new(
						(
							name => '_EXTENDS_', 
							value => $super_class
						)))
						if ($super_class ne '');
					}
					s/^\s*//;
					s/#.*//;
					$sub = $_;
					$sub =~ s/\{\s*\}//;
					$self->root->error->msgStderr("    $sub") if ($self->root->o_debug);
					my $subname = $sub;
					$subname =~ s/^\s*sub\s*//;
					$subname =~ s/\s+\:\s*method//;
					if ($self->classReference->branch($class_name))
					{
						$self->classReference->branch($class_name)->add
						(
							ETL::Pequel::Collection::Element->new(name => "@{[ $p->name ]}|$class_name|$subname"),
						);
					}
					if ($subname eq 'BEGIN')	# Build the attribute list
					{
						while (<PROG>)
						{
							next if (/\@attr/);	# Start of begin sub
							next if (/{/);	# Start of begin sub
							next if (/\(/);	# Start of @attr list
							last if (/\);/); # End of @attr list
							s/['",]//g;
							s/\s+//g;
							s/#.*/#/;
							next if (/^#/);
							next if ($_ eq '');
							$self->classReference->branch($class_name)->add
							(
								ETL::Pequel::Collection::Element->new(name => "@{[ $p->name ]}|$class_name|$subname|$_"),
							);
						}
					}
					next;
				}
			}
			close(PROG);
		}
	}
}
# ----------------------------------------------------------------------------------------------------
1;
