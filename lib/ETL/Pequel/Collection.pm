#!/usr/bin/perl
# ----------------------------------------------------------------------------------------------------
#  Name		: ETL::Pequel::Collection.pm
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
# ----------------------------------------------------------------------------------------------------
# TO DO:
# ----------------------------------------------------------------------------------------------------
require 5.005_62;
use strict;
use attributes qw(get reftype);
use warnings;
use vars qw($VERSION $BUILD);
$VERSION = "1.1-1";
$BUILD = 'Tue Jan 27 15:45:31 EST 2005';
# ----------------------------------------------------------------------------------------------------
{
#> To be replaced with: 
#>	Pequel::Container; Pequel::Container::List; Pequel::Container::Stack; Pequel::Container::Vector;
#>	Pequel::Iterator;

	package ETL::Pequel::Collection::Element;	#-->	package Pequel::Element;

	our $this = __PACKAGE__;

	sub BEGIN
	{
		# Create the class attributes
		our @attr =
		qw(
			name
			number
			value
			uniq
			comment
			setIndex
			PARAM
		);
#>		uniq: indicate this 'key' field contains unique values (so add with same value will fail)

		eval ("sub attr { my \$self = shift; return (qw(@{[ join(' ', @attr) ]})); } ");
		foreach (@attr)
		{
			eval
			("
				sub $_ 
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

		$self->name($params{'name'});
		$self->number($params{'number'});
		$self->value($params{'value'});
		$self->comment($params{'comment'});
		$self->PARAM($params{'PARAM'});

		$self->setIndex(-1);	# -1 means not yet in a vector;
		return $self;
	}

    sub index : method 
	{ 
		# Return the uniq index number for element.
		my $self = shift; 
		return $self->{$this}->{INDEX}; 
	}

	sub compile : method
	{
		my $self = shift; 
	}

	sub check_equals : method
	{
		# Compare two scalars and return 1 if equals else 0.
		my $self = shift;
		my $e1 = shift;
		my $e2 = shift;

		return 1 if (defined($e1) && defined($e2) && $e1 eq $e2);
		return 1 if (!defined($e1) && !defined($e2));
		return 0;
	}

	sub equals : method			# Must by overidden and also called by subclass, to process other fields.
	{
		# Compare two elements and return 1 if equals else 0.
		my $self = shift;
		my $element = shift;
		return 0 
			if
			(
				!$self->check_equals($self->name, $element->name)
				|| !$self->check_equals($self->number, $element->number)
				|| !$self->check_equals($self->value, $element->value)
				|| !$self->check_equals($self->comment, $element->comment)
			);
		return 1;
	}

	sub set : method			# Must be overidden and also called by subclass, to process other fields.
	{
		my $self = shift;
		my $element = shift;

		$self->name($element->name);
		$self->number($element->number);
		$self->value($element->value);
		$self->comment($element->comment);
	}

	sub toString : method
	{
		my $self = shift;
		return join("|", 
			$self->index, 
			$self->name, 
			(defined($self->number) ? $self->number : ''), 
			(defined($self->value) ? $self->value : ''), 
			(defined($self->comment) ? $self->comment : ''));
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Collection::Cursor;		# --> should be iterator
	use base qw(ETL::Pequel::Collection::Element);

	our $this = __PACKAGE__;

	sub BEGIN
	{
		our @attr =
		qw(
			index
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
		$self = $class->SUPER::new(@_);
		bless($self, $class);

		$self->reset;

		return $self;
	}

#?	sub indexStart {}

	sub dec : method #> private _dec
	{
		# Decrement current index by 1.
		my $self = shift;
		$self->index($self->index-1);
	}

	sub inc : method #> private _inc
	{
		# Increment current index by 1.
		my $self = shift;
		$self->index($self->index+1);
	}

	sub reset : method
	{
		# Same as removing all elements in collection.
		my $self = shift;
		$self->index(-1);
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Collection::Iterator;	# --> should be cursor
	use base qw(ETL::Pequel::Collection::Cursor);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		$self = $class->SUPER::new(@_);
		bless($self, $class);

		return $self;
	}

	sub current : method	#> private _current
	{
		# Return current element object at index.
		my $self = shift;
		return 0 if (!$self->size);
		return ${$self->_vector}[$self->index];
	}

	sub next : method
	{
		# Return next element.
		my $self = shift;
		return 0 if (!$self->size || $self->index == $#{$self->_vector});
		$self->inc;
		return ${$self->_vector}[$self->index];
	}

	sub prev : method
	{
		# Return previous element.
		my $self = shift;
		return 0 if (!$self->size || $self->index == 0);
		$self->dec;
		return ${$self->_vector}[$self->index];
	}

	sub last : method	#> use back()
	{
		# Return last element in collection.
		my $self = shift;
		return 0 if (!$self->size);
		$self->index($#{$self->_vector});
		return $self->current;
	}

	sub back : method	# lastElement()
	{
		# Return last element in collection.
		my $self = shift;
		return 0 if (!$self->size);
		$self->index($#{$self->_vector});
		return $self->current;
	}

	sub first : method #> use front()
	{
		# Return first element in collection.
		my $self = shift;
		return 0 if (!$self->size);
		$self->reset;
		return $self->next;
	}

	sub front : method
	{
		# Return first element in collection.
		my $self = shift;
		return 0 if (!$self->size);
		$self->reset;
		return $self->next;
	}
}
# ----------------------------------------------------------------------------------------------------
#{
#?	package Pequel::Collection;
#?	use base qw(Pequel::Collection::Iterator);
#?	
#?	sub add {}
#?	sub push {}
#?	sub pop {}
#?	sub shift {}
#?	sub unshift {}
#?	sub current {}
#?	sub size {}
#?	sub clear {}
#}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Collection::Vector;
	use base qw(ETL::Pequel::Collection::Iterator);
	use UNIVERSAL qw(isa can);

	our $this = __PACKAGE__;

	# Object's added to Vector must be of type Pequel::Collection::Vector::Element inherited.

	use constant CURRENT	=> int 0;
	use constant FIRST		=> int 1;
	use constant NEXT 		=> int 2;
	use constant PREV 		=> int 3;
	use constant PREVIOUS 	=> int 3;
	use constant LAST 		=> int 4;
	use constant NAME 		=> int 5;
	use constant NUMBER 	=> int 6;
	use constant VALUE 		=> int 7;
	use constant REGEX 		=> int 8;
	use constant INDEX 		=> int 99;

	sub BEGIN
	{
		our @attr =
		qw(
			_vector
			expandAll
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
		$self = $class->SUPER::new(grep(!$_->isa("ETL::Pequel::Collection::Element"), @_));
		bless($self, $class);

		$self->_vector([]);		# --> $self->collection(Pequel::Collection->new);

		foreach (grep($_->isa("ETL::Pequel::Collection::Element"), @_)) { $self->add($_); }

		return $self;
	}

	sub add : method
	{
		# Add the object to the collection.
		my $self = shift;
		my $object = shift;

		push(@{$self->_vector}, $object);
		$object->setIndex($self->lastIndex);
		$object->number($self->size) if (!defined($object->number));
		$object->name("f@{[ $self->size ]}") if (!defined($object->name));
		$object->value("") if (!defined($object->value));
		$self->last;
		return $self->size;
	}

	sub addAll : method
	{
		# Add all the objects to the collection.
		my $self = shift;
		map($self->add($_), @_);
		return $self->size;
	}

#> Should support expandAll
	sub find : method
	{
		# Search for element by name and return object if found, else return 0.
		my $self = shift;
		return $self->search(ETL::Pequel::Collection::Vector::NAME, shift);
	}

	sub exists : method
	{
		# Search for element by name and return object if found, else return 0.
		my $self = shift;
		return $self->search(ETL::Pequel::Collection::Vector::NAME, shift);
	}

#> Should support expandAll
	sub search : method
	{
		# Search for element by name/value/regex/number and return object if found, else return 0.
		my $self = shift;
		my $command = shift;
		my $key = shift;

		return 0 if (!$self->size);
		foreach my $element ($self->toArray)
		{
			return $element if ($command == ETL::Pequel::Collection::Vector::NUMBER && $element->number == $key);
			return $element if ($command == ETL::Pequel::Collection::Vector::VALUE && $element->value eq $key);
			return $element if ($command == ETL::Pequel::Collection::Vector::REGEX && $key =~ /^@{[ $element->regEx ]}$/);	
			# use diff brackets if '/' already in regEx

			return $element if ($command == ETL::Pequel::Collection::Vector::NAME && defined($key) && $element->name eq $key);
		}
		return 0;
	}

	sub toArray : method
	{
		# Return an array of all element objects in the collection.
		# Expand all collection type objects if expandAll is on.
		my $self = shift;
		$self->expandAll
			? map($_->can("toArray") ? $_->toArray : $_, @{$self->_vector})
			: @{$self->_vector};
	}
	
#<	sub toArray : method
#<	{
#<		my $self = shift;
#<		my $fldsel = shift || undef;
#<		return 
#<		(
#<			defined($fldsel) && $fldsel == Pequel::Collection::Vector::NAME 
#<			? map($_->name, @{$self->_vector})
#<			: 
#<			(
#<				defined($fldsel) && $fldsel == Pequel::Collection::Vector::NUMBER
#<				? map($_->number, @{$self->_vector})
#<				: 
#<				(
#<					defined($fldsel) && $fldsel == Pequel::Collection::Vector::VALUE
#<					? map($_->value, @{$self->_vector})
#<					: @{$self->_vector}
#<				)
#<			)
#<		)
#<	}
	
	sub size : method
	{
		# Return the number of elements in the collection.
		my $self = shift;
		return int(@{$self->_vector});
	}

	sub clear : method
	{
		# Delete all elements from collection.
		my $self = shift;
		$self->_vector([]);		# --> $self->collection(ETL::Pequel::Collection->New);
		$self->reset;
	}

	sub clone : method
	{
		# Return a new cloned copy of the collection.
		my $self = shift;
		return ETL::Pequel::Collection::Vector->new($self->toArray);
	}

	sub equals : method
	{
		my $self = shift;
		my $other_vector = shift;
		
		return 0 if ($self->size != $other_vector->size);
		foreach my $index (0..$self->lastIndex)
		{
			return 0 unless ($self->elementAt($index)->equals($other_vector->elementAt($index)));
		}
		return 1;
	}

	sub elementAt : method
	{
		my $self = shift;
		my $index = shift;

		foreach my $element ($self->toArray)
		{
			return $element if ($element->index == $index);
		}
		return 0;
	}

#>	Need to remove/fix this...or maybe Pequel::Base and this should ingerit Pequel::Root
#<	sub root : method
#<	{
#<		return Pequel::Base::root;
#<	}
#<	
	sub extract : method
	{
		# Return list of objects where name present in clause:
		my $self = shift;
		my $clause = shift;
		my @found;
		
		$clause = $self->saveQuotes($clause); #> change this - no saveQuotes
		foreach ($self->toArray)
		{
			push(@found, $_) if ($clause =~ /\b@{[ $_->name ]}\b/);
		}
		$clause = $self->restoreQuotes($clause);
		return @found;
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

#?	sub remove : method
#?	{
#?	}
#?	
#?	sub removeAt : method
#?	{
#?	}

	sub get : method #> not used(?) -- remove
	{
		my $self = shift;
		my $command = shift || ETL::Pequel::Collection::Vector::CURRENT;
	
		return $self->current if ($command == ETL::Pequel::Collection::Vector::CURRENT);
		return $self->next if ($command == ETL::Pequel::Collection::Vector::NEXT);
		return $self->prev if ($command == ETL::Pequel::Collection::Vector::PREV);
		return $self->first if ($command == ETL::Pequel::Collection::Vector::FIRST);
		return $self->last if ($command == ETL::Pequel::Collection::Vector::LAST);
		return $self->search($command, shift) 
			if 
			(
				$command == ETL::Pequel::Collection::Vector::NUMBER 
				|| $command == ETL::Pequel::Collection::Vector::NAME 
				|| $command == ETL::Pequel::Collection::Vector::VALUE
				|| $command == ETL::Pequel::Collection::Vector::REGEX
			);
		return 0;
	}

	sub regEx : method
	{
		my $self = shift;
		return $self->search(ETL::Pequel::Collection::Vector::REGEX, shift);
	}

	sub setAll : method #> not used -- remove
	{
		my $self = shift;
		my $collection = shift;
		foreach my $o ($self->toArray)
		{
			$o->set($collection->elementAt($o->index));
		}
	}	

	sub set : method #> not used(?) -- remove
	{
		my $self = shift;
		my $index = shift;
		my $element = shift;
		if ($self->elementAt($index) != 0)
		{
			$self->elementAt($index)->set($element);
		}
	}

	sub put : method #> not used(?) -- remove
	{
		my $self = shift;
		my $command = shift;
		my $key = shift;
		my $value = shift;

		my $element;
		return 0 if (($element = $self->search($command, $key)) == 0);
		$element->value($value);
	}

	sub lastIndex : method #> not used(?) -- remove
	{
		my $self = shift;
		return $#{$self->_vector};
	}

	sub elements : method
	{
		my $self = shift;
		return $self->{$this}->{ELEMENTS};	# = ETL::Pequel::Collection::Iterator->New;
	}

	sub addElement : method
	{
		my $self = shift;
		$self->add(shift);
	}

	sub toString : method
	{
		my $self = shift;
		return join("\n", map($_->toString, $self->toArray));
	}

	sub toArraySort : method
	{
		my $self = shift;
		my $fldsel = shift || ETL::Pequel::Collection::Vector::NUMBER;
		return 
		(
			$fldsel == ETL::Pequel::Collection::Vector::NAME 
			? sort { $a->name cmp $b->name } $self->toArray
			: 
			(
				$fldsel == ETL::Pequel::Collection::Vector::VALUE
				? sort { $a->value cmp $b->value } $self->toArray
				: sort { $a->number <=> $b->number } $self->toArray
			)
		)
	}

	sub toArrayUniq : method
	{
		my $self = shift;
		my %uniq;
		foreach ($self->toArray) { $uniq{$_->name} = $_; }
		return values %uniq;
	}

	sub toArrayName : method
	{
		my $self = shift;
		return map($_->name, $self->toArray);
	}

	sub toArrayNumber : method
	{
		my $self = shift;
		return map($_->number, $self->toArray);
	}

	sub toArrayValue : method
	{
		my $self = shift;
		return map($_->value, $self->toArray);
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Collection::Hierarchy;
	use base qw(ETL::Pequel::Collection::Vector);

	sub new
	{
		my $self = shift;
		my $class = ref($self) || $self;
		$self = $class->SUPER::new(@_);
		bless($self, $class);

		$self->expandAll(1);
		return $self;
	}

	sub branches : method
	{
		# Return array of all hierarchy type nodes (branches) including the root node.
		my $self = shift;
		my @branches = ( $self );

		foreach my $o (@{$self->_vector})
		{
			push(@branches, $o->branches) if ($o->isa(__PACKAGE__));
		}
		return @branches;
	}

	sub branch : method
	{
		# Return the branch sub-hierarchy with name $branch_name
		my $self = shift;
		my $branch_name = shift;

		return $self if ($self->name eq $branch_name);
		foreach my $o (@{$self->_vector})
		{
			return $o if ($o->isa(__PACKAGE__) && $o->name eq $branch_name);
			my $branch = $o->branch($branch_name) if ($o->isa(__PACKAGE__));
			return $branch if ($branch && $branch->isa(__PACKAGE__) 
				&& $branch->name eq $branch_name);
		}
		return 0;
	}

	sub tree : method
	{
		# Return a tree structure for the hierarchy as pairs of level and branch/node
		# level 0 is the root node

		my $self = shift;
		my $end = shift || 0;
		my $level = shift || 0;
		my $bar = shift || '';

		my $c = ETL::Pequel::Code->new;
		$level ? $c->add("$bar|   ") : $c->add($bar);
		my $name = $self->name;
		$name =~ s/__/::/g;
		$c->add("$bar+---$name");

		if (!$level) { $bar = '    '; }
		else
		{
			$bar .= ($end ? '    ' : '|   ') if ($self->size);
		}

		foreach my $o (@{$self->_vector})
		{
			$c->addAll
			(
				$o->tree
				(
					($o == ${$self->_vector}[$#{$self->_vector}]), 
					$level+1, 
					$bar
				)
			)
			if ($o->isa(__PACKAGE__));
		}
		return $c;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Collection::Vector::Stack; 	#-->	Pequel::Collection::Stack
	use base qw(ETL::Pequel::Collection::Vector);

	sub new
	{
		my $self = shift;
		my $class = ref($self) || $self;
		$self = $class->SUPER::new(@_);
		bless($self, $class);

		return $self;
	}

	sub push : method
	{
		my $self = shift;
		my $object = shift;
		return $self->add($object);
	}
	
	sub pushAll : method
	{
		my $self = shift;
		my $collection = shift;
		return $self->addAll($collection);
	}

	sub pop : method
	{
		my $self = shift;
		return 0 if (!$self->size);
		$self->dec if ($self->index == $self->size-1);
		return pop @{$self->_vector};
	}

	sub shift : method
	{
		my $self = shift;
		return 0 if (!$self->size);
		$self->index($#{$self->_vector} -1) if ($self->index == $#{$self->_vector});
		$self->reset if ($self->size == 1);
		return shift @{$self->_vector};
		# Need to reindex...
	}

	sub unshift : method
	{
		my $self = shift;
		my $object = shift;

		unshift(@{$self->_vector}, $object);
		return $self->size;
		# Need to reindex...
	}

	sub unshiftAll : method
	{
		my $self = shift;
		my $collection = shift;

		unshift(@{$self->_vector}, $collection->toArray);
		return $self->size;
		# Need to reindex...
	}
}
# ----------------------------------------------------------------------------------------------------
#?{
#?	package Pequel::Collection::Vector::List;
#?	use base qw(Pequel::Collection::Vector);
#?	
#?	sub ptr_next {}
#?	sub ptr_prev {}
#?	sub ptr_first {}
#?	sub ptr_last {}
#?	sub insert {}
#?	sub insertAll {}
#?}
1;
# ----------------------------------------------------------------------------------------------------
