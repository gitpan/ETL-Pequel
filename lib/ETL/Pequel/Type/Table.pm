#!/usr/bin/perl
# ----------------------------------------------------------------------------------------------------
#  Name		: ETL::Pequel::Type::Table.pm
#  Created	: 16 February 2005
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
# 20/09/2005	2.3-1		gaffie	Added pequel script chaining functionality.
# 14/09/2005	2.3-1		gaffie	Fixed code generated for external tables with single data column.
# 14/09/2005	2.3-1		gaffie	Added Pequel tables.
# 13/09/2005	2.2-9		gaffie	PEQUEL_TABLE_PATH env for runtime external table path.
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
	package ETL::Pequel::Type::Table::Element;
	use ETL::Pequel::Type;	#+++++
	use base qw(ETL::Pequel::Type::Element);

	our $this = __PACKAGE__;

	sub BEGIN
	{
		our @attr =
		(
			'dataSourceFilename',
			'keyColumn',
			'keyType',
			'engine',
			'persistent',
			'merge',
			'type',
			'loadAtRuntime',

			# ptr to vector containing list of ETL::Pequel::Table::Refkey::Element
			# updated during compileLookup()
			'refKeyList',

			# contains vector of objects one each for every expression type
			# refkey (ie where the refkey in usein use is not an input/output field:
			'refKeyExpList',

			# indicates order table used:
			# updated during compileLookup()
			'sequence',	

			# ptr to vector containing list of ETL::Pequel::Table::Field::Element
			'fields',			
			'data',
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

#>	use constant TYPE_LOCAL		=> int 1;
#>	use constant TYPE_EXTERNAL	=> int 2;
#>	use constant TYPE_PERIOD	=> int 3;
#>	use constant TYPE_MONTH		=> int 4;
#>	use constant TYPE_SQLITE	=> int 5;
#>	use constant TYPE_ORACLE	=> int 6;

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_);
		bless($self, $class);

		$self->PARAM->error->fatalError("[8001] Invalid table name '@{[ $self->name ]}'")
			unless ($self->name =~ /[_|\w|\d]+/);

		$self->loadAtRuntime($params{'load_at_runtime'} || 0);	 #--> same as ETL::Pequel::Type::Table::Local
		$self->persistent($params{'persistent'} || 0);
		$self->merge($params{'merge'} || 0);
		$self->type($params{'type'} || 'hash');		
		$self->sequence($params{'sequence'} || 0);

		$self->fields($params{'fields'} || ETL::Pequel::Collection::Vector->new);		
			# contains ETL::Pequel::Table::Field::Element

		$self->refKeyList($params{'ref_key_list'} || ETL::Pequel::Collection::Vector->new);
			# contains ETL::Pequel::Field::Element

		$self->refKeyExpList($params{'ref_key_exp_list'} || ETL::Pequel::Collection::Vector->new);
			# contains ETL::Pequel::Field::Element

		$self->data(ETL::Pequel::Collection::Vector->new);

		$self->PARAM->error->fatalError("[8002] Table '@{[ $self->name ]}' redefined")
			if ($self->PARAM->tables->exists($self->name));

		$self->dataSourceFilename($params{'data_source_filename'});		
		$self->keyColumn($params{'key_column'});		
		$self->keyType($params{'key_type'});		

		$self->PARAM->tables->add($self);

		return $self;
	}

#>	this should not be called compile due to conflict
	sub compile : method
	{
		my $self = shift;
	}

	sub codeVar : method
	{
		my $self = shift;
#?		my $key = shift;
#?		my $fld = shift || undef;
#?	
#?		$fld
#?			? $self->setSequence($key, $fld)
#?			: $self->setSequence($key);
	}

    sub setSequence : method 
	{ 
		my $self = shift; 
		my $keyexp = shift || return;		# keyfld can be an expression eg: another macro
		my $field = shift || '_KEY';

		my $ref;
		if 
		(
			(
				($ref = $self->PARAM->sections->exists('input section')->items->exists($keyexp)) != 0
				|| ($ref = $self->PARAM->sections->exists('output section')->items->exists($keyexp)) != 0
			)
		)
		{
			my $k;
			if (($k = $self->refKeyList->exists($keyexp)) == 0)
			{
				$self->refKeyList->add(ETL::Pequel::Table::Refkey::Element->new
				(
					name => $keyexp, 	
					value => $ref,
					PARAM => $self->PARAM
				));
				$k = $self->refKeyList->last;
				$self->PARAM->tables->setTableSequence($self);
			}
			$k->referenceFieldList->add(ETL::Pequel::Table::Refkey::Element->new
			(
				name => $field,
				value => $ref,
				input_field => $self->PARAM->sections->exists('input section')->items->last,
				PARAM => $self->PARAM
			));	
			$ref->refTableList->add(ETL::Pequel::Table::Refkey::Element->new
			(
				name => $field,
				table => $self,
				input_field => $ref,	# key
				PARAM => $self->PARAM
			));
		}
		elsif ($self->refKeyExpList->exists($keyexp) == 0)
		{
			$self->refKeyExpList->add(ETL::Pequel::Field::Element->new(name => $keyexp, PARAM => $self->PARAM));
			$self->refKeyList->add(ETL::Pequel::Table::Refkey::Element->new
			(
				name => $self->refKeyExpList->last->number, 
				value => $self->refKeyExpList->last,
				input_field => $self->PARAM->sections->exists('input section')->items->last,
				PARAM => $self->PARAM
			));
			$self->PARAM->tables->setTableSequence($self);
		}

		$self->PARAM->error->fatalError
		("[8005] Multiple reference keys ($keyexp) on merge type table @{[ $self->name ]} not permitted.")
			if ($self->merge && $self->refKeyList->size > 1);
	}

#<	sub codeVar : method 
#<	{ 
#<		my $self = shift; 
#<	}

    sub codeConnect : method 
	{ 
		my $self = shift; 
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		return $c;
	}

    sub codeInit : method 
	{ 
		my $self = shift; 
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		return $c;
	}

    sub codePrepare : method 
	{ 
		my $self = shift; 
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		return $c;
	}

    sub codeFetchRow : method 
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

	sub codeLoadTable : method
	{
		my $self = shift; 
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		return $c;
	}

	sub docOverview : method
	{
	}

	sub docPequelScript : method
	{
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Table::Local;
	use base qw(ETL::Pequel::Type::Table::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_,
			type => $params{'type'} || 'local',
			persistent => $params{'persistent'} || 0,
			merge => $params{'merge'} || 0,
		);
		bless($self, $class);
		return $self;
	}

    sub codeInit : method 
	{ 
		my $self = shift; 
		my $c = $self->SUPER::codeInit;
		$c->add("my \$_TABLE_@{[ $_->name ]} = \&InitLookup@{[ $_->name ]}; # ref to \%\$@{[ $_->name ]} hash");
		return $c;
	}

	sub codeLoadTable
	{
		my $self = shift;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		$c->addComment("++++++ Table @{[ $self->name ]} --> Type :@{[ ref($self) ]} ++++++");

		$c->add("sub InitLookup@{[ $self->name ]}");
		$c->add("{");
		$c->over;
		$c->add("my \%_TABLE_@{[ $self->name ]};");
		
		$c->add("\%_TABLE_@{[ $self->name ]} =");
		$c->add("(");
		$c->over;

#>		Use Code::addList, Code::nextListItem and Code::listColumns(n) for tabulation:
		foreach my $d (sort { $a->name cmp $b->name } $self->data->toArray)
		{
			$c->addNonl("'@{[ $d->name ]}' => ");
			($self->fields->size == 0)
				? $c->add("'@{[ $d->name ]}',")
				: ($self->fields->size == 1)
					? $c->add("'@{[ $d->value->first->value ]}',")
					: $c->add("['@{[ join(q{', '}, map($_->value, $d->value->toArray)) ]}'],");
		}
		$c->endList;
		$c->back;
		$c->add(");");
		$c->add("return \\\%_TABLE_@{[ $self->name ]};");
		$c->back;
		$c->add("}");
		$c->add;
		return $c;
	}

    sub codeVar : method 
	{ 
		my $self = shift; 
		my $keyfld = shift;		# keyfld can be an expression eg: another macro
		my $field = shift || undef;

		$field ? $self->setSequence($keyfld, $field) : $self->setSequence($keyfld);
		
		if ($keyfld =~ /[\(|\[|,]/)
		{
			return defined($field)
				? '${$$_TABLE_' . $self->name . '{qq{@{[ ' . $keyfld . ' ]}}}}[_T_' . $self->name . '_FLD_' . $field . ']'
				: '$$_TABLE_' . $self->name . '{qq{@{[ ' . $keyfld . ' ]}}}';
		}
		else
		{
			return defined($field)
				? '${$$_TABLE_' . $self->name . '{qq{' . $keyfld . '}}}[_T_' . $self->name . '_FLD_' . $field . ']'
				: '$$_TABLE_' . $self->name . '{qq{' . $keyfld . '}}';
		}
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Table::External;
	use base qw(ETL::Pequel::Type::Table::Element);

	our $this = __PACKAGE__;

	sub BEGIN
	{
		our @attr =
		qw(
			dbFileType
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
		$self = $class->SUPER::new
		(
			@_,
			type => $params{'type'} || 'external',
			persistent => $params{'persistent'} || 0,
			merge => $params{'merge'} || 0,
		);
		bless($self, $class);

		$self->dbFileType($params{'db_file_type'} || '');		

		return $self;
	}

	sub dbFilename : method
	{
		my $self = shift;
		return "@{[ $self->PARAM->properties('prefix') ]}/_TABLE_@{[ $self->name ]}.@{[ $self->dbFileType ]}";
	}

    sub codeInit : method 
	{ 
		my $self = shift; 
		my $c = $self->SUPER::codeInit;
		$c->add("my \$_TABLE_@{[ $_->name ]} = \&LoadTable@{[ $_->name ]}; # ref to \%\$@{[ $_->name ]} hash");
		return $c;
	}

	sub codeConnect : method
	{
		my $self = shift;
		my $c = $self->SUPER::codeConnect;
		$c->add("tie(\%_TABLE_@{[ $self->name ]}, 'Tie::DBI', 'dbi:SQLite:dbname=@{[ $self->dbFilename ]}', 'hash', 'key')");
		$c->add("or die 'Cannot open @{[ $self->dbFilename ]}:\$!'");
		return $c;
	}

    sub codePrepare : method 
	{ 
		my $self = shift; 
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		if ($self->persistent)
		{
			$c->add(qq{my \$sth_@{[ $self->name ]} = \$dbh_@{[ $self->name ]}->prepare("select * from @{[ $self->name ]} where key = ?")});
			$c->add(qq{|| die \$dbh_@{[ $self->name ]}->errstr;});
		}
		return $c;
	}

    sub codeFetchRow : method 
	{ 
		my $self = shift; 
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		if ($self->persistent)
		{
			foreach my $refkey (sort $self->refKeyList->toArray)
			{
				$c->add("\$sth_@{[ $self->name ]}->execute(@{[ $self->PARAM->parser->compile($refkey->name) ]});");
				$c->add("my \$row_@{[ $self->name ]}_@{[ $refkey->name ]} = \$sth_@{[ $self->name ]}->fetchrow_hashref;");
				$c->add;
			}
		}
		return $c;
	}

    sub codeClose : method 
	{ 
		my $self = shift; 
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		if ($self->persistent)
		{
			$c->add("\$sth_@{[ $self->name ]}->finish;");
			$c->add("\$dbh_@{[ $self->name ]}->disconnect;");
			$c->verboseMessage("_TABLE_@{[ $self->name ]} file closed");
		}
		return $c;
	}

	sub codeLoadTable : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		$c->addComment("++++++ Table @{[ $self->name ]} --> Type :@{[ ref($self) ]} ++++++");

		$c->add("sub LoadTable@{[ $self->name ]}");
		$c->openBlock("{");
			$c->add("my \%_TABLE_@{[ $self->name ]};");
		
			if ($self->persistent)
			{
				$c->add("if (-e '@{[ $self->dbFilename ]}')");
				$c->openBlock("{");
					$c->addAll($self->codeConnect);
					$c->verboseMessage("Table @{[ $self->name ]} loaded.");
					$c->add("return \\\%_TABLE_@{[ $self->name ]};");
				$c->closeBlock;
			}

			($self->dataSourceFilename =~ /^\//)
				? $c->add("my \$dsf = '$self->dataSourceFilename';")
				: $self->PARAM->properties('prefix') ne ''
					? $c->add("my \$dsf = '@{[ $self->PARAM->properties('prefix') ]}/@{[ $self->dataSourceFilename ]}';")
					: $c->add(qq{my \$dsf = "\@{[ \$ENV{PEQUEL_TABLE_PATH} eq '' ? '' : \$ENV{PEQUEL_TABLE_PATH} . '/']}" . '@{[ $self->dataSourceFilename ]}';});

			$c->verboseMessage("Loading table @{[ $self->name ]} from \$dsf...");

			if ($self->persistent)
			{
				$c->addAll($self->codeConnect);
			}

			my $sort = "@{[ $self->PARAM->properties('sort_cmd') ]}";
			$sort .= " @{[ $self->PARAM->properties('sort_args') ]}";
			$sort .= " -u -t'|' -k @{[ $self->keyColumn ]}@{[ $self->keyType =~ /^NUMERIC/i ? 'n' : '' ]}";
			$sort .= " -T @{[ $self->PARAM->properties('sort_tmp_dir') ]}" if ($self->PARAM->properties('sort_tmp_dir'));

			($self->dataSourceFilename =~ /\.gz$|\.GZ$|\.z$|\.Z$|\.zip$/)
				? $c->addNonl("open(@{[ uc($self->name) ]}, \"@{[ $self->PARAM->properties('gzcat_cmd') ]} @{[ $self->PARAM->properties('gzcat_cmd') ]} \$dsf | $sort |\")")
				: $c->addNonl("open(@{[ uc($self->name) ]}, \"$sort \$dsf |\")");
			$c->add(' || die("Unable to open table source file $dsf");');

			$c->add("while (<@{[ uc($self->name) ]}>)");
			$c->openBlock("{");
				$c->add("chomp;");
				$c->add("my (\@flds) = split(\"[|]\", \$_, -1);");
				$c->addNonl("\$_TABLE_@{[ $self->name ]}\{\$flds[@{[ $self->keyColumn-1 ]}]} = ");
#<				$c->add("[ @{[ $self->fields->toArray > 1 ? '@' : '$' ]}flds[ @{[ join(',', map($_->column -1, $self->fields->toArray)) ]} ]];");
				($self->fields->size == 0)
					? $c->add("'1',")
					: ($self->fields->size == 1)
						? $c->add("\$flds[ @{[ join(',', map($_->column -1, $self->fields->toArray)) ]} ];")
						: $c->add("[ @{[ $self->fields->toArray > 1 ? '@' : '$' ]}flds[ @{[ join(',', map($_->column -1, $self->fields->toArray)) ]} ]];");
				$c->verboseMessage("Table @{[ $self->name ]} \$. records...", "\$. % 100000 == 0");
			$c->closeBlock;

			$c->verboseMessage("Table @{[ $self->name ]} loaded \$. records.");
			$c->add("close(@{[ uc($self->name) ]});");
			if ($self->persistent)
			{
				$c->add("untie(\%_TABLE_@{[ $self->name ]});");
				$c->addAll($self->codeConnect);
			}
			$c->add("return \\\%_TABLE_@{[ $self->name ]};");
		$c->closeBlock;
		$c->add;
		return $c;
	}

    sub codeVar : method 
	{ 
		my $self = shift; 
		my $keyfld = shift;		# keyfld can be an expression eg: another macro
		my $field = shift || undef;

		$field ? $self->setSequence($keyfld, $field) : $self->setSequence($keyfld);
		
		if ($self->persistent)
		{
			return defined($field)
				? '$$row_' . $self->name . "_$keyfld" . '{' . lc($field) . '}'
				: 'defined($row_' . $self->name . "_$keyfld)";
		}
		else
		{
			if ($keyfld =~ /[\(|\[|,]/)
			{
				return defined($field)
					? '${$$_TABLE_' . $self->name . '{qq{@{[ ' . $keyfld . ' ]}}}}[_T_' . $self->name . '_FLD_' . $field . ']'
					: '$$_TABLE_' . $self->name . '{qq{@{[ ' . $keyfld . ' ]}}}';
			}
			else
			{
				return defined($field)
					? '${$$_TABLE_' . $self->name . '{qq{' . $keyfld . '}}}[_T_' . $self->name . '_FLD_' . $field . ']'
					: '$$_TABLE_' . $self->name . '{qq{' . $keyfld . '}}';
			}
		}
	}

	sub dataSourceFieldNameList : method
	{
		my $self = shift;

		my $col = 0;
		my %flist;
		foreach my $f (sort { $a->column <=> $b->column } $self->fields->toArray) 
		{ 
			$flist{$f->column} = lc($f->name);
			next if ($self->PARAM->properties('table_drop_unused_fields'));
			while (++$col < $f->column)
			{
				$flist{$col} = "f$col";
			}
		}
		$flist{$self->keyColumn} = 'key';
		if (!$self->PARAM->properties('table_drop_unused_fields'))
		{
			while (++$col < $self->keyColumn)
			{
				$flist{$col} = "f$col";
			}
		}
		return map($flist{$_}, sort { $a <=> $b } keys %flist);
	}

	sub dataSourceFieldColumnList : method
	{
		my $self = shift;

		if ($self->PARAM->properties('table_drop_unused_fields'))
		{
			return sort { $a <=> $b } $self->keyColumn, map($_->column, $self->fields->toArray);
		}

		my $col = 0;
		my %flist;
		foreach my $f (sort { $a->column <=> $b->column } $self->fields->toArray) 
		{ 
			$flist{$f->column} = lc($f->name);
			next if ($self->PARAM->properties('table_drop_unused_fields'));
			while (++$col < $f->column)
			{
				$flist{$col} = "f$col";
			}
		}
		$flist{$self->keyColumn} = 'key';
		if (!$self->PARAM->properties('table_drop_unused_fields'))
		{
			while (++$col < $self->keyColumn)
			{
				$flist{$col} = "f$col";
			}
		}
		return sort { $a <=> $b } keys %flist;
	}

	sub selectList : method
	{
		my $self = shift;
		return "@{[ join(', ', 'key', map(lc, $self->fields->toArrayName)) ]}";
	}

}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Table::External::Pequel;
	use base qw(ETL::Pequel::Type::Table::External);

	sub new : method
	{
        my $self = shift;
        my $class = ref($self) || $self;
        my %params = @_;
        $self = $class->SUPER::new
		(
			@_
		);
        bless($self, $class);
		return $self;
	}

	sub codeLoadTable : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		my $pqlt = $self->PARAM->pequel_script->find($self->dataSourceFilename)->value;
		my $delim = $pqlt->PARAM->properties('output_delimiter');

		$c->addComment("++++++ Table @{[ $self->name ]} --> Type :@{[ ref($self) ]} ++++++");

		$c->add("sub LoadTable@{[ $self->name ]}");
		$c->openBlock("{");
			$c->add("my \%_TABLE_@{[ $self->name ]};");
			$c->verboseMessage("Loading table @{[ $self->name ]} from @{[ $self->dataSourceFilename ]}...");

			$c->add("my \$pid = open(@{[ uc($self->name) ]}, '-|'); # Fork");
			$c->add("my \$count=0;");
			$c->add("if (\$pid) # Parent");
			$c->openBlock("{");
				$c->add("while (<@{[ uc($self->name) ]}>)");
				$c->openBlock("{");
					$c->add("chomp;");
					$c->add("my (\@flds) = split(\"[$delim]\", \$_, -1);");
					$c->addNonl("\$_TABLE_@{[ $self->name ]}\{\$flds[@{[ $self->keyColumn-1 ]}]} = ");
					($self->fields->size == 0)
						? $c->add("'1',")
						: ($self->fields->size == 1)
							? $c->add("\$flds[ @{[ join(',', map($_->column -1, $self->fields->toArray)) ]} ];")
							: $c->add("[ @{[ $self->fields->toArray > 1 ? '@' : '$' ]}flds[ @{[ join(',', map($_->column -1, $self->fields->toArray)) ]} ]];");
					$c->verboseMessage("Table @{[ $self->name ]} \$. records...", "\$. % 100000 == 0");
				$c->closeBlock;
				$c->add("\$count=\$.;");
				$c->add("close(@{[ uc($self->name) ]});");
			$c->closeBlock;
			$c->add("else # Child");
			$c->openBlock("{");
			$c->add("&p_LoadTable@{[ $self->name ]}::LoadTable@{[ $self->name ]};");
			$c->add("exit(0);");
			$c->closeBlock;

			$c->verboseMessage("Table @{[ $self->name ]} loaded \$count records.");
			$c->add("close(@{[ uc($self->name) ]});");
			$c->add("return \\\%_TABLE_@{[ $self->name ]};");
		$c->closeBlock;
		$c->add;
		$c->openBlock("{");
			$c->add("package p_LoadTable@{[ $self->name ]};");
			$c->add("sub LoadTable@{[ $self->name ]}");
			$c->openBlock("{");
			$c->addAll($pqlt->PARAM->ENGINE);
			$c->closeBlock;
		$c->closeBlock;
		return $c;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Table::External::Inline;
	use base qw(ETL::Pequel::Type::Table::External);

	our $this = __PACKAGE__;

	sub BEGIN
	{
		our @attr =
		qw(
			db
			keyFieldName
		);
		# keyFieldName -- Need to use this instead of literal 'key'

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
        $self = $class->SUPER::new
		(
			@_,
			type => $params{'type'} || 'inline',
			persistent => $params{'persistent'} || 0,
			merge => $params{'merge'} || 0,
		);
        bless($self, $class);
		return $self;
	}

    sub cacheRecs : method 
	{ 
		my $self = shift; 
		return $self->PARAM->properties('cache_recs') ? "[current_cache_rec]" : "";
	}

    sub codeInit : method 
	{ 
		my $self = shift; 
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		$c->add("\&LoadTable@{[ $_->name ]}; # Create database for @{[ $_->name ]}");
		return $c;
	}

    sub codeVar : method 
	{ 
		my $self = shift; 
		my $keyfld = shift;
		my $field = shift || undef;
			
		$field ? $self->setSequence($keyfld, $field) : $self->setSequence($keyfld);

		$self->PARAM->error->fatalError
			("[8006] Invalid key field spec for table @{[ $self->name ]}; no such input field ($keyfld)")
				if (!$self->PARAM->sections->exists('input section')->items->exists($keyfld));

		$self->PARAM->error->fatalError
			("[8007] No such field '$field' in table @{[ $self->name ]}")
				if (defined($field) && !$self->fields->exists($field));
		
		return defined($field)
			? "\$I_VAL[_I_@{[ $self->name ]}_${keyfld}_FLD_${field}]"
			: "\$I_VAL[_I_@{[ $self->name ]}_${keyfld}_FLD_KEY] ne ''";
	}

    sub codeClose : method 
	{ 
		my $self = shift; 
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		return $c;
	}

    sub codeFetchRow : method 
	{ 
		my $self = shift; 
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		return $c;
	}

    sub codePrepare : method 
	{ 
		my $self = shift; 
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		return $c;
	}
}
# ----------------------------------------------------------------------------------------------------
1;
