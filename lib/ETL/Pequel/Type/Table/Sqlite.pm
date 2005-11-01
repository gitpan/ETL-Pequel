#!/usr/bin/perl
# ----------------------------------------------------------------------------------------------------
#  Name		: ETL::Pequel::Type::Table::Sqlite.pm
#  Created	: 25 March 2005
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
$VERSION = "2.4-3";
$BUILD = 'Tuesday November  1 08:45:13 GMT 2005';
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Table::Sqlite;
	use ETL::Pequel::Collection;
	use ETL::Pequel::Type;
	use ETL::Pequel::Type::Table;
	use base qw(ETL::Pequel::Type::Table::External::Inline);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_,
			type => $params{'type'} || 'sqlite',
			persistent => 1,
			merge => $params{'merge'} || 0,
			db_file_type => $params{'db_file_type'} || 'sqlt',
		);
		bless($self, $class);

		$self->PARAM->dbtypes->exists('sqlite')->add
		(
			ETL::Pequel::Type::Db::Sqlite::Element->new
			(
				name => 'sqlite_abstract_db', 
				PARAM => $self->PARAM
			)
		)
			unless ($self->PARAM->dbtypes->exists('sqlite')->exists('sqlite_abstract_db'));
	
		$self->PARAM->dbtypes->exists('sqlite')->exists('sqlite_abstract_db')->useList->add($self);
		$self->db($self->PARAM->dbtypes->exists('sqlite')->exists('sqlite_abstract_db'));
		$self->PARAM->dbtypes->tableList->add($self);
		
		$self->PARAM->properties('use_inline', 1);

		return $self;
	}

	sub codeLoadTable : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		$c->addComment("++++++ Table @{[ $self->name ]} --> Type :@{[ ref($self) ]} ++++++");

		$c->add("sub LoadTable@{[ $self->name ]}");
		$c->openBlock('{');
			$c->add("my \%_TABLE_@{[ $self->name ]};");
		
			$c->verboseMessage("Loading (@{[ $self->merge ? 'merge' : 'lookup' ]}) table @{[ $self->name ]} from @{[ $self->dataSourceFilename ]}...");
			$c->add("my \$exists = (-e \"@{[ $self->dbFilename ]}\");");
			if ($self->PARAM->properties('reload_tables'))
			{
				$c->add("if (\$exists)");
				$c->add("{");
				$c->over;
					$c->verboseMessage("Drop table @{[ $self->name ]}...");
					$c->add("system(\"rm @{[ $self->dbFilename ]}\");");
					$c->add("if ((\$exists = (-e \"@{[ $self->dbFilename ]}\")) != 0)");
					$c->add("{");
					$c->over;
						$c->verboseMessage("** Unable to drop table @{[ $self->name ]}!");
					$c->back;
					$c->add("}");
				$c->back;
				$c->add("}");
			}
			$c->add("return if (\$exists);") 
				unless ($self->PARAM->properties('display_table_stats'));
			$c->add("use DBI;");
			$c->add("my \$dbh = DBI->connect");
			$c->openBlock('(');
				$c->add("\"dbi:SQLite:dbname=@{[ $self->dbFilename ]}\", '', '', ");
				$c->add("{ RowCacheSize => 5000, RaiseError => 0, AutoCommit => 0 }");
			$c->closeBlock;
			$c->add("or die \"Cannot open @{[ $self->dbFilename ]}:\$!\";");

			$c->add("\$dbh->{PrintError} = 0;");
			$c->add("\$dbh->do(\"PRAGMA synchronous=OFF\");");
			$c->add("\$dbh->do(\"PRAGMA count_changes=OFF\");");
			$c->add("\$dbh->do(\"PRAGMA full_column_names=OFF\");");
#?			$c->add("\$dbh->do(\"PRAGMA cache_size=10000\");");
#?			$c->add("\$dbh->do(\"PRAGMA temp_store=2\");");
#?			$c->add("\$dbh->do(\"PRAGMA show_datatypes=OFF\");");
			$c->add("my \$sqlite = \$dbh->{sqlite_version};");

			$c->add("if (!\$exists)");
			$c->openBlock("{");
				$c->verboseMessage("Creating table @{[ $self->name ]} from @{[ $self->dataSourceFilename ]}...");

				$c->add("my \@flist =");
				$c->openBlock('(');
					map($c->add("'$_',"), $self->dataSourceFieldNameList);
#<					$c->add("'key',");
#<					map($c->add("'@{[ lc($_->name) ]}',"), $self->fields->toArray);
					$c->endList;
				$c->closeBlock(');');

				$c->add("my \%flist =");
				$c->openBlock('(');
					map
					(
						$_ eq 'key' 
							? $c->add("$_ => '@{[ $self->keyType ]} NOT NULL PRIMARY KEY',") 
							: $c->add("$_ => 'varchar',"), 
						$self->dataSourceFieldNameList
					);
					$c->endList;
				$c->closeBlock(');');

				$c->addNonl("\$dbh->do(\"CREATE TABLE @{[ $self->name ]} ");
				$c->add("( \@{[ join(',', map(qq{\$_ \$flist{\$_}}, \@flist)) ]} )\");");
				$c->add("\$dbh->commit;");
				$c->verboseMessage
				("Loading sqlite (v\$sqlite) table @{[ $self->name ]} from @{[ $self->dataSourceFilename ]}...");

#?				my $cut = $self->PARAM->properties('table_drop_unused_fields')
#?					? "cut -d'|' -f@{[ join(',', sort { $a <=> $b } keys %flist) ]}"
#?					: "";
				my $sort = "@{[ $self->PARAM->properties('sort_cmd') ]}";
				$sort .= " @{[ $self->PARAM->properties('sort_args') ]}";
				$sort .= " -u -t'|' -k @{[ $self->keyColumn ]}@{[ $self->keyType =~ /^NUMBER/i ? 'n' : '' ]}";
				$sort .= " -T @{[ $self->PARAM->properties('sort_tmp_dir') ]}" if ($self->PARAM->properties('sort_tmp_dir'));

				($self->dataSourceFilename =~ /\.gz$|\.GZ$|\.z$|\.Z$|\.zip$/)
					? $c->add("open(@{[ uc($self->name) ]}, \"gzcat @{[ $self->dataSourceFilename ]} | $sort |\");")
					: $c->add("open(@{[ uc($self->name) ]}, \"$sort @{[ $self->dataSourceFilename ]} |\");");

				$c->add("\$dbh->do(\"BEGIN;\");");
				$c->addNonl("my \$sth = \$dbh->prepare(\"INSERT INTO @{[ $self->name ]} ");
				$c->add("( \@{[ join(', ', \@flist) ]} ) VALUES( \@{[ join(', ', map('?', \@flist)) ]} )\");");

				$c->add("while (<@{[ uc($self->name) ]}>)");
				$c->openBlock;
					$c->add("chomp;");
					$c->add("my (\@flds) = split(\"[|]\", \$_, -1);");	
					$c->addNonl("\$sth->execute(\$flds[@{[ $self->keyColumn -1 ]}], ");
					$c->add("@{[ $self->fields->toArray > 1 ? '@' : '$' ]}flds[ @{[ join(',', map($_->column -1, $self->fields->toArray)) ]} ]);");
					$c->verboseMessage("Table @{[ $self->name ]} loaded \$. records...", "\$. % 100000 == 0");
				$c->closeBlock;
				$c->add("close(@{[ uc($self->name) ]});");
				$c->add("\$sth->finish;");
				$c->add("\$dbh->commit;");
			$c->closeBlock;	# END if (!$exists)
			if ($self->PARAM->properties('verbose'))
			{
				$c->add("my \$sth = \$dbh->prepare(\"SELECT count(1) FROM @{[ $self->name ]}\");");
				$c->add("\$sth->execute;");
				$c->add("my \$loaded = \$sth->fetchrow_array;");
				$c->add("\$sth->finish;");
				$c->add("\$dbh->disconnect;");
				$c->verboseMessage("\$loaded records for table @{[ $self->name ]}.");
			}
		$c->closeBlock;	# END sub 
		return $c;
	}

    sub codeInlineClose : method 
	{ 
		my $self = shift; 
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		$c->addNonl("sqlite_exec(db_@{[ $self->name ]}, \"END;\", 0, 0, 0);");
		$c->add    ("sqlite_close(db_@{[ $self->name ]});");
		return $c;
	}

    sub codeInlineInit : method 
	{ 
		my $self = shift; 
		my $c = ETL::Pequel::Code::C->new(PARAM => $self->PARAM);
		return $c;
	}

    sub codeInlineOpen : method 
	{ 
		my $self = shift; 
		my $c = ETL::Pequel::Code::C->new(PARAM => $self->PARAM);

		$c->addBar;
		$c->addComment("++++++ Table @{[ $self->name ]} --> Type :@{[ ref($self) ]} ++++++");
		$c->add("if ((db_@{[ $self->name ]} = sqlite_open_db(\"@{[ $self->dbFilename ]}\")) == 0)");
		$c->open("{");
		$c->add("return 0;");
		$c->close("}");
		return $c;
	}

    sub codeInlinePragma : method 
	{ 
		my $self = shift; 
		my $c = ETL::Pequel::Code::C->new(PARAM => $self->PARAM);

		my %SQLITE_PRAGMA = 
		(
			synchronous => 'OFF',
			count_changes => 'OFF',
			empty_result_callbacks => 'OFF',
			full_column_names => 'OFF',
			show_datatypes => 'OFF',
#			cache_size => '500',
#			temp_store => '2',
		);
		$c->addComment("++++++ Table @{[ $self->name ]} --> Type :@{[ ref($self) ]} ++++++");
		foreach my $pragma (sort keys %SQLITE_PRAGMA)
		{
			$c->add("if ((ret = sqlite_exec(db_@{[ $self->name ]}, \"PRAGMA @{[ $pragma ]} = @{[ $SQLITE_PRAGMA{$pragma} ]};\", 0, 0, &pzErrMsg)) != SQLITE_OK)");
			$c->open("{");
			$c->add("fprintf(stderr, \"** db_@{[ $self->name ]}: Cannot execute PRAGMA @{[ $pragma ]}=@{[ $SQLITE_PRAGMA{$pragma} ]} (%d-%s)\\n\", ret, pzErrMsg);");
			$c->add("freemem(pzErrMsg);");
			$c->add("return 0;");
			$c->close("}");
		}
		return $c;
	}

    sub codeInlinePrep : method 
	{ 
		my $self = shift; 
		my $c = ETL::Pequel::Code::C->new(PARAM => $self->PARAM);

		$c->addComment("++++++ Table @{[ $self->name ]} --> Type :@{[ ref($self) ]} ++++++");
		$c->add("if ((ret = sqlite_exec(db_@{[ $self->name ]}, \"BEGIN TRANSACTION ON CONFLICT ABORT;\", 0, 0, &pzErrMsg)) != SQLITE_OK)");
		$c->open("{");
		$c->add("fprintf(stderr, \"** db_@{[ $self->name ]}: Cannot execute BEGIN statement (%d-%s)\\n\", ret, pzErrMsg);");
		$c->add("freemem(pzErrMsg);");
		$c->add("return 0;");
		$c->close("}");

		foreach ($self->refKeyList->toArray)
		{
			if ($self->merge)
			{
				$c->add("sprintf(sql, \"select @{[ $self->selectList ]} from @{[ $self->name ]} order by key\");");
			}
			else
			{
				$c->add("sprintf(sql, \"select @{[ $self->selectList ]} from @{[ $self->name ]} where key = ?\");");
			}
			$c->add("if (sqlite_compile(db_@{[ $self->name ]}, sql, 0, &ppVm_@{[ $self->name ]}_@{[ $_->name ]}, &pzErrMsg) != SQLITE_OK)");
			$c->open("{");
			$c->add("fprintf(stderr, \"** Error compiling sql for db_@{[ $self->name ]}\->@{[ $self->name ]}_@{[ $_->name ]} (%s)\\n\", pzErrMsg);");
			$c->add("return 0;");
			$c->close("}");
		}
		return $c;
	}

	sub codeInlineReset : method
	{
		my $self = shift; 
		my $c = ETL::Pequel::Code::C->new(PARAM => $self->PARAM);
		foreach my $k ($self->refKeyList->toArray)
		{
			my $pazValue = "pazValue_@{[ $self->name ]}_@{[ $k->name ]}";
			my $ppVm = "ppVm_@{[ $self->name ]}_@{[ $k->name ]}";

			$c->addComment("++++++ Table @{[ $self->name ]} (@{[ $k->name ]}) --> Type :@{[ ref($self) ]} ++++++");
			if ($self->merge)
			{
				$c->add("static int count_@{[ $self->name ]}_@{[ $k->name ]} = 0;") if ($self->PARAM->properties('sqlite_merge_optimize'));
				$c->add("if ($pazValue == 0 && last_step_@{[ $self->name ]}_@{[ $k->name ]} == SQLITE_ROW)");
				$c->open("{");
				$c->add(	"last_step_@{[ $self->name ]}_@{[ $k->name ]} = sqlite_step($ppVm, &pN, &$pazValue, 0);");
				$c->close("}");

				$c->add(	"while");
				$c->open(	"(");
				$c->add(		"last_step_@{[ $self->name ]}_@{[ $k->name ]} == SQLITE_ROW");
				$c->add(		"&& $pazValue != 0");
				$c->add(		"&& strcmp(fields@{[ $self->cacheRecs ]}\[_I_@{[ $k->name ]}], $pazValue\[0]) > 0");
				$c->add(		"&& (last_step_@{[ $self->name ]}_@{[ $k->name ]} = sqlite_step($ppVm, &pN, &$pazValue, 0)) == SQLITE_ROW");
				$c->close(	")");
				$c->open(	"{");
				$c->add(		"if ($pazValue == 0) break;");
				$c->add(		"if (strcmp(fields@{[ $self->cacheRecs ]}\[_I_@{[ $k->name ]}], $pazValue\[0]) <= 0) break;");
				if ($self->PARAM->properties('sqlite_merge_optimize'))
				{
					$c->add("if (++count_@{[ $self->name ]}_@{[ $k->name ]} > @{[ $self->PARAM->properties('sqlite_merge_optimize_count') ]})");
					$c->add("{");
					$c->over;
						$c->add("sprintf(sql, \"select @{[ $self->selectList ]} from @{[ $self->name ]} where key >= @{[ $self->keyType eq 'INTEGER' ? '%s' : '\'%s\'' ]} order by key\", ");
						$c->over;
						$c->add("fields@{[ $self->cacheRecs ]}\[_I_@{[ $k->name ]}]);");
						$c->back;
					    $c->add("if (sqlite_compile(db_@{[ $self->name ]}, sql, 0, &$ppVm, &pzErrMsg) != SQLITE_OK)");
					    $c->add("{");
						$c->over;
							$c->add("fprintf(stderr, \"** Error re-compiling sql for db_@{[ $self->name ]}\->@{[ $self->name ]}_@{[ $k->name ]} (%s)\\n\", pzErrMsg);");
							$c->add("fprintf(stderr, \"** SQL:\%s;\\n\", sql);");
					        $c->add("return 0;");
						$c->back;
					    $c->add("}");
					    $c->add("count_@{[ $self->name ]}_@{[ $k->name ]} = 0;");
					$c->back;
					$c->add("}");
				}
				$c->close(	"}");
				$c->add(	"if ($pazValue != 0 && strcmp(fields@{[ $self->cacheRecs ]}\[_I_@{[ $k->name ]}], $pazValue\[0]) == 0)");
				$c->open(	"{");
				$c->add(		"pthread_mutex_lock(&g_mutex_I_VAL);") if ($self->PARAM->properties('num_threads'));
				$c->add(		"av_store(I_VAL, _I_@{[ $self->name ]}_@{[ $k->name ]}_FLD_KEY, newSVpvn($pazValue\[0], strlen($pazValue\[0])));");
			}
			else
			{
				$c->add("sqlite_reset($ppVm, 0);");
				$c->add("if ((ret = sqlite_bind($ppVm, 1, fields@{[ $self->cacheRecs ]}\[_I_@{[ $k->name ]}], -1, 0)) != SQLITE_OK)");
				$c->open("{");
				$c->add(	"fprintf(stderr, \"** Error binding to db_@{[ $self->name ]}\->@{[ $self->name ]}_@{[ $k->name ]} (%s)\\n\", sqlite_error_string(ret));");
				$c->add(	"croak(\"exiting\");");
				$c->close("}");
				$c->add("if (sqlite_step($ppVm, &pN, &$pazValue, 0) == SQLITE_ROW)");
				$c->open("{");
				$c->add(		"pthread_mutex_lock(&g_mutex_I_VAL);") if ($self->PARAM->properties('num_threads'));
				$c->add(		"av_store(I_VAL, _I_@{[ $self->name ]}_@{[ $k->name ]}_FLD_KEY, newSVpvn($pazValue\[0], strlen($pazValue\[0])));");
			}
			foreach my $rf (grep($_->name eq '_KEY' && $_->inputField->refTableList->size, $k->referenceFieldList->toArray))
			{
				$c->add("if (fields@{[ $self->cacheRecs ]}\[_I_@{[ $rf->inputField->name ]}] == 0)"); 
				$c->over;
				$c->add("fields@{[ $self->cacheRecs ]}\[_I_@{[ $rf->inputField->name ]}] = &$pazValue\[0]\[0];"); 
				$c->back;
			}
			foreach my $f ($self->fields->toArray)
			{
				$c->add("av_store(I_VAL, _I_@{[ $self->name ]}_@{[ $k->name ]}_FLD_@{[ $f->name ]}, newSVpvn($pazValue\[@{[ $f->number ]}], strlen($pazValue\[@{[ $f->number ]}])));");
				foreach my $rf (grep($_->name eq $f->name && $_->inputField->refTableList->size, $k->referenceFieldList->toArray))
				{
					$c->add("if (fields@{[ $self->cacheRecs ]}\[_I_@{[ $rf->inputField->name ]}] == 0)"); 
					$c->over;
					$c->add("fields@{[ $self->cacheRecs ]}\[_I_@{[ $rf->inputField->name ]}] = &$pazValue\[@{[ $f->number ]}]\[0];"); 
					$c->back;
				}
			}
			$c->add("pthread_mutex_unlock(&g_mutex_I_VAL);") if ($self->PARAM->properties('num_threads'));
			$c->close("}");
		}
		return $c;
	}
    
    sub codeInlineDecl : method 
	{ 
		my $self = shift; 
		my $c = ETL::Pequel::Code::C->new(PARAM => $self->PARAM);
		$c->add("static sqlite \*db_@{[ $self->name ]} = 0;");
		map
		(
			$c->add("sqlite_vm \*ppVm_@{[ $self->name ]}_@{[ $_->name ]};"),
			$self->refKeyList->toArray
		);
		$c->add;
		return $c;
	}

    sub codeInlineValueDecl : method 
	{ 
		my $self = shift; 
		my $c = ETL::Pequel::Code::C->new(PARAM => $self->PARAM);
		foreach my $k ($self->refKeyList->toArray)
		{
			$c->add("static const char **pazValue_@{[ $self->name ]}_@{[ $k->name ]};");
			$c->add("static int last_step_@{[ $self->name ]}_@{[ $k->name ]} = SQLITE_ROW;")
				if ($self->merge);
		}
		return $c;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Table::Sqlite::Merge;
	use base qw(ETL::Pequel::Type::Table::Sqlite);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new
		(
			@_,
			merge => 1,
		);
		bless($self, $class);
		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
1;
