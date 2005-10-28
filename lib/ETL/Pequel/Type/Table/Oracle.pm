#!/usr/bin/perl
# ----------------------------------------------------------------------------------------------------
#  Name		: ETL::Pequel::Type::Table::Oracle.pm
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
# 17-3-2005		1.1-2		gaffie	password hiding when loading tables.
# ----------------------------------------------------------------------------------------------------
# TO DO		:
# ----------------------------------------------------------------------------------------------------
require 5.005_62;
use strict;
use attributes qw(get reftype);
use warnings;
use vars qw($VERSION $BUILD);
$VERSION = "1.1-2";
$BUILD = 'Thu Mar 17 10:56:39 EST 2005';

# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Table::Oracle;
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
			type => $params{'type'} || 'oracle',
			persistent => 1,
			merge => $params{'merge'} || 0,
			db_file_type => $params{'db_file_type'} || 'oracle',
		);
        bless($self, $class);

		$self->PARAM->dbtypes->exists('oracle')->add
		(
			ETL::Pequel::Type::Db::Oracle::Element->new
			(
				name => $params{'db_name'}, 
				username => $params{'username'},
				password => $params{'password'},
				PARAM => $self->PARAM
			)
		)
			unless ($self->PARAM->dbtypes->exists('oracle')->exists($params{'db_name'}));

		$self->PARAM->dbtypes->exists('oracle')->exists($params{'db_name'})->useList->add($self);
		$self->db($self->PARAM->dbtypes->exists('oracle')->exists($params{'db_name'}));
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

			$c->verboseMessage("Table @{[ $self->name ]}");
			$c->verboseMessage("-->datasource:@{[ $self->dataSourceFilename ]}...");
			$c->add("use DBI;");
			$c->add("my \$password = '@{[ $self->db->password ]}';");
			$c->add("if (\$password eq '')");
			$c->openBlock('{');
				$c->add("\$password = \`grep -iw @{[ $self->db->name ]} \\\$HOME/.password | grep -iw @{[ $self->db->username ]} | awk '{ print \\\$3 }'\`;");
				$c->add("chomp(\$password);");
			$c->closeBlock;
			$c->add("my \$dbh = DBI->connect");
			$c->openBlock('(');
				$c->addNonl("'dbi:Oracle:@{[ $self->db->name ]}', ");
				$c->add("'@{[ $self->db->username ]}', \$password, ");
				$c->add("{ RowCacheSize => 5000, RaiseError => 0, AutoCommit => 0 }");
			$c->closeBlock;
			$c->add("or die \"Cannot connect to @{[ $self->db->name ]}:\$!\";");

			$c->add("\$dbh->{PrintError} = 0;");

			$c->add("my \@tables = \$dbh->tables();");
			$c->add("map(s/\\\"//g, \@tables);");
			$c->add("my \$table_exists = grep(m/\\.@{[ $self->name ]}\$/i, \@tables);");
			if ($self->PARAM->properties('reload_tables'))
			{
				$c->add("if (\$table_exists)");
				$c->add("{");
				$c->over;
					$c->add("\$dbh->do(\"DROP TABLE @{[ $self->name ]}\");");
					$c->add("\$dbh->commit or die \$dbh->errstr;");
					$c->add("\@tables = \$dbh->tables();");
					$c->add("map(s/\\\"//g, \@tables);");
					$c->add("\$table_exists = grep(m/\\.@{[ $self->name ]}\$/i, \@tables);");
					$c->add("if (\$table_exists)");
					$c->add("{");
					$c->over;
						$c->verboseMessage("-->** Unable to drop table @{[ $self->name ]}!");
					$c->back;
					$c->add("}");
					$c->add("else");
					$c->add("{");
					$c->over;
						$c->verboseMessage("-->dropped table.");
					$c->back;
					$c->add("}");
				$c->back;
				$c->add("}");
			}
			$c->add("if (!\$table_exists)");
			$c->openBlock("{");
				$c->verboseMessage("-->creating database table...");

				$c->add("my \@flist =");
				$c->openBlock('(');
					map($c->add("'$_',"), $self->dataSourceFieldNameList);
					$c->endList;
				$c->closeBlock(');');

				$c->add("my \%flist =");
				$c->openBlock('(');
					map
					(
						$_ eq 'key' 
							? $c->add("$_ => '@{[ $self->keyType ]} @{[ $self->merge ? q{}: q{PRIMARY KEY} ]}',") 
							: $c->add("$_ => 'varchar2(128)',"), 
						$self->dataSourceFieldNameList
					);
					$c->endList;
				$c->closeBlock(');');

				$c->addNonl("\$dbh->do(\"CREATE TABLE @{[ $self->name ]} ");
				$c->add("( \@{[ join(',', map(qq{\$_ \$flist{\$_}}, \@flist)) ]} )\");");
				$c->add("\$dbh->commit or die \$dbh->errstr;");
				$c->add("open(CTL, '>@{[ $self->dbFilename ]}.ctl');");
#?				$c->add("print CTL \"\# Pequel generated oracle sql-loader control file\";");
				$c->add("print CTL \"LOAD\";");
				$c->add("print CTL \"append into table @{[ $self->name ]}\";");
				$c->add("print CTL \"FIELDS TERMINATED BY '|' TRAILING NULLCOLS\";");
				$c->add("print CTL \"(\";");
				$c->add("print CTL \"\@{[ join(qq{,\\n}, \@flist) ]}\";");
				$c->add("print CTL \")\";");
				$c->add("close(CTL);");

				my $cut = $self->PARAM->properties('table_drop_unused_fields')
					? "cut -d'|' -f@{[ join(',', $self->dataSourceFieldColumnList) ]}"
					: "";
				my $sort = "@{[ $self->PARAM->properties('sort_cmd') ]}";
				$sort .= " @{[ $self->PARAM->properties('sort_args') ]}";
				$sort .= " -u -t'|' -k @{[ $self->keyColumn ]}@{[ $self->keyType =~ /^NUMBER/i ? 'n' : '' ]}";
				$sort .= " -T @{[ $self->PARAM->properties('sort_tmp_dir') ]}" if ($self->PARAM->properties('sort_tmp_dir'));
				$sort .= " | $cut " if ($self->PARAM->properties('table_drop_unused_fields'));

				($self->dataSourceFilename =~ /\.gz$|\.GZ$|\.z$|\.Z$|\.zip$/)
					? $c->add("system(\"gunzip -c @{[ $self->dataSourceFilename ]} | $sort > @{[ $self->dbFilename ]}.dat\");")
					: $c->add("system(\"$sort @{[ $self->dataSourceFilename ]} > @{[ $self->dbFilename ]}.dat\");");

				$c->add("my \$sqlldr =");
				$c->over;
					$c->add("'control=@{[ $self->dbFilename ]}.ctl '");
					$c->add(". 'direct=true '");
					$c->add(". 'data=@{[ $self->dbFilename ]}.dat '");
					$c->add(". 'bad=@{[ $self->dbFilename ]}.bad '");
					$c->add(". 'log=@{[ $self->dbFilename ]}.log '");
					$c->add(". 'rows=@{[ $self->PARAM->properties('oracle_sqlldr_rows') ]}';");
#<					$c->add(". 'userid=@{[ $self->db->username ]}/@{[ $self->db->password ]}\@@{[ $self->db->name ]}';");
				$c->back;

				$c->add("open(SQLLDR, \">@{[ $self->dbFilename ]}.sqlldr\");");
				$c->add("print SQLLDR \"UID=@{[ $self->db->username ]}; export UID\";");
				$c->add("(\$password eq '')");
				$c->over;
					$c->add("? print SQLLDR \"PWD=\`grep -iw @{[ $self->db->name ]} \\\$HOME/.password | grep -iw @{[ $self->db->username ]} | awk '{ print \\\$3 }'\`; export PWD\"");
					$c->add(": print SQLLDR \"PWD=\$password; export PWD\";");
				$c->back;
				$c->add("print SQLLDR \"DB=@{[ $self->db->name ]}; export DB\";");
				$c->add("print SQLLDR \"sqlldr \$sqlldr \<<EOF\";");
				$c->add("print SQLLDR \"\\\$UID/\\\$PWD\\\@\\\$DB\";");
				$c->add("print SQLLDR \"EOF\";");
				$c->add("close(SQLLDR);");
				$c->add("system(\"sh @{[ $self->dbFilename ]}.sqlldr 2>&1 >@{[ $self->dbFilename ]}.err\");");

#<				$c->add("system(\"sqlldr \$sqlldr 2>&1 >@{[ $self->dbFilename ]}.err\");");

				if ($self->dataSourceFilename =~ /\.gz$|\.GZ$|\.z$|\.Z$|\.zip$/)
				{
					$c->add("system(\"rm @{[ $self->dbFilename ]}.dat\");");
				}
			$c->closeBlock;	# END if (!$exists)
			if ($self->PARAM->properties('display_table_stats'))
			{
				$c->add("my \$sth = \$dbh->prepare(\"SELECT count(1) FROM @{[ $self->name ]}\");");
				$c->add("\$sth->execute;");
				$c->add("my \$loaded = \$sth->fetchrow_array;");
				$c->verboseMessage("-->\$loaded records.");
				$c->add("\$sth->finish;");
			}
			$c->add("\$dbh->disconnect;");
		$c->closeBlock;	# END sub 
		return $c;
	}

    sub codeInlineClose : method 
	{ 
		my $self = shift; 
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		foreach my $k ($self->refKeyList->toArray)
		{
			$c->add("OCIHandleFree((dvoid *)stmthp_@{[ $self->name ]}_@{[ $k->name ]}, (ub4)OCI_HTYPE_STMT);");
		}
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
		return $c;
	}

    sub codeInlinePragma : method 
	{ 
		my $self = shift; 
		my $c = ETL::Pequel::Code::C->new(PARAM => $self->PARAM);
		return $c;
	}

    sub codeInlinePrep : method 
	{ 
		my $self = shift; 
		my $c = ETL::Pequel::Code::C->new(PARAM => $self->PARAM);

		$c->add;
		$c->addComment("++++++ Table @{[ $self->name ]} --> Type :@{[ ref($self) ]} ++++++");

		foreach my $k ($self->refKeyList->toArray)
		{
			my $tblKey = "@{[ $self->name ]}_@{[ $k->name ]}";

			$c->add("if ((status = OCIHandleAlloc(envhp, (dvoid **)&stmthp_$tblKey, OCI_HTYPE_STMT, 0, 0)) != OCI_SUCCESS)");
			$c->add("{");
			$c->over;
				$c->add("oracle_checkerr(errhp, status, (text*)\"allocate statement handle $tblKey\");");
			$c->back;
			$c->add("}");

			$c->add
			(
				"text *sql_$tblKey = (text*)\"select @{[ $self->selectList ]} from @{[ $self->name ]} "
				. ($self->merge ? "FULL" : "where key = :key")
##				. ($self->merge ? "order by key" : "where key = :key")
				. '";'
			);
			$c->add("if ((status = OCIStmtPrepare(stmthp_$tblKey, errhp, sql_$tblKey, ");
			$c->over;
				$c->add("strlen((const char*)sql_$tblKey), OCI_NTV_SYNTAX, 0)) != OCI_SUCCESS)");
			$c->back;
			$c->add("{");
			$c->over;
				$c->add("oracle_checkerr(errhp, status, (text*)\"prepare statement $tblKey\");");
			$c->back;
			$c->add("}");

			if ($self->merge)
			{
				if ($self->PARAM->properties('oracle_prefetch_count'))
				{ 
					$c->add("if ((status = OCIAttrSet(stmthp_$tblKey, OCI_HTYPE_STMT, &prefetch, (ub4)0, OCI_ATTR_PREFETCH_ROWS, errhp)) != OCI_SUCCESS)");
					$c->add("{");
					$c->over;
						$c->add("oracle_checkerr(errhp, status, (text*)\"prefetch attribute $tblKey\");");
					$c->back;
					$c->add("}");
				}

				# set iters arg to 0 so first OCIStmntFetch will return 1st row...
				$c->add("if ((status = OCIStmtExecute(svchp_@{[ $self->db->name ]}, stmthp_$tblKey, errhp, ");
				$c->over;
					$c->add("(ub4)0, (ub4)0, (CONST OCISnapshot*)NULL, (OCISnapshot*)NULL, OCI_DEFAULT)) != OCI_SUCCESS)");
				$c->back;
				$c->add("{");
				$c->over;
					$c->add("oracle_checkerr(errhp, status, (text*)\"execute statement $tblKey\");");
				$c->back;
				$c->add("}");
			}

			$c->addAll($self->codeInlineDefine($k));
		}
		return $c;
	}

	sub codeInlineDefine
	{
		my $self = shift; 
		my $k = shift;
		my $tblKey = "@{[ $self->name ]}_@{[ $k->name ]}";
		my $c = ETL::Pequel::Code::C->new(PARAM => $self->PARAM);
		my $i=0;
		$c->add("if ((status = OCIDefineByPos(stmthp_$tblKey, &define_${tblKey}_KEY, errhp, ");
		$c->over;
		($self->keyType =~ /^NUMBER/)
			? $c->add("(ub4)@{[ ++$i ]}, &${tblKey}_KEY, sizeof(${tblKey}_KEY), (ub2)SQLT_INT, ")
			: $c->add("(ub4)@{[ ++$i ]}, ${tblKey}_KEY, STD_FLD_LEN+1, (ub2)SQLT_STR, ");
		$c->add("(void*)&indicator_${tblKey}_KEY, 0, 0, OCI_DEFAULT)) != OCI_SUCCESS)");
		$c->back;
		$c->add("{");
		$c->over;
			$c->add("oracle_checkerr(errhp, status, (text*)\"define ${tblKey}_KEY\");");
		$c->back;
		$c->add("}");
	
		foreach my $f ($self->fields->toArray)
		{
			$c->add("if ((status = OCIDefineByPos(stmthp_$tblKey, &define_${tblKey}_@{[ $f->name ]}, errhp, ");
			$c->over;
			$c->add("(ub4)@{[ ++$i ]}, @{[ $tblKey ]}_@{[ $f->name ]}, STD_FLD_LEN+1, (ub2)SQLT_STR, ");
			$c->add("(void*)&indicator_@{[ $tblKey ]}_@{[ $f->name ]}, 0, 0, OCI_DEFAULT)) != OCI_SUCCESS)");
			$c->back;
			$c->add("{");
			$c->over;
				$c->add("oracle_checkerr(errhp, status, (text*)\"define @{[ $tblKey ]}_@{[ $f->name ]}\");");
			$c->back;
			$c->add("}");
		}
		return $c;
	}

	sub codeInlineReset : method
	{
		my $self = shift; 
		my $c = ETL::Pequel::Code::C->new(PARAM => $self->PARAM);
		foreach my $k ($self->refKeyList->toArray)
		{
			my $tblKey = "@{[ $self->name ]}_@{[ $k->name ]}";
			$c->addComment("++++++ Table @{[ $self->name ]} (@{[ $k->name ]}) --> Type :@{[ ref($self) ]} ++++++");
			if ($self->merge)
			{
				$c->add("static int count_$tblKey = 0;") if ($self->PARAM->properties('merge_optimize'));

				if ($self->PARAM->properties('oracle_use_merge_fetch_macro'))
				{
					($self->keyType =~ /^NUMBER/)
						? $c->add("_OracleMergeFetchNumeric(@{[ $self->name ]}_@{[ $k->name ]},_I_@{[ $k->name ]})")
						: $c->add("_OracleMergeFetchString(@{[ $self->name ]}_@{[ $k->name ]},_I_@{[ $k->name ]})");
				}
				else
				{
					$c->add("if (${tblKey}_KEY == 0 && last_step_$tblKey == OCI_SUCCESS)");
					$c->open("{");
						$c->add("last_step_$tblKey ");
						$c->over;
							$c->add("= OCIStmtFetch(stmthp_$tblKey, errhp, (ub4) 1, (ub4) OCI_FETCH_NEXT, (ub4) 0);");
						$c->back;
					$c->close("}");
		
					$c->add("while");
					$c->open("(");
						$c->add("last_step_$tblKey == OCI_SUCCESS");
					$c->add("&& ${tblKey}_KEY != 0");
		
					($self->keyType =~ /^NUMBER/)
						? $c->add("&& atol(fields@{[ $self->cacheRecs ]}\[_I_@{[ $k->name ]}]) > ${tblKey}_KEY")
						: $c->add("&& strcmp(fields@{[ $self->cacheRecs ]}\[_I_@{[ $k->name ]}], ${tblKey}_KEY) > 0");
		
					$c->add("&& (last_step_$tblKey ");
					$c->over;
						$c->add("= OCIStmtFetch(stmthp_$tblKey, errhp, (ub4) 1, (ub4) OCI_FETCH_NEXT, (ub4) 0)) == OCI_SUCCESS");
					$c->back;
					$c->close(")");
					$c->open("{");
						$c->add("if (${tblKey}_KEY == 0) break;");
		
					($self->keyType =~ /^NUMBER/)
						? $c->add("if (atol(fields@{[ $self->cacheRecs ]}\[_I_@{[ $k->name ]}]) <= ${tblKey}_KEY) break;")
						: $c->add("if (strcmp(fields@{[ $self->cacheRecs ]}\[_I_@{[ $k->name ]}], ${tblKey}_KEY) <= 0) break;");
		
					if ($self->PARAM->properties('merge_optimize'))
					{
						$c->add("if (++count_$tblKey > @{[ $self->PARAM->properties('merge_optimize_count') ]})");
						$c->add("{");
						$c->over;
							$c->add("sprintf(sql, \"select @{[ $self->selectList ]} from @{[ $self->name ]} where key >= @{[ $self->keyType =~ /^NUMBER/ ? '%s' : '\'%s\'' ]}\", ");
#<							$c->add("sprintf(sql, \"select @{[ $self->selectList ]} from @{[ $self->name ]} where key >= @{[ $self->keyType =~ /^NUMBER/ ? '%s' : '\'%s\'' ]} order by key\", ");
							$c->over;
							$c->add("fields@{[ $self->cacheRecs ]}\[_I_@{[ $k->name ]}]);");
							$c->back;
							$c->add("if ((status = OCIStmtPrepare(stmthp_$tblKey, errhp, sql, ");
							$c->over;
								$c->add("strlen((const char*)sql), OCI_NTV_SYNTAX, 0)) != OCI_SUCCESS)");
							$c->back;
							$c->add("{");
							$c->over;
								$c->add("oracle_checkerr(errhp, status, (text*)\"prepare statement @{[ $self->name ]}_@{[ $k->name ]}\");");
							$c->back;
							$c->add("}");
		
							$c->addAll($self->codeInlineDefine($k));
		
							if ($self->PARAM->properties('oracle_prefetch_count'))
							{ 
								$c->add("if ((status = OCIAttrSet(stmthp_$tblKey, OCI_HTYPE_STMT, &prefetch, (ub4)0, OCI_ATTR_PREFETCH_ROWS, errhp)) != OCI_SUCCESS)");
								$c->add("{");
								$c->over;
									$c->add("oracle_checkerr(errhp, status, (text*)\"prefetch attribute @{[ $self->name ]}_@{[ $k->name ]}\");");
								$c->back;
								$c->add("}");
							}
		
							$c->add("if ((status = OCIStmtExecute(svchp_@{[ $self->db->name ]}, stmthp_$tblKey, errhp, ");
							$c->over;
								$c->add("(ub4)0, (ub4)0, (CONST OCISnapshot*)NULL, (OCISnapshot*)NULL, OCI_DEFAULT)) != OCI_SUCCESS)");
							$c->back;
							$c->add("{");
							$c->over;
								$c->add("oracle_checkerr(errhp, status, (text*)\"execute statement $tblKey\");");
							$c->back;
							$c->add("}");
						
						    $c->add("count_$tblKey = 0;");
						$c->back;
						$c->add("}");
					}
					$c->close(	"}");
		
					($self->keyType =~ /^NUMBER/)
						? $c->add("if (${tblKey}_KEY != 0 && atol(fields@{[ $self->cacheRecs ]}\[_I_@{[ $k->name ]}]) == ${tblKey}_KEY)")
						: $c->add("if (${tblKey}_KEY != 0 && strcmp(fields@{[ $self->cacheRecs ]}\[_I_@{[ $k->name ]}], ${tblKey}_KEY) == 0)");
				}
			}
			else
			{
#?				$c->add("if ((status = OCIBindByName(stmthp_$tblKey, &bndhp_$tblKey, errhp, (text *)\":key\", (sb4)strlen((char *)\":key\"), ");
    			$c->add("if ((status = OCIBindByPos(stmthp_$tblKey, &bndhp_$tblKey, errhp, (ub4)1,");
				$c->over;
					$c->add("(dvoid *)fields@{[ $self->cacheRecs ]}\[_I_@{[ $k->name ]}], (sb4)strlen(fields@{[ $self->cacheRecs ]}\[_I_@{[ $k->name ]}])+1, SQLT_STR, ");
        			$c->add("(dvoid *)0, (ub2 *)0, (ub2 *)0, (ub4)0, (ub4 *)0, (ub4)OCI_DEFAULT)) != OCI_SUCCESS)");
				$c->back;
    			$c->add("{");
				$c->over;
        			$c->add("(void) OCIErrorGet(errhp, (ub4)1, (text *)NULL, &errcode, errbuf, (ub4) sizeof(errbuf), OCI_HTYPE_ERROR);");
					$c->add("oracle_checkerr(errhp, status, (text*)\"bind @{[ $self->name ]}_@{[ $k->name ]}\");");
				$c->back;
				$c->add("}");
#<				$c->add("else fprintf(stderr, \"bind done\\n\");");

				$c->add("if (OCIStmtExecute(svchp_@{[ $self->db->name ]}, stmthp_$tblKey, errhp, (ub4) 1, ");
				$c->over;
					$c->add("(ub4) 0, (CONST OCISnapshot *) NULL, (OCISnapshot *) NULL, OCI_DEFAULT) == OCI_SUCCESS)");
				$c->back;
			}
			$c->open("{");
				$c->add("pthread_mutex_lock(&g_mutex_I_VAL);") if ($self->PARAM->properties('num_threads'));
				if ($self->PARAM->properties('use_av_store_macro'))
				{
					($self->keyType =~ /^NUMBER/)
						? $c->add("_av_store_numeric($tblKey, KEY);")
						: $c->add("_av_store_string($tblKey, KEY);");
				}
				else
				{
					$c->add("if (!indicator_${tblKey}_KEY)");
					$c->over;
						$c->add("av_store(I_VAL, _I_${tblKey}_FLD_KEY, ");
						$c->over;
							$c->add("newSVpvf(\"%@{[ $self->keyType =~ /^NUMBER/ ? 'ld' : 's' ]}\", ${tblKey}_KEY));");
						$c->back;
					$c->back;
				}

				foreach my $rf (grep($_->name eq '_KEY' && $_->inputField->refTableList->size, $k->referenceFieldList->toArray))
				{
					$c->add("if (fields@{[ $self->cacheRecs ]}\[_I_@{[ $rf->inputField->name ]}] == 0)"); 
					$c->over;
					($self->keyType =~ /^NUMBER/)
						? $c->add("fields@{[ $self->cacheRecs ]}\[_I_@{[ $rf->inputField->name ]}] = &${tblKey}_KEY;")
						: $c->add("fields@{[ $self->cacheRecs ]}\[_I_@{[ $rf->inputField->name ]}] = &${tblKey}_KEY\[0];"); 
					$c->back;
				}
				foreach my $f ($self->fields->toArray)
				{
					if ($self->PARAM->properties('use_av_store_macro'))
					{
						$c->add("_av_store_string($tblKey, @{[ $f->name ]});");
					}
					else
					{
						$c->add("if (!indicator_${tblKey}_@{[ $f->name ]})");
						$c->over;
							$c->add("av_store(I_VAL, _I_${tblKey}_FLD_@{[ $f->name ]}, ");
							$c->over;
								$c->add("newSVpvf(\"%s\", ${tblKey}_@{[ $f->name ]}));");
							$c->back;
						$c->back;
					}
					foreach my $rf (grep($_->name eq $f->name && $_->inputField->refTableList->size, $k->referenceFieldList->toArray))
					{
						$c->add("if (fields@{[ $self->cacheRecs ]}\[_I_@{[ $rf->inputField->name ]}] == 0)"); 
						$c->over;
						$c->add("fields@{[ $self->cacheRecs ]}\[_I_@{[ $rf->inputField->name ]}] = &${tblKey}_@{[ $f->name ]}\[0];"); 
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
		foreach my $k ($self->refKeyList->toArray)
		{
			$c->add("static OCIStmt\* stmthp_@{[ $self->name ]}_@{[ $k->name ]} = (OCIStmt*)0;");
			$c->add("static OCIBind\* bndhp_@{[ $self->name ]}_@{[ $k->name ]} = (OCIBind*)0;") if (!$self->merge);
			$c->add("static OCIDefine\* define_@{[ $self->name ]}_@{[ $k->name ]}_KEY = (OCIDefine*)0;");
			$c->add("static sb2 indicator_@{[ $self->name ]}_@{[ $k->name ]}_KEY;");

			($self->keyType =~ /^NUMBER/)
				? $c->add("static sword @{[ $self->name ]}_@{[ $k->name ]}_KEY;")
				: $c->add("static text @{[ $self->name ]}_@{[ $k->name ]}_KEY\[STD_FLD_LEN];");

			foreach my $f ($self->fields->toArray)
			{
				$c->add("static text @{[ $self->name ]}_@{[ $k->name ]}_@{[ $f->name ]}\[STD_FLD_LEN];");
				$c->add("static OCIDefine\* define_@{[ $self->name ]}_@{[ $k->name ]}_@{[ $f->name ]} = (OCIDefine*)0;");
				$c->add("static sb2 indicator_@{[ $self->name ]}_@{[ $k->name ]}_@{[ $f->name ]};");
#?				$c->add("static OCIString *vstr_@{[ $self->name ]}_@{[ $k->name ]}_@{[ $f->name ]} = (OCIString *)0;");
			}
			$c->add;
		}

		return $c;
	}

    sub codeInlineValueDecl : method 
	{ 
		my $self = shift; 
		my $c = ETL::Pequel::Code::C->new(PARAM => $self->PARAM);
		foreach my $k ($self->refKeyList->toArray)
		{
#<			$c->add("static text *pazValue_@{[ $self->name ]}_@{[ $k->name ]};");
			$c->add("static int last_step_@{[ $self->name ]}_@{[ $k->name ]} = OCI_SUCCESS;")
				if ($self->merge);
		}
		return $c;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Table::Oracle::Merge;
	use ETL::Pequel::Collection;
	use ETL::Pequel::Type;
	use ETL::Pequel::Type::Table;
	use base qw(ETL::Pequel::Type::Table::Oracle);

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
