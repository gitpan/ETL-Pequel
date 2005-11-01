#!/usr/bin/perl
# ----------------------------------------------------------------------------------------------------
#  Name		: ETL::Pequel::Engine::Inline.pm
#  Created	: 15 March 2005
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
# 15/09/2005	2.3-1		gaffie	Fixed code generation when using 'use_inline' with input_file specified.
# 31/08/2005	2.2-8		gaffie	Added o_inline_lddlflags
# 31/08/2005	2.2-8		gaffie	Added o_inline_make
# 31/08/2005	2.2-8		gaffie	Added o_inline_libs, o_inline_inc.
# 31/08/2005	2.2-8		gaffie	Replaced o_inline_parse_input_quotes by o_input_delimiter_extra.
# 30/08/2005	2.2-8		gaffie	added: inline_ccflags, inline_optimize
# 26/08/2005	2.2-8		gaffie	Added o_inline_parse_input_quotes implementation via readsplit()
# 26/08/2005	2.2-8		gaffie	Added codeInlineReadSplitQuoted
# 26/08/2005	2.2-8		gaffie	Updated codeInlineReadSplit -- was missing last fld when no delim at eol
# 26/08/2005	2.2-8		gaffie	Fixed Config=>NAME -- subst '/' by '::'.
# 26/08/2005	2.2-8		gaffie	Added inline_cc option.
# ----------------------------------------------------------------------------------------------------
# TO DO:
# ----------------------------------------------------------------------------------------------------
require 5.005_62;
use strict;
use attributes qw(get reftype);
use warnings;
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Engine::Inline;
	use ETL::Pequel::Code; #+++++
	use base qw(ETL::Pequel::Code);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		$self = $class->SUPER::new(@_);
		bless($self, $class);
		return $self;
	}

	sub generate : method
	{
		my $self = shift;

		$self->addBar;
		$self->addComment("***** I N L I N E *****");
		$self->add("use Inline");
		$self->over;
		$self->add("C => Config =>");
		my $iname = $self->PARAM->properties('script_name');
		$iname =~ s/\//::/g;
		$iname =~ s/-/_/g;
		$iname = lc($iname);
		$iname =~ s/\.pql$//;
		$self->add("NAME => '@{[ $iname ]}',"); # BUG: name must not include path.
		$self->add("CC => '@{[ $self->PARAM->properties('inline_cc') ]}',");
		$self->add("MAKE => '@{[ $self->PARAM->properties('inline_make') ]}',") if ($self->PARAM->properties('inline_make'));
		$self->add("LDDLFLAGS => '@{[ $self->PARAM->properties('inline_lddlflags') ]}',") if ($self->PARAM->properties('inline_lddlflags'));
		$self->add("CLEAN_AFTER_BUILD => '@{[ $self->PARAM->properties('inline_clean_after_build') ]}',");
		$self->add("CLEAN_BUILD_AREA => '@{[ $self->PARAM->properties('inline_clean_build_area') ]}',");
		$self->add("PRINT_INFO => '@{[ $self->PARAM->properties('inline_print_info') ]}',");
		$self->add("BUILD_NOISY => '@{[ $self->PARAM->properties('inline_build_noisy') ]}',");
		$self->add("BUILD_TIMERS => '@{[ $self->PARAM->properties('inline_build_timers') ]}',");
		$self->add("FORCE_BUILD => '@{[ $self->PARAM->properties('inline_force_build') ]}',");
		$self->add("DIRECTORY => '@{[ $self->PARAM->properties('inline_directory') ]}',")
			if ($self->PARAM->properties('inline_directory') ne '');
		$self->addNonl("LIBS => '");
		$self->addNonl($self->PARAM->properties('inline_libs'));
		$self->addNonl(" ");
		$self->addAll($self->PARAM->dbtypes->codeInlineConfigLibs);
		$self->add("',");
		$self->addNonl("INC => '");
		$self->addNonl($self->PARAM->properties('inline_inc'));
		$self->addNonl(" ");
		$self->addAll($self->PARAM->dbtypes->codeInlineConfigIncludes);
		$self->add("',");
		$self->addNonl("CCFLAGS => '");
		$self->addNonl($self->PARAM->properties('inline_ccflags'));
		$self->addNonl(" ");
		$self->addAll($self->PARAM->dbtypes->codeInlineConfigFlags);
		$self->add("',");
		$self->addNonl("OPTIMIZE => '");
		$self->addNonl($self->PARAM->properties('inline_optimize'));
		$self->addNonl(" ");
		$self->addAll($self->PARAM->dbtypes->codeInlineConfigOptimize);
		$self->add("',");
		$self->endList;
		$self->add(";");
		$self->back;
		$self->add;
#<		$self->add("use Inline C => <<'END_OF_C_CODE'");
		$self->add("use Inline C => q~");
		$self->add;

		$self->addAll($self->PARAM->dbtypes->codeInlineIncludes);

		$self->add("#include <pthread.h>") if ($self->PARAM->properties('num_threads'));
		$self->add;

		$self->add("#define GFMAXPIPBUFFER      @{[ $self->PARAM->sections->exists('input section')->items->size * 256 ]}");
		$self->add("#define GFMAXPIPFLDS        @{[ $self->PARAM->sections->exists('input section')->items->size + 1 ]}");
		$self->add("#define GFCACHERECS         @{[ $self->PARAM->properties('cache_recs') ]}") if ($self->PARAM->properties('cache_recs'));
		$self->add;

		$self->addAll($self->PARAM->dbtypes->codeInlineFieldNamesDecl);
		$self->add;
		$self->addAll($self->PARAM->dbtypes->codeInlineDecl);

		$self->addNonl("static const char *fields");
		$self->addNonl("[GFCACHERECS]") if $self->PARAM->properties('cache_recs');
		$self->addNonl("[GFMAXPIPFLDS];");
		$self->add;
		$self->add("static FILE *fstream = (FILE*)0;") if ($self->PARAM->sections->exists('sort by')->items->size || $self->PARAM->properties('input_file') ne '');
		$self->add;

		$self->addAll($self->PARAM->dbtypes->codeInlineFunctions);

		$self->addAll($self->PARAM->dbtypes->codeInlineInit);
		$self->addAll($self->PARAM->dbtypes->codeInlineOpen);
		$self->addAll($self->PARAM->dbtypes->codeInlinePragma);
		$self->addAll($self->PARAM->dbtypes->codeInlinePrep);
		$self->addAll($self->PARAM->dbtypes->codeInlineClose);
        $self->codeInlineReadCache if ($self->PARAM->properties('cache_recs'));

		if ($self->PARAM->properties('num_threads'))
		{
			$self->addAll($self->PARAM->dbtypes->codeInlineValueDecl); 
			$self->add;
			$self->add("static AV* I_VAL;") if ($self->PARAM->properties('num_threads'));
			$self->add("pthread_mutex_t g_mutex_I_VAL = PTHREAD_MUTEX_INITIALIZER;") if ($self->PARAM->properties('num_threads'));
			$self->codeInlineThreadsReset;
		}

		$self->codeInlineReadSplit;
		$self->codeInlineOpenSortStream if ($self->PARAM->sections->exists('sort by')->items->size || $self->PARAM->properties('input_file') ne '');

#<		$self->add("END_OF_C_CODE");
		$self->add("~; #End of Inline-C Code");
	}

	sub codeInlineOpenSortStream : method
	{
		my $self = shift;
		$self->add("int OpenSortStream (int fd)");
		$self->openBlock("{");
			$self->add("if ((fstream = fdopen(fd, \"r\")) == (FILE*)0)");
			$self->over;
				$self->add("croak(\"@{[ $self->PARAM->properties('script_name') ]}:Unable to open input file stream.\");");
			$self->back;
			$self->add("return 1;");
		$self->closeBlock("}");
	}

	sub codeInlineReset : method
	{
		my $self = shift; 
		foreach my $t (sort { $a->sequence <=> $b->sequence } $self->PARAM->dbtypes->tableList->toArray)
		{
			$self->addAll($t->codeInlineReset($t));
		}
	}

	sub codeInlineThreadsReset : method
	{
		my $self = shift;
		
		my @tbls_in_thread;
		my $thread_num;
		my $max_threads=0;

		foreach my $t (sort { $a->sequence <=> $b->sequence } $self->PARAM->dbtypes->tableList->toArray)
		{
			$thread_num = 1 if (++$thread_num > $self->PARAM->properties('num_threads'));
			$tbls_in_thread[$thread_num]++;
			$max_threads = $thread_num if ($max_threads < $thread_num);
		}
		$self->PARAM->properties('num_threads', $max_threads);

		my @tlist = (sort { $a->sequence <=> $b->sequence } $self->PARAM->dbtypes->tableList->toArray);
		
		foreach my $thread_num (1..$self->PARAM->properties('num_threads'))
		{
			$self->add;
			$self->add("void *thread_${thread_num} ( void* data )");
			$self->open;
			$self->add("int current_cache_rec = *((int*)data);");
			$self->add("int pN;");
			$self->add("int ret;");

#>>>This should be called from ETL::Pequel::Type::Db...
			$self->add("char *pzErrMsg = 0;");
			$self->add("char sql[4096];") if ($self->PARAM->properties('sqlite_merge_optimize'));
			foreach my $tbl_num (1..$tbls_in_thread[$thread_num])
			{
				my $t = shift(@tlist);
				$self->addAll($t->codeInlineReset($t));
			}
			$self->add("pthread_exit(NULL);");
			$self->add("return NULL;");
			$self->close;
		}
	}

	sub codeInlineThreadsCreateFunc : method
	{
		my $self = shift;

		foreach my $th (1..$self->PARAM->properties('num_threads'))
		{
			$self->add("pthread_t thread_${th}_tid;");
			if ($self->PARAM->properties('cache_recs'))
			{
				$self->add("if (pthread_create(&thread_${th}_tid, NULL, (void *(*)(void *))thread_${th}, (void*)&current_cache_rec))");
			}
			else
			{
				$self->add("if (pthread_create(&thread_${th}_tid, NULL, (void *(*)(void *))thread_${th}, (void*)0))");
			}
			$self->open;
			$self->add("croak(\"ERROR: Unable to create thread_${th}\");");
			$self->close;
		}
		$self->add("void* pThreadStatus;");
		foreach my $th (1..$self->PARAM->properties('num_threads'))
		{
			$self->add("pthread_join(thread_${th}_tid, &pThreadStatus);");
		}
	}

	sub codeInlineReadCache : method
	{
		my $self = shift;
		
		my $delim = $self->PARAM->properties('input_delimiter') =~ /^\\s/ ? ' ' : $self->PARAM->properties('input_delimiter');

		$self->add("int readcache ()");
		$self->add("{");
		$self->add(qq{
    register char *p;
    register int recs;
    register int f;
    static char inp[GFCACHERECS][GFMAXPIPBUFFER];
    static eof=0;

    if (eof) return 0;
    recs = 0;
    while (recs < GFCACHERECS)
    {
        f=0;
        if (@{[ $self->PARAM->sections->exists('sort by')->items->size || $self->PARAM->properties('input_file') ne '' ? '!fgets(inp[recs], GFMAXPIPBUFFER, fstream)' : '!gets(inp[recs])' ]} ) { eof=1; return recs; }   
        @{[ $self->PARAM->sections->exists('sort by')->items->size ? "inp[recs][strlen(inp[recs])-1] = '\\0';" : '' ]}
        memset(fields[recs], 0, sizeof(fields[recs]));
        p = inp[recs];
        fields[recs][0] = p;
        while (*p)
        {
            if (*p == '$delim')
            {
                fields[recs][++f] = p + 1;
                *p = '\\0';
            }
            p++;
        }
        recs++;
    }
    return recs;
	});
		$self->add("}");
	}

	sub codeInlineReadSplit : method
	{
		my $self = shift;
		my $delim = $self->PARAM->properties('input_delimiter') =~ /^\\s/ ? ' ' : $self->PARAM->properties('input_delimiter');
		$self->add("int readsplit (SV* I_VAL_ref)");
		$self->open("{");

#>>> Should be in Db::type...
		$self->add("char sql[4096];");
		$self->add("int ret;");
		$self->add("char *pzErrMsg = 0;");
		if ($self->PARAM->properties('cache_recs'))
		{
    		$self->add("static int current_cache_maxrecs=GFCACHERECS;");
    		$self->add("static int current_cache_rec=GFCACHERECS;");

    		$self->add("if (++current_cache_rec >= current_cache_maxrecs)");
    		$self->open("{");
				$self->add("if (current_cache_maxrecs < GFCACHERECS) return 0;");
				$self->add("if ((current_cache_maxrecs = readcache()) == 0) return 0;");
				$self->add("current_cache_rec = 0;");
    		$self->close("}");
			$self->addNonl("register AV* ") if (!$self->PARAM->properties('num_threads'));
			$self->add("I_VAL = (AV*)SvRV(I_VAL_ref);");
    
    		$self->add("if (!SvROK(I_VAL_ref)) croak(\"I_VAL_ref is not a reference\");");
			$self->add("av_clear(I_VAL);");
    
    		$self->add("register int f=0;");
    		$self->add("for (f=0; f < GFMAXPIPFLDS; f++)");
    		$self->open("{");
				$self->add("if (fields[current_cache_rec][f] == 0) av_store(I_VAL, f, newSVpvn(\"\", 0));");
				$self->add("else av_store(I_VAL, f, newSVpvn(fields[current_cache_rec][f], strlen(fields[current_cache_rec][f])));");
    		$self->close("}");
		}
		else
		{
			my $quote_char;
			my $br_open_char;
			my $br_close_char;
			if ($self->PARAM->properties('input_delimiter_extra'))
			{
				foreach my $d (split(//, $self->PARAM->properties('input_delimiter_extra')))
				{
					$quote_char=$d if ($d =~ /\"|\'|\`/);
					if ($d =~ /\[|\{|\(/)
					{
						$br_open_char = $d;
						$br_close_char = ($br_open_char eq '[' ? ']' : ($br_open_char eq '{' ? '}' : ')'));
					}
				}
			}
    		$self->add("register char *p;");
    		$self->add("static char inp[GFMAXPIPBUFFER];");
			$self->add("register AV* I_VAL;") if (!$self->PARAM->properties('num_threads'));
    		$self->add("register int i=0;");
			$self->add("int inside_quotes=0;") if ($self->PARAM->properties('input_delimiter_extra'));
			$self->add("int inside_sqb=0;") if ($self->PARAM->properties('input_delimiter_extra'));
			$self->add;
        	$self->add("if (@{[ $self->PARAM->sections->exists('sort by')->items->size || $self->PARAM->properties('input_file') ne '' ? '!fgets(inp, GFMAXPIPBUFFER, fstream)' : '!gets(inp)' ]} ) return 0;");
    		$self->add("if (!SvROK(I_VAL_ref)) croak(\"I_VAL_ref is not a reference\");");
			$self->add;
			$self->add("I_VAL = (AV*)SvRV(I_VAL_ref);");
			$self->add("av_clear(I_VAL);");
			$self->add;
        	$self->add("inp[strlen(inp)-1] = '\\0';") if ($self->PARAM->sections->exists('sort by')->items->size);
    		$self->add("memset(fields, 0, sizeof(fields));");
			$self->add;
    		$self->add("p = inp;");
    		$self->add("fields[0] = p;");
    		$self->add("while (*p) ");
    		$self->open("{");
				if ($self->PARAM->properties('input_delimiter_extra'))
				{
					$self->add("if (!inside_quotes && (*p == '$br_open_char' || *p == '$br_close_char')) { inside_sqb = !inside_sqb; }");
					$self->add("if (!inside_sqb && *p == '$quote_char') { inside_quotes = !inside_quotes; }");
					$self->add("if (!inside_quotes && !inside_sqb && *p == '$delim') ");
				}
				else
				{
					$self->add("if (*p == '$delim') ");
				}
				$self->open("{");
					$self->add("*p = '\\0';");
					$self->add("av_store(I_VAL, i, newSVpvn(fields[i], strlen(fields[i])));");
					$self->add("fields[++i] = p + 1;");
				$self->close("}");
				$self->add("p++;");
    		$self->close("}");
			$self->add("av_store(I_VAL, i, newSVpvn(fields[i], strlen(fields[i])));");
    		$self->add("while (++i < GFMAXPIPFLDS)");
			$self->over;
				$self->add("av_store(I_VAL, i, newSVpvn(\"\", 0));");
			$self->back;
		}
#>>> Should be in Db::type...
    	$self->add("int pN;");

		if ($self->PARAM->properties('num_threads'))
		{
			$self->codeInlineThreadsCreateFunc;
		}
		else
		{
			$self->addAll($self->PARAM->dbtypes->codeInlineValueDecl); 
			$self->codeInlineReset;
		}
    	$self->add("return 1;");
		$self->close("}");
	}
}
# ----------------------------------------------------------------------------------------------------
1;
