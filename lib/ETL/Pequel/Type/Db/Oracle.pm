#!/usr/bin/perl
# ----------------------------------------------------------------------------------------------------
#  Name		: ETL::Pequel::Type::Db::Oracle.pm
#  Created	: 15 March 2005
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
	package ETL::Pequel::Type::Db::Oracle::Element;
	use ETL::Pequel::Collection;
	use ETL::Pequel::Type;
	use ETL::Pequel::Type::Db;
	use base qw(ETL::Pequel::Type::Db::Element);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_);
		bless($self, $class);
		return $self;
	}

#>	ETL::Pequel::Type::Code::...
	sub codeInlineInit : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code::C->new(PARAM => $self->PARAM);
		$c->addBar;
		$c->addComment("++++++ Db @{[ $self->name ]} --> Type :@{[ ref($self) ]} ++++++");
		$c->addBar;
		$c->add(qq{
    text* username_@{[ $self->name ]} = (text*)"@{[ $self->username ]}";
    text* password_@{[ $self->name ]} = (text*)"@{[ $self->password ]}";
    text* server_@{[ $self->name ]} = (text*)"@{[ $self->name ]}";

    if 
    (
        (status = OCIHandleAlloc(envhp, 
        (dvoid**)&authp_@{[ $self->name ]}, OCI_HTYPE_SESSION, 
        (size_t)0, (dvoid**)0)) != OCI_SUCCESS
    )
    {
        oracle_checkerr(errhp, status, (text*)"Unable to allocate authenication handle");
    }
    //	@{[ $self->PARAM->properties('noverbose') ? '' : "fprintf(stderr, \"allocated authenication handle\\n\");" ]}

    // attach server
    if 
    (
        (status = OCIHandleAlloc(envhp, 
        (dvoid**)&srvhp_@{[ $self->name ]}, OCI_HTYPE_SERVER, 
        (size_t)0, (dvoid**)0)) != OCI_SUCCESS
    )
    {
        oracle_checkerr(errhp, status, (text*)"Unable to allocate server handle");
    }
    //	@{[ $self->PARAM->properties('noverbose') ? '' : "fprintf(stderr, \"allocated server handle\\n\");" ]}

    if 
    (
        (status = OCIServerAttach(srvhp_@{[ $self->name ]}, 
        errhp, 
        server_@{[ $self->name ]}, 
        (sb4)strlen((char*)server_@{[ $self->name ]}), OCI_DEFAULT)) != OCI_SUCCESS
    )
    {
        oracle_checkerr(errhp, status, (text*)"Unable to attach server");
    //  fprintf(stderr, "Error (%d): Unable to attach server '%s'\\n", status, (const char*)server_@{[ $self->name ]});
    }
    //  @{[ $self->PARAM->properties('noverbose') ? '' : "fprintf(stderr, \"attached server '%s'\\n\", (const char*)server_@{[ $self->name ]});" ]}

    if 
    (
        (status = OCIHandleAlloc(envhp, 
        (dvoid**)&svchp_@{[ $self->name ]}, OCI_HTYPE_SVCCTX, 
        (size_t)0, (dvoid**)0)) != OCI_SUCCESS
    )
    {
        oracle_checkerr(errhp, status, (text*)"Unable to allocate service handle");
    }
    //  @{[ $self->PARAM->properties('noverbose') ? '' : "fprintf(stderr, \"allocated service handle\\n\");" ]}

    if 
    (
        (status = OCIAttrSet(svchp_@{[ $self->name ]}, OCI_HTYPE_SVCCTX, 
        (dvoid*)srvhp_@{[ $self->name ]}, (ub4)0, OCI_ATTR_SERVER, 
        errhp)) != OCI_SUCCESS
    )
    {
        oracle_checkerr(errhp, status, (text*)"Unable to set server handle in service handle");
    }
    //  @{[ $self->PARAM->properties('noverbose') ? '' : "fprintf(stderr, \"set server handle in service handle\\n\");" ]}

    // log on
    if 
    (
        (status = OCIAttrSet(authp_@{[ $self->name ]}, OCI_HTYPE_SESSION, 
        (dvoid*)username_@{[ $self->name ]}, 
        (ub4)strlen((char*)username_@{[ $self->name ]}), OCI_ATTR_USERNAME, 
        errhp)) != OCI_SUCCESS
    )
    {
        oracle_checkerr(errhp, status, (text*)"Unable to set username as attributes on authentication handle");
    }
    if 
    (
        (status = OCIAttrSet(authp_@{[ $self->name ]}, OCI_HTYPE_SESSION, 
        (dvoid*)password_@{[ $self->name ]}, 
        (ub4)strlen((char*)password_@{[ $self->name ]}), OCI_ATTR_PASSWORD, 
        errhp)) != OCI_SUCCESS
    )
    {
        oracle_checkerr(errhp, status, (text*)"Unable to set password as attributes on authentication handle");
    }
    //  @{[ $self->PARAM->properties('noverbose') ? '' : "fprintf(stderr, \"set username/password as attributes on authentication handle\\n\");" ]}

    if 
    (
        (status = OCISessionBegin(svchp_@{[ $self->name ]}, 
        errhp, 
        authp_@{[ $self->name ]}, 
        credt, OCI_DEFAULT)) != OCI_SUCCESS
    )
    {
        oracle_checkerr(errhp, status, (text*)"Unable to log on");
    //  fprintf(stderr, "Error (%d): Unable to log on as '%s'\\n", status, (const char*)username_@{[ $self->name ]});
    }
    //  @{[ $self->PARAM->properties('noverbose') ? '' : "fprintf(stderr, \"logged on as '%s'\\n\", (const char*)username_@{[ $self->name ]});" ]}

    if 
    (
        (status = OCIAttrSet(svchp_@{[ $self->name ]}, OCI_HTYPE_SVCCTX, 
        (dvoid*)authp_@{[ $self->name ]}, (ub4)0, OCI_ATTR_SESSION, 
        errhp)) != OCI_SUCCESS
    )
    {
        oracle_checkerr(errhp, status, (text*)"Unable to set authentication handle in service handle");
    }
    //  @{[ $self->PARAM->properties('noverbose') ? '' : "fprintf(stderr, \"set authentication handle in service handle\\n\");" ]}
		});
		# No init require at table level so nothing more to do.
		return $c;
	}
    
	sub codeInlineOpen : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		foreach ($self->useList->toArray) { $c->addAll($_->codeInlineOpen); }
		return $c;
	}

	sub codeInlineClose : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		foreach ($self->useList->toArray) { $c->addAll($_->codeInlineClose); }	# Table Level
		$c->add("OCISessionEnd(svchp_@{[ $self->name ]}, errhp, authp_@{[ $self->name ]}, (ub4)OCI_DEFAULT);");
		$c->add("OCIServerDetach(srvhp_@{[ $self->name ]}, errhp, (ub4)OCI_DEFAULT );");

		$c->add("OCIHandleFree((dvoid *)authp_@{[ $self->name ]}, (ub4)OCI_HTYPE_SESSION);");
		$c->add("OCIHandleFree((dvoid *)srvhp_@{[ $self->name ]}, (ub4)OCI_HTYPE_SERVER);");
		$c->add("OCIHandleFree((dvoid *)svchp_@{[ $self->name ]}, (ub4)OCI_HTYPE_SVCCTX);");
		return $c;
	}
    
	sub codeInlinePragma : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		foreach ($self->useList->toArray) { $c->addAll($_->codeInlinePragma); }
		return $c;
	}
    
	sub codeInlinePrep : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		foreach ($self->useList->toArray) { $c->addAll($_->codeInlinePrep); }
		return $c;
	}
    
	sub codeInlineReset : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		foreach ($self->useList->toArray) { $c->addAll($_->codeInlineReset); }
		return $c;
	}
    
    sub codeInlineDecl : method 
	{ 
		my $self = shift; 
		my $c = ETL::Pequel::Code::C->new(PARAM => $self->PARAM);
    	$c->add("static OCISvcCtx* svchp_@{[ $self->name ]} = (OCISvcCtx*)0; // service context handle");
		$c->add("static OCIServer* srvhp_@{[ $self->name ]} = (OCIServer*)0; // server handle");
		$c->add("static OCISession* authp_@{[ $self->name ]} = (OCISession*)0; // user session (authentication) handle");
		$c->add;

		foreach ($self->useList->toArray) { $c->addAll($_->codeInlineDecl); }
		return $c;
	}

    sub codeInlineValueDecl : method 
	{ 
		my $self = shift; 
		my $c = ETL::Pequel::Code::C->new(PARAM => $self->PARAM);
		foreach ($self->useList->toArray) { $c->addAll($_->codeInlineValueDecl); }
		return $c;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Db::Oracle;			#--> vector of ETL::Pequel::Type::Db::Oracle::Element;
	use ETL::Pequel::Collection;
	use ETL::Pequel::Type;
	use ETL::Pequel::Type::Db;
	use base qw(ETL::Pequel::Type::Db::Vector);

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
		my $self = shift;
		my $param = shift;
		my $class = ref($self) || $self;
		$self = $class->SUPER::new(@_, name => 'oracle');
		bless($self, $class);
		$self->PARAM($param);
		return $self;
	}

	sub codeConnect : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code::Perl->new(PARAM => $self->PARAM);
		$c->add("OracleConnect(@{[ $self->PARAM->sections->find('sort by')->items->size ? '$fd' : '' ]});");
		return $c;
	}

	sub codeDisconnect : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code::Perl->new(PARAM => $self->PARAM);
		$c->add("OracleDisconnect();");
		return $c;
	}

	sub codeInlineInit : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		$c->add("int oracle_open_all ()");
		$c->openBlock("{");
		$c->add(qq{
    ub4 init_mode = @{[ $self->PARAM->properties('num_threads') ? 'OCI_THREADED' : 'OCI_DEFAULT' ]};	// or OCI_OBJECT
    ub4 credt = OCI_CRED_RDBMS;
    sword status;

    // initialize OCI and set up handles
    if
    (
        OCIInitialize
        (
            OCI_THREADED, (dvoid*)0,
            (dvoid* (*)(dvoid*, size_t))0,
            (dvoid* (*)(dvoid*, dvoid*, size_t))0,
            (void (*)(dvoid*, dvoid*))0
        ) != OCI_SUCCESS
    )
    {
        fprintf(stderr, "ERROR: failed to initialize OCI\\n");
        exit(1);
    }
    //  @{[ $self->PARAM->properties('noverbose') ? '' : 'fprintf(stderr, "initialized OCI\\n");' ]}

    if ((status = OCIEnvInit(&envhp, OCI_DEFAULT, (size_t)0, (dvoid**)0)) != OCI_SUCCESS)
    {
        oracle_checkerr(errhp, status, (text*)"Unable to initialize environment handle");
        exit(1);
    }
    //  @{[ $self->PARAM->properties('noverbose') ? '' : 'fprintf(stderr, "initialized environment handle\\n");' ]}

    if 
    (
        (status = OCIHandleAlloc(envhp, 
        (dvoid**)&errhp, OCI_HTYPE_ERROR, 
        (size_t)0, (dvoid**)0)) != OCI_SUCCESS
    )
    {
        oracle_checkerr(errhp, status, (text*)"Unable to allocate error handle");
    }
    //  @{[ $self->PARAM->properties('noverbose') ? '' : 'fprintf(stderr, "allocated error handle\\n");' ]}
		});

		foreach ($self->toArray) { $c->addAll($_->codeInlineInit); }

		$c->add("return 1;");
		$c->closeBlock("}");

		return $c;
	}
    
	sub codeInlineOpen : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		foreach ($self->toArray) { $c->addAll($_->codeInlineOpen); }	# DB Level
		return $c;
	}

	sub codeInlineClose : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		$c->add("void OracleDisconnect ()");
		$c->openBlock("{");
		foreach ($self->toArray) { $c->addAll($_->codeInlineClose); }	# DB Level
		$c->add("OCIHandleFree((dvoid *)errhp, (ub4)OCI_HTYPE_ERROR);");
		$c->add("OCIHandleFree((dvoid *)envhp, (ub4)OCI_HTYPE_ENV);");
		$c->closeBlock("}");
		return $c;
	}

	sub codeInlinePragma : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		foreach ($self->toArray) { $c->addAll($_->codeInlinePragma); }
		return $c;
	}
    
	sub codeInlinePrep : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		$c->add("int oracle_prep_all ()");
		$c->openBlock("{");
		$c->add("ub4 prefetch = @{[ $self->PARAM->properties('oracle_prefetch_count') ]};")
			if ($self->PARAM->properties('oracle_prefetch_count'));
		$c->add("sword status;");
		foreach ($self->toArray) { $c->addAll($_->codeInlinePrep); }
		$c->add("return 1;");
		$c->closeBlock("}");
		return $c;
	} 
	sub codeInlineReset : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		foreach ($self->toArray) { $c->addAll($_->codeInlineReset); }
		return $c;
	}
    
    sub codeInlineDecl : method 
	{ 
		my $self = shift; 
		my $c = ETL::Pequel::Code::C->new(PARAM => $self->PARAM);
		$c->add;
		$c->add("int oracle_open_all();");
		$c->add("int oracle_prep_all();");
		$c->add;

		$c->add("sword status;");
		$c->add("text errbuf[512];");
		$c->add("sb4 errcode =0;");
		$c->add;

		$c->add("\#define STD_FLD_LEN 128");

		$c->add("#define _OracleMergeFetchNumeric(tbl,ikey) \\");
		$c->over;
			$c->add("if (tbl##_KEY == 0 && last_step_##tbl == OCI_SUCCESS) \\");
			$c->add("{ \\");
			$c->over;
				$c->addNonl("last_step_##tbl = OCIStmtFetch(stmthp_##tbl, ");
				$c->add("errhp, (ub4) 1, (ub4) OCI_FETCH_NEXT, (ub4) OCI_DEFAULT); \\");
			$c->back;
			$c->add("} \\");
			$c->add("while \\");
			$c->add("( \\");
			$c->over;
				$c->add("last_step_##tbl == OCI_SUCCESS \\");
				$c->add("&& tbl##_KEY != 0 \\");
				$c->add("&& atol(fields@{[ $self->cacheRecs ]}\[ikey]) > tbl##_KEY \\");
				$c->addNonl("&& (last_step_##tbl = OCIStmtFetch(stmthp_##tbl, ");
				$c->add("errhp, (ub4) 1, (ub4) OCI_FETCH_NEXT, (ub4) OCI_DEFAULT)) == OCI_SUCCESS \\");
			$c->back;
			$c->add(") \\");
			$c->add("{ \\");
			$c->over;
				$c->add("if (tbl##_KEY == 0) break; \\");
				$c->add("if (atol(fields@{[ $self->cacheRecs ]}\[ikey]) <= tbl##_KEY) break; \\");
			$c->back;
			$c->add("} \\");
    		$c->add("if (tbl##_KEY != 0 && atol(fields@{[ $self->cacheRecs ]}\[ikey]) == tbl##_KEY)");
		$c->back;
		$c->add;

		$c->add("#define _OracleMergeFetchString(tbl,ikey) \\");
		$c->over;
			$c->add("if (tbl##_KEY == 0 && last_step_##tbl == OCI_SUCCESS) \\");
			$c->add("{ \\");
			$c->over;
				$c->addNonl("last_step_##tbl = OCIStmtFetch(stmthp_##tbl, ");
				$c->add("errhp, (ub4) 1, (ub4) OCI_FETCH_NEXT, (ub4) OCI_DEFAULT); \\");
			$c->back;
			$c->add("} \\");
			$c->add("while \\");
			$c->add("( \\");
			$c->over;
				$c->add("last_step_##tbl == OCI_SUCCESS \\");
				$c->add("&& tbl##_KEY != 0 \\");
				$c->add("&& strcmp(fields@{[ $self->cacheRecs ]}\[ikey], (const char*)tbl##_KEY) > 0 \\");
				$c->addNonl("&& (last_step_##tbl = OCIStmtFetch(stmthp_##tbl, ");
				$c->add("errhp, (ub4) 1, (ub4) OCI_FETCH_NEXT, (ub4) OCI_DEFAULT)) == OCI_SUCCESS \\");
			$c->back;
			$c->add(") \\");
			$c->add("{ \\");
			$c->over;
				$c->add("if (tbl##_KEY == 0) break; \\");
				$c->add("if (strcmp(fields@{[ $self->cacheRecs ]}\[ikey], (const char*)tbl##_KEY) <= 0) break; \\");
			$c->back;
			$c->add("} \\");
    		$c->add("if (tbl##_KEY != 0 && strcmp(fields@{[ $self->cacheRecs ]}\[ikey], (const char*)tbl##_KEY) == 0)");
		$c->back;
		$c->add;

		$c->add("\#define _av_store_numeric(tbl,fld) \\");
		$c->over;
		$c->add("if (!indicator_##tbl##_##fld) \\");
		$c->over;
		$c->add("av_store(I_VAL, _I_##tbl##_FLD_##fld, newSVpvf(\"%ld\", tbl##_##fld));");
		$c->back;
		$c->back;
		$c->add;

		$c->add("\#define _av_store_string(tbl,fld) \\");
		$c->over;
		$c->add("if (!indicator_##tbl##_##fld) \\");
		$c->over;
		$c->add("av_store(I_VAL, _I_##tbl##_FLD_##fld, newSVpvf(\"%s\", tbl##_##fld));");
		$c->back;
		$c->back;
		$c->add;

    	$c->add("static OCIEnv* envhp = (OCIEnv*)0; // environment handle");
    	$c->add("static OCIError* errhp = (OCIError*)0; // error handle");
		foreach ($self->toArray) { $c->addAll($_->codeInlineDecl); }
		return $c;
	}

    sub codeInlineValueDecl : method 
	{ 
		my $self = shift; 
		my $c = ETL::Pequel::Code::C->new(PARAM => $self->PARAM);
		$c->add("ub4 prefetch = @{[ $self->PARAM->properties('oracle_prefetch_count') ]};")
			if ($self->PARAM->properties('oracle_prefetch_count'));
		foreach ($self->toArray) { $c->addAll($_->codeInlineValueDecl); }
		return $c;
	}

    sub codeInlineConfigOptimize : method
	{ 
		my $self = shift; 
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		return $c;
	}

    sub codeInlineConfigFlags
	{ 
		my $self = shift; 
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		return $c;
	}

    sub codeInlineConfigLibs : method 
	{ 
		my $self = shift; 
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
#LIBS = -L$(ORACLE_HOME)/lib -lclntsh -lclient -lsqlplus -lcore4 -lnetwork -lnlsrtl3 -lcore4
		$c->addNonl("-L@{[ $self->PARAM->properties('oracle_home') ]}/lib -L@{[ $self->PARAM->properties('oracle_home') ]}/rdbms/lib -lpthread -lclntsh -lc");
		return $c;
	}

    sub codeInlineConfigIncludes : method 
	{ 
		my $self = shift; 
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		$c->addNonl("-I@{[ $self->PARAM->properties('oracle_home') ]}/rdbms/demo -I@{[ $self->PARAM->properties('oracle_home') ]}/rdbms/public");
		return $c;
	}

    sub codeInlineIncludes : method 
	{ 
		my $self = shift; 
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		$c->add("#include <oci.h>");
		return $c;
	}

	sub codeInlineFunctions : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		$c->add("int OracleConnect (@{[ $self->PARAM->sections->find('sort by')->items->size ? 'int fd' : '' ]})");
		$c->openBlock("{");
		$c->add("oracle_open_all();");
		$c->add("oracle_prep_all();");
		$c->add("fstream = fdopen(fd, \"r\");") if ($self->PARAM->sections->find('sort by')->items->size);
		$c->add("return 1;");
		$c->closeBlock("}");

		$c->add;
		$c->add("void oracle_checkerr(OCIError *errhp, sword status, text *msg)");
		$c->openBlock("{");
    		$c->add("text errbuf[512];");
    		$c->add("sb4 errcode = 0;");
			$c->add;
    		$c->add("switch (status)");
			$c->openBlock("{");
				$c->add("case OCI_SUCCESS:");
				$c->over;
					$c->add("break;");
				$c->back;

				$c->add("case OCI_SUCCESS_WITH_INFO:");
				$c->over;
					$c->add("(void) fprintf(stderr, \"** Oracle Error - OCI_SUCCESS_WITH_INFO\\n\");");
					$c->add("break;");
				$c->back;

				$c->add("case OCI_NEED_DATA:");
				$c->over;
					$c->add("(void) fprintf(stderr, \"** Oracle Error - OCI_NEED_DATA\\n\");");
					$c->add("break;");
				$c->back;

				$c->add("case OCI_NO_DATA:");
				$c->over;
					$c->add("(void) fprintf(stderr, \"** Oracle Error - OCI_NODATA\\n\");");
					$c->add("break;");
				$c->back;

				$c->add("case OCI_ERROR:");
				$c->over;
					$c->addNonl("(void) OCIErrorGet((dvoid *)errhp, (ub4) 1, (text *) NULL,");
            		$c->addNonl("&errcode,");
            		$c->add("errbuf, (ub4) sizeof(errbuf), OCI_HTYPE_ERROR);");
					$c->add("(void) fprintf(stderr, \"** Oracle Error - %s - %.*s\\n\", msg, 512, errbuf);");
					$c->add("break;");
				$c->back;

				$c->add("case OCI_INVALID_HANDLE:");
				$c->over;
					$c->add("(void) fprintf(stderr, \"** Oracle Error - OCI_INVALID_HANDLE\\n\");");
					$c->add("break;");
				$c->back;

				$c->add("case OCI_STILL_EXECUTING:");
				$c->over;
					$c->add("(void) fprintf(stderr, \"** Oracle Error - OCI_STILL_EXECUTE\\n\");");
					$c->add("break;");
				$c->back;

				$c->add("case OCI_CONTINUE:");
				$c->over;
					$c->add("(void) fprintf(stderr, \"** Oracle Error - OCI_CONTINUE\\n\");");
					$c->add("break;");
				$c->back;

				$c->add("default:");
				$c->over;
					$c->add("break;");
				$c->back;
			$c->closeBlock("}");
		$c->closeBlock("}");
		$c->add;

		return $c;
	}
}
# ----------------------------------------------------------------------------------------------------
1;
