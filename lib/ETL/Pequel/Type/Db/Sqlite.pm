#!/usr/bin/perl
# ----------------------------------------------------------------------------------------------------
#  Name		: ETL::Pequel::Type::Db::Sqlite.pm
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
#use Pequel::Collection;
#use Pequel::Type;
#use Pequel::Type::Db;
use vars qw($VERSION $BUILD);
$VERSION = "2.4-3";
$BUILD = 'Tuesday November  1 08:45:13 GMT 2005';
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Type::Db::Sqlite::Element;
	use ETL::Pequel::Collection;	#+++++
	use ETL::Pequel::Type;	#+++++
	use ETL::Pequel::Type::Db;	#+++++
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

	sub codeInlineClose : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		foreach ($self->useList->toArray) { $c->addAll($_->codeInlineClose); }	# Table Level
		return $c;
	}

	sub codeInlineInit : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		foreach ($self->useList->toArray) { $c->addAll($_->codeInlineInit); }
		return $c;
	}
    
	sub codeInlineOpen : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		foreach ($self->useList->toArray) { $c->addAll($_->codeInlineOpen); }
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
	package ETL::Pequel::Type::Db::Sqlite;			#--> vector of ETL::Pequel::Type::Db::Sqlite::Element;
	use ETL::Pequel::Collection;	#+++++
	use ETL::Pequel::Type;	#+++++
	use ETL::Pequel::Type::Db;	#+++++
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
		$self = $class->SUPER::new(@_, name => 'sqlite');
		bless($self, $class);
		$self->PARAM($param);
		return $self;
	}

	sub codeConnect : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code::Perl->new(PARAM => $self->PARAM);
		$c->add("SqliteConnect(@{[ $self->PARAM->sections->find('sort by')->items->size ? '$fd' : '' ]});");
		return $c;
	}

	sub codeDisconnect : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code::Perl->new(PARAM => $self->PARAM);
		$c->add("SqliteDisconnect();");
		return $c;
	}

	sub codeInlineClose : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		$c->add("void SqliteDisconnect ()");
		$c->openBlock("{");
		foreach ($self->toArray) { $c->addAll($_->codeInlineClose); }	# DB Level
		$c->closeBlock("}");
		return $c;
	}

    sub codeInlineDecl : method 
	{ 
		my $self = shift; 
		my $c = ETL::Pequel::Code::C->new(PARAM => $self->PARAM);
		foreach ($self->toArray) { $c->addAll($_->codeInlineDecl); }
		$c->add;
		$c->add("int sqlite_open_all();");
		$c->add("int sqlite_pragma_all();");
		$c->add("int sqlite_prep_all();");
		$c->add;
		return $c;
	}

	sub codeInlineInit : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		$c->add("int SqliteConnect (@{[ $self->PARAM->sections->find('sort by')->items->size ? 'int fd' : '' ]})");
		$c->openBlock("{");
		$c->add("sqlite_open_all();");
		$c->add("sqlite_pragma_all();");
		$c->add("sqlite_prep_all();");
		$c->add("fstream = fdopen(fd, \"r\");") if ($self->PARAM->sections->find('sort by')->items->size);
		$c->add("return 1;");
		$c->closeBlock("}");
		return $c;
	}
    
	sub codeInlineOpen : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		$c->add("int sqlite_open_all ()");
		$c->openBlock("{");
		$c->add("char *pzErrMsg = 0;");
		foreach ($self->toArray) { $c->addAll($_->codeInlineOpen); }	# DB Level
		$c->add("return 1;");
		$c->closeBlock("}");
		return $c;
	}

	sub codeInlinePragma : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		$c->add("int sqlite_pragma_all ()");
		$c->openBlock("{");
		$c->add("char *pzErrMsg = 0;");
		$c->add("register int ret;");
		foreach ($self->toArray) { $c->addAll($_->codeInlinePragma); }
		$c->add("return 1;");
		$c->closeBlock("}");
		return $c;
	}
    
	sub codeInlinePrep : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		$c->add("int sqlite_prep_all ()");
		$c->openBlock("{");
		$c->add("char *pzErrMsg = 0;");
		$c->add("char sql[4096];");
		$c->add("register int ret;");
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
    
    sub codeInlineValueDecl : method 
	{ 
		my $self = shift; 
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		foreach ($self->toArray) { $c->addAll($_->codeInlineValueDecl); }
		return $c;
	}

    sub codeInlineConfigOptimize : method
	{ 
		my $self = shift; 
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		return $c;
	}

    sub codeInlineConfigFlags : method
	{ 
		my $self = shift; 
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		$c->addNonl("-DNDEBUG=1");
		return $c;
	}

    sub codeInlineConfigLibs : method 
	{ 
		my $self = shift; 
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		$c->addNonl("-L@{[ $self->PARAM->properties('sqlite_dir') ]}/bld/.libs -lpthread -lexc -lsqlite");
		return $c;
	}

    sub codeInlineConfigIncludes : method 
	{ 
		my $self = shift; 
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		$c->addNonl("-I@{[ $self->PARAM->properties('sqlite_dir') ]}/bld -I@{[ $self->PARAM->properties('sqlite_dir') ]}/src");
		return $c;
	}

    sub codeInlineIncludes : method 
	{ 
		my $self = shift; 
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		$c->add("#include \"sqlite.h\"");
		$c->add("#include \"sqliteInt.h\"");
		return $c;
	}

	sub codeInlineFunctions : method
	{
		my $self = shift;
		my $c = ETL::Pequel::Code->new(PARAM => $self->PARAM);
		$c->add("sqlite *sqlite_open_db (char *DbFilename)");
		$c->add("{");
		$c->add("
    sqlite *db = 0;

    if ( !sqliteOsFileExists(DbFilename) )
    {
        fprintf(stderr,\"Database %s does not exist\\n\", DbFilename);
        return 0;
    }
    char *zErrMsg = 0;
    if ((db = sqlite_open(DbFilename, 0666, &zErrMsg)) == 0)
    {
        if ((db = sqlite_open(DbFilename, 0444, &zErrMsg)) == 0)
        {
            if ( zErrMsg )
            {
                fprintf(stderr,\"Unable to open database %s: %s\\n\", DbFilename, zErrMsg);
                freemem(zErrMsg);
            }
            else
            {
                fprintf(stderr,\"Unable to open database %s\\n\", DbFilename);
            }
            return 0;
        }
        else
        {
            fprintf(stderr,\"Database %s opened READ ONLY!\\n\", DbFilename);
        }
    }
    return db;
		");
		$c->add("}");
		return $c;
	}
}
# ----------------------------------------------------------------------------------------------------
1;
