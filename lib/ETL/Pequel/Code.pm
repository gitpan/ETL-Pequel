#!/usr/bin/perl
# ----------------------------------------------------------------------------------------------------
#  Name		: ETL::Pequel::Code.pm
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
# 25/8/2005		1.1-2		gaffie	supress addCommentBegin()/End() unless --debug option specified.
# 25/8/2005		1.1-2		gaffie	updated sprintRaw so as to wrap code line > 110 chars.
# ----------------------------------------------------------------------------------------------------
# TO DO:
# ----------------------------------------------------------------------------------------------------
require 5.005_62;
use strict;
use attributes qw(get reftype);
use warnings;
use vars qw($VERSION $BUILD);
$VERSION = "1.1-2";
$BUILD = 'Thursday August 25 12:25:22 BST 2005';

# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Code::Line::Element;
	use ETL::Pequel::Collection;	#++++
	use base qw(ETL::Pequel::Collection::Element);

	our $this = __PACKAGE__;

	sub BEGIN
	{
		our @attr =
		qw(
			noNewline
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
		$self = $class->SUPER::new(@_);
		bless($self, $class);

		$self->noNewline($params{'no_newline'});
#?		if ($self->root->o_debug_generate)
#?		{
#?			print STDERR "CODE(@{[ $self->name ]}:", 
#?			(defined($self->value) ? $self->value : ''), 
#?				"-->Nonl=", $self->noNewline, "\n";
#?		}

		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Code::Generic;
	use ETL::Pequel::Collection;	#++++
	use base qw(ETL::Pequel::Collection::Vector::Stack);
	use UNIVERSAL qw(isa);

	use constant NO_NEWLINE => int 1;

	our $this = __PACKAGE__;

	sub BEGIN
	{
		our @attr =
		qw(
			blockType
			currentTab
			tabs
			showComments
			floatComments
			tabSize
			text
			raw
			PARAM
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
		$self = $class->SUPER::new(@_);
		bless($self, $class);

		$self->blockType([]);
		$self->currentTab(0);
		$self->tabs(0);
		$self->showComments(1);
		$self->floatComments($params{'floatComments'} || 0);
		$self->tabSize($params{'tabSize'} || 4);
		$self->text('');
		$self->PARAM($params{'PARAM'});

		return $self;
	}

#<	sub add : method
#<	{
#<		my $self = shift;
#<	
#<		return $self->SUPER::add(Pequel::Code::Line::Element->new
#<		(
#<			name => 'code', 
#<			value => shift || "", 
#<			no_newline => shift || 0
#<		));
#<	}

	sub add : method
	{
		my $self = shift;
		my $o = shift;
		my $newline = shift || 0;

		return ref($o)
			? $self->SUPER::add($o)
			: $self->SUPER::add(ETL::Pequel::Code::Line::Element->new
				(
					name => 'code', 
					value => $o,
					no_newline => $newline,
				));
	}

	sub _add : method
	{
		my $self = shift;
		return $self->SUPER::add(ETL::Pequel::Code::Line::Element->new(@_));
	}

	sub addNonl : method
	{
		my $self = shift;
		$self->add(shift, ETL::Pequel::Code::Generic::NO_NEWLINE);
	}

	sub addAll : method
	{
		my $self = shift;
		my @o_code = @_;	#--> array of Pequel::Code and Pequel::Code::Element;

#		map($self->over, 1..$self->tabs);
		foreach my $c (@o_code)
		{
			(ref($c) =~ /Element$/)
			? $self->SUPER::add($c)
			: map($self->SUPER::add($_), $c->toArray);
		}
#		map($self->back, 1..$self->tabs);
	}

	sub nl : method
	{
		my $self = shift;
		$self->add;
	}

	sub over : method
	{
		my $self = shift;
		$self->SUPER::add(ETL::Pequel::Code::Line::Element->new
		(
			name => 'indent', 
			value => $self->tabSize
		));
		$self->tabs($self->tabs+1);
	}
	
	sub back : method
	{
		my $self = shift;
		$self->SUPER::add(ETL::Pequel::Code::Line::Element->new(name => 'back'));
		$self->tabs($self->tabs-1);
	}
	
	sub open : method
	{
		my $self = shift;
		return $self->openBlock(@_);
	}

	sub openBlock : method
	{
		my $self = shift;
		my $bracket = shift || '{';
		$self->add("$bracket");
		$self->over;
		push(@{$self->blockType}, substr($bracket, 0, 1));
	}
	
	sub close : method
	{
		my $self = shift;
		return $self->closeBlock(@_);
	}

	sub closeBlock : method
	{
		my %bracket = ('(' => ')', '{' => '}', '[' => ']', '<' => '>' );
		my $self = shift;
		$self->PARAM->error->fatalError("[3001] Program Error: closeBlock() missing matching openBlock")
			if (scalar(@{$self->blockType}) == 0);
		my $last = pop(@{$self->blockType});
		my $bracket = shift || $bracket{$last};
		$self->back;
		$self->add("$bracket");
		$self->add;
	}

	sub endList : method
	{
		my $self = shift;
		my $v = $self->last->value; 
		$v =~ s/,\s*$//;
		$self->last->value($v);
	}

	sub prepare : method
	{
		my $self = shift;
		my $line_continues = 0;
		foreach my $o ($self->toArray)
		{
			if ($o->name eq 'indent')
			{
				$self->currentTab($self->currentTab + $self->tabSize);
				next;
			}
			elsif ($o->name eq 'open_block')
			{
				# this is wrong -- should set currentTab:
				$self->appendText(' ' x $self->currentTab) unless ($line_continues);
				$self->appendText("{");
				$self->appendText("\n") unless ($o->noNewline);
			}
			elsif ($o->name eq 'close_block')
			{
				$self->appendText(' ' x $self->currentTab) unless ($line_continues);
				$self->appendText("}");
				$self->appendText("\n") unless ($o->noNewline);
			}
			elsif ($o->name eq 'back')
			{
				$self->currentTab($self->currentTab - $self->tabSize);
				next;
			}
			elsif ($o->name eq 'code')
			{
				$self->appendText(' ' x $self->currentTab) unless ($line_continues);
				$self->appendText($o->value);
				$self->appendText("\n") unless ($o->noNewline);
			}
			elsif ($o->name eq 'comment' && $self->showComments)
			{
				$self->appendText('#') unless ($self->floatComments);
				$self->appendText(' ' x ($self->currentTab-($self->tabSize))) unless ($line_continues);
				$self->appendText((' ' x ($self->currentTab-($self->tabSize))) . '#') if (!$line_continues && $self->floatComments);
				$self->appendText($o->value);
				$self->appendText("\n") unless ($o->noNewline);
			}
			elsif ($o->name eq 'ccomment' && $self->showComments)
			{
				$self->appendText('//') unless ($self->floatComments);
				$self->appendText(' ' x ($self->currentTab-($self->tabSize))) unless ($line_continues);
				$self->appendText((' ' x ($self->currentTab-($self->tabSize))) . '//') if (!$line_continues && $self->floatComments);
				$self->appendText($o->value);
				$self->appendText("\n") unless ($o->noNewline);
			}
			elsif ($o->name eq 'ccbar')
			{
				$self->appendText('//' . ('-+' x 50)) unless ($self->floatComments);
				$self->appendText((' ' x ($self->currentTab-($self->tabSize))) . '//' . ('-+' x 50)) if ($self->floatComments);
				$self->appendText("\n") unless ($o->noNewline);
			}
			elsif ($o->name eq 'bar')
			{
				$self->appendText('#' . ('-+' x 50)) unless ($self->floatComments);
				$self->appendText((' ' x ($self->currentTab-($self->tabSize))) . '#' . ('-+' x 50)) if ($self->floatComments);
				$self->appendText("\n") unless ($o->noNewline);
			}
			$line_continues = $o->noNewline;
		}
	}

	sub appendText : method
	{
		my $self = shift;
		my $text = shift;
		return if (!length($text));
		$self->text($self->text . $text);
	}

	sub sprint : method
	{
		my $self = shift;
		$self->prepare unless($self->text);
		return $self->text;
	}
	
	sub sprintRaw : method
	{
		my $self = shift;
		$self->raw(1);	# Not used really.
		$self->prepare unless($self->text);
		my $raw_text;
		my $maxlen=110;
		foreach (split(/\n/, $self->text))
		{
			my $line = $_;
			while (length($line) > $maxlen)
			{
				$raw_text .= ' ' . substr($line, 0, $maxlen) . "\n";
				$line = substr($line, $maxlen);
			}
			$raw_text .= ' ' . $line . "\n";
		}
		$self->raw(0);
		return $raw_text;
	}

	sub print : method
	{
		my $self = shift;
		$self->prepare unless($self->text);
		print $self->text;
	}
	
	sub printToFile
	{
		my $self = shift;
		my $filename = shift;
		open(OFILE, ">$filename") || $self->PARAM->error->fatalError("[3002] Cannot open print file $filename");
		print OFILE $self->text;
		close(OFILE);
	}

	sub printStderr : method
	{
		my $self = shift;
		print STDERR $self->text;
	}

	sub showCode
	{
		my $self = shift;
		my $line_number = 1;
		foreach (split(/\n/, $self->text))
		{
			print sprintf("%5d %s\n", $line_number++, $_);
		}
	}

	sub podToPdf : method
	{
		my $self = shift;
		my $podfilename = shift;
		my $title = shift;
		my $version = shift;
		my $manual_type = shift;
		my $email = shift || $self->PARAM->properties('doc_email');

		my $have_pod2pdf = `which pequelpod2pdf`;
		chomp($have_pod2pdf);

		if ($have_pod2pdf)
		{
			my $cmd;
			$self->printToFile("$podfilename");
			$cmd = "pequelpod2pdf ";
			$cmd .= "--title \"$title\" ";
			$cmd .= "--version \"$version\" ";
			$cmd .= "--type \"$manual_type\" ";
			$cmd .= "--email \"$email\" $podfilename";
			system($cmd);
		}
	}

	sub leftIndentBlock : method
	{
		# shift all lines in code object to left
		my $self = shift;
	}

	sub rightIndentBlock : method
	{
		# shift all lines in code object to right
		my $self = shift;
		$self->unshift(ETL::Pequel::Code::Line::Element->new
		(
			name => 'indent', 
			value => $self->tabSize
		));
		$self->back;
	}


	sub codeOpenLog : method
	{
		my $self = shift;
		$self->add("open(_Pequel_LOGFILE, \">>@{[ $self->PARAM->properties('logfilename') ]}\")");
		$self->over;
		$self->add("or print STDERR \"Unable to open logfile @{[ $self->PARAM->properties('logfilename') ]}\";");
		$self->back;
		$self->verboseMessage("*** Logging to @{[ $self->PARAM->properties('logfilename') ]}");
	}

	sub codeWriteLog : method
	{
		my $self = shift;
		my $msg = shift;
		my $cond = shift || undef;

		$self->addNonl("print _Pequel_LOGFILE $msg");
		if ($cond)
		{
			$self->add;
			$self->over;
			$self->add(" $cond;");
			$self->back;
		}
		else
		{
			$self->add(";");
		}
	}

	sub verboseMessage : method
	{
		my $self = shift;
		my $msg = shift;
		my $cond = shift || undef;
		if (!$self->PARAM->properties('noverbose') && $self->PARAM->properties('verbose'))
		{
			$self->addNonl("print STDERR '[@{[ $self->PARAM->properties('script_name') ]} ' . localtime() . \"] $msg\"");
			$self->addNonl(" if ($cond)") if ($cond);
			$self->add(";");
		}
		return unless ($self->PARAM->properties('logging'));
		$self->addNonl("print _Pequel_LOGFILE '[@{[ $self->PARAM->properties('script_name') ]} ' . localtime() . \"] $msg\"");
		$self->addNonl(" if ($cond)") if ($cond);
		$self->add(";");
	}
# 	+++++++
	sub verboseCCMessage : method
	{
		my $self = shift;
		my $msg = shift;
		return if ($self->PARAM->properties('noverbose') || !$self->PARAM->properties('verbose'));
		$self->add("printf(stderr, \"$msg\\n\");");
	}

	sub append : method		# --> use addAll instead
	{
		my $self = shift;
		my $code = shift;	#--> ptr to Pequel::Code;
		map($_->add($_), $code->toArray);
	}

#> Move to Pequel::Code::Perl;
	sub addCommentBegin : method
	{
		my $self = shift;
		my $msg = shift || '';
		
		return unless ($self->PARAM->properties('debug'));

		$self->SUPER::add(ETL::Pequel::Code::Line::Element->new
		(
			name => 'comment', 
			value => ">>>>> BEGIN @{[ join('::', caller()) ]}:$msg >>>>>",
		));
	}

	sub addCommentEnd : method
	{
		my $self = shift;
		my $msg = shift || '';
		
		return unless ($self->PARAM->properties('debug'));

		$self->SUPER::add(ETL::Pequel::Code::Line::Element->new
		(
			name => 'comment', 
			value => "<<<<< END @{[ join('::', caller()) ]}:$msg <<<<<",
		));
	}

	sub addComment : method
	{
		my $self = shift;
		$self->SUPER::add(ETL::Pequel::Code::Line::Element->new
		(
			name => 'comment', 
			value => shift, 
			no_newline => shift || 0
		));
	}

	sub addBar : method
	{
		my $self = shift;
		my $count = shift || 1;

		foreach (1..$count)
		{
			$self->SUPER::add(ETL::Pequel::Code::Line::Element->new
			(
				name => 'bar', 
#<<				value => ('#' . ('-+' x 50)), 
				no_newline => shift || 0
			));
		}
	}
		
#?	# reset the indentation to 0:
#?	sub reset : method
#?	{
#?		my $self = shift;
#?	}
#?	
#?	# use for column tabulation (specify/return number of columns)
#?	sub columns : method
#?	{
#?	}
#?	
#?	# add an item to the list (for tabulation)
#?	sub addList : method
#?	{
#?	}
#?	
#?	sub pipeTo
#?	{
#?		my $self = shift;
#?		my $pipe = shift;
#?		open(PIPE, "| $pipe") || $self->root->fatalError("[3003] Cannot open pipe $pipe");
#?		print PIPE $self->text;
#?		close(PIPE);
#?	}

	sub addFmt : method
	{
		my $self = shift;
		my $line = shift;
		my $maxline = 70;

		if (length($line) > $maxline)
		{
			$line =~ s/(^\s+)//;
			my $pre_space = defined($1) ? length($1) : 0;
			while (length($line)+$pre_space > $maxline)
			{
				$self->add(' ' x $pre_space . substr($line, 0, $maxline-$pre_space) . ' \\');
				$line = substr($line, $maxline-$pre_space);
			}
			$self->add(' ' x $pre_space . $line) if (length($line));
		}
		else
		{
			$self->add($line);
		}
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Code::C;
	use base qw(ETL::Pequel::Code::Generic);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_);
		bless($self, $class);
		return $self;
	}

	sub addCommentBegin : method
	{
		my $self = shift;
		my $msg = shift || '';
		
		$self->_add
		(
			name => 'ccomment', 
			value => ">>>>> BEGIN @{[ join('::', caller()) ]}:$msg >>>>>",
		);
	}

	sub addCommentEnd : method
	{
		my $self = shift;
		my $msg = shift || '';
		
		$self->_add
		(
			name => 'ccomment', 
			value => "<<<<< END @{[ join('::', caller()) ]}:$msg <<<<<",
		);
	}

	sub addComment : method
	{
		my $self = shift;
		$self->_add
		(
			name => 'ccomment', 
			value => shift, 
			no_newline => shift || 0
		);
	}

	sub addBar : method
	{
		my $self = shift;
		my $count = shift || 1;

		foreach (1..$count)
		{
			$self->_add
			(
				name => 'ccbar', 
#<<				value => ('#' . ('-+' x 50)), 
				no_newline => shift || 0
			);
		}
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Code::Perl;
	use base qw(ETL::Pequel::Code::Generic);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_);
		bless($self, $class);
		return $self;
	}

	sub addCommentBegin : method
	{
		my $self = shift;
		my $msg = shift || '';
		
		$self->_add
		(
			name => 'comment', 
			value => ">>>>> BEGIN @{[ join('::', caller()) ]}:$msg >>>>>",
		);
	}

	sub addCommentEnd : method
	{
		my $self = shift;
		my $msg = shift || '';
		
		$self->_add
		(
			name => 'comment', 
			value => "<<<<< END @{[ join('::', caller()) ]}:$msg <<<<<",
		);
	}

	sub addComment : method
	{
		my $self = shift;
		$self->_add
		(
			name => 'comment', 
			value => shift, 
			no_newline => shift || 0
		);
	}

	sub addBar : method
	{
		my $self = shift;
		my $count = shift || 1;

		foreach (1..$count)
		{
			$self->_add
			(
				name => 'bar', 
#<<				value => ('#' . ('-+' x 50)), 
				no_newline => shift || 0
			);
		}
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Code::Pod;
	use base qw(ETL::Pequel::Code::Generic);

	our $this = __PACKAGE__;

	sub BEGIN
	{
		our @attr =
		qw(
			insideBegin
			pdfName
			podName
			docTitle
			docVersion
			docType
			docEmail
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
		$self = $class->SUPER::new(@_);
		bless($self, $class);
		$self->insideBegin(0);
		return $self;
	}

	sub pod : method
	{
		my $self = shift;
		$self->add("=pod");
		$self->add;
	}

	sub add : method
	{
		my $self = shift;
		my $text = shift || '';
		$self->SUPER::add($text);
		$self->SUPER::add unless ($self->insideBegin);
	}

	sub head1 : method
	{
		my $self = shift;
		my $text = shift;
		$self->add("=head1 $text");
		$self->add;
	}

	sub head2 : method
	{
		my $self = shift;
		my $text = shift;
		$self->add("=head2 $text");
		$self->add;
	}

	sub item : method
	{
		my $self = shift;
		my $text = shift;
		$self->add("=item $text");
		$self->add;
	}

	sub itemBold : method
	{
		my $self = shift;
		my $text = shift;
		$self->add("=item B<$text>");
		$self->add;
	}

	sub itemBoldItalic : method
	{
		my $self = shift;
		my $text = shift;
		$self->add("=item F<$text>");
		$self->add;
	}

	sub begin : method
	{
		my $self = shift;
		$self->add("=begin");
		$self->add;
		$self->insideBegin(1);
	}

	sub end : method
	{
		my $self = shift;
		$self->add("=end");
		$self->add;
		$self->insideBegin(0);
	}

	sub page : method		#--> ETL::Pequel::Code::Pdf
	{
		my $self = shift;
		$self->add("=page");
		$self->add;
	}

	sub over : method
	{
		my $self = shift;
		my $count = shift || 4;
		$self->add("=over $count");
		$self->add;
	}

	sub back : method
	{
		my $self = shift;
		$self->add("=back");
		$self->add;
	}

	sub setBold : method
	{
		my $self = shift;
		return "B<$_[0]>";
	}

	sub setItalic : method
	{
		my $self = shift;
		return "I<$_[0]>";
	}

	sub setHighlight : method
	{
		my $self = shift;
		return "F<$_[0]>";
	}

	sub setLink : method
	{
		my $self = shift;
		return "L<$_[0]>";
	}

	sub podToPdf : method
	{
		my $self = shift;
		my $have_pod2pdf = `which pequelpod2pdf`;
		chomp($have_pod2pdf);

		if ($have_pod2pdf)
		{
			my $cmd;
			$self->printToFile($self->podName);
			$cmd = "pequelpod2pdf ";
			$cmd .= "--title \"@{[ $self->docTitle ]}\" ";
			$cmd .= "--version \"@{[ $self->docVersion ]}\" ";
			$cmd .= "--type \"@{[ $self->docType ]}\" ";
			$cmd .= "--email \"@{[ $self->PARAM->properties('doc_email') ]}\" @{[ $self->podName ]}";
			system($cmd);
		}
	}
}
# ----------------------------------------------------------------------------------------------------
{
	package ETL::Pequel::Code;
	use base qw(ETL::Pequel::Code::Generic);

	sub new : method
	{
		my $self = shift;
		my $class = ref($self) || $self;
		my %params = @_;
		$self = $class->SUPER::new(@_);
		bless($self, $class);
		return $self;
	}
}
# ----------------------------------------------------------------------------------------------------
1;
