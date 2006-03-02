" Vim syntax file
" Language:	PEQUEL
" Maintainer:	M Gaffiero <gaffie@users.sourceforge.net>
" Last Change:	2005 Sep 2

" For version 5.x: Clear all syntax items
" For version 6.x: Quit when a syntax file was already loaded
if version < 600
  syntax clear
elseif exists("b:current_syntax")
  finish
endif

" this language is oblivious to case
syn case ignore

" A bunch of keywords
syn match pequelSection		"^options"
syn match pequelSection		"^input\s*section"
syn match pequelSection		"^output\s*section"
syn match pequelSection		"^sort\s*by"
syn match pequelSection		"^sort\s*output"
syn match pequelSection		"^group\s*by"
syn match pequelSection		"^load\s*table"
syn match pequelSection		"^divert\s*record"
syn match pequelSection		"^copy\s*record"
syn match pequelSection		"^divert\s*input\s*record"
syn match pequelSection		"^copy\s*input\s*record"
syn match pequelSection		"^divert\s*output\s*record"
syn match pequelSection		"^copy\s*output\s*record"
syn match pequelSection		"^reject"
syn match pequelSection		"^filter"
syn match pequelSection		"^init\s*_MONTH"
syn match pequelSection		"^init\s*_PERIOD"
syn match pequelSection		"^init\s*table"
syn match pequelSection		"^laod\s*table"
syn match pequelSection		"^load\s*table\s*pequel"
syn match pequelSection		"^having"
syn match pequelSection		"^use\s*package"
syn match pequelSection		"^init\s*table"
syn match pequelSection		"^load\s*table\s*sqlite\s*merge"
syn match pequelSection		"^load\s*table\s*sqlite"
syn match pequelSection		"^load\s*table\s*oracle\s*merge"
syn match pequelSection		"^load\s*table\s*oracle"
syn match pequelSection		"^field\s*preprocess"
syn match pequelSection		"^dedup\s*on"
syn match pequelSection		"^field\s*postprocess"
syn match pequelSection		"^summary\s*section"
syn match pequelSection		"^display\s*message\s*on\s*input"
syn match pequelSection		"^display\s*message\s*on\s*input\s*abort"
syn match pequelSection		"^display\s*message\s*on\s*output"
syn match pequelSection		"^display\s*message\s*on\s*output\s*abort"
syn match pequelSection		"^table[ ]\s*\w*\s*[(]"me=e-1 contains=,pequelParamList

"syn match pequelSection		"^description\s*section"
"syn match pequelSection		"^description\s*section" nextgroup=pequelSection 
syn match pequelDescSection	"^description\s*section" nextgroup=pequelCmdText
syn match pequelCmdText	".*$" contained 

syn match pequelType		"^\s*string"
syn match pequelType		"^\s*numeric"
syn match pequelType		"^\s*date"
syn match pequelType		"^\s*time"
syn match pequelType		"^\s*array"
syn match pequelType		"^\s*decimal"
syn keyword pequelType		where
syn keyword pequelType		min max sum avg range mode values count
syn keyword pequelType		count_distinct sum_distinct minimum maximum 
syn keyword pequelType		first last avg_distinct flag values_all
syn keyword pequelType		values_uniq serial mean median variance
syn keyword pequelType		stddev distinct
syn keyword pequelOption	header optimize noverbose input_delimiter_extra
syn keyword pequelOption	input_delimiter output_delimiter input_file output_file
syn keyword pequelOption	script_name discard_header header noheader
syn keyword pequelOption	addpipe noaddpipe optimize nooptimize nulls nonulls
syn keyword pequelOption	reject_file default_datetype default_list_delimiter hash
syn keyword pequelOption	transfer suppress_output num_threads sort_tmp_dir
syn keyword pequelOption	output_file_append lock_output
syn keyword pequelOption	logfilename logging display_table_stats reload_tables
syn keyword pequelOption	load_tables_only table_drop_unused_fields table_dir oracle_prefetch_count
syn keyword pequelOption	oracle_home oracle_sqlldr_rows oracle_use_merge_fetch_macro sqlite_dir
syn keyword pequelOption	sqlite_merge_optimize sqlite_merge_optimize_count use_inline
syn keyword pequelOption	inline_cc inline_libs inline_inc inline_ccflags inline_optimize
syn keyword pequelOption	inline_lddlflags inline_make inline_clean_after_build inline_clean_build_area
syn keyword pequelOption	inline_build_noisy inline_build_timers inline_force_build inline_print_info
syn keyword pequelOption	inline_directory inline_cache_recs use_av_store_macro inline_merge_optimize
syn keyword pequelOption	inline_merge_optimize_count doc_title doc_version doc_email
syn keyword pequelOption	dumpcode debug_show_caller debug debug_generate
syn keyword pequelOption	debug_parser diagnostics tinfo minfo pequelsrclist
syn keyword pequelOption	pequelprogref version usage viewcode
syn keyword pequelOption	syntax_check list pequeldoc detail
syn keyword pequelOption	pack_output output_pack_fmt
syn keyword pequelOption	unpack_input input_pack_fmt
syn keyword pequelOption	input_record_limit rmctrlm
syn keyword pequelOption	gzcat_cmd gzcat_args cat_cmd cat_args
syn keyword pequelOption	sort_cmd sort_args cpp_cmd cpp_args
syn keyword pequelOption	use_piped_chain
syn keyword pequelOption	show_synonyms
syn keyword pequelOption	exec_min_lines

syn match pequelParamList "(.*)"hs=s+1,he=e-1 containedin=pequelOptions contains=ALL
syn cluster pequelOptions contains=pequelOption,pequelParamList 

" operators
syn match pequelLogicalOperator "&&"
syn match pequelLogicalOperator "||"
syn match pequelRangeOperator "\.\."
syn match pequelAlternateOperator "[*+]"
syn match pequelAlternateOperator ":[+*]:"
syn match pequelArithmeticOperator "<<"
syn match pequelArithmeticOperator ">>"
syn match pequelRelationalOperator "[<>!=]="
syn match pequelRelationalOperator "[<>]"
syn match pequelAssignmentOperator "[:?]\=="
syn match pequelAssignmentOperator "?:="
syn match pequelArithmeticOperator "[-%]"

"syn match pequelParamList "(.*)"hs=s+1,he=e-1 containedin=pequelMacro,pequelTable
syn match pequelArray					"@\w*"
syn match pequelArray					"@\w*->\w*"
syn match pequelMacro					"&\w*[\s*(]"me=e-1 contains=ALLBUT,pequelParamList
syn keyword pequelPerlMacro				sprintf uc lc ucfirst lcfirst
syn keyword pequelPerlMacro				length oct ord
syn keyword pequelPerlMacro				abs atan2 cos exp hex int log oct rand sin sqrt srand
syn match pequelTable					"%\w*\s*[( ]"me=e-1 contains=pequelParamList
syn match pequelTableMemberOperator 	"->"
"syn region pequelArrayFunction			start=pequelArray end=
"syn match pequelTableField				"->\w*"hs=s+2 contains=pequelTableMemberOperator
syn match pequelTableField				"\[\w*\]"
syn keyword pequelConditional			eq ne gt lt ge le cmp not and or xor 
syn match pequelDerivedFieldOperator 	"=>"

" Strings and characters:
syn region pequelString		start=+"+  skip=+\\\\\|\\"+  end=+"+ oneline
syn region pequelString		start=+'+  skip=+\\\\\|\\'+  end=+'+ oneline contains=ALLBUT,pequelDescSection

syn keyword pequelTodo contained TODO XXX FIXME

" comments
syn region pequelComment start=+#+ end=+$+ contains=pequelNumber,pequelTodo
syn region pequelComment start=+//+ end=+$+ contains=pequelNumber,pequelTodo

syn sync minlines=1

" Define the default highlighting.
" For version 5.7 and earlier: only when not done already
" For version 5.8 and later: only when an item doesn't have highlighting yet
if version >= 508 || !exists("did_pequel_syn_inits")
  if version < 508
    let did_pequel_syn_inits = 1
    command -nargs=+ HiLink hi link <args>
  else
    command -nargs=+ HiLink hi def link <args>
  endif

  " The default highlighting.
  HiLink pequelSection				Special
  HiLink pequelDescSection			Special
  HiLink pequelStatement			Special
  HiLink pequelMacro				Function
  HiLink pequelPerlMacro			Function
  HiLink pequelArray				PreProc
  HiLink pequelTable				Label
  HiLink pequelTableField			Label
  HiLink pequelComment				Comment
  HiLink pequelOption				PequelType
  HiLink pequelAggregate			PequelType
  HiLink pequelType					Type
  HiLink pequelOperator				Operator
  HiLink pequelTableMemberOperator	pequelOperator
  HiLink pequelDerivedFieldOperator	pequelOperator
  HiLink pequelConditional			Conditional
  HiLink pequelParamList			String
  HiLink pequelString				String
  HiLink pequelLogicalOperator 		pequelOperator
  HiLink pequelRangeOperator 		pequelOperator
  HiLink pequelAlternateOperator 	pequelOperator
  HiLink pequelAlternateOperator 	pequelOperator
  HiLink pequelArithmeticOperator 	pequelOperator
  HiLink pequelArithmeticOperator 	pequelOperator
  HiLink pequelArithmeticOperator 	pequelOperator
  HiLink pequelRelationalOperator 	pequelOperator
  HiLink pequelRelationalOperator 	pequelOperator
  HiLink pequelAssignmentOperator 	pequelOperator
  HiLink pequelAssignmentOperator 	pequelOperator


  delcommand HiLink
endif

let b:current_syntax = "pequel"
" vim:ts=4
