//
//  Standard ISO/ANSI SQL2 (also known as SQL92) grammar
//  DML subset of the Full SQL Level
//  Copyright (C) 2003 Lubos Vnuk
//
//{ Foreword
//    This grammar file contains a performance&size-optimized DML subset of ISO/ANSI SQL2,
//    developed by Lubos Vnuk (lvnuk[atsign]host.sk) using the "ISO/IEC 9075:1992, Database Language SQL"
//    standard and the SQL2 BNF notation (further referred to as BNF) that can be obtained at:
//    http://cui.unige.ch/db-research/Enseignement/analyseinfo/SQL92/BNFindex.html
//    DML (Data Manipulation Language) includes these SQL statements: SELECT, INSERT, UPDATE and DELETE.
//    
//    The grammar has been created for the LL(k) parser generator ANTLR 2.7.2 (www.antlr.org).
//    It will generate the lexer and parser source code in C++.
//    Most of the "subtoken typecasting" within the lexer is just a matter of performance tuning. 
//    Note also that DML grammar uses only a subset of the Full SQL reserverd word set!
//}
//{ Legal statement
//    This software may be used freely for educational purposes. Other use with author's permission only.
//    It is provided "as is" without warranty of any kind, express or implied, including but not
//    limited to the warranties of merchantability, fitness for a particular purpose and noninfringement.
//    In no event shall Lubos Vnuk be liable for any claim, damages or other liability in connection
//    with the software or the use or other dealings in the software.
//}
//{ Credits
//    I'd like to thank
//    - Eva Balan for her stamina 
//    - Terence Parr for creating ANTLR (ANother Tool for Language Recognition)
//    - Neil Hodgson for developing SciTE (Scintilla Text Editor - www.scintilla.org)
//}
//{ Release history
//    DmlSQL2 1.00 - 2003Jul01 - Initial release 
//}
//{ To-do (TDO) list
//    TDO0: Tree building
//    TDO1: Unicode character repertoire support
//}
//{ Not Supported Feature (NSF) list
//    NSF0: Character repertoires (<char_set_name> IS supported but the lexer works with 8-bit chars only)
//    NSF1: Semantic analysis as the meta data is not available (automatic data type conversion is presumed)
//    NSF2: Embedded SQL languages Ada,C,Cobol,Fortran,MUMPS,Pascal and PLI except for <EMBDD_VARIABLE_NAME>
//}
//{ Extended Feature (EXF) list
//    EXF0: <query_primary> may also be a simple <table_ref>
//    EXF1: Components of <char_string_lit> and <NATIONAL_CHAR_STRING_LIT> may contain <NEWLINE> character
//    EXF2: <char_set_name> may contain a generalized <id> in place of the <SQL_language_id>
//    EXF3: <REGULAR_ID> and <DELIMITED_ID> may contain more then 128 characters
//    EXF4: <parameter_name> may also be a COLON UNSIGNED_INTEGER or a COLON id (PERIOD id)*
//    EXF5: <select_stmt> and <query_spec> may contain an <into_clause>
//}
//{ Abbreviations used in rule's names
//  
//	author authorization
//	aux    auxiliary
//	char   character
//	dec    declaration
//	def    defined
//	def    definition
//	diag   diagnostics
//	dyn    dynamic
//	embdd  embedded
//	exec   executable
//	exp    expression
//	fct    function
//	frac   fractional
//	id     identifier
//	implt  implementation
//	lit    literal
//	num    numeric
//	op     operator
//	pos    positionned
//	ref    referential
//	rep    repertoire
//	spec   specification
//	std    standard
//	stmt   statement
//	univ   universal 
//}
//
header {
//  Global header starts here, at the top of all generated files

// #include <iostream> // if you want to use some cout's in the actions
ANTLR_USING_NAMESPACE(std)
ANTLR_USING_NAMESPACE(antlr)

//  Global header ends here
}

options {
	// Global options for the entire grammar file
	language=Cpp;
	namespaceAntlr="";
	namespaceStd=""; 
	genHashLines = false;
}

// 
// DmlSQL2Lexer 
{
//  Class preamble starts here - right before the class definition in the generated class file

//  Class preamble ends here
}
class DmlSQL2Lexer extends Lexer;
//{ TOKENS produced by the lexer
//{ The lexer provides these non-protected tokens:
//       REGULAR_ID; EXACT_NUM_LIT; CHAR_STRING; DELIMITED_ID; PERCENT; AMPERSAND;
//       LEFT_PAREN; RIGHT_PAREN; ASTERISK; PLUS_SIGN; COMMA; MINUS_SIGN; SOLIDUS;
//       COLON; SEMICOLON; LESS_THAN_OP; EQUALS_OP; GREATER_THAN_OP; QUESTION_MARK;
//}      VERTICAL_BAR; LEFT_BRACKET; RIGHT_BRACKET; INTRODUCER
//{ The lexer provides these imaginary tokens based on subtoken typecasting:
//	 UNSIGNED_INTEGER; APPROXIMATE_NUM_LIT; QUOTE; PERIOD; UNDERSCORE; DOUBLE_PERIOD; 
//       NOT_EQUALS_OP; LESS_THAN_OR_EQUALS_OP; GREATER_THAN_OR_EQUALS_OP; CONCATENATION_OP; 
//}      NATIONAL_CHAR_STRING_LIT; BIT_STRING_LIT; HEX_STRING_LIT; EMBDD_VARIABLE_NAME 
//  The lexer also provides SQL2RW_xxxx and SQL2NRW_xxxx literal tokens - see importVocab.
//}

options {
    importVocab = DmlSQL2Imp; // Main import vocabulary file
    exportVocab = DmlSQL2Lex; // Lexer's exportVocab is parser's importVocab
    testLiterals = false; // For literals' translation see <REGULAR_ID>
    k = 2; // Lookahead
    defaultErrorHandler = false; // Automatic error handling
    caseSensitive = true; // The grammar per se may be case sensitive
    caseSensitiveLiterals = false; // Keywords are case insensitive
    charVocabulary = '\0' .. '\377'; // Accept 8-bit characters
    filter = ANY_CHAR; // Filter off all unhandled characters
}
/*
tokens { // moved to the import vocabulary
	UNSIGNED_INTEGER; // Imaginary token based on subtoken typecasting - see the rule <EXACT_NUM_LIT>
	APPROXIMATE_NUM_LIT; // Imaginary token based on subtoken typecasting - see the rule <EXACT_NUM_LIT>
	QUOTE;  // Imaginary token based on subtoken typecasting - see the rule <CHAR_STRING>
	PERIOD;  // Imaginary token based on subtoken typecasting - see the rule <EXACT_NUM_LIT>
	MINUS_SIGN; // Imaginary token based on subtoken typecasting - see the rule <SEPARATOR>
	UNDERSCORE; // Imaginary token based on subtoken typecasting - see the rule <INTRODUCER>
	DOUBLE_PERIOD;  // Imaginary token based on subtoken typecasting - see the rule <EXACT_NUM_LIT>
	NOT_EQUALS_OP; // Imaginary token based on subtoken typecasting - see the rule <LESS_THAN_OP>
	LESS_THAN_OR_EQUALS_OP ; // Imaginary token based on subtoken typecasting - see the rule <LESS_THAN_OP>
	GREATER_THAN_OR_EQUALS_OP; // Imaginary token based on subtoken typecasting - see the rule <GREATER_THAN_OP>
	CONCATENATION_OP; // Imaginary token based on subtoken typecasting - see the rule <VERTICAL_BAR>
	NATIONAL_CHAR_STRING_LIT; // Imaginary token based on subtoken typecasting - see the rule <REGULAR_ID>
	BIT_STRING_LIT; // Imaginary token based on subtoken typecasting - see the rule <REGULAR_ID>
	HEX_STRING_LIT; // Imaginary token based on subtoken typecasting - see the rule <REGULAR_ID>
	EMBDD_VARIABLE_NAME; // Imaginary token based on subtoken typecasting - see the rule <COLON>
}
*/
{
//  Class body inset starts here - at the top within the generated class body

//  Class body inset ends here
}

//{ Rule #442 <REGULAR_ID> additionally encapsulates a few STRING_LITs.
//  Within testLiterals all reserved and non-reserved words are being resolved
REGULAR_ID : 
	( NATIONAL_CHAR_STRING_LIT {$setType(NATIONAL_CHAR_STRING_LIT);} 
	| BIT_STRING_LIT {$setType(BIT_STRING_LIT);} 
	| HEX_STRING_LIT {$setType(HEX_STRING_LIT);} 
	)
	// REGULAR_ID
	| (SIMPLE_LETTER) (SIMPLE_LETTER | '_' | '0'..'9')* {$setType(testLiteralsTable(REGULAR_ID));} 
;
//}

//{ Rule #238 <EXACT_NUM_LIT> 
//  This rule is a bit tricky - it resolves the ambiguity with <PERIOD> and <DOUBLE_PERIOD>
//  It also incorporates <mantisa> and <exponent> for the <APPROXIMATE_NUM_LIT>
//  Rule #501 <signed_integer> was incorporated directly in the token <APPROXIMATE_NUM_LIT>
//  See also the rule #617 <unsigned_num_lit>
EXACT_NUM_LIT :
	  UNSIGNED_INTEGER
		( '.' (UNSIGNED_INTEGER)?
		|	{$setType(UNSIGNED_INTEGER);}
		) ( ('E' | 'e') ('+' | '-')? UNSIGNED_INTEGER {$setType(APPROXIMATE_NUM_LIT);} )?
	| '.' UNSIGNED_INTEGER ( ('E' | 'e') ('+' | '-')? UNSIGNED_INTEGER {$setType(APPROXIMATE_NUM_LIT);} )?
	| '.' 	{$setType(PERIOD);}
	| '.' '.' {$setType(DOUBLE_PERIOD);}
;
//}

//  Rule #176 <DIGIT> was incorporated by <UNSIGNED_INTEGER> 
//{ Rule #615 <UNSIGNED_INTEGER> - subtoken typecast in <EXACT_NUM_LIT> 
protected
UNSIGNED_INTEGER : 
	('0'..'9')+ 
;
//}

//{ Rule #358 <NATIONAL_CHAR_STRING_LIT> - subtoken typecast in <REGULAR_ID>, it also incorporates <character_representation>
//  Lowercase 'n' is a usual addition to the standard
protected
NATIONAL_CHAR_STRING_LIT : 
	('N' | 'n') ('\'' (options{greedy=true;}: ~('\'' | '\r' | '\n' ) | '\'' '\'' | NEWLINE)* '\'' (SEPARATOR)? )+ 
;
//}

//{ Rule #040 <BIT_STRING_LIT> - subtoken typecast in <REGULAR_ID>
//  Lowercase 'b' is a usual addition to the standard
protected
BIT_STRING_LIT : 
	('B' | 'b') ('\'' ('0' | '1')* '\'' (SEPARATOR)? )+ 
;
//}

//{ Rule #284 <HEX_STRING_LIT> - subtoken typecast in <REGULAR_ID>
//  Lowercase 'x' is a usual addition to the standard
protected
HEX_STRING_LIT : 
	('X' | 'x') ('\'' ('a'..'f' | 'A'..'F' | '0'..'9')* '\'' (SEPARATOR)? )+ 
;
//}

//{ Rule #--- <CHAR_STRING> is a base for Rule #065 <char_string_lit> , it incorporates <character_representation>
//  and a superfluous subtoken typecasting of the "QUOTE"
CHAR_STRING : 
	  ('\'' (options{greedy=true;}: ~('\'' | '\r' | '\n') | '\'' '\'' | NEWLINE)* '\'' (SEPARATOR)? )+ 
	| '\'' {$setType(QUOTE);}
;
//}

//{ Rule #163 <DELIMITED_ID>
DELIMITED_ID : 
	'"' (~('"' | '\r' | '\n') | '"' '"')+ '"' 
;
//}

//{ Rule #546 <SQL_SPECIAL_CHAR> was split into single rules
//  DOUBLE_QUOTE : '"' ; // incorporated in the rule #163 <DELIMITED_ID>
PERCENT : '%' ; // Not used in the parser
AMPERSAND : '&' ; // Not used in the parser
//protected QUOTE : '\'' ; // subtoken typecast within <CHAR_STRING> 
LEFT_PAREN : '(' ; 
RIGHT_PAREN : ')' ; 
ASTERISK : '*' ; 
PLUS_SIGN : '+'	; 
COMMA : ',' ; 
//MINUS_SIGN : '-' ; // subtoken typecast within <SEPARATOR>
//protected PERIOD : '.' ; // subtoken typecast within <EXACT_NUM_LIT>
SOLIDUS : '/' ; 
//  Rule #234 <EMBDD_VARIABLE_NAME> was incorpotated in the rule <COLON>
//{ Rule #089 <COLON> is the leading character of <parameter_name> as well as <EMBDD_VARIABLE_NAME>
//  One should explicitly write a <SEPARATOR> after the <COLON> if they want to designate <parameter_name> !
COLON :
	  ':'
	| ':' (SIMPLE_LETTER | '0'..'9' | '.' | '_' | '#' | '$' | '&' | '%' | '@')+ {$setType(EMBDD_VARIABLE_NAME);}
; 
//}
SEMICOLON : ';' ; 
LESS_THAN_OP : '<' ('>' {$setType(NOT_EQUALS_OP);} | '=' {$setType(LESS_THAN_OR_EQUALS_OP);})?; 
EQUALS_OP : '=' ; 
NOT_EQUALS_OP_ALT : "!=" ; // DanielW: add != recognition.
GREATER_THAN_OP : '>' ('=' {$setType(GREATER_THAN_OR_EQUALS_OP);})?; 
QUESTION_MARK : '?' ; 
// protected UNDERSCORE : '_' SEPARATOR ; // subtoken typecast within <INTRODUCER>
VERTICAL_BAR : '|' ('|' {$setType(CONCATENATION_OP);})?; 

//{ Rule #532 <SQL_EMBDD_LANGUAGE_CHAR> was split into single rules:
LEFT_BRACKET : '[' ; // Not used in the parser
RIGHT_BRACKET : ']' ; // Not used in the parser
//}
//{ Double character rules are all typecast subtokens:
//protected NOT_EQUALS_OP :	'<' '>' ; // subtoken typecast within <LESS_THAN_OP>
//protected LESS_THAN_OR_EQUALS_OP : '<' '=' ; // subtoken typecast within <LESS_THAN_OP>
//protected GREATER_THAN_OR_EQUALS_OP : '>' '=' ; // subtoken typecast within <GREATER_THAN_OP>
//protected CONCATENATION_OP : '|' '|';  // subtoken typecast within <VERTICAL_BAR>
//protected DOUBLE_PERIOD : '.' '.' ; // subtoken typecast within <EXACT_NUM_LIT>
//}
//{ Rule #319 <INTRODUCER>
INTRODUCER : '_' (SEPARATOR {$setType(UNDERSCORE);})?; 
//}
//}

//{ Rule #504 <SIMPLE_LETTER> - simple_latin _letter was generalised into SIMPLE_LETTER
//  Unicode is yet to be implemented - see NSF0
protected
SIMPLE_LETTER : 
	'a'..'z' | 'A'..'Z' | '\177'..'\377' 
;
//}

//{ Rule #479 <SEPARATOR>
//  It was originally a protected rule set to be filtered out but the <COMMENT> and <MINUS_SIGN> clashed.
//protected 
SEPARATOR : 
	  '-' {$setType(MINUS_SIGN);}
	| COMMENT { $setType(ANTLR_USE_NAMESPACE(antlr)Token::SKIP); }
	| (SPACE | NEWLINE)+	{ $setType(ANTLR_USE_NAMESPACE(antlr)Token::SKIP); }
;
//}

//{ Rule #097 <COMMENT>
protected 
COMMENT : 
	'-' '-' ( ~('\r' | '\n') )* NEWLINE 
;
//}

//{ Rule #360 <NEWLINE>
protected 
NEWLINE : 
	( '\r' (options{greedy=true;}: '\n')? | '\n' ) {newline();} 
;
//}

//{ Rule #522 <SPACE>
protected 
SPACE : 
	  ' ' | '\t' 
;
//}

//{ Rule #--- <ANY_CHAR> is an artificial rule, see the filter option
protected
ANY_CHAR : 
	.
;
//}

//
//  DmlSQL2Parser
{
//  Class preamble starts here - right before the class definition in the generated class file

//  Class preamble ends here
}

class DmlSQL2Parser extends Parser;
options {
    importVocab = DmlSQL2Lex; // Import vocabulary module from the lexer
    exportVocab = DmlSQL2; // Export vocabulary
    k = 2; // Lookahead
    defaultErrorHandler = false; // Automatic error handling
    codeGenMakeSwitchThreshold=4; // Code optimization
    codeGenBitsetTestThreshold=8; // Code optimization
    buildAST = true; // Automatic AST building
}

{
//  Class body inset starts here - at the top within the generated class body
//  Class body inset ends here
}

// ---------------
// Top-level rules
// ---------------
//{ Rule #--- <any_token> is a helper rule for the lexer debugging
any_token :! 
	. // {cout << tokenNames[LA(1)] << " " << LT(1)->getText() << endl;}
;
//}

//{ Rule #--- <sql_script> is a proxy rule for a sequence of optional <sql_stmt> delimited by <SEMICOLON>
sql_script : 
	(sql_stmt)? ( SEMICOLON (sql_stmt)? )*

;
//}

//{ Rule #--- <sql_single_stmt> is a proxy rule for an optional <sql_stmt> followed by an optional <SEMICOLON>
sql_single_stmt : 
	(sql_stmt)? (SEMICOLON)?
;
//}

//  Rule #538 <sql_prefix> is not supported, see NSF2
//  Rule #550 <sql_terminator> is not supported, see NSF2
//  Rule #177 <directly_exec_stmt>  was generalized into the <sql_stmt> rule 
//  Rule #181 <direct_sql_stmt> was generalized into the <sql_stmt> rule 
//{ Rule #--- <sql_stmt> is a proxy rule for, in this parser, DML statements only
sql_stmt : 
	  sql_data_stmt 
;
//}

//  Rule #180 <direct_sql_data_stmt> was merged with the <sql_data_stmt>
//  Rule #525 <sql_data_change_stmt> was incorporated in the rule #526 <sql_data_stmt>
//{ Rule #526 <sql_data_stmt>
sql_data_stmt : 
	  select_stmt 
	| insert_stmt 
	| update_stmt 
	| delete_stmt 
;
//}

//  Rule #179 <direct_select_stmt_n_rows> was replaced by the <select_stmt> rule
//  Rule #210 <dyn_select_stmt> was replaced by the rule #127 <cursor_spec>
//  Rule #127 <cursor_spec> was replaced by the rule <select_stmt>
//  Rule #211 <dyn_single_row_select_stmt> was incorporated in the rule <select_stmt>
//{ Rule #--- <select_stmt> was refined to include an optional <into_clause> - see EXF5
select_stmt : 
	query_exp 
	  ( into_clause (order_by_clause)? (limit_clause)? (updatability_clause)?
	  | order_by_clause (into_clause)? (limit_clause)? (updatability_clause)?
	  | updatability_clause (into_clause)?
      | limit_clause
	  |
	  ) 
;
//}

//{ MySQL limit <limit_clause> 
limit_clause : 
	"limit" i:UNSIGNED_INTEGER {handleLimit(i_AST);} 
;
//}

//{ Rule #--- <into_clause>, see EXF5
into_clause : 
	"into" target_spec (COMMA target_spec)* 
;
//}

//{ Rule #383 <order_by_clause>
order_by_clause : 
	"order" "by" i:sort_spec_list {handleOrderBy(i_AST);} 
;
//}

//{ Rule #520 <sort_spec_list>
sort_spec_list : 
	sort_spec (COMMA sort_spec)* 
;
//}

//{ Rule #519 <sort_spec>
sort_spec : 
	sort_key (collate_clause)? (ordering_spec)? 
;
//}

//{ Rule #518 <sort_key>
sort_key : 
	  column_ref 
	| UNSIGNED_INTEGER 
;
//}

//{ Rule #382 <ordering_spec>
ordering_spec : 
	  "asc" 
	| "desc" 
;
//}

//{ Rule #619 <updatability_clause>
updatability_clause : 
	"for" ( "read" "only" | "update" ("of" column_name_list)? ) 
;
//}

//{ Rule #303 <insert_stmt>
insert_stmt : 
	"insert" "into" table_name insert_columns_and_source 
;
//}

//  Rule #302 <insert_column_list> was replaced by <column_name_list> in the rule #301 <insert_columns_and_source>
//{ Rule #301 <insert_columns_and_source>
insert_columns_and_source : 
	  (LEFT_PAREN column_name_list/*insert_column_list*/ RIGHT_PAREN)=> LEFT_PAREN column_name_list/*insert_column_list*/ RIGHT_PAREN query_exp 
	| query_exp 
	| "default" "values" 
;
//}

//  Rule #623 <update_stmt_searched> was incorporated in the rule <update_stmt>
//  Rule #622 <update_stmt_pos> was incorporated in the rule <update_stmt>
//  Rule #212 <dyn_update_stmt_pos> was incorporated in the rule <update_stmt>
//  Rule #410 <prep_dyn_update_stmt_pos> was incorporated in the rule <update_stmt>
//{ Rule #--- <update_stmt> wraps <update_stmt_searched>,<update_stmt_pos>,<dyn_update_stmt_pos> and <prep_dyn_update_stmt_pos> 
update_stmt : 
	  "update"
	    ( table_name "set" set_clause_list ( "where" (search_condition|"current" "of" dyn_cursor_name) )?   
	    | "set" set_clause_list "where" "current" "of" cursor_name
	    )
;
//}

//{ Rule #482 <set_clause_list>
set_clause_list : 
	set_clause (COMMA set_clause)* 
;
//}

//  Rule #377 <object_column> was replaced by the rule <column_name>
//{ Rule #481 <set_clause>
set_clause : 
	column_name/*object_column*/ EQUALS_OP update_source 
;
//}

//{ Rule #621 <update_source>
update_source : 
	  value_exp 
	| "null" 
	| "default"  
;
//}

//  Rule #161 <delete_stmt_searched> was incorporated in the rule <delete_stmt>
//  Rule #162 <delete_stmt_pos> was incorporated in the rule <delete_stmt>
//  Rule #206 <dyn_delete_stmt_pos> was incorporated in the rule <delete_stmt>
//  Rule #409 <prep_dyn_delete_stmt_pos> was incorporated in the rule <delete_stmt>
//{ Rule #--- <delete_stmt> wraps <delete_stmt_searched>,<delete_stmt_pos>,<dyn_delete_stmt_pos> and <prep_dyn_delete_stmt_pos> 
delete_stmt : 
	  "delete"
	    ( "from" table_name ( "where" (search_condition|"current" "of" dyn_cursor_name) )? 
	    | "where" "current" "of" cursor_name
	    )
;
//}

// ----------------
// Supporting rules
// ----------------
//  Rule #003 <actual_id> was incorporated in the rule #291 <id>
//{ Rule #291 <id> incorporates the rule #003 <actual_id> and <char_set_spec> is <char_set_name>
//  The tricky semantic predicate {true}? for the non_reserved_word is just to make the generator
//  effectively put the alternative to the "default" branch of "switch" statement and use a BitSet
id :
	(INTRODUCER char_set_name)? 
	( REGULAR_ID 
	| DELIMITED_ID 
	| {true}? non_reserved_word 
	) 
;
//}

//  Rule #295 <implt_def_char_rep_name> was replaced by the rule <char_set_name>
//  Rule #298 <implt_def_univ_char_name> was replaced by the rule <char_set_name>
//  Rule #556 <std_char_rep_name> was replaced by the rule <char_set_name>
//  Rule #559 <std_univ_char_form_name> was replaced by the rule <char_set_name>
//  Rule #624 <user_def_char_rep_name> was replaced by the rule <char_set_name>
//  Rule #064 <char_set_spec> is superfluous - it only encapsulates the rule #062 <char_set_name>
//{ Rule #062 <char_set_name> incorporates the rule #534 <sql_language_id>
// This is a generalised version where REGULAR_ID is substituted by <id> to enable left factoring:
char_set_name: id (PERIOD id (PERIOD id)?)?
;
//
//  Another version of <char_set_name> with Syntactic predicate variant: 
//char_set_name: (id PERIOD id PERIOD)=> id PERIOD id PERIOD (REGULAR_ID | non_reserved_word)
//	 | (id PERIOD)=> id PERIOD (REGULAR_ID | non_reserved_word)
//	 | (REGULAR_ID | non_reserved_word)
//;
//  One more version of <char_set_name> with k=4:
//char_set_name: (options{greedy=true;}:(options{greedy=true;}:id PERIOD)? id PERIOD)? REGULAR_ID
//;
//}


//  Rule #056 <catalog_name> was incorporated in the rule #465 <schema_name>
//{ Rule #465 <schema_name> was originally EBNF - schema_name : ( catalog_name "." )? unqualified_schema_name
//  but as catalog_name and unqualified_schema_name are both ids they got sucked in: (id ".")? id 
//  and in order to keep lookahead as low as possible I have used left factoring: id ("." id)?
schema_name :
	id (PERIOD id)?
;
//}

//{ Rule #424 <qualified_name> incorpotates <schema_name> to enable left_factoring
//  It needs k=2 if used in <table_name> see <select_sublist>
qualified_name : 
	i:id (options{greedy=true;}:PERIOD j:id (options{greedy=true;}:PERIOD k:id)?)? {handleQualifiedName(i_AST, j_AST, k_AST);}
;
//}

//{ Rule #474 <select_list>
select_list : 
	  ASTERISK {handleSelectStar();}
	| a:select_sublist (COMMA select_sublist)* {handleSelectList(a_AST);}
;
//}

//{ Rule #476 <select_sublist> with disambiguating syntactic predicate
// (slow backtracking but that's LL(k))
select_sublist : 
	  (table_name PERIOD ASTERISK)=> table_name PERIOD ASTERISK
	| derived_column
;
//}

//  Rule #033 <as_clause> was incorporated in the rule #167 <derived_column>
//{ Rule #167 <derived_column>
// danielw: Make "as" optional to comply with MySQL.  Side effects unknown.
derived_column : 
	val:value_exp (("as")? lbl:column_name)? {handleAlias(val_AST, lbl_AST);}
;
//}

//{ Rule #630 <value_exp_primary> was reorganized to resolve ambiguities and tune performance
value_exp_primary : 
	  fct:set_fct_spec {handleSetFctSpec(fct_AST);}
	| case_exp 
	| cast_spec 
    | (function_ref LEFT_PAREN) => function_spec
	| {LA(1) == INTRODUCER}? ((column_ref)=>column_ref | unsigned_value_spec)
	| {LA(1) != INTRODUCER}? column_ref
	| unsigned_value_spec
	| (LEFT_PAREN value_exp RIGHT_PAREN)=> LEFT_PAREN value_exp RIGHT_PAREN 
	| scalar_subquery 
;
//}

// DanielW: add function_spec
function_parameter_spec :
      value_exp (COMMA value_exp)*
    ;
   
function_ref :
        id (options{greedy=true;}:PERIOD id)?
        ;
//    	id (options{greedy=true;}:PERIOD id)? 
//	id (options{greedy=true;}:PERIOD id (options{greedy=true;}:PERIOD id)?)?

function_spec : 
        a:function_ref LEFT_PAREN function_parameter_spec RIGHT_PAREN
    ;



//  Rule #267 <general_set_fct> was incorporated directly in the rule #490 <set_fct_spec>
//  Rule #491 <set_fct_type> was incorporated directly in the rule #490 <set_fct_spec>
//{ Rule #490 <set_fct_spec> incorporates the rules #491 <set_fct_type> and #267 <general_set_fct>
//  in order to keep the k down to 2.
set_fct_spec : 
	    "count" LEFT_PAREN ( ASTERISK | (set_quantifier)? value_exp ) RIGHT_PAREN 
	| ( "avg"
	  | "max" 
	  | "min" 
	  | "sum" ) LEFT_PAREN (set_quantifier)? value_exp RIGHT_PAREN  
;
//}

//{ Rule #495 <set_quantifier>
set_quantifier : 
	  "distinct" 
	| "all" 
;
//}

//{ Rule #050 <case_exp>
case_exp : 
	  case_abbreviation 
	| case_spec 
;
//}

//{ Rule #049 <case_abbreviation>
case_abbreviation : 
	  "nullif" LEFT_PAREN value_exp COMMA value_exp RIGHT_PAREN 
	| "coalesce" LEFT_PAREN value_exp (COMMA value_exp)* RIGHT_PAREN 
;
//}

//{ Rule #052 <case_spec>
case_spec : 
	  simple_case 
	| searched_case 
;
//}

//  Rule #051 <case_operand> was replaced by <value_exp> in the rule #503 <simple_case>
//{ Rule #503 <simple_case>
simple_case : 
	"case" value_exp/*case_operand*/ (simple_when_clause)+ (else_clause)? "end" 
;
//}

//  Rule #636 <when_operand> was replaced by <value_exp> in the rule #514 <simple_when_clause>
//{ Rule #514 <simple_when_clause> incorporates the rule #636 <when_operand>
simple_when_clause : 
	"when" value_exp/*when_operand*/ "then" result 
;
//}

//{ Rule #218 <else_clause>
else_clause : 
	"else" result 
;
//}

//  Rule #445 <result_exp> was replaced by <value_exp> in the rule #444 <result>
//{ Rule #444 <result> incorporates the rule #445 <result_exp>
result : 
	  value_exp /*result_exp*/ 
	| "null" 
;
//}

//{ Rule #469 <searched_case>
searched_case : 
	"case" (searched_when_clause)+ (else_clause)? "end" 
;
//}

//{ Rule #470 <searched_when_clause>
searched_when_clause : 
	"when" search_condition "then" result 
;
//}

//{ Rule #471 <search_condition>
search_condition : 
	boolean_term (boolean_term_op boolean_term)* 
;
//}

//{ Rule #--- <boolean_term_op>
boolean_term_op :
	"or" 
;
//}

//{ Rule #047 <boolean_term>
boolean_term : 
	boolean_factor (boolean_factor_op boolean_factor)* 
;
//}

//{ Rule #--- <boolean_factor_op>
boolean_factor_op :
	"and" 
;
//}

//{ Rule #045 <boolean_factor>
boolean_factor : 
	("not")? boolean_test 
;
//}

//{ Rule #048 <boolean_test>
boolean_test : 
	boolean_primary ("is" ("not")? truth_value)? 
;
//}

//{ Rule #608 <truth_value>
truth_value : 
	  "true" 
	| "false" 
	| "unknown" 
;
//}

//{ Rule #046 <boolean_primary>
boolean_primary : 
	  (predicate)=> predicate 
	| LEFT_PAREN search_condition RIGHT_PAREN 
;
//}

//{ Rule #407 <predicate> was refined - left factoring
predicate : 
	  row_value_constructor 
	    ( comp_predicate
	    | ("not")? ( between_predicate 
	               | in_predicate 
	               | like_predicate 
	               )
	    | null_predicate 
	    | quantified_comp_predicate 
	    | match_predicate 
	    | overlaps_predicate 
	    ) 
	| exists_predicate 
	| unique_predicate 
;
//}

//{ Rule #102 <comp_predicate>
comp_predicate : 
//	row_value_constructor 
	comp_op row_value_constructor 
;
//}

//{ Rule #101 <comp_op>
comp_op : 
	  EQUALS_OP 
	| NOT_EQUALS_OP
	| LESS_THAN_OP 
	| GREATER_THAN_OP 
	| LESS_THAN_OR_EQUALS_OP
	| GREATER_THAN_OR_EQUALS_OP
    | NOT_EQUALS_OP_ALT
;
//}


//{ Rule #034 <between_predicate>
between_predicate : 
//	row_value_constructor ("not")? 
	"between" row_value_constructor "and" row_value_constructor 
;
//}

//{ Rule #320 <in_predicate>
in_predicate : 
//	row_value_constructor ("not")? 
	"in" in_predicate_value 
;
//}

//{ Rule #321 <in_predicate_value>
in_predicate_value : 
	  (table_subquery)=> table_subquery
	| LEFT_PAREN in_value_list RIGHT_PAREN 
;
//}

//{ Rule #322 <in_value_list>
in_value_list : 
	value_exp (COMMA value_exp)* 
;
//}

//  Rule #346 <match_value> was merged within the rule #338 <like_predicate>
//{ Rule #338 <like_predicate> was extended as <match_value> is a subset of value_exp anyway
like_predicate : 
//	match_value ("not")? 
	"like" pattern ("escape" escape_char)? 
;
//}

//{ Rule #395 <pattern>
pattern : 
	char_value_exp 
;
//}

//{ Rule #237 <escape_char>
escape_char : 
	char_value_exp 
;
//}

//{ Rule #370 <null_predicate>
null_predicate : 
//	row_value_constructor 
	"is" ("not")? "null" 
;
//}

//  Rule #014 <all> was incorporated in the <quantifier> rule #427
//  Rule #427 <quantifier> was incorporated in the rule #426 <quantified_comp_predicate>
//{ Rule #426 <quantified_comp_predicate>
quantified_comp_predicate : 
//	row_value_constructor 
	comp_op ("all" | "some" | "any") table_subquery 
;
//}

//{ Rule #243 <exists_predicate>
exists_predicate : 
	"exists" table_subquery 
;
//}

//{ Rule #612 <unique_predicate>
unique_predicate : 
	"unique" table_subquery 
;
//}

//{ Rule #344 <match_predicate>
match_predicate : 
//	row_value_constructor 
	"match" ("unique")? ("full" | "partial")? table_subquery 
;
//}

//{ Rule #385 <overlaps_predicate>
overlaps_predicate : 
//	row_value_constructor 
	"overlaps" row_value_constructor 
;
//}

//{ Rule #054 <cast_spec>
cast_spec : 
	"cast" LEFT_PAREN cast_operand "as" cast_target RIGHT_PAREN 
;
//}

//{ Rule #053 <cast_operand>
cast_operand : 
	  value_exp 
	| "null"
;
//}

//{ Rule #055 <cast_target>
cast_target : 
	  domain_name 
	| data_type 
;
//}

//{ Rule #140 <data_type>
data_type : 
	  char_string_type ("character" "set" char_set_name)? 
	| national_char_string_type 
	| bit_string_type 
	| num_type 
	| datetime_type 
	| interval_type 
;
//}

//{ Rule #333 <length>
length : 
	UNSIGNED_INTEGER 
;
//}

//{ Rule #066 <char_string_type>
char_string_type : 
	  "character" (LEFT_PAREN length RIGHT_PAREN)? 
	| "char" (LEFT_PAREN length RIGHT_PAREN)? 
	| "character" "varying" LEFT_PAREN length RIGHT_PAREN 
	| "char" "varying" LEFT_PAREN length RIGHT_PAREN 
	| "varchar" LEFT_PAREN length RIGHT_PAREN 
;
//}

//{ Rule #359 <national_char_string_type>
national_char_string_type : 
	  "national" 
		( "character" (LEFT_PAREN length RIGHT_PAREN)? 
		| "char" (LEFT_PAREN length RIGHT_PAREN)? 
		| "character" "varying" LEFT_PAREN length RIGHT_PAREN 
		| "char" "varying" LEFT_PAREN length RIGHT_PAREN
		)
	| "nchar" (LEFT_PAREN length RIGHT_PAREN)? 
	| "nchar" "varying" LEFT_PAREN length RIGHT_PAREN 
;
//}

//{ Rule #041 <bit_string_type>
bit_string_type : 
	  "bit" (LEFT_PAREN length RIGHT_PAREN)? 
        | "bit" "varying" LEFT_PAREN length RIGHT_PAREN 
;
//}

//{ Rule #406 <precision>
precision : 
	UNSIGNED_INTEGER 
;
//}

//{ Rule #458 <scale>
scale : 
	UNSIGNED_INTEGER 
;
//}

//{ Rule #374 <num_type>
num_type : 
	  exact_num_type 
	| approximate_num_type 
;
//}

//{ Rule #025 <approximate_num_type>
approximate_num_type : 
	  "float" (LEFT_PAREN precision RIGHT_PAREN)? 
	| "real" 
	| "double" "precision" 
;
//}

//{ Rule #239 <exact_num_type>
exact_num_type : 
	  "numeric" ( LEFT_PAREN precision (COMMA scale)? RIGHT_PAREN )? 
        | "decimal" ( LEFT_PAREN precision (COMMA scale)? RIGHT_PAREN )? 
	| "dec" ( LEFT_PAREN precision (COMMA scale)? RIGHT_PAREN )? 
	| "integer" 
	| "int" 
	| "smallint" 
;
//}

//{ Rule #146 <datetime_type>
datetime_type : 
	  "date"
	| "time" (LEFT_PAREN time_precision RIGHT_PAREN)? ("with" "time" "zone")?
	| "timestamp" (LEFT_PAREN timestamp_precision RIGHT_PAREN)? ("with" "time" "zone")? 
;
//}

//{ Rule #316 <interval_type>
interval_type : 
	"interval" interval_qualifier 
;
//}

//{ Rule #457 <scalar_subquery>
scalar_subquery : 
	subquery 
;
//}

//{ Rule #568 <subquery>
subquery : 
	LEFT_PAREN query_exp RIGHT_PAREN 
;
//}

//  Rule #365 <non_join_query_exp> was incorporated in the rule #428 <query_exp>
//{ Rule #428 <query_exp> incorporates the rule #365 <non_join_query_exp>
//  The alternative <joined_table> was merged in the rule #429 <query_primary>
query_exp : 
	query_term ( ("union" | "except") ("all")? (corresponding_spec)? query_term )*
;
//}

//  Rule #367 <non_join_query_term> was incorporated in the rule #431 <query_term>
//{ Rule #431 <query_term> incorporates the rule #367 <non_join_query_term>
//  The alternative <joined_table> was merged in the rule #429 <query_primary>
query_term : 
	query_primary ( "intersect" ("all")? (corresponding_spec)? query_primary)*
;
//}

//  Rule #120 <corresponding_column_list> was replaced by <column_name_list> in the rule #121 <corresponding_spec>
//{ Rule #121 <corresponding_spec>
corresponding_spec : 
	"corresponding" ("by" LEFT_PAREN column_name_list/*corresponding_column_list*/ RIGHT_PAREN)? 
;
//}

//  Rule #366 <non_join_query_primary> was incorporated in the rule #429 <query_primary>
//{ Rule #429 <query_primary> incorporates the rule #367 <non_join_query_primary>
query_primary : 
	  simple_table 
	| table_ref // EXF0 eliminating the syntactic predicate with <joined_table>
;
//}

//{ Rule #507 <simple_table>
simple_table : 
	  query_spec 
	| table_value_constructor 
	| explicit_table 
;
//}

//  Rule #475 <select_stmt_single_row> was incorporated in the rule #430 <query_spec>
//{ Rule #430 <query_spec> additionally incorporates the rule #475 <select_stmt_single_row> - see EXF5
query_spec : 
	"select" (set_quantifier)? select_list (into_clause)? table_exp 
;
//}

//{ Rule #579 <table_value_constructor>
table_value_constructor : 
	"values" table_value_const_list
;
//}

//{ Rule #580 <table_value_const_list>
table_value_const_list :
	row_value_constructor (COMMA row_value_constructor)* 
;
//}

//  Rule #451 <row_subquery> was merged in the rule #452 <row_value_constructor> as it is redundant with
//{ Rule #452 <row_value_constructor> was refined due to NSF1
//  <row_subquery> alt. is a subset of <value_exp> so it clashes with <row_value_constructor_elem> alt.
row_value_constructor : 
	  (row_value_constructor_elem)=>row_value_constructor_elem
	| LEFT_PAREN row_value_const_list RIGHT_PAREN
//	| row_subquery 
;
//}

//  Rule #371 <null_spec> was incorporated in the rule #455 <row_value_constructor_elem>
//  Rule #159 <default_spec> was incorporated in the rule #455 <row_value_constructor_elem>
//{ Rule #455 <row_value_constructor_elem> incorporates the rules #371 <null_spec> and #159 <default_spec>
row_value_constructor_elem : 
	  value_exp 
	| "null" 
	| "default"  
;
//}

//{ Rule #456 <row_value_const_list>
row_value_const_list : 
	row_value_constructor_elem (COMMA row_value_constructor_elem)* 
;
//}

//{ Rule #244 <explicit_table>
explicit_table : 
	"table" table_name 
;
//}

//{ Rule #325 <joined_table> - in order to avoid resursion, I have resolved the <cross_join> and <qualified_join>
//  The original recursive reference to <joined_table> may be skipped as <table_ref_aux> can also
//  be a <subquery>, which means it will come down to <joined_table> anyway.
joined_table : 
	table_ref_aux (qualified_join | cross_join)
; 
//}

//{ Rule #577 <table_ref> was refined to avoid recursion, see also rule #325 <joined_table>
//  The original recursive reference to <joined_table> may be skipped as <table_ref_aux> can also
//  be a <subquery>, which means it will come down to <joined_table> anyway.
table_ref : 
	table_ref_aux (options{greedy=true;}:qualified_join | cross_join)*
;
//}

//  Rule #169 <derived_table> was replaced by <table_subquery> in the rule <table_ref_aux>
//{ Rule #--- <table_ref_aux> was introduced to avoid recursion, see also rule #325 <joined_table>
table_ref_aux : 
	(table_name | /*derived_table*/table_subquery) (("as")? correlation_name (LEFT_PAREN derived_column_list RIGHT_PAREN)?)? 
;
//}

//{ Rule #168 <derived_column_list>
derived_column_list : 
	column_name_list 
;
//}

//{ Rule #578 <table_subquery>
table_subquery : 
	subquery 
;
//}

//{ Rule #122 <cross_join> - leading  <table_ref> was skipped to avoid recursion, see rule #325 <joined_table>
cross_join : 
	"cross" "join" table_ref
;
//}

//  Rule #329 <join_type> was incorporated in the rule #422 <qualified_join>
//{ Rule #422 <qualified_join> incorporates the rule #329 <join_type> to conform to the Syntax Rules.
//  The leading <table_ref> was skipped to avoid recursion, see rule #325 <joined_table>.
qualified_join : 
//	("natural")? (join_type)? "join" table_ref (options{greedy=true;}:join_spec)? 
	  ( "inner" | outer_join_type ("outer")? )? "join" table_ref join_spec 
	| "natural" ( "inner" | outer_join_type ("outer")? )? "join" table_ref 
	| "union" "join" table_ref 
;
//}

//{ Rule #384 <outer_join_type>
outer_join_type : 
	  "left" 
	| "right" 
	| "full" 
;
//}

//{ Rule #328 <join_spec>
join_spec : 
	  join_condition 
	| named_columns_join 
;
//}

//{ Rule #327 <join_condition>
join_condition : 
	"on" search_condition 
;
//}

//  Rule #326 <join_column_list> was replaced by the rule #094 <column_name_list>
//{ Rule #357 <named_columns_join>
named_columns_join : 
	"using" LEFT_PAREN column_name_list/*join_column_list*/ RIGHT_PAREN 
;
//}

//{ Rule #575 <table_exp>
table_exp : 
	from_clause 
	(where_clause)? 
	(group_by_clause)? 
	(having_clause)? 
;
//}

//{ Rule #265 <from_clause>
from_clause : 
	"from" table_ref_list 
;
//}

//{ Rule #--- <table_ref_list> was introduced to keep the identical fashion of <table_exp> clauses
table_ref_list : 
	a:table_ref (COMMA b:table_ref)*  {handleTableList(a_AST, b_AST);}
;
//}

//{ Rule #637 <where_clause>
where_clause : 
	"where" search_condition 
;
//}

//{ Rule #281 <group_by_clause>
group_by_clause : 
	"group" "by" a:grouping_column_ref_list {handleGroupBy(a_AST);}
;
//}

//{ Rule #279 <grouping_column_ref>
grouping_column_ref : 
	a:column_ref (collate_clause)? {handleGroupColumn(a_AST);}
;
//}

//{ Rule #280 <grouping_column_ref_list>
grouping_column_ref_list : 
	grouping_column_ref (COMMA grouping_column_ref)*
;
//}

//{ Rule #282 <having_clause>
having_clause : 
	"having" search_condition 
;
//}

//{ Rule #618 <unsigned_value_spec>
unsigned_value_spec : 
	  unsigned_lit 
	| general_value_spec ;
//}

//{ Rule #268 <general_value_spec>
general_value_spec : 
	  parameter_spec 
	| dyn_parameter_spec 
	| variable_spec 
	| "user"
	| "current_user"
	| "session_user"
	| "system_user"
	| "value" 
;
//}

//  Rule #299 <indicator_parameter> was incorporated in the rule #390 <parameter_spec>
//{ Rule #390 <parameter_spec>
parameter_spec : 
	parameter_name (/*indicator_parameter*/("indicator")? parameter_name)? 
;
//}

//{ Rule #209 <dyn_parameter_spec>
dyn_parameter_spec : 
	QUESTION_MARK 
;
//}

//  Rule #300 <indicator_variable> was incorporated in the rule #632 <variable_spec>
//{ Rule #632 <variable_spec>
variable_spec : 
	EMBDD_VARIABLE_NAME (/*indicator_variable*/("indicator")? EMBDD_VARIABLE_NAME)? 
;
//}

//{ Rule #375 <num_value_exp> - just a generic <value_exp> due to the data type equivalence, see also NSF1
num_value_exp : 
	value_exp 
;
//}

//{ Rule #566 <string_value_exp> - the <bit_value_exp> was merged with <char_value_exp>
string_value_exp : 
	char_value_exp 
;
//}

//{ Rule #148 <datetime_value_exp> - just a generic <value_exp> due to the data type equivalence, see also NSF1
datetime_value_exp : 
	value_exp 
;
//}

//{ Rule #317 <interval_value_exp> - just a generic <value_exp> due to the data type equivalence, see also NSF1
interval_value_exp : 
	value_exp
;
//}
//{ Rule #069 <char_value_exp> - just a generic <value_exp> due to the data type equivalence, see also NSF1
char_value_exp : 
    value_exp
;
//}

//  Rule #103 <concatenation> was incorporated in the rule #629 <value_exp>, see NSF1
//{ Rule #629 <value_exp> must have been totally redesigned to resolve the issue of data type equivalence, see NSF1
value_exp : 
	term (options{greedy=true;}:(term_op | CONCATENATION_OP) term )* 
;
//}

//{ Rule #567 <string_value_fct> - the alternative <bit_value_fct> is commented as it is just a subset of <char_value_fct>
string_value_fct : 
	  char_value_fct 
//	| bit_value_fct 
;
//}

//{ Rule #070 <char_value_fct>
char_value_fct : 
	  char_substring_fct 
	| fold 
	| form_conversion 
	| char_translation 
	| trim_fct 
;
//}

//  Rule #554 <start_position> was replaced by <num_value_exp>
//  Rule #565 <string_length> was replaced by <num_value_exp>
//{ Rule #067 <char_substring_fct>
char_substring_fct : 
	"substring" LEFT_PAREN char_value_exp "from" num_value_exp/*start_position */ 
	("for" num_value_exp/*string_length*/)? 
	RIGHT_PAREN 
;
//}

//{ Rule #259 <fold>
fold : 
	("upper" | "lower") LEFT_PAREN char_value_exp RIGHT_PAREN 
;
//}

//{ Rule #260 <form_conversion>
form_conversion : 
	"convert" LEFT_PAREN char_value_exp "using" form_conversion_name RIGHT_PAREN 
;
//}

//{ Rule #068 <char_translation>
char_translation : 
	"translate" LEFT_PAREN char_value_exp "using" translation_name RIGHT_PAREN 
;
//}

//{ Rule #604 <trim_fct>
trim_fct : 
	"trim" LEFT_PAREN trim_operands RIGHT_PAREN 
;
//}

//  Rule #603 <trim_char> was replaced by <char_value_exp>
//  Rule #606 <trim_source> was replaced by <char_value_exp>
//{ Rule #605 <trim_operands> was refined to avoid ambiguity in: ((trim_spec)? (char_value_exp)? "from") char_value_exp
trim_operands : 
	  trim_spec "from" char_value_exp/*trim_source*/ 
	| trim_spec char_value_exp/*trim_char*/ "from" char_value_exp/*trim_source*/ 
	| "from" char_value_exp/*trim_source*/ 
	| char_value_exp/*trim_char or trim_source*/ ("from" char_value_exp/*trim_source*/)? 
;
//}

//{ Rule #607 <trim_spec>
trim_spec : 
	  "leading" 
	| "trailing" 
	| "both" 
;
//}

//{ Rule #--- <term_op> is a redundant rule to <sign> to distinguish between unary and binary operators
term_op :
	PLUS_SIGN | MINUS_SIGN 
;
//}

//{ Rule #584 <term> was refined to avoid infinite recursion
term : 
	factor (options{greedy=true;}:factor_op factor)* 
;
//}

//{ Rule #--- <factor_op> is additional used for simplicity in the rule #313 <interval_term>
factor_op :
	ASTERISK | SOLIDUS 
;
//}

//  Rule #057 <char_factor> was merged within the rule #255 <factor>, see also NSF1
//{ Rule #255 <factor> incorporates the rules #141 <datetime_factor> and #307 <interval_factor>
factor : 
	(sign)? gen_primary ("at" time_zone_specifier | collate_clause)? 
;
//}

//{ Rule #084 <collate_clause>
collate_clause : 
	"collate" collation_name 
;
//}

//  Rule #373 <num_primary> was merged in the rule <gen_primary>, see also NSF1
//  Rule #059 <char_primary> was merged in the rule <gen_primary>, see also NSF1
//{ Rule #--- <gen_primary> is a proxy rule containing various data types, see NSF1
//  incorporates the rules #373 <num_primary>, #059 <char_primary, #144 <datetime_primary> and #311 <interval_primary>
gen_primary : 
	  value_exp_primary (interval_qualifier)? 
	| num_value_fct
	| string_value_fct 
	| datetime_value_fct
;
//}

//{ Rule #376 <num_value_fct>
num_value_fct : 
	  position_exp 
	| extract_exp 
	| length_exp 
;
//}

//{ Rule #405 <position_exp>
position_exp : 
	"position" LEFT_PAREN char_value_exp "in" char_value_exp RIGHT_PAREN 
;
//}

//{ Rule #252 <extract_exp>
extract_exp : 
	"extract" LEFT_PAREN extract_field "from" extract_source RIGHT_PAREN 
;
//}

//{ Rule #253 <extract_field>
extract_field : 
	  datetime_field 
	| time_zone_field 
;
//}

//{ Rule #142 <datetime_field>
datetime_field : 
	  non_second_datetime_field 
	| "second" 
;
//}

//{ Rule #593 <time_zone_field>
time_zone_field : 
	  "timezone_hour" 
	| "timezone_minute" 
;
//}

//{ Rule #254 <extract_source> - <interval_value_exp> was commented as it is an equivalent of <datetime_value_exp>, see NSF1
extract_source : 
	  datetime_value_exp 
//	| interval_value_exp 
;
//}

//{ Rule #334 <length_exp>
length_exp : 
	  char_length_exp 
	| octet_length_exp 
	| bit_length_exp 
;
//}

//{ Rule #058 <char_length_exp>
char_length_exp : 
	("char_length" | "character_length") LEFT_PAREN string_value_exp RIGHT_PAREN 
;
//}

//{ Rule #380 <octet_length_exp>
octet_length_exp : 
	"octet_length" LEFT_PAREN string_value_exp RIGHT_PAREN 
;
//}

//{ Rule #038 <bit_length_exp>
bit_length_exp : 
	"bit_length" LEFT_PAREN string_value_exp RIGHT_PAREN 
;
//}

/*  Rule #043 <bit_value_exp> - is commented as it is a subset of the rule #069 <char_value_exp>, see also NSF1
bit_value_exp : 
	bit_factor (CONCATENATION_OP bit_factor)*
;
*/

/*  Rule #039 <bit_primary> - is commented as it is a subset of the rule #059 <char_primary>, see also NSF1
bit_primary : 
	  value_exp_primary 
	| string_value_fct 
;
*/

/*  Rule #037 <bit_factor> - is commented as it is a subset of the rule #057 <char_factor>, see also NSF1
bit_factor : 
	bit_primary 
;
*/

/*  Rule #044 <bit_value_fct> - is commented as it is a subset of the rule #070 <char_value_fct>, see also NSF1
bit_value_fct : 
	bit_substring_fct 
;
*/

/*  Rule #042 <bit_substring_fct> - is commented as it is a subset of the rule #067 <char_substring_fct>, see also NSF1
bit_substring_fct : 
	"substring" LEFT_PAREN bit_value_exp "from" num_value_exp 
	("for" num_value_exp)? 
	RIGHT_PAREN 
;
*/

//  Rule #145 <datetime_term> was incorporated in the rule #141 <datetime_factor>
//  Rule #592 <time_zone> was incorporated in the rule #141 <datetime_factor>
//  Rule #141 <datetime_factor> was merged into the rule #255 <factor>, see also NSF1
//{ Rule #594 <time_zone_specifier>
time_zone_specifier : 
	  "local" 
	| "time" "zone" interval_value_exp 
;
//}

//  Rule #144 <datetime_primary> was merged in the rule <gen_primary>, see also NSF1
//{ Rule #149 <datetime_value_fct>
datetime_value_fct : 
	  current_date_value_fct 
	| current_time_value_fct 
	| currenttimestamp_value_fct 
;
//}

//{ Rule #123 <currenttimestamp_value_fct>
currenttimestamp_value_fct : 
	"current_timestamp" (LEFT_PAREN timestamp_precision RIGHT_PAREN)?
;
//}

//{ Rule #124 <current_date_value_fct>
current_date_value_fct : 
	"current_date" 
;
//}

//{ Rule #125 <current_time_value_fct>
current_time_value_fct : 
	"current_time" (LEFT_PAREN time_precision RIGHT_PAREN)? 
;
//}

//  Rule #587 <time_frac_seconds_prec> was incorporated in <timestamp_precision> and <time_precision>
//{ Rule #586 <timestamp_precision>
timestamp_precision : 
	UNSIGNED_INTEGER/*time_frac_seconds_prec*/ 
;
//}

//{ Rule #589 <time_precision>
time_precision : 
	UNSIGNED_INTEGER/*time_frac_seconds_prec*/ 
;
//}

//  Rule #318 <interval_value_exp_1> was incorporated int the rule #317 <interval_value_exp>
//  Rule #311 <interval_primary> was merged in the rule <gen_primary>, see also NSF1
//  Rule #307 <interval_factor> was merged into the rule #255 <factor>, see also NSF1
//  Rule #313 <interval_term> was merged into the rule #584 <term>, see also NSF1
//  Rule #425 <qualifier> was replaced by <table_name> as it is ambiguous
//  Rule #341 <local_table_name> was replaced by <qualified_local_table_name>
//{ Rule #576 <table_name>
table_name : 
      qualified_name 
	| qualified_local_table_name 
;
//}

function_name :
     qualified_name 
    ;
//{ Rule #423 <qualified_local_table_name>
qualified_local_table_name :
	"module" PERIOD id 
;
//}

//{ Rule #186 <domain_name>
domain_name : 
	qualified_name 
;
//}

//{ Rule #093 <column_name>
column_name : 
	id 
;
//}

//{ Rule #094 <column_name_list>
column_name_list : 
	column_name (COMMA column_name)* 
;
//}

//{ Rule #119 <correlation_name>
correlation_name : 
	id 
;
//}

//{ Rule #126 <cursor_name>
cursor_name : 
	id 
;
//}

//{ Rule #204 <dyn_cursor_name>
dyn_cursor_name : 
	  {LA(1) == INTRODUCER}? ((cursor_name)=>cursor_name | extended_cursor_name) 
	| {LA(1) != INTRODUCER}? cursor_name 
	| extended_cursor_name 
;
//}

//{ Rule #246 <extended_cursor_name>
extended_cursor_name : 
	("global" | "local")? simple_value_spec 
;
//}

//{ Rule #511 <simple_value_spec>
simple_value_spec : 
	  parameter_name 
	| EMBDD_VARIABLE_NAME 
	| lit 
;
//}

//{ Rule #563 <stmt_name>
stmt_name : 
	id 
;
//}

//{ Rule #389 <parameter_name> - an extended version - see EXF4
parameter_name : 
//	COLON id 
	COLON 
	  ( id (PERIOD id)*
	  | UNSIGNED_INTEGER
	  )
;
//}

//{ Rule #582 <target_spec>
target_spec : 
	  parameter_spec 
	| variable_spec 
;
//}

//{ Rule #095 <column_ref> incorpotates <catalog_name>; <schema_name> and <table_name> to enable left_factoring
column_ref : 
	a:id (PERIOD b:id (PERIOD c:id (PERIOD d:id)?)?)?  {handleColumnRef(a_AST, b_AST, c_AST, d_AST);}
;
//}

//{ Rule #600 <translation_name>
translation_name : 
	qualified_name 
;
//}

//{ Rule #087 <collation_name>
collation_name : 
	qualified_name 
;
//}

//{ Rule #261 <form_conversion_name>
form_conversion_name : 
	qualified_name 
;
//}

//{ Rule #500 <sign>
sign : 
	PLUS_SIGN | MINUS_SIGN 
;
//}

//{ Rule #617 <unsigned_num_lit> has been refined because <UNSIGNED_INTEGER> has to be a separate token as it is
//  directly referenced from other rules whereby it is also a subtype of <unsigned_num_lit>
unsigned_num_lit : 
	  UNSIGNED_INTEGER
	| EXACT_NUM_LIT
	| APPROXIMATE_NUM_LIT
;
//}

//{ Rule #065 <char_string_lit>
char_string_lit :
	(INTRODUCER char_set_name)? CHAR_STRING
;
//}

//{ Rule #616 <unsigned_lit>
unsigned_lit : 
	  unsigned_num_lit 
	| general_lit 
;
//}

//{ Rule #266 <general_lit>
general_lit : 
	  char_string_lit 
	| NATIONAL_CHAR_STRING_LIT 
	| BIT_STRING_LIT 
	| HEX_STRING_LIT 
	| datetime_lit 
	| interval_lit 
;
//}

//{ Rule #143 <datetime_lit>
datetime_lit : 
	  date_lit 
	| time_lit 
	| timestamp_lit 
;
//}

//  Rule #147 <datetime_value> was replaced by the rule #615 <UNSIGNED_INTEGER>
//  Rule #638 <years_value> was incorporated directly in the rule #152 <date_value>
//  Rule #350 <months_value> was incorporated directly in the rule #152 <date_value>
//  Rule #153 <days_value> was incorporated directly in the rule #152 <date_value>
//  Rule #152 <date_value> was incorporated directly in the rule #151 <date_string>
//  Rule #151 <date_string> was replaced by <CHAR_STRING> in the rule #150 <date_lit>
//{ Rule #150 <date_lit> - <date_string> was replaced by <CHAR_STRING>
date_lit : 
	"date" CHAR_STRING/*date_string*/ 
;
//}

//  Rule #--- <interval_string> (no number in BNF) was incorporated in the rule #310 <interval_lit>
//{ Rule #310 <interval_lit> - <interval_string> was replaced by <CHAR_STRING>
interval_lit : 
	"interval" (sign)? CHAR_STRING/*interval_string*/ interval_qualifier 
;
//}

//  Rule #515 <single_datetime_field> was incorporated in the rule #312 <interval_qualifier> to avoid a syntactic predicate
//{ Rule #312 <interval_qualifier> sucked in the rule <single_datetime_field> to avoid a syntactic predicate
interval_qualifier : 
	  start_field 
	    ( "to" end_field
	    | /*single_datetime_field*/
	    ) 
	| "second" /*single_datetime_field*/
	    ( LEFT_PAREN UNSIGNED_INTEGER/*interval_leading_fieldprec*/ 
		(COMMA UNSIGNED_INTEGER/*interval_frac_seconds_prec*/)?
	      RIGHT_PAREN 
	    )? 
;
//}

//  Rule #309 <interval_leading_fieldprec> was incorporated in the rule #553 <start_field> and #515 <single_datetime_field>
//{ Rule #553 <start_field>
start_field : 
	non_second_datetime_field (LEFT_PAREN UNSIGNED_INTEGER/*interval_leading_fieldprec*/ RIGHT_PAREN)? 
;
//}

//  Rule #308 <interval_frac_seconds_prec> was incorporated in the rule #234 <end_field> and #515 <single_datetime_field>
//{ Rule #235 <end_field>
end_field : 
	  non_second_datetime_field 
	| "second" (LEFT_PAREN UNSIGNED_INTEGER/*interval_frac_seconds_prec*/ RIGHT_PAREN)? 
;
//}

//{ Rule #369 <non_second_datetime_field>
non_second_datetime_field : 
	  "year" 
	| "month" 
	| "day" 
	| "hour" 
	| "minute" 
;
//}

//{ Rule #340 <lit>
lit : 
	  signed_num_lit 
	| general_lit 
;
//}

//{ Rule #502 <signed_num_lit>
signed_num_lit : 
	(sign)? unsigned_num_lit 
;
//}

//  Rule #--- <timestamp_string> (no number in BNF) was replaced by <CHAR_STRING>
//{ Rule #585 <timestamp_lit> - <timestamp_string> was replaced by <CHAR_STRING>
timestamp_lit : 
	"timestamp" CHAR_STRING/*timestamp_string*/  
;
//}

//  Rule #290 <hours_value> was incorporated directly in the rule #590 <time_string>
//  Rule #348 <minutes_value> was incorporated directly in the rule #590 <time_string>
//  Rule #472 <seconds_integer_value> was incorporated directly in the rule #590 <time_string>
//  Rule #--- <seconds_fraction> (no number in BNF) was incorporated directly in the rule #590 <time_string>
//  Rule #473 <seconds_value> was incorporated directly in the rule #590 <time_string>
//  Rule #591 <time_value>  was incorporated in the rule #590 <time_string>
//  Rule #590 <time_string> was replaced by <CHAR_STRING> in the rule #588 <time_lit>
//{ Rule #588 <time_lit> - <time_string> was replaced by <CHAR_STRING>
time_lit : 
	"time" CHAR_STRING/*time_string*/  
;
//}

//  Rule #443 <reserved_word> was incorporated in the lexer as SQL2RW_xxxx literals
//{ Rule #368 <non_reserved_word> - to no avail in DmlSQL2 but at least a single word to keep the encapsulating rules intact 
non_reserved_word : 
 	  "ada" 
;
//}

