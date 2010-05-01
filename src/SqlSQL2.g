//
//  Standard ISO/ANSI SQL2 (also known as SQL92) grammar
//  Full SQL Level 
//  Copyright (C) 2003 Lubos Vnuk 
//
//{ Foreword
//    This software is an SQL2 grammar, extending the DmlSQL2 grammar with the non-DML statements, 
//    developed by Lubos Vnuk (lvnuk[atsign]host.sk) using the "ISO/IEC 9075:1992, Database Language SQL"
//    standard and the SQL2 BNF notation (further referred to as BNF) that can be obtained at:
//    http://cui.unige.ch/db-research/Enseignement/analyseinfo/SQL92/BNFindex.html
//    
//    The grammar has been created for the LL(k) parser generator ANTLR 2.7.2 (www.antlr.org).
//    It will generate the lexer and parser source code in C++.
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
//    SqlSQL2 1.00 - 2003Jul01 - Initial release 
//}
//{ To-do (TDO) list
//    TDO0: Tree building
//    TDO1: Unicode character repertoire support
//}
//{ Not Supported Feature (NSF) list
//    NSF0: Character repertoires (<char_set_name> is supported but the lexer works with 8-bit chars only)
//    NSF1: Semantic analysis as the meta data is not available (automatic data type conversion is presumed)
//    NSF2: Embedded SQL languages Ada,C,Cobol,Fortran,MUMPS,Pascal and PLI except for <EMBDD_VARIABLE_NAME>
//    NSF3: SQL Flagger - <sql_object_indentifier> and associated subrules are irrelevant
//}
//{ Extended Feature (EXF) list
//    EXF1: Components of <char_string_lit> and <NATIONAL_CHAR_STRING_LIT> may contain <NEWLINE> character
//    EXF2: <char_set_name> may contain a generalized <id> in place of the <SQL_language_id>
//    EXF3: <REGULAR_ID> and <DELIMITED_ID> may contain more then 128 characters
//    EXF4: <parameter_name> may also be a COLON UNSIGNED_INTEGER or a COLON id (PERIOD id)*
//    EXF5: <select_stmt> may contain an <into_clause>
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
#include "parserBase.h"
#include "boost/shared_ptr.hpp"
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
// SqlSQL2Lexer is derived from DmlSQL2Lexer
{
//  Class preamble starts here - right before the class definition in the generated class file

//  Class preamble ends here
}
class SqlSQL2Lexer extends DmlSQL2Lexer;
options {
    importVocab = SqlSQL2Imp; // Main import vocabulary file
    exportVocab = SqlSQL2Lex; // Lexer's exportVocab is parser's importVocab
    defaultErrorHandler = false; // Automatic error handling
}

{
//  Class body inset starts here - at the top within the generated class body

//  Class body inset ends here
}
// This redundant definition is here because ANTLR requires at least one rule per class
PERCENT : '%' ; // Not used in the parser
//
// SqlSQL2Parser is derived from DmlSQL2Parser
{
//  Class preamble starts here - right before the class definition in the generated class file

//  Class preamble ends here
}

class SqlSQL2Parser extends DmlSQL2Parser;
options {
    importVocab = SqlSQL2Lex; // Import vocabulary from the lexer
    exportVocab = SqlSQL2; // Export vocabulary
    defaultErrorHandler = false; // Automatic error handling
    k = 2; // Lookahead can't be lower than in the supergrammar
    buildAST = true; // Automatic AST building
}

{
//  Class body inset starts here - at the top within the generated class body

public:

void handleColumnRef(RefAST a, RefAST b, RefAST c, RefAST d) {
    if(_columnRefHandler.get()) {
        (*_columnRefHandler)(a, b, c, d);
    }
    return; // Do-nothing placeholder
}
void handleQualifiedName(RefAST a, RefAST b, RefAST c) {
    if(_qualifiedNameHandler.get()) {
        (*_qualifiedNameHandler)(a, b, c);
    }
    return; // Do-nothing placeholder
}
void handleTableList(RefAST a, RefAST b) {
    if(_tableListHandler.get()) {
        (*_tableListHandler)(a, b);
    }
    return; // Do-nothing placeholder
}
void handleAlias(RefAST a, RefAST b) {
    if(_aliasHandler.get()) {
        (*_aliasHandler)(a, b);
    }
    return; // Do-nothing placeholder
}
void handleSetFctSpec(RefAST fct) {
    if(_setFctSpecHandler.get()) {
        (*_setFctSpecHandler)(fct);
    }
    return; // Do-nothing placeholder
}
public: // Public (Until the functionality is complete?)
boost::shared_ptr<VoidThreeRefFunc> _qualifiedNameHandler;
boost::shared_ptr<VoidFourRefFunc> _columnRefHandler;
boost::shared_ptr<VoidTwoRefFunc> _tableListHandler;
boost::shared_ptr<VoidTwoRefFunc> _aliasHandler;
boost::shared_ptr<VoidOneRefFunc> _setFctSpecHandler;
//  Class body inset ends here
}

// ---------------
// Top-level rules
// ---------------
//{ Rule #--- <sql_stmt> is a proxy rule for all types of sql statements
sql_stmt : 
	  sql_data_stmt 
	| sql_schema_stmt 
	| sql_transaction_stmt 
	| 
	( options {generateAmbigWarnings=false;}:
         // Keeping this order avoids the clash of the "set" statements
	 // due to the linear approximation of the lookahead 
	    sql_session_stmt 	// LA(1) is surely "set"
	  | sql_connection_stmt
	)
	| sql_dyn_stmt
	| system_descriptor_stmt 
	| get_diag_stmt
	| declare_cursor
	| temporary_table_decl
;
//}

//{ Rule #543 <sql_schema_stmt>
sql_schema_stmt : 
	  sql_schema_def_stmt 
	| sql_schema_manipulat_stmt 
;
//}

//{ Rule #541 <sql_schema_def_stmt>
sql_schema_def_stmt : 
	  schema_def 
	| table_def 
	| view_def 
	| grant_stmt 
	| domain_def 
	| char_set_def 
	| collation_def 
	| translation_def 
	| assertion_def 
;
//}

//{ Rule #542 <sql_schema_manipulat_stmt>
sql_schema_manipulat_stmt : 
	  drop_schema_stmt 
	| alter_table_stmt 
	| drop_table_stmt 
	| drop_view_stmt 
	| revoke_stmt 
	| alter_domain_stmt 
	| drop_domain_stmt 
	| drop_char_set_stmt 
	| drop_collation_stmt 
	| drop_translation_stmt 
	| drop_assertion_stmt 
;
//}

//{ Rule #551 <sql_transaction_stmt>
sql_transaction_stmt : 
	  commit_stmt 
	| rollback_stmt 
	| set_constraints_mode_stmt 
	| set_transaction_stmt 
;
//}

//{ Rule #524 <sql_connection_stmt>
sql_connection_stmt : 
	  connect_stmt 
	| disconnect_stmt 
	| set_connection_stmt 
;
//}

//{ Rule #545 <sql_session_stmt>
sql_session_stmt : 
	  set_catalog_stmt 
	| set_schema_stmt 
	| set_names_stmt 
	| set_session_author_id_stmt 
	| set_local_time_zone_stmt 
;
//}

//  Rule #529 <sql_dyn_data_stmt> was incorporated in the rule #530 <sql_dyn_stmt>
//{ Rule #530 <sql_dyn_stmt> - <system_descriptor_stmt> was hoisted in the <sql_stmt> to avoid ambiguity
sql_dyn_stmt : 
	  prepare_stmt
//	| system_descriptor_stmt 
	| deallocate_prepared_stmt 
	| describe_stmt 
	| execute_stmt 
	| execute_immediate_stmt 
	| allocate_cursor_stmt 
	| fetch_stmt 
	| open_stmt 
	| close_stmt 
;
//}

//{ Rule #463 <schema_def>
schema_def : 
	"create" "schema" schema_name_clause (schema_char_set_spec)? (schema_element)* 
;
//}

//  Rule #459 <schema_author_id> was replaced by <author_id>
//{ Rule #466 <schema_name_clause> 
schema_name_clause : 
	  schema_name ("authorization" /*schema_author_id*/author_id)?
	| "authorization" /*schema_author_id*/author_id 
;
//}

//{ Rule #461 <schema_char_set_spec>
schema_char_set_spec : 
	"default" "character" "set" char_set_name 
;
//}

//{ Rule #464 <schema_element>
schema_element : 
	  table_def 
	| view_def 
	| grant_stmt 
	| domain_def 
	| assertion_def 
	| char_set_def 
	| collation_def 
	| translation_def 
;
//}

//{ Rule #572 <table_def>
table_def : 
	"create" 
	  ( ("global" | "local") "temporary" "table" table_name table_element_list (on_commit_clause)? 
	  | "table" table_name table_element_list 
	  )
;
//}

//{ Rule #574 <table_element_list>
table_element_list : 
	LEFT_PAREN table_element (COMMA table_element)* RIGHT_PAREN 
;
//}

//{ Rule #573 <table_element>
table_element : 
	  table_constraint_def
	| column_def 
;
//}

//{ Rule #--- <on_commit_clause> is a helper rule for the rule #583 <temporary_table_decl> and #572 <table_def>
on_commit_clause :
	"on" "commit" ("delete" | "preserve") "rows" 
;
//}

//{ Rule #571 <table_constraint_def>
table_constraint_def : 
	(constraint_name_def)? table_constraint (constraint_attributes)? 
;
//}

//{ Rule #117 <constraint_name_def>
constraint_name_def : 
	"constraint" constraint_name 
;
//}

//{ Rule #114 <constraint_attributes>
constraint_attributes : 
	  constraint_check_time (options{greedy=true;}:("not")? "deferrable")? 
	| ("not")? "deferrable" (constraint_check_time)? 
;
//}

//{ Rule #115 <constraint_check_time>
constraint_check_time : 
	"initially" ("deferred" | "immediate") 
;
//}

//{ Rule #570 <table_constraint>
table_constraint : 
	  unique_constraint_def 
	| ref_constraint_def 
	| check_constraint_def 
;
//}

//{ Rule #071 <check_constraint_def>
check_constraint_def : 
	"check" LEFT_PAREN search_condition RIGHT_PAREN 
;
//}

//  Rule #610 <unique_column_list> was replaced by <column_name_list> in the rule #611 <unique_constraint_def>
//{ Rule #611 <unique_constraint_def>
unique_constraint_def :
	  unique_spec (LEFT_PAREN /*unique_column_list*/column_name_list RIGHT_PAREN)?
;
//}

//{ Rule #613 <unique_spec>
unique_spec : 
	  "unique" 
	| "primary" "key" 
;
//}

//  Rule #436 <referencing_columns> was replace by <column_name_list>
//{ Rule #440 <ref_constraint_def>
ref_constraint_def : 
	"foreign" "key" LEFT_PAREN /*referencing_columns*/column_name_list RIGHT_PAREN refs_spec 
;
//}

//  Rule #345 <match_type> was incorporated in the rule #437 <refs_spec>
//{ Rule #437 <refs_spec> incorporates the rule #345 <match_type>
refs_spec : 
	"references" refd_table_and_columns ( "match" ("full" | "partial") )? (ref_triggered_action)? 
;
//}

//  Rule #439 <ref_column_list> was replace by <column_name_list>
//{ Rule #435 <refd_table_and_columns>
refd_table_and_columns : 
	table_name (LEFT_PAREN /*ref_column_list*/column_name_list RIGHT_PAREN)? 
;
//}

//  Rule #620 <update_rule> was incorporated in the rule #441 <ref_triggered_action>
//  Rule #160 <delete_rule> was incorporated in the rule #441 <ref_triggered_action>
//{ Rule #441 <ref_triggered_action> incorporates the rule #620 <update_rule> and #160 <delete_rule>
ref_triggered_action : 
	  "on" "update" ref_action ("on" "delete" ref_action)?
	| "on" "delete" ref_action ("on" "update" ref_action)? 
;
//}

//{ Rule #438 <ref_action>
ref_action : 
	  "cascade" 
	| "set" "null" 
	| "set" "default" 
	| "no" "action" 
;
//}

//{ Rule #092 <column_def>
column_def : 
	column_name (data_type | domain_name) (default_clause)? (column_constraint_def)* (collate_clause)? 
;
//}

//  Rule #158 <default_option> was incorporated in the rule #157 <default_clause>
//{ Rule #157 <default_clause> incorporates the rule #158 <default_option>
default_clause : 
	"default"
	  ( lit 
	  | datetime_value_fct 
	  | "user"
	  | "current_user"
	  | "session_user"
	  | "system_user"
	  | "null" 
	  )
;
//}

//{ Rule #091 <column_constraint_def>
column_constraint_def : 
	(constraint_name_def)? column_constraint (options{greedy=true;}:constraint_attributes)? 
;
//}

//{ Rule #090 <column_constraint>
column_constraint : 
	  "not" "null" 
	| unique_spec 
	| refs_spec 
	| check_constraint_def 
;
//}

//  Rule #634 <view_column_list> was replaced by <column_name_list> in the rule #635 <view_def>
//  Rule #336 <levels_clause> was incorporated in the rule #635 <view_def>
//{ Rule #635 <view_def> incorporates the rule #336 <levels_clause>
view_def : 
	"create" "view" table_name (LEFT_PAREN /*view_column_list*/column_name_list RIGHT_PAREN)? "as" query_exp
	  ("with" ("cascaded" | "local")? "check" "option")? 
;
//}

//{ Rule #277 <grant_stmt> was refined to conform to the Syntax Rules
grant_stmt : 
	"grant" 
	  ( privileges "on" ("table")? table_name 
	  | "usage" "on" object_name 
	  ) 
	 "to" grantee (COMMA grantee)* ("with" "grant" "option")? 
;
//}

//  Rule #002 <action_list> was incorporated in the rule #417 <privileges>
//{ Rule #417 <privileges> incorporates the rule #002 <action_list>
privileges : 
	  "all" "privileges" 
	| action (COMMA action)* 
;
//}

//  Rule #418 <privilege_column_list> was replaced by the rule <column_name_list> in the rule #001 <action>
//{ Rule #001 <action> incorporates the rule #418 <privilege_column_list>
//  The "usage" alternative was incorporated directly in the #277 <grant_stmt> and #447 <revoke_stmt>
//  in order to comply with Syntax Rules.
action : 
	  "select" 
	| "delete" 
	| "insert" (LEFT_PAREN column_name_list RIGHT_PAREN)? 
	| "update" (LEFT_PAREN column_name_list RIGHT_PAREN)? 
	| "references" (LEFT_PAREN column_name_list RIGHT_PAREN)? 
;
//}

//{ Rule #276 <grantee>
grantee : 
	  "public"
	| author_id 
;
//}

//{ Rule #185 <domain_def>
domain_def : 
	"create" "domain" domain_name ("as")? data_type (default_clause)? (domain_constraint)* (collate_clause)? 
;
//}

//{ Rule #184 <domain_constraint>
domain_constraint : 
	(constraint_name_def)? check_constraint_def (constraint_attributes)? 
;
//}

//  Rule #242 <existing_char_set_name> was replaced by the rule <char_set_name> in the rule #063 <char_set_source>
//  Rule #063 <char_set_source> was incorporated in the rule #061 <char_set_def>
//{ Rule #061 <char_set_def> incorporates the rule #063 <char_set_source>
char_set_def : 
	"create" "character" "set" char_set_name ("as")? 
	  "get" /*existing_char_set_name*/char_set_name (collate_clause | limited_collation_def)? 
;
//}

//{ Rule #339 <limited_collation_def>
limited_collation_def : 
	"collation" "from" collation_source 
;
//}

//{ Rule #088 <collation_source>
collation_source : 
	  collating_sequence_def 
	| translation_collation 
;
//}

//  Rule #462 <schema_collation_name> was replaced by the rule <collation_name> in the rule #085 <collating_sequence_def>
//{ Rule #085 <collating_sequence_def>
collating_sequence_def : 
	  external_collation 
	| collation_name/*schema_collation_name*/ 
	| "desc" LEFT_PAREN collation_name RIGHT_PAREN 
	| "default" 
;
//}

//  Rule #557 <std_collation_name> was replaced by the rule <collation_name> in the rule #249 <external_collation_name>
//  Rule #296 <implt_def_collation_name> was replaced by the rule <collation_name> in the rule #249 <external_collation_name>
//  Rule #249 <external_collation_name> was replaced by the rule <collation_name> in the rule #248 <external_collation>
//{ Rule #248 <external_collation> - <CHAR_STRING> replaced the QUOTE collation_name QUOTE
external_collation : 
	"external" LEFT_PAREN CHAR_STRING RIGHT_PAREN 
;
//}

//{ Rule #598 <translation_collation>
translation_collation : 
	"translation" translation_name ("then" "collation" collation_name)? 
;
//}

//  Rule #386 <pad_attribute> was incorporated in the rule #086 <collation_def>
//{ Rule #086 <collation_def> incorporates the rule #386 <pad_attribute>
collation_def : 
	"create" "collation" collation_name "for" /*char_set_spec*/char_set_name "from" collation_source ("no" "pad" | "pad" "space")?
;
//}

//  Rule #521 <source_char_set_spec> was replaced by the rule <char_set_name> in the rule #599 <translation_def>
//  Rule #581 <target_char_set_spec> was replaced by the rule <char_set_name> in the rule #599 <translation_def>
//  Rule #601 <translation_source> was replaced by the rule <translation_spec> in the rule #599 <translation_def>
//{ Rule #599 <translation_def>
translation_def : 
	"create" "translation" translation_name 
	"for" /*source_char_set_spec*/char_set_name
	"to"/*target_char_set_spec*/char_set_name
	"from" /*translation_source*/translation_spec 
;
//}

//  Rule #467 <schema_translation_name> was replaced by the rule <translation_name> in the rule #602 <translation_spec>
//{ Rule #602 <translation_spec>
translation_spec : 
	  external_translation 
	| "identity" 
	| translation_name
;
//}

//  Rule #297 <implt_def_translation_name> was replaced by the rule <translation_name> in the rule #251 <external_translation_name>
//  Rule #558 <std_translation_name> was replaced by the rule <translation_name> in the rule #251 <external_translation_name>
//  Rule #251 <external_translation_name> was replaced by the rule <translation_name> in the rule #250 <external_translation>
//{ Rule #250 <external_translation> - <CHAR_STRING> replaced the QUOTE translation_name QUOTE
external_translation : 
	"external"  LEFT_PAREN CHAR_STRING RIGHT_PAREN 
;
//}

//{ Rule #031 <assertion_def>
assertion_def : 
	"create" "assertion" constraint_name assertion_check (constraint_attributes)?
;
//}

//{ Rule #030 <assertion_check>
assertion_check : 
	"check" LEFT_PAREN search_condition RIGHT_PAREN 
;
//}

//{ Rule #190 <drop_behavior>
drop_behavior : 
	  "cascade" 
	| "restrict" 
;
//}

//{ Rule #198 <drop_schema_stmt>
drop_schema_stmt : 
	"drop" "schema" qualified_name drop_behavior 
;
//}

//{ Rule #022 <alter_table_stmt>
alter_table_stmt : 
	"alter" "table" table_name alter_table_action 
;
//}

//{ Rule #021 <alter_table_action>
alter_table_action : 
	  add_column_def 
	| alter_column_def 
	| drop_column_def 
	| add_table_constraint_def 
	| drop_table_constraint_def 
;
//}

//{ Rule #011 <add_column_def>
add_column_def : 
	"add" ("column")? column_def 
;
//}

//{ Rule #018 <alter_column_def>
alter_column_def : 
	"alter" ("column")? column_name alter_column_action 
;
//}

//  Rule #483 <set_column_default_clause> was incorporated in the rule #017 <alter_column_action>
//  Rule #194 <drop_column_default_clause> was incorporated in the rule #017 <alter_column_action>
//{ Rule #017 <alter_column_action>
alter_column_action : 
	  "set" default_clause 
	| "drop" "default" 
;
//}

//{ Rule #193 <drop_column_def>
drop_column_def : 
	"drop" ("column")? column_name drop_behavior 
;
//}

//{ Rule #013 <add_table_constraint_def>
add_table_constraint_def : 
	"add" table_constraint_def 
;
//}

//{ Rule #199 <drop_table_constraint_def>
drop_table_constraint_def : 
	"drop" "constraint" constraint_name drop_behavior 
;
//}

//{ Rule #200 <drop_table_stmt>
drop_table_stmt : 
	"drop" "table" table_name drop_behavior 
;
//}

//{ Rule #202 <drop_view_stmt>
drop_view_stmt : 
	"drop" "view" table_name drop_behavior 
;
//}

//{ Rule #447 <revoke_stmt> was refined to conform to the Syntax Rules
revoke_stmt : 
	"revoke" ("grant" "option" "for")? 
	  ( privileges "on" ("table")? table_name 
	  | "usage" "on" object_name 
	  )
	"from" grantee (COMMA grantee)* drop_behavior 
;
//}

//{ Rule #020 <alter_domain_stmt>
alter_domain_stmt : 
	"alter" "domain" domain_name alter_domain_action 
;
//}

//  Rule #489 <set_domain_default_clause> was incorporated in the rule #019 <alter_domain_action>
//  Rule #196 <drop_domain_default_clause> was incorporated in the rule #019 <alter_domain_action>
//  Rule #012 <add_domain_constraint_def> was incorporated in the rule #019 <alter_domain_action>
//  Rule #195 <drop_domain_constraint_def> was incorporated in the rule #019 <alter_domain_action>
//{ Rule #019 <alter_domain_action>
alter_domain_action : 
	  "set" default_clause 
	| "drop" "default" 
	| "add" domain_constraint 
	| "drop" "constraint" constraint_name 
;
//}

//{ Rule #197 <drop_domain_stmt>
drop_domain_stmt : 
	"drop" "domain" domain_name drop_behavior 
;
//}

//{ Rule #191 <drop_char_set_stmt>
drop_char_set_stmt : 
	"drop" "character" "set" char_set_name 
;
//}

//{ Rule #192 <drop_collation_stmt>
drop_collation_stmt : 
	"drop" "collation" collation_name 
;
//}

//{ Rule #201 <drop_translation_stmt>
drop_translation_stmt : 
	"drop" "translation" translation_name 
;
//}

//{ Rule #189 <drop_assertion_stmt>
drop_assertion_stmt : 
	"drop" "assertion" constraint_name 
;
//}

//{ Rule #100 <commit_stmt>
commit_stmt : 
	"commit" ("work")? 
;
//}

//{ Rule #450 <rollback_stmt>
rollback_stmt : 
	"rollback" ("work")? 
;
//}

//{ Rule #485 <set_constraints_mode_stmt>
set_constraints_mode_stmt : 
	"set" "constraints" constraint_name_list ("deferred" | "immediate") 
;
//}

//{ Rule #118 <constraint_name_list>
constraint_name_list : 
	  "all"
	| constraint_name (COMMA constraint_name)* 
;
//}

//{ Rule #499 <set_transaction_stmt>
set_transaction_stmt : 
	"set" "transaction" transaction_mode (COMMA transaction_mode)* 
;
//}

//  Rule #323 <isolation_level> was incorporated in the rule #597 <transaction_mode>
//  Rule #596 <transaction_access_mode> was incorporated in the rule #597 <transaction_mode>
//  Rule #372 <number_of_conditions> was replaced by <simple_value_spec> in the rule #175 <diag_size>
//  Rule #175 <diag_size> was incorporated in the rule #597 <transaction_mode>
//{ Rule #597 <transaction_mode>
transaction_mode : 
	  "isolation" "level" level_of_isolation 
	| ("read" "only" | "read" "write") 
	| "diagnostics" "size" /*number_of_conditions*/simple_value_spec  
;
//}

//{ Rule #337 <level_of_isolation>
level_of_isolation : 
	  "read" "uncommitted" 
	| "read" "committed" 
	| "repeatable" "read" 
	| "serializable" 
;
//}


//{ Rule #113 <connect_stmt>
connect_stmt : 
	"connect" "to" connection_target 
;
//}

//  Rule #544 <sql_server_name> was replaced by the <simple_value_spec> in the rule #112 <connection_target>
//  Rule #110 <connection_name> was replaced by the <simple_value_spec>
//  Rule #625 <user_name> was replaced by the <simple_value_spec> in the rule #112 <connection_target>
//{ Rule #112 <connection_target>
connection_target : 
	  /*sql_server_name*/simple_value_spec	("as" /*connection_name*/simple_value_spec)? 
	      ("user" /*user_name*/simple_value_spec)? 
	| "default" 
;
//}

//  Rule #182 <disconnect_object> was incorporated in the rule #183 <disconnect_stmt>
//{ Rule #183 <disconnect_stmt> incorporates the rule #182 <disconnect_object>
disconnect_stmt : 
	"disconnect" 
	  ( connection_object 
	  | "all" 
	  | "current"  
	  )
;
//}

//{ Rule #484 <set_connection_stmt>
set_connection_stmt : 
	"set" "connection" connection_object 
;
//}

//{ Rule #111 <connection_object>
connection_object : 
	  "default" 
	| /*connection_name*/simple_value_spec 
;
//}


//{ Rule #480 <set_catalog_stmt>
set_catalog_stmt : 
	"set" "catalog" value_spec 
;
//}

//{ Rule #496 <set_schema_stmt>
set_schema_stmt : 
	"set" "schema" value_spec 
;
//}

//{ Rule #494 <set_names_stmt>
set_names_stmt : 
	"set" "names" value_spec 
;
//}

//{ Rule #497 <set_session_author_id_stmt>
set_session_author_id_stmt : 
	"set" "session" "authorization" value_spec 
;
//}

//  Rule #498 <set_time_zone_value> was incorporated in the rule #493 <set_local_time_zone_stmt>
//{ Rule #493 <set_local_time_zone_stmt> incorporates the rule #498 <set_time_zone_value>
set_local_time_zone_stmt : 
	"set" "time" "zone"  (interval_value_exp | "local")
;
//}

//  Rule #016 <allocate_descriptor_stmt> was incorporated in the rule #569 <system_descriptor_stmt>
//  Rule #154 <deallocate_descriptor_stmt> was incorporated in the rule #569 <system_descriptor_stmt>
//  Rule #488 <set_descriptor_stmt> was incorporated in the rule #569 <system_descriptor_stmt>
//  Rule #271 <get_descriptor_stmt> was incorporated in the rule #569 <system_descriptor_stmt>
//  Rule #379 <occurrences> was replaced by the <simple_value_spec>
//{ Rule #569 <system_descriptor_stmt>
system_descriptor_stmt : 
	  "allocate" "descriptor" descriptor_name ("with" "max" /*occurrences*/simple_value_spec)? 
	| "deallocate" "descriptor" descriptor_name 
	| "set" "descriptor" descriptor_name set_descriptor_information 
	| "get" "descriptor" descriptor_name get_descriptor_information 
;
//}

//  Rule #486 <set_count> was incorporated in the rule #487 <set_descriptor_information>
//  Rule #324 <item_number> was replaced by <simple_value_spec>
//{ Rule #487 <set_descriptor_information> incorporates <set_count> and <item_number>
set_descriptor_information : 
        "count" EQUALS_OP simple_value_spec 
      | "value" /*item_number*/simple_value_spec set_item_information (COMMA set_item_information)* 
;
//}

//{ Rule #492 <set_item_information>
set_item_information : 
	descriptor_item_name EQUALS_OP simple_value_spec 
;
//}

//{ Rule #173 <descriptor_item_name>
descriptor_item_name : 
	  "type" 
	| "length" 
	| "octet_length" 
	| "returned_length" 
	| "returned_octet_length" 
	| "precision" 
	| "scale" 
	| "datetime_interval_code" 
	| "datetime_interval_precision" 
	| "nullable" 
	| "indicator" 
	| "data" 
	| "name" 
	| "unnamed" 
	| "collation_catalog" 
	| "collation_schema" 
	| "collation_name" 
	| "character_set_catalog" 
	| "character_set_schema" 
	| "character_set_name" 
;
//}

//  Rule #269 <get_count> was incorporated in the rule #270 <get_descriptor_information>
//{ Rule #270 <get_descriptor_information> incorporates <set_count> and <item_number>
get_descriptor_information : 
	  simple_target_spec EQUALS_OP "count" 
	| "value" /*item_number*/simple_value_spec get_item_information (COMMA get_item_information)* 
;
//}

//{ Rule #273 <get_item_information>
get_item_information : 
	simple_target_spec EQUALS_OP descriptor_item_name 
;
//}


//{ Rule #408 <prepare_stmt> 
prepare_stmt : 
	"prepare" sql_stmt_name "from" sql_stmt_variable 
;
//}

//{ Rule #548 <sql_stmt_variable> - the alternative <lit> was removed to conform to the Syntax Rules of the rule #408 <prepare_stmt>
sql_stmt_variable : 
	  parameter_name 
	| EMBDD_VARIABLE_NAME 
;
//}

//{ Rule #155 <deallocate_prepared_stmt>
deallocate_prepared_stmt : 
	"deallocate" "prepare" sql_stmt_name 
;
//}

//  Rule #170 <describe_input_stmt> was incorporated in the rule #172 <describe_stmt>
//  Rule #171 <describe_output_stmt> was incorporated in the rule #172 <describe_stmt>
//{ Rule #172 <describe_stmt>
describe_stmt : 
	  "describe" "input" sql_stmt_name using_descriptor 
	| "describe" ("output")? sql_stmt_name using_descriptor
;
//}

//{ Rule #628 <using_descriptor>
using_descriptor : 
	("using" | "into") "sql" "descriptor" descriptor_name 
;
//}

//  Rule #446 <result_using_clause> was replaced by <using_clause> in the rule #241 <execute_stmt>
//  Rule #391 <parameter_using_clause> was replaced by <using_clause> in the rule #241 <execute_stmt>
//{ Rule #241 <execute_stmt>
execute_stmt : 
	"execute" sql_stmt_name 
	  (  ("into")=>/*result_using_clause*/using_clause (/*parameter_using_clause*/using_clause)? 
	  |  (/*parameter_using_clause*/using_clause)?
	  )
;
//}

//{ Rule #240 <execute_immediate_stmt>
execute_immediate_stmt : 
	"execute" "immediate" sql_stmt_variable 
;
//}

//{ Rule #015 <allocate_cursor_stmt>
allocate_cursor_stmt : 
	"allocate" extended_cursor_name ("insensitive")? ("scroll")? "cursor" "for" extended_stmt_name 
;
//}

//  Rule #207 <dyn_fetch_stmt> was incorporated in the rule #257 <fetch_stmt>
//{ Rule #257 <fetch_stmt> incorporates also the rule <dyn_fetch_stmt>
fetch_stmt : 
	"fetch" ( (fetch_orientation)? "from" )? dyn_cursor_name ( ("into")=>using_clause | {false}? )
;
//}

//{ Rule #256 <fetch_orientation>
fetch_orientation : 
	  "next" 
	| "prior" 
	| "first" 
	| "last" 
	| ("absolute" | "relative") simple_value_spec 
;
//}

//  Rule #208 <dyn_open_stmt> was incorporated in the rule #381 <open_stmt>
//{ Rule #381 <open_stmt> incorporates also the rule <dyn_open_stmt>
open_stmt : 
	"open" dyn_cursor_name (using_clause)?
;
//}

//  Rule #203 <dyn_close_stmt> was incorporated in the rule #072 <close_stmt>
//{ Rule #072 <close_stmt> incorporates also the rule <dyn_close_stmt>
close_stmt : 
	"close" dyn_cursor_name (using_clause)?
;
//}

//  Rule #528 <sql_diag_stmt> was replaced by the rule #272 <get_diag_stmt>
//  Rule #527 <sql_diag_information> was incorporated in the rule #272 <get_diag_stmt>
//{ Rule #272 <get_diag_stmt> incorporates the rule #527 <sql_diag_information>
get_diag_stmt : 
	"get" "diagnostics" (stmt_information | condition_information) 
;
//}

//{ Rule #560 <stmt_information>
stmt_information : 
	stmt_information_item (COMMA stmt_information_item)* 
;
//}

//  Rule #562 <stmt_info_item_name> was incorporated in the rule #561 <stmt_information_item>
//{ Rule #561 <stmt_information_item> incorporates the rule #562 <stmt_info_item_name>
stmt_information_item : 
	simple_target_spec EQUALS_OP 
	  ( "number" 
	  | "more" 
	  | "command_function" 
	  | "dynamic_function" 
	  | "row_count"
	  ) 
;
//}

//  Rule #109 <condition_number> was replaced by <simple_value_spec> in <condition_information>
//{ Rule #106 <condition_information>
condition_information : 
	"exception" /*condition_number*/simple_value_spec  
	   condition_information_item (COMMA condition_information_item)* 
;
//}

//  Rule #108 <condition_info_item_name> was incorporated in the rule #107 <condition_information_item>
//{ Rule #107 <condition_information_item> incorporates the rule #108 <condition_info_item_name>
condition_information_item : 
	simple_target_spec EQUALS_OP 
	  ( "condition_number" 
	  | "returned_sqlstate" 
	  | "class_origin" 
	  | "subclass_origin" 
	  | "server_name" 
	  | "connection_name" 
	  | "constraint_catalog" 
	  | "constraint_schema" 
	  | "constraint_name" 
	  | "catalog_name" 
	  | "schema_name" 
	  | "table_name" 
	  | "column_name" 
	  | "cursor_name" 
	  | "message_text" 
	  | "message_length" 
	  | "message_octet_length" 
) 
;
//}

//  Rule #205 <dyn_declare_cursor> was incorporated in the rule #156 <declare_cursor>
//{ Rule #156 <declare_cursor> incorporates the rule #205 <dyn_declare_cursor>
declare_cursor : 
	"declare" cursor_name ("insensitive")? ("scroll")? "cursor" "for" 
	  ( (stmt_name)=> ( (joined_table)=> select_stmt | stmt_name )
	  | select_stmt
	  )
;
//}

//{ Rule #583 <temporary_table_decl>
temporary_table_decl : 
	"declare" "local" "temporary" "table" qualified_name table_element_list (on_commit_clause)? 
;
//}

// ----------------
// Supporting rules
// ----------------
//{ Rule #429 <query_primary> was redefined to comply with the Standard
query_primary : 
	  simple_table 
	| (joined_table)=>joined_table
	| LEFT_PAREN query_exp RIGHT_PAREN
;
//}

//{ Rule #--- <author_id>
author_id : 
	  id
;
//}

//{ Rule #116 <constraint_name>
constraint_name : 
	qualified_name 
;
//}

//{ Rule #378 <object_name> - modified version without the table object in order to comply with Syntax Rules
//  The "table" alternative was incorporated directly in the #277 <grant_stmt> and #447 <revoke_stmt>
object_name : 
	  "domain" domain_name 
	| "collation" collation_name 
	| "character" "set" char_set_name 
	| "translation" translation_name 
;
//}

//{ Rule #631 <value_spec>
value_spec : 
	  lit 
	| general_value_spec 
;
//}

//{ Rule #174 <descriptor_name>
descriptor_name : 
	("global" | "local")? simple_value_spec 
;
//}

//{ Rule #547 <sql_stmt_name> resolves the conflict between <id> and <char_string_lit>
sql_stmt_name : 
	  {LA(1) == INTRODUCER}? ((stmt_name)=>stmt_name | extended_stmt_name) 
	| {LA(1) != INTRODUCER}? stmt_name 
	| extended_stmt_name 
;
//}

//{ Rule #247 <extended_stmt_name>
extended_stmt_name : 
	("global" | "local")? simple_value_spec 
;
//}

//{ Rule #508 <simple_target_spec>
simple_target_spec : 
          parameter_name
	| EMBDD_VARIABLE_NAME
;
//}

//  Rule #029 <argument> was replaced by the rule #582 <target_spec> in the rule #627 <using_clause>
//  Rule #626 <using_arguments> was incorporated in the rule #627 <using_clause>
//{ Rule #627 <using_clause> incorporates the rule #626 <using_arguments>
using_clause : 
	  ("using" | "into") /*argument*/target_spec (COMMA /*argument*/target_spec)* 
	| using_descriptor  
;
//}

//  Rule #258 <fetch_target_list> was replaced by the rule <target_list>
//  Rule #477 <select_target_list> was replaced by the rule <target_list>
//{ Rule #--- <target_list> was introduced to substitute the rules <fetch_target_list> and <select_target_list>
target_list : 
	target_spec (COMMA target_spec)* 
;
//}

//{ Rule #368 <non_reserved_word> - full version - see DmlSQL2.g
non_reserved_word : 
	  "ada" 
	| "c" | "catalog_name" 
	| "character_set_catalog" | "character_set_name" 
	| "character_set_schema" | "class_origin" | "cobol" | "collation_catalog" 
	| "collation_name" | "collation_schema" | "column_name" | "command_function" 
	| "committed" 
	| "condition_number" | "connection_name" | "constraint_catalog" | "constraint_name" 
	| "constraint_schema" | "cursor_name" 
	| "data" | "datetime_interval_code" 
	| "datetime_interval_precision" | "dynamic_function" 
	| "fortran" 
	| "length" 
	| "message_length" | "message_octet_length" | "message_text" | "more" | "mumps" 
	| "name" | "nullable" | "number" 
	| "pascal" | "pli" 
	| "repeatable" | "returned_length" | "returned_octet_length" | "returned_sqlstate" 
	| "row_count" 
	| "scale" | "schema_name" | "serializable" | "server_name" | "subclass_origin" 
	| "table_name" | "type" 
	| "uncommitted" | "unnamed" 
;
//}

