parser grammar QSMySqlParser;

import MySqlParser;

options { tokenVocab=QSMySqlLexer; }

// same as MySqlParser, adds qservFunctionSpec
// Simplified approach for expression
expression
    : notOperator=(NOT | '!') expression                            #notExpression
    | expression logicalOperator expression                         #logicalExpression
    | predicate IS NOT? testValue=(TRUE | FALSE | UNKNOWN)          #isExpression
    | predicate                                                     #predicateExpression
    | qservFunctionSpec                                             #qservFunctionSpecExpression
    ;
    
// todo this probably wants to be in its own derived grammar file
qservFunctionSpec
	: (	QSERV_AREASPEC_BOX | QSERV_AREASPEC_CIRCLE  
	  | QSERV_AREASPEC_ELLIPSE | QSERV_AREASPEC_POLY   	
	  | QSERV_AREASPEC_HULL) '(' constants ')'
	;  	
 
 // same as MySqlParser except adds (val, min, max) keywords to betweenPredicate
 predicate
    : predicate NOT? IN '(' (selectStatement | expressions) ')'     #inPredicate
    | predicate IS nullNotnull                                      #isNullPredicate
    | left=predicate comparisonOperator right=predicate             #binaryComparasionPredicate
    | predicate comparisonOperator 
      quantifier=(ALL | ANY | SOME) '(' selectStatement ')'         #subqueryComparasionPredicate
    | val=predicate NOT? BETWEEN min=predicate AND max=predicate    #betweenPredicate
    | predicate SOUNDS LIKE predicate                               #soundsLikePredicate
    | predicate NOT? LIKE predicate (ESCAPE STRING_LITERAL)?        #likePredicate
    | predicate NOT? regex=(REGEXP | RLIKE) predicate               #regexpPredicate
    | (LOCAL_ID VAR_ASSIGN)? expressionAtom                         #expressionAtomPredicate
    ;
 