parser grammar QSMySqlParser;

import MySqlParser;

options { tokenVocab=QSMySqlLexer; }

// adds QSERV_RESULT_DELETE built-in procedure name:
callStatement
    : CALL fullId
      (
        '(' (constants | expressions)? ')'
      )?
    | CALL QSERV_RESULT_DELETE
      (
        '(' constant ')'
      )?
    ;

// adds `MINUS?` before REAL_LITERAL
constant
    : stringLiteral | decimalLiteral
    | MINUS? REAL_LITERAL | BIT_STRING
    | NOT? nullLiteral=(NULL_LITERAL | NULL_SPEC_LITERAL)
    ;

// same as MySqlParser, adds qservFunctionSpecExpression
// Simplified approach for expression
expression
    : notOperator=(NOT | '!') expression                            #notExpression
    | expression logicalOperator expression                         #logicalExpression
    | predicate                                                     #predicateExpression
    | ((DECIMAL_LITERAL EQUAL_SYMBOL qservFunctionSpec) |
       (qservFunctionSpec EQUAL_SYMBOL DECIMAL_LITERAL) |
        qservFunctionSpec)                                           #qservFunctionSpecExpression
    ;

qservFunctionSpec
	: (	QSERV_AREASPEC_BOX | QSERV_AREASPEC_CIRCLE
	  | QSERV_AREASPEC_ELLIPSE | QSERV_AREASPEC_POLY
	  | QSERV_AREASPEC_HULL) '(' constants ')'
	;

// same as MySqlParser except:
// * adds (val, min, max) keywords to betweenPredicate
predicate
   : predicate NOT? IN '(' (selectStatement | expressions) ')'     #inPredicate
   | predicate IS nullNotnull                                      #isNullPredicate
   | left=predicate comparisonOperator right=predicate             #binaryComparasionPredicate
   | val=predicate NOT? BETWEEN min=predicate AND max=predicate    #betweenPredicate
   | predicate NOT? LIKE predicate (ESCAPE STRING_LITERAL)?        #likePredicate
   | (LOCAL_ID VAR_ASSIGN)? expressionAtom                         #expressionAtomPredicate
   ;

 decimalLiteral
    : MINUS? DECIMAL_LITERAL | ZERO_DECIMAL | ONE_DECIMAL | TWO_DECIMAL
    ;
