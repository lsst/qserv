/*
MySQL (Positive Technologies) grammar
The MIT License (MIT).
Copyright (c) 2015-2017, Ivan Kochurkin (kvanttt@gmail.com), Positive Technologies.
Copyright (c) 2017, Ivan Khudyashev (IHudyashov@ptsecurity.com)

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

parser grammar MySqlParser;

// Top Level Description

root
    : sqlStatements? MINUSMINUS? EOF
    ;

sqlStatements
    : (sqlStatement MINUSMINUS? SEMI | emptyStatement)* 
    (sqlStatement (MINUSMINUS? SEMI)? | emptyStatement)
    ;
    
sqlStatement
    : dmlStatement
    | administrationStatement
    ;

emptyStatement
    : SEMI
    ;

dmlStatement
    : selectStatement
    | callStatement
    ;

administrationStatement
    : setStatement
    ;


// Data Manipulation Language

//    Primary DML Statements

callStatement
    : CALL fullId
      (
        '(' (constants | expressions)? ')'
      )?
    ;

selectStatement
    : querySpecification                               #simpleSelect
    ;

// details

orderByClause
    : ORDER BY orderByExpression (',' orderByExpression)*
    ;

orderByExpression
    : expression order=(ASC | DESC)?
    ;

tableSources
    : tableSource (',' tableSource)*
    ;

tableSource
    : tableSourceItem joinPart*                                     #tableSourceBase
    ;

tableSourceItem
    : tableName
      (PARTITION '(' uidList ')' )? (AS? alias=uid)?                #atomTableItem
    ;

joinPart
    : (INNER | CROSS)? JOIN tableSourceItem
      (
        ON expression
        | USING '(' uidList ')'
      )?                                                            #innerJoin
    | NATURAL ((LEFT | RIGHT) OUTER?)? JOIN tableSourceItem         #naturalJoin
    ;

//    Select Statement's Details

querySpecification
    : SELECT selectSpec* selectElements
      fromClause? orderByClause? limitClause?
    | SELECT selectSpec* selectElements
    fromClause? orderByClause? limitClause?
    ;

// details

selectSpec
    : (ALL | DISTINCT | DISTINCTROW)
    | HIGH_PRIORITY | STRAIGHT_JOIN | SQL_SMALL_RESULT
    | SQL_BIG_RESULT | SQL_BUFFER_RESULT
    | (SQL_CACHE | SQL_NO_CACHE)
    | SQL_CALC_FOUND_ROWS
    ;

selectElements
    : (star='*' | selectElement ) (',' selectElement)*
    ;

selectElement
    : fullId '.' '*'                                                #selectStarElement
    | fullColumnName (AS? uid)?                                     #selectColumnElement
    | functionCall (AS? uid)?                                       #selectFunctionElement
    | (LOCAL_ID VAR_ASSIGN)? expression (AS? uid)?                  #selectExpressionElement
    ;

fromClause
    : FROM tableSources
      (WHERE whereExpr=expression)?
      (
        GROUP BY
        groupByItem (',' groupByItem)*
        (WITH ROLLUP)?
      )?
      (HAVING havingExpr=expression)?
    ;

groupByItem
    : expression order=(ASC | DESC)?
    ;

limitClause
    : LIMIT
    (
      (offset=decimalLiteral ',')? limit=decimalLiteral
      | limit=decimalLiteral OFFSET offset=decimalLiteral
    )
    ;

// Administration Statements

//    Account management statements

// details

//    Set and show statements

setStatement
    : SET variableClause '=' expression
      (',' variableClause '=' expression)*                          #setVariable
    ;

// details

variableClause
    : LOCAL_ID | GLOBAL_ID | ( ('@' '@')? (GLOBAL | SESSION)  )? uid
    ;


// Common Clauses

//    DB Objects

fullId
    : uid (DOT_ID | '.' uid)?
    ;

tableName
    : fullId
    ;

fullColumnName
    : uid (dottedId dottedId? )?
    ;

uid
    : simpleId
    //| DOUBLE_QUOTE_ID
    | REVERSE_QUOTE_ID
    | CHARSET_REVERSE_QOUTE_STRING
    ;
    
simpleId
    : ID
    | keywordsCanBeId
    | functionNameBase
    ;

dottedId
    : DOT_ID
    | '.' uid
    ;


//    Literals

decimalLiteral
    : DECIMAL_LITERAL | ZERO_DECIMAL | ONE_DECIMAL | TWO_DECIMAL
    ;

stringLiteral
    : (
        STRING_CHARSET_NAME? STRING_LITERAL 
        | START_NATIONAL_STRING_LITERAL
      ) STRING_LITERAL+
    | (
        STRING_CHARSET_NAME? STRING_LITERAL 
        | START_NATIONAL_STRING_LITERAL
      )
    ;

nullNotnull
    : NOT? (NULL_LITERAL | NULL_SPEC_LITERAL)
    ;

constant
    : stringLiteral | decimalLiteral
    | REAL_LITERAL | BIT_STRING
    | NOT? nullLiteral=(NULL_LITERAL | NULL_SPEC_LITERAL)
    ;

//    Common Lists

uidList
    : uid (',' uid)*
    ;

expressions
    : expression (',' expression)*
    ;

constants
    : constant (',' constant)*
    ;

//    Functions

functionCall
    : aggregateWindowedFunction                                     #aggregateFunctionCall
    | scalarFunctionName '(' functionArgs? ')'                      #scalarFunctionCall
    | fullId '(' functionArgs? ')'                                  #udfFunctionCall
    ;

aggregateWindowedFunction
    : (AVG | MAX | MIN | SUM) 
      '(' aggregator=(ALL | DISTINCT)? functionArg ')'
    | COUNT '(' (starArg='*' | aggregator=ALL? functionArg) ')'
    | COUNT '(' aggregator=DISTINCT functionArgs ')'
    | (
        BIT_AND | BIT_OR | BIT_XOR | STD | STDDEV | STDDEV_POP 
        | STDDEV_SAMP | VAR_POP | VAR_SAMP | VARIANCE
      ) '(' aggregator=ALL? functionArg ')'
    | GROUP_CONCAT '(' 
        aggregator=DISTINCT? functionArgs 
        (ORDER BY 
          orderByExpression (',' orderByExpression)* 
        )? (SEPARATOR separator=STRING_LITERAL)? 
      ')'
    ;

scalarFunctionName
    : functionNameBase
    | ASCII | CURDATE | CURRENT_DATE | CURRENT_TIME
    | CURRENT_TIMESTAMP | CURTIME | DATE_ADD | DATE_SUB
    | IF | INSERT | LOCALTIME | LOCALTIMESTAMP | MID | NOW
    | REPLACE | SUBSTR | SUBSTRING | SYSDATE | TRIM
    | UTC_DATE | UTC_TIME | UTC_TIMESTAMP
    ;

functionArgs
    : (constant | fullColumnName | functionCall | expression) 
    (
      ',' 
      (constant | fullColumnName | functionCall | expression)
    )*
    ;

functionArg
    : constant | fullColumnName | functionCall | expression
    ;


//    Expressions, predicates

// Simplified approach for expression
expression
    : notOperator=(NOT | '!') expression                            #notExpression
    | expression logicalOperator expression                         #logicalExpression
    | predicate                                                     #predicateExpression
    ;

predicate
    : predicate NOT? IN '(' (selectStatement | expressions) ')'     #inPredicate
    | predicate IS nullNotnull                                      #isNullPredicate
    | left=predicate comparisonOperator right=predicate             #binaryComparasionPredicate
    | predicate NOT? BETWEEN predicate AND predicate                #betweenPredicate
    | predicate NOT? LIKE predicate (ESCAPE STRING_LITERAL)?        #likePredicate
    | (LOCAL_ID VAR_ASSIGN)? expressionAtom                         #expressionAtomPredicate
    ;


// Add in ASTVisitor nullNotnull in constant
expressionAtom
    : constant                                                      #constantExpressionAtom
    | fullColumnName                                                #fullColumnNameExpressionAtom
    | functionCall                                                  #functionCallExpressionAtom
    | '(' expression (',' expression)* ')'                          #nestedExpressionAtom
    | left=expressionAtom bitOperator right=expressionAtom          #bitExpressionAtom
    | left=expressionAtom mathOperator right=expressionAtom         #mathExpressionAtom
    ;

comparisonOperator
    : '=' | '>' | '<' | '<' '=' | '>' '=' 
    | '<' '>' | '!' '=' | '<' '=' '>'
    ;

logicalOperator
    : AND | '&' '&' | XOR | OR | '|' '|'
    ;

bitOperator
    : '<' '<' | '>' '>' | '&' | '^' | '|'
    ;

mathOperator
    : '*' | '/' | '%' | DIV | MOD | '+' | '-' | '--'
    ;


//    Simple id sets
//     (that keyword, which can be id)
 
keywordsCanBeId
    : ACCOUNT | ACTION | AFTER | AGGREGATE | ALGORITHM | ANY
    | AT | AUTHORS | AUTOCOMMIT | AUTOEXTEND_SIZE
    | AUTO_INCREMENT | AVG_ROW_LENGTH | BEGIN | BINLOG | BIT
    | BLOCK | BOOL | BOOLEAN | BTREE | CASCADED | CHAIN
    | CHANNEL | CHECKSUM | CIPHER | CLIENT | COALESCE | CODE
    | COLUMNS | COLUMN_FORMAT | COMMENT | COMMIT | COMPACT
    | COMPLETION | COMPRESSED | COMPRESSION | CONCURRENT
    | CONNECTION | CONSISTENT | CONTAINS | CONTEXT
    | CONTRIBUTORS | COPY | CPU | DATA | DATAFILE | DEALLOCATE
    | DEFAULT_AUTH | DEFINER | DELAY_KEY_WRITE | DIRECTORY
    | DISABLE | DISCARD | DISK | DO | DUMPFILE | DUPLICATE
    | DYNAMIC | ENABLE | ENCRYPTION | ENDS | ENGINE | ENGINES 
    | ERROR | ERRORS | ESCAPE | EVEN | EVENT | EVENTS | EVERY
    | EXCHANGE | EXCLUSIVE | EXPIRE | EXTENT_SIZE | FAULTS
    | FIELDS | FILE_BLOCK_SIZE | FILTER | FIRST | FIXED
    | FOLLOWS | FULL | FUNCTION | GLOBAL | GRANTS
    | GROUP_REPLICATION | HASH | HOST | IDENTIFIED
    | IGNORE_SERVER_IDS | IMPORT | INDEXES | INITIAL_SIZE
    | INPLACE | INSERT_METHOD | INSTANCE | INVOKER | IO
    | IO_THREAD | IPC | ISOLATION | ISSUER | KEY_BLOCK_SIZE
    | LANGUAGE | LAST | LEAVES | LESS | LEVEL | LIST | LOCAL
    | LOGFILE | LOGS | MASTER | MASTER_AUTO_POSITION
    | MASTER_CONNECT_RETRY | MASTER_DELAY
    | MASTER_HEARTBEAT_PERIOD | MASTER_HOST | MASTER_LOG_FILE
    | MASTER_LOG_POS | MASTER_PASSWORD | MASTER_PORT
    | MASTER_RETRY_COUNT | MASTER_SSL | MASTER_SSL_CA
    | MASTER_SSL_CAPATH | MASTER_SSL_CERT | MASTER_SSL_CIPHER
    | MASTER_SSL_CRL | MASTER_SSL_CRLPATH | MASTER_SSL_KEY
    | MASTER_TLS_VERSION | MASTER_USER
    | MAX_CONNECTIONS_PER_HOUR | MAX_QUERIES_PER_HOUR
    | MAX_ROWS | MAX_SIZE | MAX_UPDATES_PER_HOUR
    | MAX_USER_CONNECTIONS | MEMORY | MERGE | MID | MIGRATE
    | MIN_ROWS | MODIFY | MUTEX | MYSQL | NAME | NAMES
    | NCHAR | NEVER | NO | NODEGROUP | NONE | OFFLINE | OFFSET
    | OJ | OLD_PASSWORD | ONE | ONLINE | ONLY | OPTIMIZER_COSTS
    | OPTIONS | OWNER | PACK_KEYS | PAGE | PARSER | PARTIAL
    | PARTITIONING | PARTITIONS | PASSWORD | PHASE | PLUGINS
    | PLUGIN_DIR | PORT | PRECEDES | PREPARE | PRESERVE | PREV
    | PROCESSLIST | PROFILE | PROFILES | PROXY | QUERY | QUICK
    | REBUILD | RECOVER | REDO_BUFFER_SIZE | REDUNDANT
    | RELAYLOG | RELAY_LOG_FILE | RELAY_LOG_POS | REMOVE
    | REORGANIZE | REPAIR | REPLICATE_DO_DB | REPLICATE_DO_TABLE
    | REPLICATE_IGNORE_DB | REPLICATE_IGNORE_TABLE
    | REPLICATE_REWRITE_DB | REPLICATE_WILD_DO_TABLE
    | REPLICATE_WILD_IGNORE_TABLE | REPLICATION | RESUME
    | RETURNS | ROLLBACK | ROLLUP | ROTATE | ROW | ROWS
    | ROW_FORMAT | SAVEPOINT | SCHEDULE | SECURITY | SERVER
    | SESSION | SHARE | SHARED | SIGNED | SIMPLE | SLAVE
    | SNAPSHOT | SOCKET | SOME | SOUNDS | SOURCE
    | SQL_AFTER_GTIDS | SQL_AFTER_MTS_GAPS | SQL_BEFORE_GTIDS
    | SQL_BUFFER_RESULT | SQL_CACHE | SQL_NO_CACHE | SQL_THREAD
    | START | STARTS | STATS_AUTO_RECALC | STATS_PERSISTENT
    | STATS_SAMPLE_PAGES | STATUS | STOP | STORAGE | STRING
    | SUBJECT | SUBPARTITION | SUBPARTITIONS | SUSPEND | SWAPS
    | SWITCHES | TABLESPACE | TEMPORARY | TEMPTABLE | THAN
    | TRANSACTION | TRUNCATE | UNDEFINED | UNDOFILE
    | UNDO_BUFFER_SIZE | UNKNOWN | UPGRADE | USER | VALIDATION
    | VALUE | VARIABLES | VIEW | WAIT | WARNINGS | WITHOUT
    | WORK | WRAPPER | X509 | XA | XML
    ;

functionNameBase
    : ABS | ACOS | ADDDATE | ADDTIME | AES_DECRYPT | AES_ENCRYPT 
    | AREA | ASBINARY | ASIN | ASTEXT | ASWKB | ASWKT 
    | ASYMMETRIC_DECRYPT | ASYMMETRIC_DERIVE 
    | ASYMMETRIC_ENCRYPT | ASYMMETRIC_SIGN | ASYMMETRIC_VERIFY 
    | ATAN | ATAN2 | BENCHMARK | BIN | BIT_COUNT | BIT_LENGTH 
    | BUFFER | CEIL | CEILING | CENTROID | CHARACTER_LENGTH 
    | CHARSET | CHAR_LENGTH | COERCIBILITY | COLLATION 
    | COMPRESS | CONCAT | CONCAT_WS | CONNECTION_ID | CONV 
    | CONVERT_TZ | COS | COT | COUNT | CRC32 
    | CREATE_ASYMMETRIC_PRIV_KEY | CREATE_ASYMMETRIC_PUB_KEY 
    | CREATE_DH_PARAMETERS | CREATE_DIGEST | CROSSES | DATE 
    | DATEDIFF | DATE_FORMAT | DAY | DAYNAME | DAYOFMONTH 
    | DAYOFWEEK | DAYOFYEAR | DECODE | DEGREES | DES_DECRYPT 
    | DES_ENCRYPT | DIMENSION | DISJOINT | ELT | ENCODE 
    | ENCRYPT | ENDPOINT | ENVELOPE | EQUALS | EXP | EXPORT_SET 
    | EXTERIORRING | EXTRACTVALUE | FIELD | FIND_IN_SET | FLOOR 
    | FORMAT | FOUND_ROWS | FROM_BASE64 | FROM_DAYS 
    | FROM_UNIXTIME | GEOMCOLLFROMTEXT | GEOMCOLLFROMWKB 
    | GEOMETRYCOLLECTION | GEOMETRYCOLLECTIONFROMTEXT 
    | GEOMETRYCOLLECTIONFROMWKB | GEOMETRYFROMTEXT 
    | GEOMETRYFROMWKB | GEOMETRYN | GEOMETRYTYPE | GEOMFROMTEXT 
    | GEOMFROMWKB | GET_FORMAT | GET_LOCK | GLENGTH | GREATEST 
    | GTID_SUBSET | GTID_SUBTRACT | HEX | HOUR | IFNULL 
    | INET6_ATON | INET6_NTOA | INET_ATON | INET_NTOA | INSTR 
    | INTERIORRINGN | INTERSECTS | ISCLOSED | ISEMPTY | ISNULL 
    | ISSIMPLE | IS_FREE_LOCK | IS_IPV4 | IS_IPV4_COMPAT 
    | IS_IPV4_MAPPED | IS_IPV6 | IS_USED_LOCK | LAST_INSERT_ID 
    | LCASE | LEAST | LEFT | LENGTH | LINEFROMTEXT | LINEFROMWKB
    | LINESTRING | LINESTRINGFROMTEXT | LINESTRINGFROMWKB | LN 
    | LOAD_FILE | LOCATE | LOG | LOG10 | LOG2 | LOWER | LPAD 
    | LTRIM | MAKEDATE | MAKETIME | MAKE_SET | MASTER_POS_WAIT 
    | MBRCONTAINS | MBRDISJOINT | MBREQUAL | MBRINTERSECTS 
    | MBROVERLAPS | MBRTOUCHES | MBRWITHIN | MD5 | MICROSECOND 
    | MINUTE | MLINEFROMTEXT | MLINEFROMWKB | MONTH | MONTHNAME 
    | MPOINTFROMTEXT | MPOINTFROMWKB | MPOLYFROMTEXT 
    | MPOLYFROMWKB | MULTILINESTRING | MULTILINESTRINGFROMTEXT 
    | MULTILINESTRINGFROMWKB | MULTIPOINT | MULTIPOINTFROMTEXT 
    | MULTIPOINTFROMWKB | MULTIPOLYGON | MULTIPOLYGONFROMTEXT 
    | MULTIPOLYGONFROMWKB | NAME_CONST | NULLIF | NUMGEOMETRIES 
    | NUMINTERIORRINGS | NUMPOINTS | OCT | OCTET_LENGTH | ORD 
    | OVERLAPS | PERIOD_ADD | PERIOD_DIFF | PI | POINT 
    | POINTFROMTEXT | POINTFROMWKB | POINTN | POLYFROMTEXT 
    | POLYFROMWKB | POLYGON | POLYGONFROMTEXT | POLYGONFROMWKB 
    | POSITION| POW | POWER | QUARTER | QUOTE | RADIANS | RAND 
    | RANDOM_BYTES | RELEASE_LOCK | REVERSE | RIGHT | ROUND 
    | ROW_COUNT | RPAD | RTRIM | SECOND | SEC_TO_TIME 
    | SESSION_USER | SHA | SHA1 | SHA2 | SIGN | SIN | SLEEP 
    | SOUNDEX | SQL_THREAD_WAIT_AFTER_GTIDS | SQRT | SRID 
    | STARTPOINT | STRCMP | STR_TO_DATE | ST_AREA | ST_ASBINARY 
    | ST_ASTEXT | ST_ASWKB | ST_ASWKT | ST_BUFFER | ST_CENTROID 
    | ST_CONTAINS | ST_CROSSES | ST_DIFFERENCE | ST_DIMENSION 
    | ST_DISJOINT | ST_DISTANCE | ST_ENDPOINT | ST_ENVELOPE 
    | ST_EQUALS | ST_EXTERIORRING | ST_GEOMCOLLFROMTEXT 
    | ST_GEOMCOLLFROMTXT | ST_GEOMCOLLFROMWKB 
    | ST_GEOMETRYCOLLECTIONFROMTEXT 
    | ST_GEOMETRYCOLLECTIONFROMWKB | ST_GEOMETRYFROMTEXT 
    | ST_GEOMETRYFROMWKB | ST_GEOMETRYN | ST_GEOMETRYTYPE 
    | ST_GEOMFROMTEXT | ST_GEOMFROMWKB | ST_INTERIORRINGN 
    | ST_INTERSECTION | ST_INTERSECTS | ST_ISCLOSED | ST_ISEMPTY
    | ST_ISSIMPLE | ST_LINEFROMTEXT | ST_LINEFROMWKB 
    | ST_LINESTRINGFROMTEXT | ST_LINESTRINGFROMWKB 
    | ST_NUMGEOMETRIES | ST_NUMINTERIORRING 
    | ST_NUMINTERIORRINGS | ST_NUMPOINTS | ST_OVERLAPS 
    | ST_POINTFROMTEXT | ST_POINTFROMWKB | ST_POINTN 
    | ST_POLYFROMTEXT | ST_POLYFROMWKB | ST_POLYGONFROMTEXT 
    | ST_POLYGONFROMWKB | ST_SRID | ST_STARTPOINT 
    | ST_SYMDIFFERENCE | ST_TOUCHES | ST_UNION | ST_WITHIN 
    | ST_X | ST_Y | SUBDATE | SUBSTRING_INDEX | SUBTIME 
    | SYSTEM_USER | TAN | TIME | TIMEDIFF | TIMESTAMP 
    | TIMESTAMPADD | TIMESTAMPDIFF | TIME_FORMAT | TIME_TO_SEC 
    | TOUCHES | TO_BASE64 | TO_DAYS | TO_SECONDS | UCASE 
    | UNCOMPRESS | UNCOMPRESSED_LENGTH | UNHEX | UNIX_TIMESTAMP
    | UPDATEXML | UPPER | UUID | UUID_SHORT 
    | VALIDATE_PASSWORD_STRENGTH | VERSION 
    | WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS | WEEK | WEEKDAY 
    | WEEKOFYEAR | WEIGHT_STRING | WITHIN | YEAR | YEARWEEK 
    | Y_FUNCTION | X_FUNCTION
    ;
