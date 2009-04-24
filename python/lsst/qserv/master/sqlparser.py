
# Extended from Paul McGuire's simpleSQL.py which was a sample from
# the pyparsing project ( http://pyparsing.wikispaces.com/ )
# Some changes:
# support for BETWEEN in WHERE expressions

from pyparsing import \
    Literal, CaselessLiteral, Word, Upcase,\
    delimitedList, Optional, Combine, Group, alphas, nums, \
    alphanums, ParseException, Forward, oneOf, quotedString, \
    ZeroOrMore, restOfLine, Keyword, upcaseTokens

def getTokens(qstr):
    return simpleSQL.parseString(qstr)
    
def test(qstr):
    print qstr,"->"
    try:
        tokens = simpleSQL.parseString(qstr)
        print "tokens = ",        tokens
        print "tokens.columns =", tokens.columns
        print "tokens.tables =",  tokens.tables
        print "tokens.where =", tokens.where
        print tokens.column
    except ParseException, err:
        print " "*err.loc + "^\n" + err.msg
        print err
    print


# define SQL tokens
selectStmt = Forward()
selectToken = Keyword("select", caseless=True)
fromToken   = Keyword("from", caseless=True)

ident          = Word( alphas, alphanums + "_$" ).setName("identifier")
columnName     = delimitedList( ident, ".", combine=True )
columnName.setParseAction(upcaseTokens)

columnNameList = Group( delimitedList( columnName ) )
tableName      = delimitedList( ident, ".", combine=True )
tableName.setParseAction(upcaseTokens)
tableNameList  = Group( delimitedList( tableName ) )

whereExpression = Forward()
and_ = Keyword("and", caseless=True)
or_ = Keyword("or", caseless=True)
in_ = Keyword("in", caseless=True)
between_ = Keyword("between", caseless=True)

E = CaselessLiteral("E")
binop = oneOf("= != < > >= <= eq ne lt le gt ge", caseless=True)
arithSign = Word("+-",exact=1)
realNum = Combine( Optional(arithSign) + ( Word( nums ) + "." + Optional( Word(nums) )  |
                                                         ( "." + Word(nums) ) ) + 
            Optional( E + Optional(arithSign) + Word(nums) ) )
intNum = Combine( Optional(arithSign) + Word( nums ) + 
            Optional( E + Optional("+") + Word(nums) ) )

columnRval = realNum | intNum | quotedString | columnName # need to add support for alg expressions

def addWhereCondition(tokens):
    print "where condition:", tokens
    print tokens.items()
    print dir(tokens)
    pass
whereCondition = Group(
    ( columnName + binop + columnRval ) |
    ( columnName + in_ + "(" + delimitedList( columnRval ) + ")" ) |
    ( columnName + in_ + "(" + selectStmt + ")" ) |
    ( columnName.setResultsName("column") + between_ + columnRval + and_ + columnRval ) |
    ( "(" + whereExpression + ")" ) 
    )
whereCondition.addParseAction(addWhereCondition)

whereExpression << whereCondition + ZeroOrMore( ( and_ | or_ ) + whereExpression ) 

# define the grammar
selectStmt      << ( selectToken + 
                   ( '*' | columnNameList ).setResultsName( "columns" ) + 
                   fromToken + 
                   tableNameList.setResultsName( "tables" ) + 
                   Optional( Group( CaselessLiteral("where") + whereExpression ), "" ).setResultsName("where") )

simpleSQL = selectStmt

# define Oracle comment format, and ignore them
oracleSqlComment = "--" + restOfLine
simpleSQL.ignore( oracleSqlComment )

# For lspeed, we need to take range specs for ra and decl and emit
# specs that can be applied against our chunk table map.
# chunk table schema (tentative)
# CREATE TABLE chunkmap (chunkid int, 
#                        subchunkId int, 
#                        nodeId int,
#                        ramin float, ramax float
#                        declmin float, declmax float );

def findLocationFromQuery(query):
    """"return tuples of (chunkid, subchunkid, nodeid) from a
    bounding-box query"""

    # parse query
    # extract WHERE clause for its RA and DECL ranges
    # apply ranges to chunkmap table to find out:
    # 1. what chunks are needed (and what subchunks are needed)
    # 2. where these chunk/subchunks are.
    # the end goal is that we want to take the original query,
    # and fan it out to the different nodes.  each query-replicant is
    # tagged with what chunk and subchunk it concerns, and is grouped
    # by chunk and node.
    pass

