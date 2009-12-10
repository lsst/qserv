
from collections import defaultdict
from itertools import imap
import string
import sys

# Extended from Paul McGuire's simpleSQL.py which was a sample from
# the pyparsing project ( http://pyparsing.wikispaces.com/ )
# Some changes:
# support for BETWEEN in WHERE expressions
# support for aliasing.  AS keyword is required, otherwise pyparsing
# mistakes "FROM" for an alias (pyparsing is not recursive descent). 

from pyparsing import \
    Literal, CaselessLiteral, Word, Upcase,\
    delimitedList, Optional, Combine, Group, alphas, nums, \
    alphanums, ParseException, Forward, oneOf, quotedString, \
    ZeroOrMore, restOfLine, Keyword, upcaseTokens, Or

class Grammar:

    def actionWrapper(self, aList):
       """Creates a parseAction whose actions are changeable
       after the grammar is defined.
       """
       def action(tokens):
          for a in aList:
             tokens = a(tokens)
          return tokens
       return action

    def __init__(self):
        # define SQL tokens
        selectStmt = Forward()
        selectToken = Keyword("select", caseless=True)
        fromToken   = Keyword("from", caseless=True)
        asToken   = Keyword("as", caseless=True)
        whereToken = Keyword("where", caseless=True)
        semicolon = Literal(";")

        ident = Word( alphas, alphanums + "_$" ).setName("identifier")

        columnName     = delimitedList( ident, ".", combine=True )
        #columnName.setParseAction(upcaseTokens)
        columnNameList = Group( columnName + ZeroOrMore("," + columnName))

        functionExpr = ident + Optional("."+ident) + Literal('(') + columnNameList + Literal(')')
        alias = Forward()
        identExpr  = functionExpr | ident
        self.identExpr = identExpr # Debug
        self.functionExpr = functionExpr # Debug
        alias = ident.copy()

        selectableName = identExpr | columnName

        selectableList = Group( selectableName + ZeroOrMore(","+selectableName))
        columnRef = columnName
        functionSpec = functionExpr
        valueExprPrimary = functionSpec | columnRef
        numPrimary = valueExprPrimary ## | numericValFunc
        factor = Optional(Literal("+") | Literal("-")) + numPrimary
        term = Forward()
        term << (factor | (term + Literal("*") + factor) | (term + Literal("/") + factor))
        numericExpr = Forward()
        numericExpr << (term | (numericExpr + Literal('+') + term) | (numericExpr + Literal('-') + term))
        valueExpr = numericExpr ## | stringExpr | dateExpr | intervalExpr
        derivedColumn = valueExpr + Optional(asToken + alias)
        selectSubList = derivedColumn + ZeroOrMore("," + derivedColumn)


        tableName      = delimitedList( ident, ".", combine=True )
        # don't upcase table names anymore
        # tableName.setParseAction(upcaseTokens) 
        self.tableAction = []
        tableName.addParseAction(self.actionWrapper(self.tableAction))
        tableName.setResultsName("table")
        tableAlias = tableName + asToken + ident.setResultsName("aliasName")
        tableAlias.setResultsName("alias")
        genericTableName = tableAlias | tableName
        genericTableName = genericTableName.setResultsName("tablename")
        tableNameList  = Group( genericTableName 
                                + ZeroOrMore("," + genericTableName))

        whereExpression = Forward()
        and_ = Keyword("and", caseless=True)
        or_ = Keyword("or", caseless=True)
        in_ = Keyword("in", caseless=True)
        between_ = Keyword("between", caseless=True)
    
        E = CaselessLiteral("E")
        binop = oneOf("= != < > >= <= eq ne lt le gt ge", caseless=True)
        arithSign = Word("+-",exact=1)
        realNum = Combine( Optional(arithSign) + ( Word( nums ) + "." 
                                                   + Optional( Word(nums) )  |
                                                   ( "." + Word(nums) ) ) + 
                           Optional( E + Optional(arithSign) + Word(nums) ) )
        intNum = Combine( Optional(arithSign) + Word( nums ) + 
                          Optional( E + Optional("+") + Word(nums) ) )

        # need to add support for alg expressions
        columnRval = realNum | intNum | quotedString | numericExpr

        self.whereExpAction = []

        namedRv = columnRval.setResultsName("column")
        whereConditionFlat = Group(
            ( namedRv + binop + columnRval ) |
            ( namedRv + in_ + "(" + columnRval + ZeroOrMore(","+namedRv) + ")" ) |
            ( namedRv + in_ + "(" + selectStmt + ")" ) |
            ( namedRv + between_ + namedRv + and_ + namedRv ) )

        whereConditionFlat.addParseAction(self.actionWrapper(self.whereExpAction))
        whereCondition = Group(whereConditionFlat 
                               | ( "(" + whereExpression + ")" ))

        #whereExpression << whereCondition.setResultsName("wherecond")
        #+ ZeroOrMore( ( and_ | or_ ) + whereExpression ) 
        def scAnd(tok):
            print "scAnd", tok
            if "TRUE" == tok[0][0]:
                tok = tok[2] 
            elif "TRUE" == tok[2][0]:
                tok = tok[0]
                
            return tok
        def scOr(tok):
            print "scOr", tok
            if ("TRUE" == tok[0][0]) or ("TRUE" == tok[2][0]):
                tok = [["TRUE"]]
            return tok
        def scWhere(tok):
            newtok = []
            i = 0
            while i < len(tok):
                if str(tok[i]) in ["TRUE",str(["TRUE"])] and (i+1) < len(tok):
                    if str(tok[i+1]).upper() == "AND":
                        i += 2
                        continue
                    elif str(tok[i+i]).upper() == "OR":
                        break
                newtok.append(tok[i])
                i += 1
            return newtok
        
        def collapseWhere(tok):
            #collapse.append(tok[0][1])
            if ["TRUE"] == tok.asList()[0][1]:
                tok = [] 
            return tok
        andExpr = and_ + whereExpression
        orExpr = or_ + whereExpression
        whereExpression << whereCondition + ZeroOrMore(
            andExpr | orExpr)
        whereExpression.addParseAction(scWhere)

        self.selectPart = selectToken + ( '*' | selectSubList ).setResultsName( "columns" )
        whereClause = Group(whereToken + 
                                     whereExpression).setResultsName("where") 
        whereClause.addParseAction(collapseWhere)
        self.fromPart = fromToken + tableNameList.setResultsName("tables")

        # define the grammar
        selectStmt      << ( self.selectPart +                         
                             fromToken + 
                             tableNameList.setResultsName( "tables" ) +
                             whereClause)
        
        self.simpleSQL = selectStmt + semicolon

        # define Oracle comment format, and ignore them
        oracleSqlComment = "--" + restOfLine
        self.simpleSQL.ignore( oracleSqlComment )


class QueryMunger:
    def __init__(self, query):
        self.original = query
        self.pVariables = set(["ra", "decl"])


        #self.inequalities = set(["<",">","<=",">="])
        # Conversion won't really work if ra/decl appear on both sides.
        minTemp = "%sMin %s %s"
        maxTemp = "%sMax %s %s" 
        self.inequalities = { "<" : lambda t: minTemp % self._fixOrder(t),
                              ">" : lambda t: maxTemp % self._fixOrder(t),
                              "<=" : lambda t: minTemp % self._fixOrder(t),
                              ">=" : lambda t: maxTemp % self._fixOrder(t)
                              }
        pass

    def _fixOrder(self, tokList):
        if tokList[0] in self.pVariables:
            return tuple(tokList)
        else:
            r = tokList[:]
            r.reverse()
            return tuple(r)
    def disasmBetween(self, tokList):
        x = tokList
        return {"col" : x[0],
                "min" : x[2],
                "max" : x[4]}

    def convertBetween(self, tokList):
        (col, dum, cmin, dum_, cmax) = tokList
        clist = ["%s between %sMin and %sMax" % (cmin, col, col),
                 "%s between %sMin and %sMax" % (cmax, col, col),
                 "%sMin between %s and %s" % (col, cmin, cmax)]
        return "(%s)" % " OR ".join(clist)

    def convertInequality(self, tokList):
        return self.inequalities[tokList[1]](tokList[:])
        
    def convertPVar(self, v):
        if getattr(v, "split", False):
            last = v.split(".")[-1]
            if last.lower() in self.pVariables:
                return last
        return v

    def expandSubQueries(self, chunk, sublist):
        g = Grammar()
        def replaceObj(tokens):
            for i in range(len(tokens)):
                if tokens[i].upper() == "OBJECT":
                    tokens[i] = "Subchunks_${chunk}.Object_${chunk}_${subc}"
            
        g.tableAction.append(replaceObj)
        parsed = g.simpleSQL.parseString(self.original)
        querytemplate = string.Template(self._flattenNoSpace(parsed))
        #sublist = [sublist[0]] ## DEBUG: do only one subchunk
        chunkqueries = [querytemplate.substitute({'chunk': chunk, 'subc':s}) for s in sublist]
        return chunkqueries

    def collectSubChunkTuples(self, chunktuples):
        # Use dictionary to collect tuples by chunk #.
        d = defaultdict(list)
        for chunk, subchunk in chunktuples:
            d[chunk].append(subchunk)
        
        return d
    
    def computePartMapQuery(self):
        g = Grammar()
        tableList = []
        whereList = []

        def disasm(tokens):
            x = tokens[0] # Unnest
            if x[1].upper() == "BETWEEN":
                return self.disasmBetween(x)
        def convert(tokens):
            # Unnest and blindly un-qualify partition-vars
            x = tokens[0]
            if x[1].upper() == "BETWEEN":
                return self.convertBetween(x)
            if x[1] in self.inequalities:
                return self.convertInequality(x)
            return tokens
            
        # Track the where expressions
        def accuWhere(tok):
            whereList.append(tok)
            return tok
        def compose(tok):
            return convert(tok)

        # Needed: real dataflow parsing
        # For now, cheat and assume that *.ra and *.decl 
        # should be used for bounding boxes.
        def onlyStatic(tok):
            condition = tok[0]
            #print "checking for static in",condition
            for i in range(len(condition)):
                condition[i] = self.convertPVar(condition[i])
            #print "now",condition
                

            def partMapCares(token):
                if getattr(token, '__iter__', False):
                    return map(partMapCares, token)
                return token.lower() in self.pVariables
            
            # Always collapse function calls since we can't pre-eval them.
            if "(" in condition.asList() or not filter(partMapCares, condition):
                tok[0] = "TRUE";

            return tok
        g.whereExpAction.append(onlyStatic)
        g.whereExpAction.append(convert)
        #print self.original, "BEFORE_____"
        t = g.simpleSQL.parseString(self.original, parseAll=True)
        #print self.original, "AFTER_____"
        #flatTokens = self._flatten(t)
        flatWhere = self._flatten(t.where)
        
        pquery = "SELECT chunkid,subchunkid FROM partmap %s;" % flatWhere
        
        return pquery

    def _flatten(self, l):
        if isinstance(l, str): return l
        else: return " ".join(map(self._flatten, l))

    def _flattenNoSpace(self, l):
        if isinstance(l, str): return l
        else:
            nospaceBefore = ",.();" ## set([",", "(", ")"])
            nospaceAfter = ",.(" ## set([",", "(", ")"])
            spaced = []
            for x in l:
                flat = self._flattenNoSpace(x)
                if spaced and (flat not in nospaceBefore
                               and spaced[-1] not in nospaceAfter):
                      spaced.append(" ")
                spaced.append(self._flattenNoSpace(x)) 

            return "".join(spaced)
    pass


def getTokens(qstr):
    g = Grammar()
    whereList = []
    tableList = []
    def accumulateWhere(tok):
        whereList.append(tok)
    def accumulateTable(tok):
        tableList.append(tok)
    def alterTable(tok):
        return ["partmap"]
    g.tableAction.append(accumulateTable)
    g.whereExpAction.append(accumulateWhere)
    t = g.simpleSQL.parseString(qstr)
    def flatten(l):
        if isinstance(l, str):
            return l
        else:
            return " ".join(map(flatten, l))

    flatTokens = flatten(t)
    flatWhere = flatten(t.where)
    flatFirst = flatTokens[:flatTokens.find(flatWhere)]
    flatTable = flatten(tableList)
    print "unflat----where[1]---", t.where[0][1]
    print "whereflatten", flatWhere 
    print "wherelist---", whereList
    print "flatFirst", flatFirst
    flatMunge = flatFirst[:flatFirst.find(flatTable)]
    print "flatmunge", flatMunge
    pquery = "SELECT chunkid,subchunkid FROM partmap %s;" % flatten(t.where)
    print pquery
    chunkquery = "%s %s%s" % (flatMunge, flatTable, "_%d_%d"%(12,42))
    print chunkquery
    blog = 0
    for i in whereList:
        print blog, i
        blog += 1
    return t



def test(qstr):
    g = Grammar()
    print qstr,"->"
    try:
        tokens = g.simpleSQL.parseString(qstr)
        print "tokens = ",        tokens
        print "tokens.columns =", tokens.columns
        print "tokens.tables =",  tokens.tables
        print "tokens.where =", tokens.where
        print tokens.column
    except ParseException, err:
        print " "*err.loc + "^\n" + err.msg
        print err
    print
    pass



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

qs=[ """SELECT o1.id as o1id,o2.id as o2id,spdist(o1.ra, o1.decl, o2.ra, o2.decl) 
  AS dist FROM Object AS o1, Object AS o2 WHERE dist < 25 AND o1.id != o2.id;""",
     """SELECT o1.id as o1id,o2.id as o2id,spdist(o1.ra, o1.decl, o2.ra, o2.decl) 
  AS dist FROM Object AS o1, Object AS o2 
  WHERE o1.ra between 5 and 6 AND dist < 0.5 AND o1.id != o2.id;""",
"""SELECT o1.id as o1id,o2.id as o2id,LSST.spdist(o1.ra, o1.decl, o2.ra, o2.decl) \n  AS dist FROM Object AS o1, Object AS o2 WHERE o1.ra between 10.5 and 11.5 and o2.decl between 9.7 and 10 AND LSST.spdist(o1.ra, o1.decl, o2.ra, o2.decl) < 1 AND o1.id != o2.id;""",
     """SELECT id FROM Object where ra between 2 and 5 AND blah < 3 AND decl > 4;""", 
     """SELECT id FROM Object where blah < 3 AND decl > 4;""",
     "SELECT id,LSST.spdist(ra,decl,ra,decl) FROM Object WHERE id=1;",
     """SELECT id,LSST.spdist(ra,decl,ra,decl) FROM Object WHERE LSST.spdist(ra,decl,ra,decl) < 1 AND id=1;""",
     """SELECT o1.id as o1id,o2.id as o2id,LSST.spdist(o1.ra, o1.decl, o2.ra, o2.decl) 
  AS dist FROM Object AS o1, Object AS o2 
  WHERE ABS(o1.ra - o2.ra) < 0.00083 / COS(RADIANS(o2.decl))
    AND LSST.spdist(o1.ra, o1.decl, o2.ra, o2.decl) < 0.001 
    AND o1.id != o2.id;"""]
## Make sure qserv-worker mysql user (e.g. qsmaster) has privileges to execute LSST.spdist
## GRANT EXECUTE ON LSST.* TO 'qsmaster'@'localhost';
## -- might need to invoke GRANT with superuser.
qsl=[]
def mytest():
    for q in qs:
        qm = QueryMunger(q)
        qsl.append(q)
        s = qm.computePartMapQuery()
        print "Query:", q, "\ngives pmquery:", s
        
    pass

if __name__ == '__main__':
    quer = "select * from obj where ra between 2 and 5 and decl between 1 and 10;"
    test(quer)
    tokens = getTokens(quer)
    # print "aslist", tokens.asList()
    # print "where", tokens.where
    # print "wherecond", tokens.where.asList()
    pass


    
