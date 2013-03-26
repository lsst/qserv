
# Test:
# import SQLReader
# SQLReader.reader("/tmp/a.sql")

import re

def SQLPrimaryReader(line):
    """ Function to parse a "PRIMARY ..." line
    """
    # Examples:
    # PRIMARY KEY (JobID)
    # PRIMARY KEY(JobID)
    # PRIMARY KEY( JobID ),
    # PRIMARY KEY(JobID, Name)
    # PRIMARY     KEY (JoBId)
    # ...
    leftParen = line.split("(")
    if (len(leftParen) != 2):
        raise Exception, "SQLReader: Missing '(' in PRIMARY line: %s" % line
    primaryKey = leftParen[0].split()
    if ((len(primaryKey) != 2) or primaryKey[0].upper() != "PRIMARY" or primaryKey[1].upper() != "KEY"): 
        raise Exception, "SQLReader: Unknown PRIMARY line: %s" % line
    rightParen = leftParen[1].split(")")
    if (len(rightParen) != 2):
        raise Exception, "SQLReader: too many parentheses: %s" % line
    commaSeparatedKeys = rightParen[0]
    return ["PRIMARY KEY", commaSeparatedKeys]    


def comment(state, tokens, fileID):
    """ State function to read comments
    """
    return None


def appendToState(state, tokens, fileID):
    """ State function to append tokens
    """
    current = state["current"]
    state[current].append(tokens)
    return None


def createTableReader(state, tokens, fileID):
    """ State function to parse table creation
    """
    state["current"] = "schema"
    accumulator = []
    for line in fileID:
        lineSplitted = line.split()
        if (lineSplitted != []):
            headToken = lineSplitted[0].upper()
            if (headToken[0:1] == ")"):
                # End of CREATE TABLE
                state["engine"] = lineSplitted
                state["schema"] = accumulator
                state["current"] = "epilogue"
                return None
            elif (headToken == "PRIMARY"):
                result = SQLPrimaryReader(line)
                accumulator.append(result)
            else:
                # Remove comma at the line end 
                line = line.partition(",")[0]
                lineSplitted = line.split()
                accumulator.append(lineSplitted)
    raise Exception, "SQLReader: missing ')' in CREATE TABLE"


def reader(filename):
    """ Stack automaton to read SQL schema 
    """
    transitionFunctionDict = {"--": comment,
                              "DROP": appendToState,
                              "SET": appendToState,
                              "CREATE": createTableReader
                              }
    myfile = file(filename)
    state = {"current": "prologue",
             "prologue": [],
             "schema": [],
             "engine": [],
             "epilogue": []}
    for line in myfile:
        lineSplitted = line.split()
        if (lineSplitted != []):
            headToken = lineSplitted[0].upper()
            if (headToken[0:2] == "/*"):
                appendToState(state, lineSplitted, myfile)
            elif (headToken in transitionFunctionDict):
                transitionFunction = transitionFunctionDict[headToken]
                transitionFunction(state, lineSplitted, myfile)
            else:
                raise Exception, "SQLReader: Missing token in transition function: %s" % headToken

    myfile.close()
    return state

