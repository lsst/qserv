
# Test:
# import SQLReader
# SQLReader.reader("/tmp/a.sql")


def comment(state, tokens, fileID):
  return None

def appendToState(state, tokens, fileID):
  current = state["current"]
  state[current].append(tokens)
  return None

def createTableReader(state, tokens, fileID):
  state["current"] = "schema"
  accumulator = []
  for line in fileID:
    lineSplitted = line.split()
    if (lineSplitted != []):
      headToken = lineSplitted[0].upper()
      if (headToken[0:1] == ")"):
        state["engine"] = lineSplitted
        state["schema"] = accumulator
        state["current"] = "epilogue"
        return None
      else:
        # Remove comma at the line end 
        line = line.partition(",")[0]
        lineSplitted = line.split()
        accumulator.append(lineSplitted)
  raise Exception, "SQLReader: missing ')' in CREATE TABLE"
    
def reader(filename):
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

