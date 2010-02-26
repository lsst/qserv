
require ("xmlrpc.http")

-- todos:
--  * enforce single bounding box
--  * supress errors "FUNCTION proxyTest.areaSpec_box does not exist"
--  * communicating with user, returning results etc
--  * talk to master via xml rpc
-- * api: invoke(cleanQueryString, hintString)
--     cleanQueryString is the query without special hints
--     hintString: "box", "1,2,11,12", "box", "5,55,6,66", "objectId", "3",
--   "  objectId", "5,6,7,8" and so on
-- * check if there is "objectId=" or "objectId IN" in the section
--   of query which we are skipping
-- * support "SHOW TABLES", "SHOW DATABASES"
-- * support DESCRIBE
-- * handle commands that are not supported (GRANT)
-- * test what happens when I change db (use x; later use y)




----------------------------------------------------------------------
--                    global variables (yuck)                       --
----------------------------------------------------------------------

rpcHost = "http://127.0.0.1"
rpcPort = 7080

rpcHP = rpcHost .. ":" .. rpcPort


-- constants (kind of)
ERR_AND_EXPECTED   = -4001
ERR_BAD_ARG        = -4002
ERR_NOT_SUPPORTED  = -4003
ERR_OR_NOT_ALLOWED = -4004
ERR_RPC_CALL       = -4005



-- error status and message
__errNo__  = 0
__errMsg__ = ""



-- query string and array containing hints
-- These two will be passed to qserv
queryToPassStr = ""
hintsToPassArr = {}


-- global flags indicating if there is already 'WHERE' 
-- in the queryStr and whether "AND' is needed
haveWhere = false
andIsNeeded = false


----------------------------------------------------------------------
--                       "public" functions                         --
----------------------------------------------------------------------

function read_query(packet)
    if string.byte(packet) == proxy.COM_QUERY then
        print("\n*******************\nIntercepted: " .. string.sub(packet, 2))

        -- massage the query string to simplify its processing
        local q = string.removeExtraWhiteSpaces(string.sub(packet,2))
            -- it is useful to always have a space
            -- even at the end of last predicate
        local qU = string.upper(q) .. ' '

        -- check for special queries that can be handled locally
        if isLocalQuery(qU) then
            return processQueryLocally(qU)
        end
        -- check for queries that are disallowed
        if isDisallowedQuery(qU) then
            return sendErr()
        end
        -- check for queries that we don't support yet
        if isNotSupported(qU) then
            return sendErr()
        end

        -- process the query and send it to qserv
        return sendToQserv(q, qU)
    end
end


----------------------------------------------------------------------
--                       "private" functions                        --
----------------------------------------------------------------------

-- q  - original query
-- qU - original query but all uppercase
function sendToQserv(q, qU)
    local p1 = string.find(qU, "WHERE")
    if p1 then
        queryToPassStr = string.sub(q, 0, p1-1)

        -- Handle special predicates, modify queryToPassStr as necessary
        local p2 = parse_predicates(qU, p1+6) -- 6=length of 'where '
        hintsToPassStr = tableToString(hintsToPassArr)
        -- Add all remaining predicates
        if ( p2 < 0 ) then
            return sendErr()
        end
        local pEnd = string.len(qU)
        queryToPassStr = queryToPassStr .. ' ' .. string.sub(q, p2, pEnd)
    else
        queryToPassStr = q
        hintsToPassStr = ""
    end

    print ("Passing query: " .. queryToPassStr)
    print ("Passing hints: " .. hintsToPassStr)

    local ok, res = 
       xmlrpc.http.call (rpcHP, "submitQuery", queryToPassStr, hintsToPassStr)
    if (ok) then
        -- print ("got via rpc " .. res)
        -- for i, v in pairs(res) do print ('\t', i, v) end
    else
        setErr(ERR_RPC_CALL, "rpc call failed for " .. rpcHP)
        return sendErr()
    end
end

----------------------------------------------------------------------

-- Output:
--    negative: failure
--    positive: number of characters processed by this function
-- This function currently detects the following special tokens:
--   AREASPEC_BOX
--   OBJECTID
--   OBJECTID IN
function parse_predicates(q, p)
    -- print ("args:"..p..", "..string.sub(q, 0, p))

    -- String that has not been parsed yet
    local s = string.sub(q, p)

    -- Number of characters parsed already, counting 
    -- from the beginning of q
    local nParsed = p

    -- Value that will be returned from this function
    local retV = 0

    while true do
        local tokenFound = false
        if string.find(s, "^AREASPEC_BOX") then
            local c = parse_areaspecbox(s)
            if c < 0 then
                return c
            end
            nParsed = nParsed + c
            s = string.sub(q, nParsed)
            tokenFound = true
        elseif string.find(s, "^OBJECTID=") then
            local c = parse_objectId(s)
            if c < 0 then
                __errMsg__ = __errMsg__ .. " ("..s..")"
                return c
            end
            nParsed = nParsed + c
            s = string.sub(q, nParsed)
            tokenFound = true
        elseif string.find(s, "^OBJECTID IN") then
            return setErr(ERR_NOT_SUPPORTED, "Sorry, objectId IN is not supported")
        end
        -- end of looking for special tokens

        if not tokenFound then
            print "Done (reached first unknown token)"
            addWhereAndIfNeeded()
            return nParsed
        end

        if string.len(s) < 4 then
            print "Done (no more predicates)"
            return nParsed
        end
           
        if string.byte(s) == 32 then -- skip space
            s = string.sub(s, 2)
            nParsed = nParsed + 1
        end
        if string.find(s, "^AND") then -- remove leading AND
            s = string.sub(s, 5)
            nParsed = nParsed + 4
        elseif string.find(s, "^OR") then
            return setErr(ERR_OR_NOT_ALLOWED, "'OR' is not allowed here: '"..s.."'")
        else
            return setErr(ERR_AND_EXPECTED, "'AND' was expected here: '"..s.."'")
        end
    end
    return retV
end

----------------------------------------------------------------------

-- Output:
--    negative: failure
--    positive: number of characters processed by this function
function parse_areaspecbox(s)
    local p1 = string.len("AREASPEC_BOX")
    -- print ("parsing args for areaspecbox: '" .. string.sub(s, p1) .. "'")
    local p2 = string.find(s, ')')
    if p2 then
        -- skip " (" in front and ")" in the end
        local params = string.sub(s, p1+2, p2-1)
        hintsToPassArr["box"] = string.sub(params, 0)

        t = csvToTable(params)
        if not 4 == table.getn(t) then
            return setErr(ERR_BAD_ARG, "Incorrect number of arguments " ..
                                       "after areaSpec_BOX: '"..params.."'")
        end
        -- addWhereAndIfNeeded()
        -- queryToPassStr = queryToPassStr .. 
        --               " ra BETWEEN "..t[1].." AND "..t[3].." AND"..
        --               " decl BETWEEN "..t[2].." AND "..t[4]
        -- andIsNeeded = true
        return p2
    end
    return setErr(ERR_BAD_ARG, "Invalid arguments after areaSpec_BOX: '"..params.."'")
end

----------------------------------------------------------------------

-- Output:
--    negative: failure
--    positive: number of characters processed by this function
function parse_objectId(s)
    local p1 = string.len("OBJECTID=")
    -- print ("parsing args for objectId: '" .. string.sub(s, p1) .. "'")
    local p2 = string.find(s, ' ')
    if p2 then
        local params = string.sub(s, p1+1, p2)
        params = string.gsub(params, ' ', '')
        hintsToPassArr["objectId"] = params
        addWhereAndIfNeeded()
        queryToPassStr = queryToPassStr..' objectId='..params
        andIsNeeded = true
        return p2-1
    end
    return setErr(ERR_BAD_ARG, "Invalid argument")
end

----------------------------------------------------------------------

-- Detects if query can be handled locally without sending it to qserv
function isLocalQuery(qU)
    if string.find(qU, "^SELECT @@VERSION_COMMENT LIMIT") or
       string.find(qU, "^SHOW DATABASES") or
       string.find(qU, "^SHOW TABLES") or
       string.find(qU, "^DESCRIBE ") or
       string.find(qU, "^DESC ") then
        return true
    end
    return false
end

----------------------------------------------------------------------

function isDisallowedQuery(qU)
    if string.find(qU, "^INSERT ") or
       string.find(qU, "^UPDATE ") or
       string.find(qU, "^LOAD ") or
       string.find(qU, "^CREATE ") or
       string.find(qU, "^ALTER ") or
       string.find(qU, "^TRUNCATE ") or
       string.find(qU, "^DROP ") then
        setErr(ERR_NOT_SUPPORTED, "Sorry, this type of queries is disallowed.")
        return true
    end
    return false
end

----------------------------------------------------------------------

function isNotSupported(qU)     
    if string.find(qU, "^EXPLAIN ") or
       string.find(qU, "^GRANT ") or
       string.find(qU, "^FLUSH ") then 
        setErr(ERR_NOT_SUPPORTED, "Sorry, this type of queries is not supported in DC3b.")
        return true
    end
    return false
end
----------------------------------------------------------------------

function addWhereAndIfNeeded()
    if not haveWhere then
        queryToPassStr = queryToPassStr .. 'WHERE'
        haveWhere = true
    elseif andIsNeeded then
        queryToPassStr = queryToPassStr .. ' AND'
    end
end

----------------------------------------------------------------------

function string.removeExtraWhiteSpaces(q)
    -- convert new lines and tabs to a space
    q = string.gsub(q, '[\n\t]+', ' ')

    -- remove all spaces before/after '='
    q = string.gsub(q, '[ ]+=', '=')
    q = string.gsub(q, '=[ ]+', '=')

    -- remove all spaces before/after ','
    q = string.gsub(q, '[ ]+,', ',')
    q = string.gsub(q, ',[ ]+', ',')

    -- remove all spaces before/after '(' and before ')'
    q = string.gsub(q, "[ ]+%(", '%(')
    q = string.gsub(q, "%([ ]+", '%(')
    q = string.gsub(q, '[ ]+%)', '%)')

    -- convert multiple spaces to a single space
    q = string.gsub(q, '[ ]+', ' ')

    return q
end

----------------------------------------------------------------------

function processQueryLocally(q)
    -- print ("Processing locally: " .. q)
    print ("Processing locally")
    return 0
end

----------------------------------------------------------------------

function setErr(errNo, errMsg)
    __errNo__  = errNo
    __errMsg__ = errMsg
    return errNo
end

----------------------------------------------------------------------

function sendErr()
    local e = -1 * __errNo__ -- mysql doesn't like negative errors
    proxy.response = {
        type     = proxy.MYSQLD_PACKET_ERR,
        errmsg   = __errMsg__,
        errcode  = e,
        sqlstate = 'Proxy',
    }
    print ("ERROR "..e..": "..__errMsg__)
    return proxy.PROXY_SEND_RESULT
end

----------------------------------------------------------------------

function tableToString(t)
    local s = ""
    for k,v in pairs(t) do 
       s = s .. '"' .. k .. '" "' .. v .. '" '
    end
    return s
end

----------------------------------------------------------------------

-- tokenizes string with comma separated values,
-- returns a table
function csvToTable(s)
  s = s .. ','        -- ending comma
  local t = {}        -- table to collect fields
  local fieldstart = 1
  repeat
    -- next field is quoted? (start with `"'?)
    if string.find(s, '^"', fieldstart) then
      local a, c
      local i  = fieldstart
      repeat
        -- find closing quote
        a, i, c = string.find(s, '"("?)', i+1)
      until c ~= '"'    -- quote not followed by quote?
      if not i then error('unmatched "') end
      local f = string.sub(s, fieldstart+1, i-1)
      table.insert(t, (string.gsub(f, '""', '"')))
      fieldstart = string.find(s, ',', i) + 1
    else                -- unquoted; find next comma
      local nexti = string.find(s, ',', fieldstart)
      table.insert(t, string.sub(s, fieldstart, nexti-1))
      fieldstart = nexti + 1
    end
  until fieldstart > string.len(s)
  return t
end
