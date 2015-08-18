-- mysqlProxy.lua -- A Lua-language script for customizing a
-- mysqlproxy instance so that it uses a Qserv frontend as a backend
-- for executing queries. While it has some responsibilities now, it
-- should eventually act as a thin wrapper to delegating all
-- functionality to a Qserv daemon.
-- For questions, contact Jacek Becla or Daniel L. Wang

require ("xmlrpc.http")

-- todos:
--  * support "objectId IN", DESCRIBE
--
--  * communication with user, returning results etc
--
--  * improve error checking:
--    - enforce single bounding box
--    - check if there is "objectId=" or "objectId IN" in the section
--      of query which we are skipping
--    - block "SHOW" except "SHOW DATABASES" and "SHOW TABLES"
--
--  * supress errors "FUNCTION proxyTest.areaSpec_box does not exist"
--
--  * test what happens when I change db (use x; later use y)

-- API between lua and qserv:
--  * invoke(cleanQueryString, hintString)
--  * cleanQueryString is the query without special hints: objectId
--    related hints are passed through, but bounding boxes are not
--  * hintString: "box", "1,2,11,12", "box", "5,55,6,66",
--    "objectId", "3", "objectId", "5,6,7,8" and so on

-------------------------------------------------------------------------------
--                             debug tool                                    --
-------------------------------------------------------------------------------
local DEBUG = os.getenv('DEBUG') or 0
DEBUG = DEBUG + 0

local MSG_ERROR = 2

function print_debug(msg, level)
 level = level or 1
 if DEBUG >= level then
     print ("DEBUG : "..msg)
 end
end

-------------------------------------------------------------------------------
--                        global variables (yuck)                            --
-------------------------------------------------------------------------------

rpcHost = "127.0.0.1"
defaultRpcPort = 7080

local rpcPort = os.getenv("QSERV_RPC_PORT")
if (rpcPort == nil) then
   rpcPort = defaultRpcPort
end
czarRpcUrl = "http://" .. rpcHost .. ":" .. rpcPort .. "/x"
print_debug("RPC url "..czarRpcUrl, 1)

-- constants (kind of)
ERR_AND_EXPECTED   = -4001
ERR_BAD_ARG        = -4002
ERR_NOT_SUPPORTED  = -4003
ERR_OR_NOT_ALLOWED = -4004
ERR_RPC_CALL       = -4005
ERR_BAD_RES_TNAME  = -4006
ERR_QSERV_GENERIC  = -4100
ERR_QSERV_PARSE    = -4110
ERR_QSERV_RUNTIME  = -4120

SUCCESS            = 0

-- query string and array containing hints
-- These two will be passed to qserv
queryToPassStr = ""
hintsToPassArr = {}

-- global variables have per-session(client) scope
-- queryErrorCount -- number of run-time errors detected during query exec.
queryErrorCount = 0

-------------------------------------------------------------------------------
--                             error handling                                --
-------------------------------------------------------------------------------

-- use line buffering to make it easier to read logs in case of errors
io.stdout:setvbuf("line")

function errors ()
    local self = { __errNo__ = 0, __errMsg__ = "" }

    ---------------------------------------------------------------------------

    local errNo = function()
        return __errNo__
    end

    ---------------------------------------------------------------------------
    local set = function(errNo, errMsg)
        __errNo__  = errNo
        __errMsg__ = errMsg
        return errNo
    end

    local append = function(errMsg)
        __errMsg__ = __errMsg__ .. errMsg
        return errNo
    end

    ---------------------------------------------------------------------------

    local send = function()
        local e = -1 * __errNo__ -- mysql doesn't like negative errors
        proxy.response = {
            type     = proxy.MYSQLD_PACKET_ERR,
            errmsg   = __errMsg__,
            errcode  = e,
            sqlstate = 'Proxy',
        }
        print ("ERROR errNo: "..e..": errMsg: "..__errMsg__)
        return proxy.PROXY_SEND_RESULT
    end

    local setAndSend = function(errNo, errMsg)
        set(errNo, errMsg)
        return send()
    end

    ---------------------------------------------------------------------------

    return {
        errNo = errNo,
        set = set,
        append = append,
        send = send,
        setAndSend = setAndSend
    }
end


err = errors()

-------------------------------------------------------------------------------
--                       random util functions                               --
-------------------------------------------------------------------------------

function utilities()

    local tableToString = function(t)
        local s = ""
        for k,v in pairs(t) do
            s = s .. '"' .. k .. '" "' .. v .. '" '
        end
        return s
    end

    ---------------------------------------------------------------------------

    -- tokenizes string with comma separated values,
    -- returns a table
    local csvToTable = function(s)
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

    ---------------------------------------------------------------------------

    local removeLeadingComment = function (q)
        qRet = q
        x1 = string.find(q, '%/%*')
        x2 = string.find(q, '%*%/')
        if x1 and x2 then
            if x1 > 1 then
                qRet = string.sub(q, 0, x1) .. string.sub(q, x2+2, string.len(q))
            else
                qRet = string.sub(q, x2+2, string.len(q))
            end
        end
        return qRet
    end

    ---------------------------------------------------------------------------

    local removeExtraWhiteSpaces = function (q)
        -- convert new lines and tabs to a space
        q = string.gsub(q, '[\n\t]+', ' ')

        -- remove all spaces at the beginning
        q = string.gsub(q, "^%s*", "")

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

    local startsWith = function (String,Start)
       return string.sub(String,1,string.len(Start))==Start
    end

    ---------------------------------------------------------------------------
    return {
        tableToString = tableToString,
        csvToTable = csvToTable,
        removeLeadingComment = removeLeadingComment,
        removeExtraWhiteSpaces = removeExtraWhiteSpaces,
        startsWith = startsWith
    }
end

utils = utilities()


-------------------------------------------------------------------------------
--                                 parser                                    --
-------------------------------------------------------------------------------

function miniParser()
    local self = { haveWhere = false, andNeeded = false }

    local setAndNeeded = function()
                            andNeeded = true
                         end

    local addWhereAndIfNeeded = function()
        if not haveWhere then
            queryToPassStr = queryToPassStr .. 'WHERE'
            haveWhere = true
        elseif andNeeded then
            queryToPassStr = queryToPassStr .. ' AND'
        end
     end

    local reset = function()
                     haveWhere = false
                     andNeeded = false
                  end


    ---------------------------------------------------------------------------

    -- Output:
    --    negative: failure
    --    positive: number of characters processed by this function
    local parseAreaSpecBox = function(s)
        local p1 = string.len("QSERV_AREASPEC_BOX")
        -- print ("parsing args for areaspecbox: '" .. string.sub(s, p1) .. "'")
        local p2 = string.find(s, ')')
        if p2 then
            -- skip " (" in front and ")" in the end
            local params = string.sub(s, p1+2, p2-1)
            hintsToPassArr["box"] = string.sub(params, 0)

            t = utils.csvToTable(params)
            if not 4 == table.getn(t) then
                return err.set(ERR_BAD_ARG, "Incorrect number of arguments " ..
                                 "after qserv_areaSpec_box: '"..params.."'")
            end
            -- addWhereAndIfNeeded()
            -- queryToPassStr = queryToPassStr ..
            --               " ra BETWEEN "..t[1].." AND "..t[3].." AND"..
            --               " decl BETWEEN "..t[2].." AND "..t[4]
            -- parser.setAndNeeded()
            return p2
        end
        return err.set(ERR_BAD_ARG,
              "Invalid arguments after qserv_areaSpec_box: '"..params.."'")
    end

    ---------------------------------------------------------------------------

    -- Output:
    --    negative: failure
    --    positive: number of characters processed by this function
    local parseObjectId = function(s)
        local p1 = string.len("OBJECTID=")
        -- print ("parsing args for objectId: '" .. string.sub(s, p1) .. "'")
        local p2 = string.find(s, ' ')
        if p2 then
            local params = string.sub(s, p1+1, p2)
            params = string.gsub(params, ' ', '')
            hintsToPassArr["objectId"] = params
            addWhereAndIfNeeded()
            queryToPassStr = queryToPassStr..' objectId='..params
            parser.setAndNeeded()
            return p2-1
        end
        return err.set(ERR_BAD_ARG, "Invalid argument")
    end

    ---------------------------------------------------------------------------

    -- Output:
    --    negative: failure
    --    positive: number of characters processed by this function
    -- This function currently detects the following special tokens:
    --   AREASPEC_BOX
    --   OBJECTID
    --   OBJECTID IN
    local parseIt = function(q, p)
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
            if string.find(s, "^QSERV_AREASPEC_BOX") then
                local c = parseAreaSpecBox(s)
                if c < 0 then
                    return c
                end
                nParsed = nParsed + c
                s = string.sub(q, nParsed)
                tokenFound = true
            elseif string.find(s, "^OBJECTID=") then
                local c = parseObjectId(s)
                if c < 0 then
                    return err.append(" ("..s..")")
                end
                nParsed = nParsed + c
                s = string.sub(q, nParsed)
                tokenFound = true
            elseif string.find(s, "^OBJECTID IN") then
                return err.set(ERR_NOT_SUPPORTED,
                               "Sorry, objectId IN is not supported")
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
                return err.set(ERR_OR_NOT_ALLOWED,
                               "'OR' is not allowed here: '"..s.."'")
            else
                return err.set(ERR_AND_EXPECTED,
                               "'AND' was expected here: '"..s.."'")
            end
        end
        return retV
    end

    ---------------------------------------------------------------------------

    return {
       setAndNeeded = setAndNeeded,
       parseIt = parseIt,
       reset = reset
    }
end

parser = miniParser()

-------------------------------------------------------------------------------
---- --                          Query type                                  --
-------------------------------------------------------------------------------

function queryType()

    -- Detects if query can be handled locally without sending it to qserv
    local isLocal = function(qU)
        if string.find(qU, "^SELECT @@VERSION_COMMENT LIMIT") or
            string.find(qU, "^SELECT @@SESSION.AUTO_INCREMENT_INCREMENT") or
            string.find(qU, "^SHOW ") or
            string.find(qU, "^SET ") or
            string.find(qU, "^DESCRIBE ") or
            string.find(qU, "^DESC ") or
            string.find(qU, "^ROLLBACK") or
            string.find(qU, "^SELECT CURRENT_USER()") then
            return true
        end
        return false
    end
    ---------------------------------------------------------------------------
    local shouldPassToResultDb = function(qU)
        if string.find(qU, "^SELECT DATABASE()") then
            return true
        else
            return false
        end
    end
    ---------------------------------------------------------------------------

    local isDisallowed = function(qU)
        if string.find(qU, "^INSERT ") or
           string.find(qU, "^UPDATE ") or
           string.find(qU, "^LOAD ") or
           string.find(qU, "^CREATE ") or
           string.find(qU, "^ALTER ") or
           string.find(qU, "^TRUNCATE ") or
           string.find(qU, "^DROP ") then
            err.set(ERR_NOT_SUPPORTED,
                    "Sorry, this type of queries is disallowed.")
            return true
        end
        return false
    end
    ---------------------------------------------------------------------------

    local isKill = function(qU)
        if string.find(qU, "^KILL ") then
            return true
        end
        return false
    end
    ---------------------------------------------------------------------------

    local isIgnored = function(qU)
        if string.find(qU, "^SET ")  then
            return true
        end
        return false
    end

    ---------------------------------------------------------------------------

    local isNotSupported = function(qU)
        if string.find(qU, "^EXPLAIN ") or
           string.find(qU, "^GRANT ") or
           string.find(qU, "^FLUSH ") then
            err.set(ERR_NOT_SUPPORTED,
                    "Sorry, this type of queries is not supported in DC3b.")
            return true
        end
        return false
    end

    ---------------------------------------------------------------------------

    return {
        isLocal = isLocal,
        shouldPassToResultDb = shouldPassToResultDb,
        isDisallowed = isDisallowed,
        isKill = isKill,
        isIgnored = isIgnored,
        isNotSupported = isNotSupported
    }
end

qType = queryType()

-------------------------------------------------------------------------------
--                            Query processing                               --
-------------------------------------------------------------------------------

function queryProcessing()

    local self = { msgTableName = nil,
                   resultTableName = nil,
                   orderByClause = nil,
                   qservError = nil }

    ---------------------------------------------------------------------------

    -- q  - original query
    -- qU - original query but all uppercase
    --
    --
    local sendToQserv = function(q, qU)
        local p1 = string.find(qU, "WHERE")
        parser.reset()
        hintsToPassArr = {} -- Reset hints (it's global)
        -- Force original query to delegate spatial work to qsmaster.
        queryToPassStr = q
        -- Add client db context
        hintsToPassArr["db"] = proxy.connection.client.default_db

        -- Need to save thread_id and reuse for killing query
        hintsToPassArr["client_dst_name"] = proxy.connection.client.dst.name
        hintsToPassArr["server_thread_id"] = proxy.connection.server.thread_id
        print ("proxy.connection.server.thread_id: " .. proxy.connection.server.thread_id)
        print ("Passing query: " .. queryToPassStr)

        -- Build hint string
        hintsToPassStr = utils.tableToString(hintsToPassArr)
        print ("Passing hints: " .. hintsToPassStr)

        local queryToPassProtect = "<![CDATA[" .. queryToPassStr .. "]]>"
        -- Wrap this in a pcall so that a meaningful error can
        -- be returned to the caller
        local pcallStatus, xmlrpcStatus, res =
           pcall(xmlrpc.http.call, czarRpcUrl,
                 "submitQuery", queryToPassProtect, hintsToPassArr)

        -- if pcall failed then xmlrpcStatus contains the related error message
        -- if pcall succeed then xmlrpcStatus contains the return code of
        -- xmlrpc.http.call
        if (not pcallStatus) then
            err_msg = xmlrpcStatus
            return err.set(ERR_RPC_CALL, "Unable to run lua xmlrpc client, message: " .. xmlrpcStatus)
        elseif (not xmlrpcStatus) then
            return err.set(ERR_RPC_CALL, "mysql-proxy RPC call failed for czar url: " .. czarRpcUrl)
        end

        qservError = res[1]
        if qservError then
           return err.set(ERR_QSERV_PARSE, "Query processing error: " .. qservError)
        end

        resultTableName = res[2]
        msgTableName = res[3]
        if res[4] then
            orderByClause = res[4]
        else
            orderByClause = ""
        end

        print ("Czar RPC response: [result: " .. resultTableName ..
               ", message: " .. msgTableName ..
               ", order_by: \"" .. orderByClause .. "\"]")

        return SUCCESS
     end

    ---------------------------------------------------------------------------

    local processLocally = function(q)
        print ("Processing locally")
        return SUCCESS
    end

    ---------------------------------------------------------------------------

    local processIgnored = function(q)
        proxy.response.type = proxy.MYSQLD_PACKET_OK
        -- Assemble result
        proxy.response.resultset = {
           fields = {
              {
                 type = proxy.MYSQL_TYPE_STRING,
                 name = command,
              },
           },
           rows = {{"Ignoring meaningless command (in Qserv)."}}
        }
        return proxy.PROXY_SEND_RESULT
    end

    ---------------------------------------------------------------------------

    local killQservQuery = function(qU)
        -- Idea: "KILL QUERY <server.thread_id>" is in the parameter, so we
        -- just have to pass along qU and the original client id so
        -- that the server can differentiate among clients and kill
        -- the right query.
        proxy.response.type = proxy.MYSQLD_PACKET_OK
        local callError, ok, res =
           pcall(xmlrpc.http.call,
                 czarRpcUrl, "killQueryUgly", qU, proxy.connection.client.dst.name)

        if (not callError) then
           err.set(ERR_RPC_CALL, "Cannot connect to Qserv master; "
                          .. "Exception in RPC call: " .. ok)
           return err.send()
        elseif (not ok) then
           print("\nKill query RPC failure: " .. res .. "---" .. proxy.connection.client.dst.name)
           err.set(ERR_RPC_CALL, "rpc call failed for " .. czarRpcUrl)
           return err.send()
        end
        -- Assemble result
        proxy.response.resultset = {
           fields = {
              {
                 type = proxy.MYSQL_TYPE_STRING,
                 name = command,
              },
           },
           rows = {{"Trying to kill query..".. qU}}
        }
        return proxy.PROXY_SEND_RESULT
    end

    ---------------------------------------------------------------------------

    local prepForFetchingResults = function(proxy)
        if not resultTableName then
            return err.set(ERR_BAD_RES_TNAME, "Invalid result table name ")
        end

        -- Severity is stored in a MySQL enum
        q1 = "SELECT chunkId, code, message, severity+0, timeStamp FROM " .. msgTableName
        proxy.queries:append(1, string.char(proxy.COM_QUERY) .. q1,
                             {resultset_is_needed = true})

        q2 = "SELECT * FROM " .. resultTableName .. " " .. orderByClause
        proxy.queries:append(2, string.char(proxy.COM_QUERY) .. q2,
                             {resultset_is_needed = true})
        q3 = "DROP TABLE " .. msgTableName
        proxy.queries:append(3, string.char(proxy.COM_QUERY) .. q3,
                             {resultset_is_needed = true})

        q4 = "DROP TABLE " .. resultTableName
        proxy.queries:append(4, string.char(proxy.COM_QUERY) .. q4,
                             {resultset_is_needed = true})

        return SUCCESS
    end

    ---------------------------------------------------------------------------

    return {
        sendToQserv = sendToQserv,
        killQservQuery = killQservQuery,
        processLocally = processLocally,
        processIgnored = processIgnored,
        prepForFetchingResults = prepForFetchingResults
    }

end

qProc = queryProcessing()


-------------------------------------------------------------------------------
--                           "public" functions                              --
-------------------------------------------------------------------------------

function read_query(packet)
    if string.byte(packet) == proxy.COM_QUERY then
        print("\n*******************\nIntercepted: " .. string.sub(packet, 2))

        -- massage the query string to simplify its processing
        local q = utils.removeLeadingComment(string.sub(packet,2))
        q = utils.removeExtraWhiteSpaces(q)
            -- it is useful to always have a space
            -- even at the end of last predicate
        local qU = string.upper(q) .. ' '

        -- check for special queries that can be handled locally
        -- note we make no modifications to proxy.queries,
        -- so the packet will be sent as-is
        if qType.isLocal(qU) then
           return qProc.processLocally(qU)
        elseif qType.isIgnored(qU) then
           return qProc.processIgnored(qU)
        elseif qType.shouldPassToResultDb(qU) then
            return -- Return nothing to indicate passthrough.
        end
        -- check for queries that are disallowed
        if qType.isDisallowed(qU) then
            return err.send()
        end
        -- check for queries that we don't support yet
        if qType.isNotSupported(qU) then
            return err.send()
        elseif qType.isKill(qU) then
           return qProc.killQservQuery(q, qU)
        end
        -- Reset error count
        queryErrorCount = 0

        -- process the query and send it to qserv
        local sendResult = qProc.sendToQserv(q, qU)
        print ("Sendresult " .. sendResult)
        if sendResult < 0 then
            return err.send()
        end

        -- configure proxy to fetch results from
        -- the appropriate result table
        if qProc.prepForFetchingResults(proxy) < 0 then
            return err.send()
        end
        return proxy.PROXY_SEND_QUERY
    end
end

function read_query_result(inj)
    -- we injected query with the id=1 (for messaging and locking purposes)
    if (inj.type == 1) then
        print("q1 - ignoring")
        local error_msg = ""
        for row in inj.resultset.rows do
            severity = tonumber(row[4])
            if (severity == MSG_ERROR) then
                queryErrorCount  = queryErrorCount + 1
                error_msg = error_msg .. "\n" .. tostring(row[3])
                -- WARN czar never returns multiple errors for now
                if (queryErrorCount > 1) then
                    error_msg = "\n-- WARN multiple errors"
                end
            else
                print("   chunkId: " .. row[1] .. ", code: " .. row[2] .. ", msg: " .. tostring(row[3]) .. ", timestamp: " .. row[5])
            end
        end
        if (queryErrorCount > 0) then
            error_msg = "Unable to return query results:" .. error_msg
            return err.setAndSend(ERR_QSERV_RUNTIME, error_msg)
        end
        return proxy.PROXY_IGNORE_RESULT
    elseif (inj.type == 3) or
        (inj.type == 4) then
        -- Proxy will complain if we try to touch 'inj' for these:
        -- (critical) (read_query_result) ...attempt to call a nil value
        -- (critical) proxy-plugin.c.303: got asked to send a resultset,
        --            but ignoring it as we already have sent 1 resultset(s).
        --            injection-id: 3
        print("cleanup q(3,4) - ignoring")
        return proxy.PROXY_IGNORE_RESULT
    elseif (queryErrorCount > 0) then
        print("q2 - already have errors, ignoring")
        return proxy.PROXY_IGNORE_RESULT
    elseif (inj.resultset.rows == nil) then
        print("q2 - no resultset.")
        return err.setAndSend(ERR_QSERV_RUNTIME,
        "Error executing query using qserv.")
    else
        print("q2 - passing")
        for row in inj.resultset.rows do
            print("   " .. row[1])
        end
    end
end
