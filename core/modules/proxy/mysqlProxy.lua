-- mysqlProxy.lua -- A Lua-language script for customizing a
-- mysqlproxy instance so that it uses a Qserv frontend as a backend
-- for executing queries. While it has some responsibilities now, it
-- should eventually act as a thin wrapper to delegating all
-- functionality to a Qserv daemon.

require ("czarProxy")

-------------------------------------------------------------------------------
--                        global variables (yuck)                            --
-------------------------------------------------------------------------------

-- constants (kind of)
ERR_AND_EXPECTED   = -4001
ERR_BAD_ARG        = -4002
ERR_NOT_SUPPORTED  = -4003
ERR_OR_NOT_ALLOWED = -4004
ERR_BAD_RES_TNAME  = -4006
ERR_QSERV_GENERIC  = -4100
ERR_QSERV_PARSE    = -4110
ERR_QSERV_RUNTIME  = -4120

SUCCESS            = 0
MSG_ERROR          = 2

-- global variables have per-session(client) scope
-- queryErrorCount -- number of run-time errors detected during query exec.
queryErrorCount = 0

-------------------------------------------------------------------------------
--                             error handling                                --
-------------------------------------------------------------------------------

function errors ()
    local self = { __errNo__ = 0, __errMsg__ = "" }

    ---------------------------------------------------------------------------

    local errNo = function()
        return self.__errNo__
    end

    ---------------------------------------------------------------------------
    local set = function(errNo, errMsg)
        self.__errNo__  = errNo
        self.__errMsg__ = errMsg
        return errNo
    end

    local append = function(errMsg)
        self.__errMsg__ = self.__errMsg__ .. errMsg
        return errNo
    end

    ---------------------------------------------------------------------------

    local send = function()
        local e = -1 * self.__errNo__ -- mysql doesn't like negative errors
        proxy.response = {
            type     = proxy.MYSQLD_PACKET_ERR,
            errmsg   = self.__errMsg__,
            errcode  = e,
            sqlstate = 'Proxy',
        }
        czarProxy.log("mysql-proxy", "ERROR", "errNo: "..e..": errMsg: "..self.__errMsg__)
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
        local k, v
        for k, v in pairs(t) do
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
        local qRet = q
        local x1 = string.find(q, '%/%*')
        local x2 = string.find(q, '%*%/')
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
---- --                          Query type                                  --
-------------------------------------------------------------------------------

function queryType()

    -- Detects if query can be handled locally without sending it to qserv
    local isLocal = function(qU)
        if string.find(qU, "^SELECT @@") or
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
           string.find(qU, "^TRUNCATE ") then
            err.set(ERR_NOT_SUPPORTED,
                    "Sorry, this type of queries is not supported.")
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
        -- SET is already in isLocal() so this laways returns false for now
        if string.find(qU, "^SET ") then
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
                   orderByClause = nil }

    ---------------------------------------------------------------------------

    -- q  - original query
    -- qU - original query but all uppercase
    --
    --
    local sendToQserv = function(q, qU)

        local hintsToPassArr = {}
        -- Force original query to delegate spatial work to qsmaster.
        local queryToPassStr = q
        -- Add client db context
        hintsToPassArr["db"] = proxy.connection.client.default_db

        -- Need to save thread_id and reuse for killing query
        hintsToPassArr["client_dst_name"] = proxy.connection.client.dst.name
        hintsToPassArr["server_thread_id"] = proxy.connection.server.thread_id
        czarProxy.log("mysql-proxy", "INFO", "proxy.connection.server.thread_id: " .. proxy.connection.server.thread_id)
        czarProxy.log("mysql-proxy", "INFO", "Passing query: " .. queryToPassStr)

        -- Build hint string
        local hintsToPassStr = utils.tableToString(hintsToPassArr)
        czarProxy.log("mysql-proxy", "INFO", "Passing hints: " .. hintsToPassStr)

        -- send query to czar
        local res = czarProxy.submitQuery(queryToPassStr, hintsToPassArr)

        local qservError = res[1]
        if qservError and qservError ~= "" then
           return err.set(ERR_QSERV_PARSE, "Query processing error: " .. qservError)
        end

        self.resultTableName = res[2]
        self.msgTableName = res[3]
        self.orderByClause = ""
        if res[4] then
            self.orderByClause = res[4]
        end

        czarProxy.log("mysql-proxy", "INFO", "Czar response: [result: " .. self.resultTableName ..
               ", message: " .. self.msgTableName ..
               ", order_by: \"" .. self.orderByClause .. "\"]")

        return SUCCESS
     end

    ---------------------------------------------------------------------------

    local processLocally = function(q)
        czarProxy.log("mysql-proxy", "INFO", "Processing locally: " .. q)
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
                 name = "command",
              },
           },
           rows = {{"Ignoring meaningless command (in Qserv)."}}
        }
        return proxy.PROXY_SEND_RESULT
    end

    ---------------------------------------------------------------------------

    local killQservQuery = function(q, qU)
        -- Idea: "KILL QUERY <server.thread_id>" is in the parameter, so we
        -- just have to pass along qU and the original client id so
        -- that the server can differentiate among clients and kill
        -- the right query.
        czarProxy.log("mysql-proxy", "INFO", "Killing query/connection: " .. q)
        czarProxy.killQuery(qU, proxy.connection.client.dst.name)

        -- Assemble result
        proxy.response.type = proxy.MYSQLD_PACKET_OK
        proxy.response.resultset = {
           fields = {
              {
                 type = proxy.MYSQL_TYPE_STRING,
                 name = "command",
              },
           },
           rows = {{"Trying to kill query: ".. qU}}
        }
        return proxy.PROXY_SEND_RESULT
    end

    ---------------------------------------------------------------------------

    local prepForFetchingResults = function(proxy)
        if not self.resultTableName then
            return err.set(ERR_BAD_RES_TNAME, "Invalid result table name")
        end

        -- Severity is stored in a MySQL enum
        local q1 = "SELECT chunkId, code, message, severity+0, timeStamp FROM " .. self.msgTableName
        proxy.queries:append(1, string.char(proxy.COM_QUERY) .. q1,
                             {resultset_is_needed = true})

        local q2 = "SELECT * FROM " .. self.resultTableName .. " " .. self.orderByClause
        proxy.queries:append(2, string.char(proxy.COM_QUERY) .. q2,
                             {resultset_is_needed = true})
        local q3 = "DROP TABLE " .. self.msgTableName
        proxy.queries:append(3, string.char(proxy.COM_QUERY) .. q3,
                             {resultset_is_needed = true})

        local q4 = "DROP TABLE " .. self.resultTableName
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
        czarProxy.log("mysql-proxy", "INFO", "Intercepted: " .. string.sub(packet, 2))

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
        czarProxy.log("mysql-proxy", "INFO", "Sendresult " .. sendResult)
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
        czarProxy.log("mysql-proxy", "INFO", "q1 - ignoring")
        local error_msg = ""
        local row
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
                czarProxy.log("mysql-proxy", "INFO", "   chunkId: " .. row[1] .. ", code: " .. row[2] ..
                              ", msg: " .. tostring(row[3]) .. ", timestamp: " .. row[5])
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
        czarProxy.log("mysql-proxy", "INFO", "cleanup q(3,4) - ignoring")
        return proxy.PROXY_IGNORE_RESULT
    elseif (queryErrorCount > 0) then
        czarProxy.log("mysql-proxy", "INFO", "q2 - already have errors, ignoring")
        return proxy.PROXY_IGNORE_RESULT
    elseif (inj.resultset.rows == nil) then
        czarProxy.log("mysql-proxy", "INFO", "q2 - no resultset.")
        return err.setAndSend(ERR_QSERV_RUNTIME,
        "Error executing query using qserv.")
    else
        czarProxy.log("mysql-proxy", "INFO", "q2 - passing")
        local row
        local i = 0
        for row in inj.resultset.rows do
            i = i + 1
            if (i > 32) then
                czarProxy.log("mysql-proxy", "DEBUG", "   ... rest is not shown")
                break
            end
            czarProxy.log("mysql-proxy", "DEBUG", "   " .. row[1])
        end
    end
end
