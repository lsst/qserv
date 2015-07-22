-- LSST Data Management System
-- Copyright 2015 AURA/LSST.
--
-- This product includes software developed by the
-- LSST Project (http://www.lsst.org/).
--
-- This program is free software: you can redistribute it and/or modify
-- it under the terms of the GNU General Public License as published by
-- the Free Software Foundation, either version 3 of the License, or
-- (at your option) any later version.
--
-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU General Public License for more details.
--
-- You should have received a copy of the LSST License Statement and
-- the GNU General Public License along with this program.  If not,
-- see <http://www.lsstcorp.org/LegalNotices/>.


-- Script that launch a SQL query on Qserv czar
-- can be used to debug mysql-proxy lua plugin
-- @author  Fabrice Jammes, IN2P3

require ("xmlrpc.http")

rpcHost = "127.0.0.1"
defaultRpcPort = 7080
local rpcPort = os.getenv("QSERV_RPC_PORT")
if (rpcPort == nil) then
    rpcPort = defaultRpcPort
end

czarRpcUrl = "http://" .. rpcHost .. ":" .. rpcPort .. "/x"

queryToPassStr = "SELECT * FROM Object WHERE objectId=430213989000"

hintsToPassArr = {}
hintsToPassArr["db"] = "qservTest_case01_qserv"
hintsToPassArr["client_dst_name"] = "127.0.0.1:4040"
hintsToPassArr["server_thread_id"] = "41"

local queryToPassProtect = "<![CDATA[" .. queryToPassStr .. "]]>"

pcall_status, xmlrpc_status, res = pcall(xmlrpc.http.call,czarRpcUrl, "submitQuery", queryToPassProtect, hintsToPassArr)

print (pcal_status, xmlrpc_status, res)
print (res[1], res[2], res[3], res[4])
