
require ("xmlrpc.http")
local ok, res = xmlrpc.http.call ("http://127.0.0.1:7080", "echo", "abcf")
print (ok)
print (res)
-- for i, v in pairs(res) do print ('\t', i, v) end


ok, res = xmlrpc.http.call ("http://127.0.0.1:7080", 
                "submitQuery", "select * from xabcf", "whatever")
print (ok)
print (res)
