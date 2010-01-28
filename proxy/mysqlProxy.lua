
-- todos:
--  * supress errors "FUNCTION proxyTest.areaSpec_box does not exist"
--  * enforce single bounding box
--  * returning errors via sql api
--  * communicating with user, returning results etc
--  * talk to master via xml rpc
-- * api: invoke(cleanQueryString, hintString)
--     cleanQueryString is the query without special hints
--     hintString: "box", "1,2,11,12", "box", "5,55,6,66", "objectId", "3",
--   "  objectId", "5,6,7,8" and so on




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


-- Output:
--    negative: failure
--    positive: number of characters processed by this function
function parse_areaspecbox(s)
    local p1 = string.len("AREASPEC_BOX")
    -- print ("parsing args for areaspecbox: '" .. string.sub(s, p1) .. "'")
    local p2 = string.find(s, ')')
    if p2 then
        params = string.sub(s, p1+1, p2)
        params = string.gsub(params, ' ', '')
        print ("ra1/dec1/ra2/dec2 for box: '" .. string.sub(params, 0) .. "'")
        return p2
    end
    print "Error parsing parameters for AREASPEC_BOX"
    return -1
end


-- Output:
--    negative: failure
--    positive: number of characters processed by this function
function parse_objectId(s)
    local p1 = string.len("OBJECTID=")
    -- print ("parsing args for objectId: '" .. string.sub(s, p1) .. "'")
    local p2 = string.find(s, ' ') -- FIXME: doesn't have to be space
    if p2 then
        params = string.sub(s, p1+1, p2)
        params = string.gsub(params, ' ', '')
        print ("objectId is: '" .. string.sub(params, 0) .. "'")
        return p2-1
    end
    print "Error parsing parameters for OBJECTID="
    return -1
end


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

    -- Value to be returned from this function
    local retV = 0

    while true do
        -- print("\nwhile loop: '" .. s .. "'")

        tokenFound = false

        if string.find(s, "^AREASPEC_BOX") then
            c = parse_areaspecbox(s)
            if c < 0 then
                print "Error parsing AREASPEC_BOX"
                retV = -1
                break
            end
            nParsed = nParsed + c
            s = string.sub(q, nParsed)
            tokenFound = true
        elseif string.find(s, "^OBJECTID=") then
            c = parse_objectId(s)
            if c < 0 then
                print "Error parsing OBJECTID"
                retV = -1
                break
            end
            nParsed = nParsed + c
            s = string.sub(q, nParsed)
            tokenFound = true
        elseif string.find(s, "^OBJECTID IN") then
            print ("OBJECTID IN not supported yet, sorry")
            retV = -1
            break
        end
        -- end of looking for special tokens

        if not tokenFound then
            print "Done (reached first unknown token)"
            retV = nParsed
            break
        else
            if string.len(s) < 4 then
                print "Done (no more predicates)"
                retV = nParsed
            else
                if string.byte(s) == 32 then -- skip space
                    s = string.sub(s, 2)
                    nParsed = nParsed + 1
                end
                if string.find(s, "^AND") then
                    s = string.sub(s, 5)
                    nParsed = nParsed + 5
                elseif string.find(s, "^OR") then
                   print "Error: found OR around special tokens."
                   retV = -1
                   break
                else
                   print ("Error: AND expected, s='"..s.."'")
                   retV = -1
                   break
                end
            end
        end
    end
    return retV
end


function read_query(packet)
    if string.byte(packet) == proxy.COM_QUERY then
        print("\n*******************\nIntercepted: " .. string.sub(packet, 2))
        q = string.removeExtraWhiteSpaces(string.sub(packet,2))
        qU = string.upper(q)
        p1 = string.find(qU, "WHERE")
	if p1 then
            p2 = parse_predicates(qU, p1+6) -- 6=length of 'where '
            pEnd = string.len(qU)
            q2 = string.sub(q, 0, p1-1) .. "WHERE " .. string.sub(q, p2-1, pEnd)
            print ("The query now is: " .. q2)
        else
            print ("There is no WHERE, query will be passed unchanged")
        end
   end
end
