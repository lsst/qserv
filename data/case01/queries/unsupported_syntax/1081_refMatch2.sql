-- Unsupported syntax:
-- ERROR 4110 (Proxy): Query processing error: QI=?: Failed to instantiate
--   query: ParseException:qserv can not parse query, near
--   "LEFT JOIN SimRefObject t ON(o2t.refObjectId=t.refObjectId)"

SELECT count(*)
FROM   Object o
       INNER JOIN RefObjMatch o2t ON (o.objectId = o2t.objectId)
       LEFT  JOIN SimRefObject t ON (o2t.refObjectId = t.refObjectId)
WHERE  closestToObj = 1
    OR closestToObj is NULL
