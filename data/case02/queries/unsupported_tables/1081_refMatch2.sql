SELECT count(*)
FROM   Object o
       INNER JOIN RefObjMatch o2t ON (o.objectId = o2t.objectId)
       LEFT  JOIN SimRefObject t ON (o2t.refObjectId = t.refObjectId)
WHERE  closestToObj = 1
    OR closestToObj is NULL
