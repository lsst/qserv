SELECT s.ra, s.decl
 FROM   Object o
 JOIN   Source s
 USING (objectId)
 WHERE  o.objectId = 433327840429024
 AND    o.latestObsTime BETWEEN s.taiMidPoint - 300 AND s.taiMidPoint + 300
