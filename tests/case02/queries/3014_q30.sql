SELECT s.ra, s.decl
 FROM   Object o
 JOIN   Source s
 USING (objectId)
 WHERE  o.objectId = 433327840428032
 AND    o.latestObsTime = s.taiMidPoint
