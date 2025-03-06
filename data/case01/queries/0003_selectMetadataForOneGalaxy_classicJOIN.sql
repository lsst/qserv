-- pragma sortresult
SELECT s.ra, s.decl, o.raRange, o.declRange 
FROM Object o, Source s 
WHERE o.objectId = 390034570102582 AND o.objectId = s.objectId AND o.latestObsTime = s.taiMidPoint;
