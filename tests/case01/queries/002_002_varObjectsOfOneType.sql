-- Select all variable objects of a specific type
-- See http://dev.lsstcorp.org/trac/wiki/dbQuery002

SELECT objectId
FROM   Object
JOIN   _ObjectToType USING(objectId)
JOIN   ObjectType USING (typeId)
WHERE  description = 'Supernova'
  AND  variability > 0.8
  AND  probability > 0.8;
