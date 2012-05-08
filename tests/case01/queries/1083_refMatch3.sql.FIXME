select objectId, sro.*, (sro.refObjectId-1)/2%pow(2,10) typeId 
from Source s 
join RefObjMatch rom using (objectId) 
join SimRefObject sro using (refObjectId) 
where isStar =1 limit 10
