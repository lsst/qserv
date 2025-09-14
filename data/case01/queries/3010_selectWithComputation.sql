-- Note: specific ordering of rows is require to ensure the MySQL and Qserv
-- results would match.
--
-- The second important comment is regarding using 'AS sourceId_hash' in
-- the SELECT list for the last selector. This is needed because of the following
-- differences in how this is reported in the result set:
--   MySQL: objectId	ra_PS	decl_PS	sourceId	src.sourceId%pow(2,10)
--   Qserv: objectId	ra_PS	decl_PS	sourceId	(src.sourceId % pow(2,10))
-- This would screw the comparison.

SELECT o.objectId,
       o.ra_PS,
       o.decl_PS,
       src.sourceId,
       src.sourceId%pow(2,10) AS sourceId_hash
FROM Object o, Source src
WHERE o.ra_PS  BETWEEN 0. AND 1.
  AND o.decl_PS BETWEEN 0. AND 1.
  AND o.objectId = src.objectId
ORDER BY o.objectId,
         src.sourceId;
