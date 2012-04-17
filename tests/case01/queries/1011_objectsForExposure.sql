-- joins, but for limited number of visits
-- sort by is here purely so that we can compare results from mysql and qserv

SELECT objectId
FROM   Source s
JOIN   Science_Ccd_Exposure sce USING (scienceCcdExposureId)
WHERE  sce.visit IN (885449631,886257441,886472151) ORDER BY objectId LIMIT 10
