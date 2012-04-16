-- joins, but for limited number of visits

SELECT objectId
FROM   Source s
JOIN   Science_Ccd_Exposure sce USING (scienceCcdExposureId)
WHERE  sce.visit IN (885449631,886257441,886472151) LIMIT 10
