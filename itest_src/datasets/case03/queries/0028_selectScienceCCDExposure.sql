-- https://dev.lsstcorp.org/trac/wiki/dbPipeQAQueries

-- pragma sortresult
SELECT sce.filterId, sce.filterName
FROM   Science_Ccd_Exposure AS sce
WHERE  sce.filterName = 'g'
   AND sce.field = 535
   AND sce.camcol = 1
   AND sce.run = 94;
