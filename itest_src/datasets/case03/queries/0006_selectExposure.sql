-- https://dev.lsstcorp.org/trac/wiki/dbPipeQAQueries

-- pragma sortresult
SELECT sce.filterName, sce.field, sce.camcol, sce.run  
FROM   Science_Ccd_Exposure AS sce
WHERE  sce.filterName = 'g'
   AND sce.field = 670
   AND sce.camcol = 2
   AND sce.run = 7202
;

