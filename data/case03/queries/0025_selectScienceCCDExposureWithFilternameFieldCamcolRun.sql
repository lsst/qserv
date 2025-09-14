-- https://dev.lsstcorp.org/trac/wiki/dbPipeQAQueries

SELECT sce.filterName, sce.field, sce.camcol, sce.run
FROM   Science_Ccd_Exposure AS sce
WHERE  sce.filterName = 'g'
   AND sce.field = 535
   AND sce.camcol = 1
   AND sce.run = 94;
