-- The syntax isn't presently supported.

-- https://dev.lsstcorp.org/trac/wiki/dbPipeQAQueries

SELECT scisql_s2CPolyToBin(54.96, -0.64, 55.12, -0.64,
                           55.12, -0.41, 54.96, -0.41) 
FROM   Science_Ccd_Exposure AS sce
WHERE  (sce.filterName = 'g')
   AND (sce.field = 670)
   AND (sce.camcol = 2)
   AND (sce.run = 7202) INTO @poly;
