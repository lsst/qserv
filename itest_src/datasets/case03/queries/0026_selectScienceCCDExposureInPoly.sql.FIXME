-- https://dev.lsstcorp.org/trac/wiki/dbPipeQAQueries

SELECT scisql_s2CPolyToBin(sce.corner1Ra, sce.corner1Decl, sce.corner2Ra, sce.corner2Decl,
                           sce.corner3Ra, sce.corner3Decl, sce.corner4Ra, sce.corner4Decl) 
FROM   Science_Ccd_Exposure AS sce
WHERE  (sce.filterName = 'g')
   AND (sce.field = 535)
   AND (sce.camcol = 1)
   AND (sce.run = 94) INTO @poly;
