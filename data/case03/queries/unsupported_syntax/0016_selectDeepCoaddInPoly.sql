-- This syntax is not presently supported in Qserv

-- https://dev.lsstcorp.org/trac/wiki/dbPipeQAQueries

SELECT scisql_s2CPolyToBin(sce.corner1Ra, sce.corner1Decl, sce.corner2Ra, sce.corner2Decl,
                           sce.corner3Ra, sce.corner3Decl, sce.corner4Ra, sce.corner4Decl)
FROM   DeepCoadd AS sce
WHERE  (sce.filterName = 'g')
   AND (sce.tract = 0)
   AND (sce.patch = '159,1') INTO @poly;

