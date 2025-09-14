-- The query wouln't work in Qserv since the column filterName
-- is mentioned more than one time in the SELECT list.
-- Qserv would complain as shown below:
-- ERROR 4110 (Proxy): Query processing error: QI=?: Failed to instantiate query: AnalysisError:Duplicate names detected in select expression, rewrite SQL query using alias: [1] '`sce`.`filtername`' at positions: 2 6

-- https://dev.lsstcorp.org/trac/wiki/dbPipeQAQueries

SELECT sce.DeepCoaddId, sce.filterName, sce.tract, sce.patch, sce.filterId, 
       sce.filterName, sce.ra, sce.decl, sce.crpix1, sce.crpix2, sce.crval1, 
       sce.crval2, sce.cd1_1, sce.cd1_2, sce.cd2_1, sce.cd2_2, sce.fluxMag0, 
       sce.fluxMag0Sigma, sce.measuredFwhm  
FROM   DeepCoadd AS sce
WHERE  (sce.filterName = 'r')
   AND (sce.tract = 0)
   AND (sce.patch = '159,2');

