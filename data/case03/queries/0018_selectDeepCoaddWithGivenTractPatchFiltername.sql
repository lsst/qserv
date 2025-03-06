-- https://dev.lsstcorp.org/trac/wiki/dbPipeQAQueries

-- pragma sortresult
SELECT sce.filterId, sce.filterName
FROM   DeepCoadd AS sce
WHERE  sce.filterName = 'r'
   AND sce.tract = 0
   AND sce.patch = '159,1';

