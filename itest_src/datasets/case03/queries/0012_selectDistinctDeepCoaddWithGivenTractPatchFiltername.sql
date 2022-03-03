-- https://dev.lsstcorp.org/trac/wiki/dbPipeQAQueries

-- pragma sortresult
SELECT DISTINCT tract, patch, filterName 
FROM   DeepCoadd
WHERE  tract = 0 
   AND patch = '159,2'
   AND filterName = 'r';

