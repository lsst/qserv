-- https://dev.lsstcorp.org/trac/wiki/dbPipeQAQueries

-- pragma sortresult
SELECT DISTINCT tract,patch,filterName
FROM DeepCoadd
;
