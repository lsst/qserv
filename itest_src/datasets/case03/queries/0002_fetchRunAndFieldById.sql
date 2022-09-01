-- https://dev.lsstcorp.org/trac/wiki/dbPipeQAQueries 

SELECT DISTINCT run, field 
FROM   Science_Ccd_Exposure
WHERE  run = 94 AND field = 535;
