-- https://dev.lsstcorp.org/trac/wiki/dbPipeQAQueries 

SELECT distinct run, field 
FROM   Science_Ccd_Exposure
WHERE  (run = 94) AND (field = 535);
