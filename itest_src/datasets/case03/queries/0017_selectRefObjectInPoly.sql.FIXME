-- https://dev.lsstcorp.org/trac/wiki/dbPipeQAQueries

SELECT sro.refObjectId, sro.isStar, sro.ra, sro.decl, sro.uMag, sro.gMag, 
       sro.rMag, sro.iMag, sro.zMag
FROM   RefObject AS sro
WHERE  (scisql_s2PtInCPoly(sro.ra, sro.decl, @poly) = 1);

