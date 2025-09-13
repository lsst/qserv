-- The syntax isn't presently supported.

-- https://dev.lsstcorp.org/trac/wiki/dbPipeQAQueries

SET @poly = scisql_s2CPolyToBin(54.9, -1.25,
                                55.0, -1.25,
                                55.0, -0.75,
                                54.9, -0.75);

SELECT sro.refObjectId, sro.isStar, sro.ra, sro.decl, sro.uMag, sro.gMag, 
       sro.rMag, sro.iMag, sro.zMag 
FROM   RefObject AS sro 
WHERE  (scisql_s2PtInCPoly(sro.ra, sro.decl, @poly) = 1);
