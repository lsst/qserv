-- Join Object and Source on a given object and for a given time interval
-- https://dev.lsstcorp.org/trac/wiki/db/Qserv/IN2P3/BenchmarkMarch2013
-- Based on https://dev.lsstcorp.org/trac/wiki/db/queries/006

SELECT s.ra, s.decl
 FROM   Object o
 JOIN   Source s
 USING (objectId)
 WHERE  o.objectId = 433327840429024
 AND    o.latestObsTime BETWEEN s.taiMidPoint - 300 AND s.taiMidPoint + 300
