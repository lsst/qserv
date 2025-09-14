-- The query is disabled because it refers to the table DeepSource
-- which doesn't exist in this catalog.

-- https://dev.lsstcorp.org/trac/wiki/dbPipeQAQueries

SELECT sce.filterName, sce.tract, sce.patch, sro.gMag, sro.ra, sro.decl, sro.isStar,
       sro.refObjectId, s.deepSourceId,  rom.nSrcMatches,s.ra, s.decl, s.x, s.y,
       s.psfFlux, s.psfFluxSigma, s.apFlux, s.apFluxSigma, s.modelFlux, s.modelFluxSigma,
       s.instFlux, s.instFluxSigma, s.shapeIxx, s.shapeIyy, s.shapeIxy, s.flagPixInterpCen,
       s.flagNegative, s.flagPixEdge, s.flagBadCentroid, s.flagPixSaturCen, s.extendedness
FROM   DeepSource AS s,
       DeepCoadd AS sce,
       RefDeepSrcMatch AS rom,
       RefObject AS sro
WHERE  (s.deepCoaddId = sce.deepCoaddId)
   AND (s.deepSourceId = rom.deepSourceId)
   AND (rom.refObjectId = sro.refObjectId)
   AND (sce.filterName = 'r')
   AND (sce.tract = 0)
   AND (sce.patch = '159,3');

