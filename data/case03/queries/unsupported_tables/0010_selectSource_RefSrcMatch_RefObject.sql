-- The query is disabled because it refers to the table DeepForcedSource
-- which doesn't exist in this catalog.

-- https://dev.lsstcorp.org/trac/wiki/dbPipeQAQueries

-- Nothing because
-- select count(*)
-- from DeepForcedSource AS s,
--      RefDeepSrcMatch AS rom
-- where (s.deepForcedSourceId = rom.deepSourceId);
-- is empty

SELECT sce.filterName, sce.field, sce.camcol, sce.run, sro.gMag, 
       sro.isStar, sro.refObjectId, s.deepForcedSourceId,  rom.nSrcMatches,s.ra, s.decl, 
       s.x, s.y, s.psfFlux, s.psfFluxSigma, s.apFlux, s.apFluxSigma, s.modelFlux, 
       s.modelFluxSigma, s.instFlux, s.instFluxSigma, s.shapeIxx, s.shapeIyy, 
       s.shapeIxy, s.flagPixInterpCen, s.flagNegative, s.flagPixEdge, 
       s.flagBadCentroid, s.flagPixSaturCen, s.extendedness
FROM   DeepForcedSource AS s, Science_Ccd_Exposure AS sce,
       RefDeepSrcMatch AS rom,
       RefObject AS sro 
WHERE  (s.scienceCcdExposureId = sce.scienceCcdExposureId)
   AND (s.deepForcedSourceId = rom.deepSourceId)
   AND (rom.refObjectId = sro.refObjectId)
   AND (sce.filterName = 'g')
   AND (sce.field = 670)
   AND (sce.camcol = 2)
   AND (sce.run = 7202)
;
