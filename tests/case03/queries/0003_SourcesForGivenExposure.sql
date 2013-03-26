-- Return empty set with W13 small dataset
-- https://dev.lsstcorp.org/trac/wiki/dbPipeQAQueries 

SELECT sce.filterName, sce.field, sce.camcol, sce.run, s.deepForcedSourceId, s.ra, s.decl, 
       s.x, s.y, s.psfFlux, s.psfFluxSigma, s.apFlux, s.apFluxSigma, s.modelFlux, 
       s.modelFluxSigma, s.instFlux, s.instFluxSigma, s.shapeIxx, s.shapeIyy, 
       s.shapeIxy, s.flagPixInterpCen, s.flagNegative, s.flagPixEdge, 
       s.flagBadCentroid, s.flagPixSaturCen, s.extendedness  
FROM   DeepForcedSource AS s,
       Science_Ccd_Exposure AS sce
WHERE  (s.scienceCcdExposureId = sce.scienceCcdExposureId)
   AND (sce.filterName = 'g')
   AND (sce.field = 535)
   AND (sce.camcol = 1)
   AND (sce.run = 94);
