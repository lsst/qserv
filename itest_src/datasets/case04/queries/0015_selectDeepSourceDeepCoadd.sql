-- https://dev.lsstcorp.org/trac/wiki/dbPipeQAQueries

-- pragma sortresult
SELECT sce.filterName, sce.tract, sce.patch, s.deepSourceId, s.ra, s.decl, 
       s.x, s.y, s.psfFlux, s.psfFluxSigma, s.apFlux, s.apFluxSigma, s.modelFlux, 
       s.modelFluxSigma, s.instFlux, s.instFluxSigma, s.shapeIxx, s.shapeIyy, 
       s.shapeIxy, s.flagPixInterpCen, s.flagNegative, s.flagPixEdge, 
       s.flagBadCentroid, s.flagPixSaturCen, s.extendedness
FROM   DeepSource AS s, 
       DeepCoadd AS sce
WHERE  (s.deepCoaddId = sce.deepCoaddId)
   AND (sce.filterName = 'r')
   AND (sce.tract = 0)
   AND (sce.patch = '159,2');

