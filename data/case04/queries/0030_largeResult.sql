-- Return a dataset over 2 megabytes in size.
-- https://dev.lsstcorp.org/trac/wiki/dbPipeQAQueries 

SELECT deepForcedSourceId, scienceCcdExposureId, filterId, deepSourceId, timeMid, expTime, ra, decl, 
raVar, declVar, radeclCov, htmId20, x, y, xVar, yVar, xyCov, psfFlux, psfFluxSigma, apFlux, 
apFluxSigma, modelFlux, modelFluxSigma, instFlux, instFluxSigma, apCorrection, apCorrectionSigma, 
shapeIx, shapeIy, shapeIxVar, shapeIyVar, shapeIxIyCov, shapeIxx, shapeIyy, shapeIxy, shapeIxxVar, 
shapeIyyVar, shapeIxyVar, shapeIxxIyyCov, shapeIxxIxyCov, shapeIyyIxyCov, extendedness, flagNegative, 
flagBadMeasCentroid, flagPixEdge, flagPixInterpAny, flagPixInterpCen, flagPixSaturAny, flagPixSaturCen, 
flagBadPsfFlux, flagBadApFlux, flagBadModelFlux, flagBadInstFlux, flagBadCentroid, flagBadShape, 
raDeepSource, declDeepSource  
FROM DeepForcedSource 
ORDER BY deepForcedSourceId;
