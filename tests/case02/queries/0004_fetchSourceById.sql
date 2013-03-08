-- Find an object with a particular object id
-- http://dev.lsstcorp.org/trac/wiki/dbQuery009

-- note that the data for mysql version is specially
-- precooked for this object to include chunkId and
-- subchunkId

SELECT
sourceId, scienceCcdExposureId, filterId, objectId, movingObjectId, procHistoryId, ra, raErrForDetection, raErrForWcs, decl, declErrForDetection, declErrForWcs, xFlux, xFluxErr, yFlux, yFluxErr, raFlux, raFluxErr, declFlux, declFluxErr, xPeak, yPeak, raPeak, declPeak, xAstrom, xAstromErr, yAstrom, yAstromErr, raAstrom, raAstromErr, declAstrom, declAstromErr, raObject, declObject, taiMidPoint, taiRange, psfFlux, psfFluxErr, apFlux, apFluxErr, modelFlux, modelFluxErr, petroFlux, petroFluxErr, instFlux, instFluxErr, nonGrayCorrFlux, nonGrayCorrFluxErr, atmCorrFlux, atmCorrFluxErr, apDia, Ixx, IxxErr, Iyy, IyyErr, Ixy, IxyErr, snr, chi2, sky, skyErr, extendedness, flux_PS, flux_PS_Sigma, flux_SG, flux_SG_Sigma, sersicN_SG, sersicN_SG_Sigma, e1_SG, e1_SG_Sigma, e2_SG, e2_SG_Sigma, radius_SG, radius_SG_Sigma, flux_flux_SG_Cov, flux_e1_SG_Cov, flux_e2_SG_Cov, flux_radius_SG_Cov, flux_sersicN_SG_Cov, e1_e1_SG_Cov, e1_e2_SG_Cov, e1_radius_SG_Cov, e1_sersicN_SG_Cov, e2_e2_SG_Cov, e2_radius_SG_Cov, e2_sersicN_SG_Cov, radius_radius_SG_Cov, radius_sersicN_SG_Cov, sersicN_sersicN_SG_Cov, flagForAssociation, flagForDetection, flagForWcs
FROM   Source 
WHERE  sourceId = 2867930095748785 
