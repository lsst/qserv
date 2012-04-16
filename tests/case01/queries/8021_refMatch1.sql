SELECT sce.visit, sce.raftName, sce.ccdName, 
       sro.gMag, sro.ra, sro.decl, sro.isStar, sro.refObjectId,  
       rom.nSrcMatches,
       s.sourceId,s.ra,s.decl,s.xAstrom,s.yAstrom,s.psfFlux,s.psfFluxSigma,
       s.apFlux,s.apFluxSigma,s.flux_ESG,s.flux_ESG_Sigma,s.flux_Gaussian,
       s.flux_Gaussian_Sigma,s.ixx,s.iyy,s.ixy,s.psfIxx,s.psfIxxSigma,
       s.psfIyy,s.psfIyySigma,s.psfIxy,s.psfIxySigma,s.resolution_SG,
       s.e1_SG,s.e1_SG_Sigma,s.e2_SG,s.e2_SG_Sigma,s.shear1_SG,s.shear1_SG_Sigma,
       s.shear2_SG,s.shear2_SG_Sigma,s.sourceWidth_SG,s.sourceWidth_SG_Sigma,
       s.flagForDetection
FROM Source AS s, 
     Science_Ccd_Exposure AS sce,
     RefSrcMatch AS rom,
     SimRefObject AS sro
WHERE (s.scienceCcdExposureId = sce.scienceCcdExposureId)
  AND (s.sourceId = rom.sourceId)
  AND (rom.refObjectId = sro.refObjectId)
  AND (sce.visit = 888241840)
  AND (sce.raftName = '1,0') 
  AND (sce.ccdName like '%')
