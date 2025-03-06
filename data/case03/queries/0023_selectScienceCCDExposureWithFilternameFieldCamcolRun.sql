-- https://dev.lsstcorp.org/trac/wiki/dbPipeQAQueries

SELECT sce.scienceCcdExposureId, sce.field, sce.camcol, sce.run,
       sce.filterId, sce.filterName, sce.ra, sce.decl, sce.crpix1, sce.crpix2, 
       sce.crval1, sce.crval2, sce.cd1_1, sce.cd1_2, sce.cd2_1, sce.cd2_2, 
       sce.fluxMag0, sce.fluxMag0Sigma, sce.fwhm
FROM   Science_Ccd_Exposure AS sce
WHERE  sce.filterName = 'g'
   AND sce.field = 535
   AND sce.camcol = 1
   AND sce.run = 94;
