-- Derived from https://dev.lsstcorp.org/trac/wiki/dbPipeQAQueries
-- Used to test BIT field support
-- Here ORDER BY fails (see DM-3187)

SELECT sce.filterName, sce.tract, sce.patch, sro.gMag, sro.ra, sro.decl, sro.isStar,
       sro.refObjectId, s.id,  rom.nSrcMatches, s.flags_pixel_interpolated_center, s.flags_negative,
       s.flags_pixel_edge, s.centroid_sdss_flags, s.flags_pixel_saturated_center
FROM   RunDeepSource AS s,
       DeepCoadd AS sce,
       RefDeepSrcMatch AS rom,
       RefObject AS sro
WHERE  (s.coadd_id = sce.deepCoaddId)
   AND (s.id = rom.deepSourceId)
   AND (rom.refObjectId = sro.refObjectId)
   AND (sce.filterName = 'r')
   AND (sce.tract = 0)
   AND (sce.patch = '159,3')
   AND (s.id = 1398582280194457)
ORDER BY s.id;
