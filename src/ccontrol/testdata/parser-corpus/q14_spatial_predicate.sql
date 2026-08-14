SELECT dataproduct_type, dataproduct_subtype, calib_level, lsst_band,
       em_min, em_max, lsst_tract, lsst_patch, lsst_filter,
       lsst_visit, lsst_detector, t_exptime, t_min, t_max,
       s_ra, s_dec, s_fov, obs_id, obs_collection,
       facility_name, instrument_name, obs_title, s_region,
       access_url, access_format
FROM ivoa.ObsCore
WHERE MBRWITHIN(POINT(53.076, -28.11), s_region_bounds)
  AND scisql_s2PtInCPoly(53.076, -28.11, s_region_scisql)
  AND 1 = 1
LIMIT 100000001
