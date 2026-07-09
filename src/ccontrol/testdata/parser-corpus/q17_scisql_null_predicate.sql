SELECT coord_ra,
       coord_dec,
       objectId,
       r_extendedness,
       scisql_nanojanskyToAbMag(g_cModelFlux) AS mag_g_cModel,
       scisql_nanojanskyToAbMag(r_cModelFlux) AS mag_r_cModel,
       scisql_nanojanskyToAbMag(i_cModelFlux) AS mag_i_cModel
FROM dp02_dc2_catalogs.Object
WHERE scisql_s2PtInCircle(coord_ra, coord_dec, 55.65, -40.0, 1.0) = 1
  AND detect_isPrimary = 1
  AND scisql_nanojanskyToAbMag(r_cModelFlux) < 27.0
  AND r_extendedness IS NOT NULL
LIMIT 100000001
