SELECT 'corpus', 'neighbor-near',
       o1.objectId AS id1,
       o2.objectId AS id2,
       scisql_angSep(o1.coord_ra, o1.coord_dec, o2.coord_ra, o2.coord_dec) AS d
FROM dp02_dc2_catalogs.Object AS o1, dp02_dc2_catalogs.Object AS o2
WHERE scisql_s2PtInCircle(o1.coord_ra, o1.coord_dec, 71.2558510402293, -40.92915006704257, 0.0046867851076136694) = 1
  AND scisql_angSep(o1.coord_ra, o1.coord_dec, o2.coord_ra, o2.coord_dec) < 0.005
  AND o1.objectId <> o2.objectId
LIMIT 100000001
