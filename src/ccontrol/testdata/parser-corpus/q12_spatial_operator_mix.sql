SELECT
    Object.objectId,
    Object.ra_PS,
    Object.decl_PS,
    Object.ra_PS % 3 AS ra_bucket,
    Object.decl_PS % 5 AS decl_bucket,
    Object.ra_PS + Object.decl_PS AS coord_sum,
    Object.ra_PS - Object.decl_PS AS coord_delta,
    Object.ra_PS * Object.decl_PS AS coord_product
FROM Object
WHERE scisql_s2PtInBox(Object.ra_PS, Object.decl_PS, 1, 3, 2, 4)
  AND Object.ra_PS % 3 > 1.5
  AND Object.decl_PS % 5 < 4.5
  AND Object.objectId IN (1, 3, 5, 7, 9, 11, 13)
  AND (Object.ra_PS < 120 OR Object.ra_PS > 140)
  AND Object.decl_PS >= 0
  AND Object.decl_PS <= 30
ORDER BY Object.ra_PS, Object.decl_PS
LIMIT 500
