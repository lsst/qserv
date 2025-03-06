-- Select objects withiin a rectangular area on the sky


-- see ticket #2056


-- Create a binary representation of the search polygon
SET @poly = scisql_s2CPolyToBin(300, 2, 0.01, 2, 0.03, 2.6,  359.9, 2.6);

-- Compute HTM ID ranges for the level 20 triangles overlapping
-- @poly. They will be stored in a temp table called scisql.Region
-- with two columns, htmMin and htmMax
CALL scisql.scisql_s2CPolyRegion(@poly, 20);

-- Select reference objects inside the polygon. The join against
-- the HTM ID range table populated above cuts down on the number of
-- SimRefObject rows that need to be tested against the polygon
SELECT refObjectId, isStar, ra, decl, rMag
FROM SimRefObject AS sro INNER JOIN
    scisql.Region AS r ON (sro.htmId20 BETWEEN r.htmMin AND r.htmMax)
WHERE scisql_s2PtInCPoly(ra, decl, @poly) = 1;

