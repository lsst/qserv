
SET @poly = scisql_s2CPolyToBin(359.5, -5.0,
                                0.05, -5.0,
                                0.05, 3.5,
                                359.5, 3.5);

SELECT count(*)
FROM Object
where scisql_s2PtInCPoly(ra_PS, decl_PS, @poly) = 1 ;
