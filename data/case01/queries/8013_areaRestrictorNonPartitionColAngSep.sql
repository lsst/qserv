-- Area restrictor chunk pruning regression test: same as 8012, but using scisql_angSep()
-- instead of scisql_s2PtInCircle().

SELECT objectId, ra_PS, decl_PS, ra_SG, decl_SG
FROM Object
WHERE scisql_angSep(ra_SG, decl_SG, 185.0, 5.0) < 0.01
