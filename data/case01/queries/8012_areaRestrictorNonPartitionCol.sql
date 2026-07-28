-- Area restrictor chunk pruning regression test: Object is partitioned on
-- ra_PS/decl_PS, so a scisql area function over ra_SG/decl_SG should NOT be
-- prune chunks. Object 55559 is placed away from the rest of the data with
-- _SG (185, 5) in a different chunk than its _PS.
--
-- See DM-55559.

SELECT objectId, ra_PS, decl_PS, ra_SG, decl_SG
FROM Object
WHERE scisql_s2PtInCircle(ra_SG, decl_SG, 185.0, 5.0, 0.01) = 1
