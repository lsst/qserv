-- Area restrictor chunk pruning regression test: ra_PS + 5.0 should not be used
-- for chunk pruning here; if it is, the matching row gets dropped. This catches
-- a bug where chunk pruning was always applied to a table's partition columns
-- (see DM-55559).

SELECT objectId, ra_PS, decl_PS
FROM Object
WHERE scisql_s2PtInCircle(
          ra_PS + 5.0,
          decl_PS,
          6.84513599224457,
          -5.01378174121599,
          0.05
      ) = 1
