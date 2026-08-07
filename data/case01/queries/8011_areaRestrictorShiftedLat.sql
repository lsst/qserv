-- Similar to 8010 but with decl_PS + 5.0

SELECT objectId, ra_PS, decl_PS
FROM Object
WHERE scisql_s2PtInCircle(
          ra_PS,
          decl_PS + 5.0,
          1.84513599224457,
          -0.01378174121599,
          0.05
      ) = 1
