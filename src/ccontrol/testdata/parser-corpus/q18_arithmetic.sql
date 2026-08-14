SELECT mpc.ssObjectId, mpc.mpcDesignation, mpc.epoch, mpc.q,
       mpc.e, mpc.incl, mpc.node, mpc.peri, mpc.mpcH
FROM dp1.MPCORB AS mpc
WHERE mpc.q / (1.0 - mpc.e) > 1.8
  AND mpc.q / (1.0 - mpc.e) < 3.7
  AND mpc.e < 1.0
  AND mpc.q > 1.3
ORDER BY mpc.ssObjectId ASC
LIMIT 100000001
