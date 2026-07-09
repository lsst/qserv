SELECT fsodo.coord_ra, fsodo.coord_dec, fsodo.diaObjectId,
       fsodo.visit, fsodo.band, fsodo.psfDiffFlux,
       fsodo.psfDiffFluxErr, vis.expMidptMJD
FROM dp1.ForcedSourceOnDiaObject AS fsodo
JOIN dp1.Visit AS vis ON vis.visit = fsodo.visit
WHERE diaObjectId IN (
    611255141361779352,
    611255759837069401,
    611256447031836758,
    609788805167185927
)
LIMIT 100000001
