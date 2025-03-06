-- Tests that the having clause is handled properly
-- pragma sortresult

SELECT SimRefObject.refObjectId,
       SimRefObject.isStar,
       SimRefObject.varClass,
       SimRefObject.ra,
       SimRefObject.decl,
       SimRefObject.htmId20,
       SimRefObject.gLat,
       SimRefObject.gLon,
       SimRefObject.sedName,
       SimRefObject.uMag,
       SimRefObject.gMag,
       SimRefObject.rMag,
       SimRefObject.iMag,
       SimRefObject.zMag,
       SimRefObject.yMag,
       SimRefObject.muRa,
       SimRefObject.muDecl,
       SimRefObject.parallax,
       SimRefObject.vRad,
       SimRefObject.redshift,
       SimRefObject.semiMajorBulge,
       SimRefObject.semiMinorBulge,
       SimRefObject.semiMajorDisk,
       SimRefObject.semiMinorDisk,
       SimRefObject.uCov,
       SimRefObject.gCov,
       SimRefObject.rCov,
       SimRefObject.iCov,
       SimRefObject.zCov,
       SimRefObject.yCov,
       refObjectId as o
    FROM SimRefObject
    ORDER BY o
    LIMIT 5;
