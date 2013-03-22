SELECT count(*) AS n,
        AVG(ra_PS),
        AVG(decl_PS),
        objectId,
        _chunkId
 FROM Object 
 GROUP BY _chunkId
