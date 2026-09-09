-- This bitwise aggregation *should* produce empty chunks. The '+ 0' below is to
-- prevent objectId from becoming a secondary index restrictor.

SELECT BIT_OR(flags), BIT_AND(flags), BIT_XOR(flags)
FROM   Object
WHERE  objectId + 0 = 430213989148129;
