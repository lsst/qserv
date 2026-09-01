-- Check identity values when every worker aggregate has empty input

SELECT BIT_OR(flags), BIT_AND(flags), BIT_XOR(flags)
FROM   Object
WHERE  objectId + 0 = -1;
