-- This bitwise aggregation has no matching rows in some chunks, with the
-- intent of exercising identity values that are produced when no matches are
-- found. The identity value ensures that the outputs can still be safely
-- merged without impacting the final result.
--
-- The '+ 0' in the WHERE clause below prevents secondary index
-- restrictor optimizations from skipping the chunks entirely, forcing the
-- query to be resolved on those chunks.
--
-- Related: see 2115 for a case where NO chunks have matching rows.

SELECT BIT_OR(flags), BIT_AND(flags), BIT_XOR(flags)
FROM   Object
WHERE  objectId + 0 = 430213989148129;
