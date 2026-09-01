-- Bitwise aggregates under GROUP BY, where each group may span several chunks and the czar
-- has to combine partials per group rather than globally.

-- pragma sortresult

SELECT uFlags, BIT_OR(flags), BIT_AND(flags), BIT_XOR(flags)
FROM   Object
GROUP BY uFlags;
