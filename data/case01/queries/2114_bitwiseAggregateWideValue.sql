-- Bitwise aggregates over a full 64-bit value with bit 63 set (sign bit) to
-- test round-tripping.
-- NOTE: the aliases prevent an output formatting mismatch with reference mysql

SELECT BIT_AND(flags | 9223372036854775808) AS wideAnd,
       BIT_OR(flags | 9223372036854775808) AS wideOr
FROM   Object;
