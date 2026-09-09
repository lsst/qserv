-- Recursive aggregate rewriting with scalar and arithmetic expressions

SELECT ROUND(100.0 * COUNT(flags) / COUNT(*), 1) AS flagsPerc
FROM   Object;
