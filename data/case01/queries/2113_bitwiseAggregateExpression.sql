-- A bitwise aggregate over an expression rather than a bare column.

SELECT BIT_OR(flags & 255) AS flagsMasked, BIT_OR(uFlags | gFlags) AS uOrGFlags FROM Object;
