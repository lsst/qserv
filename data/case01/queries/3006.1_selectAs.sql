-- Test support for multiple ORDER BY fields

SELECT ra_PS AS ra,
       decl_PS AS decl
FROM Object
WHERE ra_PS BETWEEN 0. AND 1.
AND decl_PS BETWEEN 0. AND 1.
ORDER BY ra, decl;
