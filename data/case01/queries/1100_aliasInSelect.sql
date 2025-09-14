-- tests that a query with an alias in the select list parses, runs, and returns correct results.

-- pragma sortresult

SELECT objectId - 1 AS objLessOne FROM Object;

