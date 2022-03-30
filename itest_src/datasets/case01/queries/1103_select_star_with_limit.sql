-- Tests that the having clause is handled properly
-- pragma sortresult

SELECT SimRefObject.*, refObjectId as o FROM SimRefObject ORDER BY o LIMIT 5;
