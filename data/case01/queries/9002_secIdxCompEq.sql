-- Test to verify that a query returns correct results with a secondary index equality comparison. 
-- pragma sortresult

SELECT objectId
FROM Object
WHERE objectId = 405483567466455; 
