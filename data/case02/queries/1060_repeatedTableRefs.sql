-- QMetaMysql regression test to ensure duplicate database/table pairs are not
-- registered more than once. See DM-57134.
-- pragma sortresult
SELECT a.objectId, s.sourceId, b.objectId
FROM Object a JOIN Source s ON a.objectId=s.objectId
JOIN Object b ON s.objectId=b.objectId;
