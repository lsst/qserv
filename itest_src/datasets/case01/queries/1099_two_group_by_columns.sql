-- regression test for DM-17092
-- tests that a query with a GROUP BY clause with 2 columns returns correct results.

-- pragma sortresult

select objectId, filterId from Source group by objectId, filterId;
