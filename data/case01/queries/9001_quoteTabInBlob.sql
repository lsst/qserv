-- Test to be sure that a blob with a backslash
-- in it is loaded the same in qserv as baseline
-- test databases

SELECT blobField 
FROM Source 
WHERE sourceId=29759322768015696; 
