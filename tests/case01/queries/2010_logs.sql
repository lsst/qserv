-- interesting syntax

SELECT CASE gid WHEN 1 THEN 'pipeline shutdowns seen'
                WHEN 2 THEN 'CCDs attempted'
                WHEN 3 THEN 'src writes'
                WHEN 4 THEN 'calexp writes'
       END AS descr, 
       COUNT(*) 
FROM ( SELECT CASE WHEN COMMENT LIKE 'Processing job:% visit=0 %' THEN 1
                   WHEN COMMENT LIKE 'Processing job:%' AND COMMENT NOT LIKE '% visit=0 %' THEN 2
                   WHEN COMMENT LIKE 'Ending write to BoostStorage%/src%' THEN 3
                   WHEN COMMENT LIKE 'Ending write to FitsStorage%/calexp%' THEN 4
                   ELSE 0
              END AS gid
       FROM Logs ) AS stats
WHERE gid > 0
GROUP BY gid
