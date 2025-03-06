-- pragma sortresult

SELECT sce.filterId, sce.filterName, sce.ccdName 
FROM   Science_Ccd_Exposure AS sce 
WHERE  (sce.visit = 887404831)    
   AND (sce.raftName = '3,3')    
   AND (sce.ccdName NOT LIKE '0,2');

