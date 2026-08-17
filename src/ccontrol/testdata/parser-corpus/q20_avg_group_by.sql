SELECT visitId,
       AVG(seeing) AS mean_seeing,
       AVG(magLim) AS mean_maglim
FROM dp2.VisitDetector
GROUP BY visitId
LIMIT 100000001
