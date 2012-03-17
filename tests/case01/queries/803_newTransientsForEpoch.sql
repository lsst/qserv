-- Find new transients for a given epoch
-- http://dev.lsstcorp.org/trac/wiki/dbQuery005

-- Missing in current schema: Alert table

SELECT objectId 
FROM   Alert 
JOIN   _Alert2Type USING (alertId) 
JOIN   AlertType USING (alertTypeId)
WHERE  alertTypeDescr = 'newTransients'
  AND  Alert.timeGenerated BETWEEN :timeMin AND :timeMax;
