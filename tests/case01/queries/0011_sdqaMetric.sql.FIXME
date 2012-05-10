-- interesting syntax

-- See ticket #2049
-- This is a WONTFIX because NAME_CONST is a MySQL-ism.
-- COLLATE will be low-priority if/when we have a test case for it that 
-- doesn't include MySQL-isms.

SELECT sdqa_metricId
FROM   sdqa_Metric
WHERE  metricName = NAME_CONST('metricName_',_latin1'ip.isr.numSaturatedPixels' COLLATE 'latin1_swedish_ci')