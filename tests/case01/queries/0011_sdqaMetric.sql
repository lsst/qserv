-- interesting syntax

SELECT sdqa_metricId
FROM   sdqa_Metric
WHERE  metricName = NAME_CONST('metricName_',_latin1'ip.isr.numSaturatedPixels' COLLATE 'latin1_swedish_ci')