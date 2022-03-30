-- Used to reproduce DM-1822: Fix czar assertion failure

-- pragma sortresult
SELECT *
FROM Science_Ccd_Exposure_Metadata
WHERE scienceCcdExposureId=7202320671 AND stringValue=''
