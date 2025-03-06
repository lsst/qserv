CREATE TABLE `DeepCoadd_Metadata` (
  `deepCoaddId` bigint(20) NOT NULL,
  `metadataKey` varchar(255) NOT NULL,
  `exposureType` tinyint(4) NOT NULL,
  `intValue` int(11) DEFAULT NULL,
  `doubleValue` double DEFAULT NULL,
  `stringValue` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`deepCoaddId`,`metadataKey`),
  KEY `IDX_metadataKey` (`metadataKey`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;
