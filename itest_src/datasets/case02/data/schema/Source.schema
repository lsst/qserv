-- MySQL dump 10.13  Distrib 5.1.51, for unknown-linux-gnu (x86_64)
--
-- Host: localhost    Database: rplante_DC3b_u_pt11final
-- ------------------------------------------------------
-- Server version	5.1.51

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `Source`
--

DROP TABLE IF EXISTS `Source`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `Source` (
  `sourceId` bigint(20) NOT NULL,
  `scienceCcdExposureId` bigint(20) DEFAULT NULL,
  `filterId` tinyint(4) NOT NULL,
  `objectId` bigint(20) DEFAULT NULL,
  `movingObjectId` bigint(20) DEFAULT NULL,
  `procHistoryId` int(11) NOT NULL,
  `ra` double NOT NULL,
  `raErrForDetection` float DEFAULT NULL,
  `raErrForWcs` float NOT NULL,
  `decl` double NOT NULL,
  `declErrForDetection` float DEFAULT NULL,
  `declErrForWcs` float NOT NULL,
  `xFlux` double DEFAULT NULL,
  `xFluxErr` float DEFAULT NULL,
  `yFlux` double DEFAULT NULL,
  `yFluxErr` float DEFAULT NULL,
  `raFlux` double DEFAULT NULL,
  `raFluxErr` float DEFAULT NULL,
  `declFlux` double DEFAULT NULL,
  `declFluxErr` float DEFAULT NULL,
  `xPeak` double DEFAULT NULL,
  `yPeak` double DEFAULT NULL,
  `raPeak` double DEFAULT NULL,
  `declPeak` double DEFAULT NULL,
  `xAstrom` double DEFAULT NULL,
  `xAstromErr` float DEFAULT NULL,
  `yAstrom` double DEFAULT NULL,
  `yAstromErr` float DEFAULT NULL,
  `raAstrom` double DEFAULT NULL,
  `raAstromErr` float DEFAULT NULL,
  `declAstrom` double DEFAULT NULL,
  `declAstromErr` float DEFAULT NULL,
  `raObject` double DEFAULT NULL,
  `declObject` double DEFAULT NULL,
  `taiMidPoint` double NOT NULL,
  `taiRange` float DEFAULT NULL,
  `psfFlux` double NOT NULL,
  `psfFluxErr` float NOT NULL,
  `apFlux` double NOT NULL,
  `apFluxErr` float NOT NULL,
  `modelFlux` double NOT NULL,
  `modelFluxErr` float NOT NULL,
  `petroFlux` double DEFAULT NULL,
  `petroFluxErr` float DEFAULT NULL,
  `instFlux` double NOT NULL,
  `instFluxErr` float NOT NULL,
  `nonGrayCorrFlux` double DEFAULT NULL,
  `nonGrayCorrFluxErr` float DEFAULT NULL,
  `atmCorrFlux` double DEFAULT NULL,
  `atmCorrFluxErr` float DEFAULT NULL,
  `apDia` float DEFAULT NULL,
  `Ixx` float DEFAULT NULL,
  `IxxErr` float DEFAULT NULL,
  `Iyy` float DEFAULT NULL,
  `IyyErr` float DEFAULT NULL,
  `Ixy` float DEFAULT NULL,
  `IxyErr` float DEFAULT NULL,
  `snr` float NOT NULL,
  `chi2` float NOT NULL,
  `sky` float DEFAULT NULL,
  `skyErr` float DEFAULT NULL,
  `extendedness` float DEFAULT NULL,
  `flux_PS` float DEFAULT NULL,
  `flux_PS_Sigma` float DEFAULT NULL,
  `flux_SG` float DEFAULT NULL,
  `flux_SG_Sigma` float DEFAULT NULL,
  `sersicN_SG` float DEFAULT NULL,
  `sersicN_SG_Sigma` float DEFAULT NULL,
  `e1_SG` float DEFAULT NULL,
  `e1_SG_Sigma` float DEFAULT NULL,
  `e2_SG` float DEFAULT NULL,
  `e2_SG_Sigma` float DEFAULT NULL,
  `radius_SG` float DEFAULT NULL,
  `radius_SG_Sigma` float DEFAULT NULL,
  `flux_flux_SG_Cov` float DEFAULT NULL,
  `flux_e1_SG_Cov` float DEFAULT NULL,
  `flux_e2_SG_Cov` float DEFAULT NULL,
  `flux_radius_SG_Cov` float DEFAULT NULL,
  `flux_sersicN_SG_Cov` float DEFAULT NULL,
  `e1_e1_SG_Cov` float DEFAULT NULL,
  `e1_e2_SG_Cov` float DEFAULT NULL,
  `e1_radius_SG_Cov` float DEFAULT NULL,
  `e1_sersicN_SG_Cov` float DEFAULT NULL,
  `e2_e2_SG_Cov` float DEFAULT NULL,
  `e2_radius_SG_Cov` float DEFAULT NULL,
  `e2_sersicN_SG_Cov` float DEFAULT NULL,
  `radius_radius_SG_Cov` float DEFAULT NULL,
  `radius_sersicN_SG_Cov` float DEFAULT NULL,
  `sersicN_sersicN_SG_Cov` float DEFAULT NULL,
  `flagForAssociation` smallint(6) DEFAULT NULL,
  `flagForDetection` smallint(6) DEFAULT NULL,
  `flagForWcs` smallint(6) DEFAULT NULL,
  PRIMARY KEY (`sourceId`),
  KEY `IDX_scienceCcdExposureId` (`scienceCcdExposureId`),
  KEY `IDX_filterId` (`filterId`),
  KEY `IDX_movingObjectId` (`movingObjectId`),
  KEY `IDX_objectId` (`objectId`),
  KEY `IDX_procHistoryId` (`procHistoryId`),
  KEY `IDX_Source_decl` (`decl`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2011-03-07 16:04:11
