-- MySQL dump 10.13  Distrib 5.1.61, for unknown-linux-gnu (x86_64)
--
-- Host: localhost    Database: w2013
-- ------------------------------------------------------
-- Server version	5.1.61

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `AvgForcedPhotYearly`
--

DROP TABLE IF EXISTS `AvgForcedPhotYearly`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `AvgForcedPhotYearly` (
  `deepSourceId` bigint(20) NOT NULL,
  `year` tinyint(4) NOT NULL,
  `ra` double NOT NULL,
  `decl` double NOT NULL,
  `htmId20` bigint(20) NOT NULL,
  `magFaint_g` float DEFAULT NULL,
  `q1Mag_g` float DEFAULT NULL,
  `medMag_g` float DEFAULT NULL,
  `q3Mag_g` float DEFAULT NULL,
  `magBright_g` float DEFAULT NULL,
  `nMag_g` float DEFAULT NULL,
  `magFaint_r` float DEFAULT NULL,
  `q1Mag_r` float DEFAULT NULL,
  `medMag_r` float DEFAULT NULL,
  `q3Mag_r` float DEFAULT NULL,
  `magBright_r` float DEFAULT NULL,
  `nMag_r` float DEFAULT NULL,
  `magFaint_i` float DEFAULT NULL,
  `q1Mag_i` float DEFAULT NULL,
  `medMag_i` float DEFAULT NULL,
  `q3Mag_i` float DEFAULT NULL,
  `magBright_i` float DEFAULT NULL,
  `nMag_i` float DEFAULT NULL,
  PRIMARY KEY (`deepSourceId`,`year`),
  KEY `IDX_htmId20` (`htmId20`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2013-02-08 15:04:02
