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
-- Table structure for table `RunDeepForcedSource`
--

DROP TABLE IF EXISTS `RunDeepForcedSource`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `RunDeepForcedSource` (
  `id` bigint(20) NOT NULL,
  `coord_ra` double DEFAULT NULL,
  `coord_decl` double DEFAULT NULL,
  `coord_htmId20` bigint(20) DEFAULT NULL,
  `parent` bigint(20) DEFAULT NULL,
  `flags_badcentroid` bit(1) NOT NULL,
  `centroid_sdss_x` double DEFAULT NULL,
  `centroid_sdss_y` double DEFAULT NULL,
  `centroid_sdss_xVar` double DEFAULT NULL,
  `centroid_sdss_xyCov` double DEFAULT NULL,
  `centroid_sdss_yVar` double DEFAULT NULL,
  `centroid_sdss_flags` bit(1) NOT NULL,
  `flags_pixel_edge` bit(1) NOT NULL,
  `flags_pixel_interpolated_any` bit(1) NOT NULL,
  `flags_pixel_interpolated_center` bit(1) NOT NULL,
  `flags_pixel_saturated_any` bit(1) NOT NULL,
  `flags_pixel_saturated_center` bit(1) NOT NULL,
  `flags_pixel_cr_any` bit(1) NOT NULL,
  `flags_pixel_cr_center` bit(1) NOT NULL,
  `centroid_naive_x` double DEFAULT NULL,
  `centroid_naive_y` double DEFAULT NULL,
  `centroid_naive_xVar` double DEFAULT NULL,
  `centroid_naive_xyCov` double DEFAULT NULL,
  `centroid_naive_yVar` double DEFAULT NULL,
  `centroid_naive_flags` bit(1) NOT NULL,
  `centroid_gaussian_x` double DEFAULT NULL,
  `centroid_gaussian_y` double DEFAULT NULL,
  `centroid_gaussian_xVar` double DEFAULT NULL,
  `centroid_gaussian_xyCov` double DEFAULT NULL,
  `centroid_gaussian_yVar` double DEFAULT NULL,
  `centroid_gaussian_flags` bit(1) NOT NULL,
  `shape_sdss_Ixx` double DEFAULT NULL,
  `shape_sdss_Iyy` double DEFAULT NULL,
  `shape_sdss_Ixy` double DEFAULT NULL,
  `shape_sdss_IxxVar` double DEFAULT NULL,
  `shape_sdss_IxxIyyCov` double DEFAULT NULL,
  `shape_sdss_IxxIxyCov` double DEFAULT NULL,
  `shape_sdss_IyyVar` double DEFAULT NULL,
  `shape_sdss_IyyIxyCov` double DEFAULT NULL,
  `shape_sdss_IxyVar` double DEFAULT NULL,
  `shape_sdss_flags` bit(1) NOT NULL,
  `shape_sdss_centroid_x` double DEFAULT NULL,
  `shape_sdss_centroid_y` double DEFAULT NULL,
  `shape_sdss_centroid_xVar` double DEFAULT NULL,
  `shape_sdss_centroid_xyCov` double DEFAULT NULL,
  `shape_sdss_centroid_yVar` double DEFAULT NULL,
  `shape_sdss_centroid_flags` bit(1) NOT NULL,
  `shape_sdss_flags_unweightedbad` bit(1) NOT NULL,
  `shape_sdss_flags_unweighted` bit(1) NOT NULL,
  `shape_sdss_flags_shift` bit(1) NOT NULL,
  `shape_sdss_flags_maxiter` bit(1) NOT NULL,
  `flux_psf` double DEFAULT NULL,
  `flux_psf_err` double DEFAULT NULL,
  `flux_psf_flags` bit(1) NOT NULL,
  `flux_psf_psffactor` float DEFAULT NULL,
  `flux_psf_flags_psffactor` bit(1) NOT NULL,
  `flux_psf_flags_badcorr` bit(1) NOT NULL,
  `flux_naive` double DEFAULT NULL,
  `flux_naive_err` double DEFAULT NULL,
  `flux_naive_flags` bit(1) NOT NULL,
  `flux_gaussian` double DEFAULT NULL,
  `flux_gaussian_err` double DEFAULT NULL,
  `flux_gaussian_flags` bit(1) NOT NULL,
  `flux_gaussian_psffactor` float DEFAULT NULL,
  `flux_gaussian_flags_psffactor` bit(1) NOT NULL,
  `flux_gaussian_flags_badcorr` bit(1) NOT NULL,
  `flux_sinc` double DEFAULT NULL,
  `flux_sinc_err` double DEFAULT NULL,
  `flux_sinc_flags` bit(1) NOT NULL,
  `centroid_record_x` double DEFAULT NULL,
  `centroid_record_y` double DEFAULT NULL,
  `classification_extendedness` double DEFAULT NULL,
  `aperturecorrection` double DEFAULT NULL,
  `aperturecorrection_err` double DEFAULT NULL,
  `refFlux` double DEFAULT NULL,
  `refFlux_err` double DEFAULT NULL,
  `objectId` bigint(20) NOT NULL,
  `coord_raVar` double DEFAULT NULL,
  `coord_radeclCov` double DEFAULT NULL,
  `coord_declVar` double DEFAULT NULL,
  `exposure_id` bigint(20) NOT NULL,
  `exposure_filter_id` int(11) NOT NULL,
  `exposure_time` float DEFAULT NULL,
  `exposure_time_mid` double DEFAULT NULL,
  `cluster_id` bigint(20) DEFAULT NULL,
  `cluster_coord_ra` double DEFAULT NULL,
  `cluster_coord_decl` double DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `IDX_coord_htmId20` (`coord_htmId20`),
  KEY `IDX_coord_decl` (`coord_decl`),
  KEY `IDX_parent` (`parent`),
  KEY `IDX_exposure_id` (`exposure_id`),
  KEY `IDX_exposure_filter_id` (`exposure_filter_id`),
  KEY `objectId` (`objectId`),
  KEY `coord_ra` (`coord_ra`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2013-02-08 15:04:03
