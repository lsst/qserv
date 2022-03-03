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
-- Table structure for table `RunDeepSource`
--

DROP TABLE IF EXISTS `RunDeepSource`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `RunDeepSource` (
  `id` bigint(20) NOT NULL,
  `coord_ra` double DEFAULT NULL,
  `coord_decl` double DEFAULT NULL,
  `coord_htmId20` bigint(20) DEFAULT NULL,
  `parent` bigint(20) DEFAULT NULL,
  `calib_detected` bit(1) NOT NULL,
  `calib_psf_candidate` bit(1) NOT NULL,
  `calib_psf_used` bit(1) NOT NULL,
  `flags_negative` bit(1) NOT NULL,
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
  `centroid_gaussian_x` double DEFAULT NULL,
  `centroid_gaussian_y` double DEFAULT NULL,
  `centroid_gaussian_xVar` double DEFAULT NULL,
  `centroid_gaussian_xyCov` double DEFAULT NULL,
  `centroid_gaussian_yVar` double DEFAULT NULL,
  `centroid_gaussian_flags` bit(1) NOT NULL,
  `centroid_naive_x` double DEFAULT NULL,
  `centroid_naive_y` double DEFAULT NULL,
  `centroid_naive_xVar` double DEFAULT NULL,
  `centroid_naive_xyCov` double DEFAULT NULL,
  `centroid_naive_yVar` double DEFAULT NULL,
  `centroid_naive_flags` bit(1) NOT NULL,
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
  `multishapelet_psf_inner_1` float DEFAULT NULL,
  `multishapelet_psf_outer_1` float DEFAULT NULL,
  `multishapelet_psf_ellipse_Ixx` float DEFAULT NULL,
  `multishapelet_psf_ellipse_Iyy` float DEFAULT NULL,
  `multishapelet_psf_ellipse_Ixy` float DEFAULT NULL,
  `multishapelet_psf_chisq` float DEFAULT NULL,
  `multishapelet_psf_integral` float DEFAULT NULL,
  `multishapelet_psf_flags` bit(1) NOT NULL,
  `multishapelet_psf_flags_maxiter` bit(1) NOT NULL,
  `multishapelet_psf_flags_tinystep` bit(1) NOT NULL,
  `multishapelet_psf_flags_constraint_r` bit(1) NOT NULL,
  `multishapelet_psf_flags_constraint_q` bit(1) NOT NULL,
  `multishapelet_dev_flux` double DEFAULT NULL,
  `multishapelet_dev_flux_err` double DEFAULT NULL,
  `multishapelet_dev_flux_flags` bit(1) NOT NULL,
  `multishapelet_dev_psffactor` float DEFAULT NULL,
  `multishapelet_dev_flags_psffactor` bit(1) NOT NULL,
  `multishapelet_dev_flags_badcorr` bit(1) NOT NULL,
  `multishapelet_dev_ellipse_Ixx` double DEFAULT NULL,
  `multishapelet_dev_ellipse_Iyy` double DEFAULT NULL,
  `multishapelet_dev_ellipse_Ixy` double DEFAULT NULL,
  `multishapelet_dev_psffactor_ellipse_Ixx` double DEFAULT NULL,
  `multishapelet_dev_psffactor_ellipse_Iyy` double DEFAULT NULL,
  `multishapelet_dev_psffactor_ellipse_Ixy` double DEFAULT NULL,
  `multishapelet_dev_chisq` float DEFAULT NULL,
  `multishapelet_dev_flags_maxiter` bit(1) NOT NULL,
  `multishapelet_dev_flags_tinystep` bit(1) NOT NULL,
  `multishapelet_dev_flags_constraint_r` bit(1) NOT NULL,
  `multishapelet_dev_flags_constraint_q` bit(1) NOT NULL,
  `multishapelet_dev_flags_largearea` bit(1) NOT NULL,
  `multishapelet_exp_flux` double DEFAULT NULL,
  `multishapelet_exp_flux_err` double DEFAULT NULL,
  `multishapelet_exp_flux_flags` bit(1) NOT NULL,
  `multishapelet_exp_psffactor` float DEFAULT NULL,
  `multishapelet_exp_flags_psffactor` bit(1) NOT NULL,
  `multishapelet_exp_flags_badcorr` bit(1) NOT NULL,
  `multishapelet_exp_ellipse_Ixx` double DEFAULT NULL,
  `multishapelet_exp_ellipse_Iyy` double DEFAULT NULL,
  `multishapelet_exp_ellipse_Ixy` double DEFAULT NULL,
  `multishapelet_exp_psffactor_ellipse_Ixx` double DEFAULT NULL,
  `multishapelet_exp_psffactor_ellipse_Iyy` double DEFAULT NULL,
  `multishapelet_exp_psffactor_ellipse_Ixy` double DEFAULT NULL,
  `multishapelet_exp_chisq` float DEFAULT NULL,
  `multishapelet_exp_flags_maxiter` bit(1) NOT NULL,
  `multishapelet_exp_flags_tinystep` bit(1) NOT NULL,
  `multishapelet_exp_flags_constraint_r` bit(1) NOT NULL,
  `multishapelet_exp_flags_constraint_q` bit(1) NOT NULL,
  `multishapelet_exp_flags_largearea` bit(1) NOT NULL,
  `multishapelet_combo_flux` double DEFAULT NULL,
  `multishapelet_combo_flux_err` double DEFAULT NULL,
  `multishapelet_combo_flux_flags` bit(1) NOT NULL,
  `multishapelet_combo_psffactor` float DEFAULT NULL,
  `multishapelet_combo_flags_psffactor` bit(1) NOT NULL,
  `multishapelet_combo_flags_badcorr` bit(1) NOT NULL,
  `multishapelet_combo_components_1` float DEFAULT NULL,
  `multishapelet_combo_components_2` float DEFAULT NULL,
  `multishapelet_combo_chisq` float DEFAULT NULL,
  `classification_extendedness` double DEFAULT NULL,
  `aperturecorrection` double DEFAULT NULL,
  `aperturecorrection_err` double DEFAULT NULL,
  `coord_raVar` double DEFAULT NULL,
  `coord_radeclCov` double DEFAULT NULL,
  `coord_declVar` double DEFAULT NULL,
  `coadd_id` bigint(20) NOT NULL,
  `coadd_filter_id` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `IDX_coord_htmId20` (`coord_htmId20`),
  KEY `IDX_coord_decl` (`coord_decl`),
  KEY `IDX_parent` (`parent`),
  KEY `IDX_coadd_id` (`coadd_id`),
  KEY `IDX_coadd_filter_id` (`coadd_filter_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2013-02-08 15:04:22
