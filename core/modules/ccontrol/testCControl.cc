// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015-2016 AURA/LSST.
 *
 * This product includes software developed by the
 * LSST Project (http://www.lsst.org/).
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the LSST License Statement and
 * the GNU General Public License along with this program.  If not,
 * see <http://www.lsstcorp.org/LegalNotices/>.
 */

// System headers
#include <array>
#include <string>
#include <unistd.h>

// Boost unit test header
#define BOOST_TEST_MODULE CControl_1
#include "boost/test/included/unit_test.hpp"

#include <boost/test/included/unit_test.hpp>
#include <boost/test/data/test_case.hpp>

// Qserv headers
#include "ccontrol/A4UserQueryFactory.h"
#include "ccontrol/UserQueryType.h"
#include "ccontrol/UserQueryFactory.h"
#include "query/SelectStmt.h"

namespace test = boost::test_tools;
using namespace lsst::qserv;

BOOST_AUTO_TEST_SUITE(Suite)

static const std::vector< std::string > QUERIES = {
    "SELECT objectId,iauId,ra_PS,ra_PS_Sigma,decl_PS,decl_PS_Sigma,radecl_PS_Cov,htmId20,ra_SG,ra_SG_Sigma,decl_SG,decl_SG_Sigma, radecl_SG_Cov,raRange,declRange,muRa_PS,muRa_PS_Sigma,muDecl_PS,muDecl_PS_Sigma,muRaDecl_PS_Cov,parallax_PS, parallax_PS_Sigma,canonicalFilterId,extendedness,varProb,earliestObsTime,latestObsTime,meanObsTime,flags,uNumObs, uExtendedness,uVarProb,uRaOffset_PS,uRaOffset_PS_Sigma,uDeclOffset_PS,uDeclOffset_PS_Sigma,uRaDeclOffset_PS_Cov, uRaOffset_SG,uRaOffset_SG_Sigma,uDeclOffset_SG,uDeclOffset_SG_Sigma,uRaDeclOffset_SG_Cov,uLnL_PS,uLnL_SG,uFlux_PS, uFlux_PS_Sigma,uFlux_ESG,uFlux_ESG_Sigma,uFlux_Gaussian,uFlux_Gaussian_Sigma,uTimescale,uEarliestObsTime,uLatestObsTime, uSersicN_SG,uSersicN_SG_Sigma,uE1_SG,uE1_SG_Sigma,uE2_SG,uE2_SG_Sigma,uRadius_SG,uRadius_SG_Sigma,uFlags,gNumObs, gExtendedness,gVarProb,gRaOffset_PS,gRaOffset_PS_Sigma,gDeclOffset_PS,gDeclOffset_PS_Sigma,gRaDeclOffset_PS_Cov, gRaOffset_SG,gRaOffset_SG_Sigma,gDeclOffset_SG,gDeclOffset_SG_Sigma,gRaDeclOffset_SG_Cov,gLnL_PS,gLnL_SG,gFlux_PS, gFlux_PS_Sigma,gFlux_ESG,gFlux_ESG_Sigma,gFlux_Gaussian,gFlux_Gaussian_Sigma,gTimescale,gEarliestObsTime, gLatestObsTime,gSersicN_SG,gSersicN_SG_Sigma,gE1_SG,gE1_SG_Sigma,gE2_SG,gE2_SG_Sigma,gRadius_SG,gRadius_SG_Sigma, gFlags,rNumObs,rExtendedness,rVarProb,rRaOffset_PS,rRaOffset_PS_Sigma,rDeclOffset_PS,rDeclOffset_PS_Sigma, rRaDeclOffset_PS_Cov,rRaOffset_SG,rRaOffset_SG_Sigma,rDeclOffset_SG,rDeclOffset_SG_Sigma,rRaDeclOffset_SG_Cov,rLnL_PS, rLnL_SG,rFlux_PS,rFlux_PS_Sigma,rFlux_ESG,rFlux_ESG_Sigma,rFlux_Gaussian,rFlux_Gaussian_Sigma,rTimescale, rEarliestObsTime,rLatestObsTime,rSersicN_SG,rSersicN_SG_Sigma,rE1_SG,rE1_SG_Sigma,rE2_SG,rE2_SG_Sigma,rRadius_SG, rRadius_SG_Sigma,rFlags,iNumObs,iExtendedness,iVarProb,iRaOffset_PS,iRaOffset_PS_Sigma,iDeclOffset_PS, iDeclOffset_PS_Sigma,iRaDeclOffset_PS_Cov,iRaOffset_SG,iRaOffset_SG_Sigma,iDeclOffset_SG,iDeclOffset_SG_Sigma, iRaDeclOffset_SG_Cov,iLnL_PS,iLnL_SG,iFlux_PS,iFlux_PS_Sigma,iFlux_ESG,iFlux_ESG_Sigma,iFlux_Gaussian, iFlux_Gaussian_Sigma,iTimescale,iEarliestObsTime,iLatestObsTime,iSersicN_SG,iSersicN_SG_Sigma,iE1_SG,iE1_SG_Sigma, iE2_SG,iE2_SG_Sigma,iRadius_SG,iRadius_SG_Sigma,iFlags,zNumObs,zExtendedness,zVarProb,zRaOffset_PS,zRaOffset_PS_Sigma, zDeclOffset_PS,zDeclOffset_PS_Sigma,zRaDeclOffset_PS_Cov,zRaOffset_SG,zRaOffset_SG_Sigma,zDeclOffset_SG, zDeclOffset_SG_Sigma,zRaDeclOffset_SG_Cov,zLnL_PS,zLnL_SG,zFlux_PS,zFlux_PS_Sigma,zFlux_ESG,zFlux_ESG_Sigma, zFlux_Gaussian,zFlux_Gaussian_Sigma,zTimescale,zEarliestObsTime,zLatestObsTime,zSersicN_SG,zSersicN_SG_Sigma,zE1_SG, zE1_SG_Sigma,zE2_SG,zE2_SG_Sigma,zRadius_SG,zRadius_SG_Sigma,zFlags,yNumObs,yExtendedness,yVarProb,yRaOffset_PS, yRaOffset_PS_Sigma,yDeclOffset_PS,yDeclOffset_PS_Sigma,yRaDeclOffset_PS_Cov,yRaOffset_SG,yRaOffset_SG_Sigma, yDeclOffset_SG,yDeclOffset_SG_Sigma,yRaDeclOffset_SG_Cov,yLnL_PS,yLnL_SG,yFlux_PS,yFlux_PS_Sigma,yFlux_ESG, yFlux_ESG_Sigma,yFlux_Gaussian,yFlux_Gaussian_Sigma,yTimescale,yEarliestObsTime,yLatestObsTime,ySersicN_SG, ySersicN_SG_Sigma,yE1_SG,yE1_SG_Sigma,yE2_SG,yE2_SG_Sigma,yRadius_SG,yRadius_SG_Sigma,yFlags FROM   Object WHERE  objectId = 430213989148129", // case01/queries/0001.1_fetchObjectById.sql
    "select ra_Ps, decl_PS FROM Object WHERE objectId IN (390034570102582, 396210733076852, 393126946553816, 390030275138483)", // case01/queries/0001.2_fetchObjectByIdIN.sql
    "SELECT objectId,iauId,ra_PS,ra_PS_Sigma,decl_PS,decl_PS_Sigma,radecl_PS_Cov,htmId20,ra_SG,ra_SG_Sigma,decl_SG,decl_SG_Sigma, radecl_SG_Cov,raRange,declRange,muRa_PS,muRa_PS_Sigma,muDecl_PS,muDecl_PS_Sigma,muRaDecl_PS_Cov,parallax_PS, parallax_PS_Sigma,canonicalFilterId,extendedness,varProb,earliestObsTime,latestObsTime,meanObsTime,flags,uNumObs, uExtendedness,uVarProb,uRaOffset_PS,uRaOffset_PS_Sigma,uDeclOffset_PS,uDeclOffset_PS_Sigma,uRaDeclOffset_PS_Cov, uRaOffset_SG,uRaOffset_SG_Sigma,uDeclOffset_SG,uDeclOffset_SG_Sigma,uRaDeclOffset_SG_Cov,uLnL_PS,uLnL_SG,uFlux_PS, uFlux_PS_Sigma,uFlux_ESG,uFlux_ESG_Sigma,uFlux_Gaussian,uFlux_Gaussian_Sigma,uTimescale,uEarliestObsTime,uLatestObsTime, uSersicN_SG,uSersicN_SG_Sigma,uE1_SG,uE1_SG_Sigma,uE2_SG,uE2_SG_Sigma,uRadius_SG,uRadius_SG_Sigma,uFlags,gNumObs, gExtendedness,gVarProb,gRaOffset_PS,gRaOffset_PS_Sigma,gDeclOffset_PS,gDeclOffset_PS_Sigma,gRaDeclOffset_PS_Cov, gRaOffset_SG,gRaOffset_SG_Sigma,gDeclOffset_SG,gDeclOffset_SG_Sigma,gRaDeclOffset_SG_Cov,gLnL_PS,gLnL_SG,gFlux_PS, gFlux_PS_Sigma,gFlux_ESG,gFlux_ESG_Sigma,gFlux_Gaussian,gFlux_Gaussian_Sigma,gTimescale,gEarliestObsTime, gLatestObsTime,gSersicN_SG,gSersicN_SG_Sigma,gE1_SG,gE1_SG_Sigma,gE2_SG,gE2_SG_Sigma,gRadius_SG,gRadius_SG_Sigma, gFlags,rNumObs,rExtendedness,rVarProb,rRaOffset_PS,rRaOffset_PS_Sigma,rDeclOffset_PS,rDeclOffset_PS_Sigma, rRaDeclOffset_PS_Cov,rRaOffset_SG,rRaOffset_SG_Sigma,rDeclOffset_SG,rDeclOffset_SG_Sigma,rRaDeclOffset_SG_Cov,rLnL_PS, rLnL_SG,rFlux_PS,rFlux_PS_Sigma,rFlux_ESG,rFlux_ESG_Sigma,rFlux_Gaussian,rFlux_Gaussian_Sigma,rTimescale, rEarliestObsTime,rLatestObsTime,rSersicN_SG,rSersicN_SG_Sigma,rE1_SG,rE1_SG_Sigma,rE2_SG,rE2_SG_Sigma,rRadius_SG, rRadius_SG_Sigma,rFlags,iNumObs,iExtendedness,iVarProb,iRaOffset_PS,iRaOffset_PS_Sigma,iDeclOffset_PS, iDeclOffset_PS_Sigma,iRaDeclOffset_PS_Cov,iRaOffset_SG,iRaOffset_SG_Sigma,iDeclOffset_SG,iDeclOffset_SG_Sigma, iRaDeclOffset_SG_Cov,iLnL_PS,iLnL_SG,iFlux_PS,iFlux_PS_Sigma,iFlux_ESG,iFlux_ESG_Sigma,iFlux_Gaussian, iFlux_Gaussian_Sigma,iTimescale,iEarliestObsTime,iLatestObsTime,iSersicN_SG,iSersicN_SG_Sigma,iE1_SG,iE1_SG_Sigma, iE2_SG,iE2_SG_Sigma,iRadius_SG,iRadius_SG_Sigma,iFlags,zNumObs,zExtendedness,zVarProb,zRaOffset_PS,zRaOffset_PS_Sigma, zDeclOffset_PS,zDeclOffset_PS_Sigma,zRaDeclOffset_PS_Cov,zRaOffset_SG,zRaOffset_SG_Sigma,zDeclOffset_SG, zDeclOffset_SG_Sigma,zRaDeclOffset_SG_Cov,zLnL_PS,zLnL_SG,zFlux_PS,zFlux_PS_Sigma,zFlux_ESG,zFlux_ESG_Sigma, zFlux_Gaussian,zFlux_Gaussian_Sigma,zTimescale,zEarliestObsTime,zLatestObsTime,zSersicN_SG,zSersicN_SG_Sigma,zE1_SG, zE1_SG_Sigma,zE2_SG,zE2_SG_Sigma,zRadius_SG,zRadius_SG_Sigma,zFlags,yNumObs,yExtendedness,yVarProb,yRaOffset_PS, yRaOffset_PS_Sigma,yDeclOffset_PS,yDeclOffset_PS_Sigma,yRaDeclOffset_PS_Cov,yRaOffset_SG,yRaOffset_SG_Sigma, yDeclOffset_SG,yDeclOffset_SG_Sigma,yRaDeclOffset_SG_Cov,yLnL_PS,yLnL_SG,yFlux_PS,yFlux_PS_Sigma,yFlux_ESG, yFlux_ESG_Sigma,yFlux_Gaussian,yFlux_Gaussian_Sigma,yTimescale,yEarliestObsTime,yLatestObsTime,ySersicN_SG, ySersicN_SG_Sigma,yE1_SG,yE1_SG_Sigma,yE2_SG,yE2_SG_Sigma,yRadius_SG,yRadius_SG_Sigma,yFlags, varBinaryField FROM   Object WHERE  objectId = 430213989148129", // case01/queries/0001.3_fetchObjectByIdSelectVARBINARY.sql
    "SELECT * FROM   Object WHERE  objectId = 430213989000", // case01/queries/0002_fetchObjectByIdNoResult.sql
    "SELECT s.ra, s.decl, o.raRange, o.declRange FROM   Object o JOIN   Source s USING (objectId) WHERE  o.objectId = 390034570102582 AND    o.latestObsTime = s.taiMidPoint", // case01/queries/0003_selectMetadataForOneGalaxy.sql
    "SELECT s.ra, s.decl, o.raRange, o.declRange FROM Object o, Source s WHERE o.objectId = 390034570102582 AND o.objectId = s.objectId AND o.latestObsTime = s.taiMidPoint;", // case01/queries/0003_selectMetadataForOneGalaxy_classicJOIN.sql
    "SELECT s.ra, s.decl, o.raRange, o.declRange FROM   Object o JOIN   Source s USING (objectId) WHERE  o.objectId = 390034570102582 AND    o.latestObsTime = s.taiMidPoint", // case01/queries/0003_selectMetadataForOneGalaxy_withUSING.sql
    //"SELECT taiMidPoint, psfFlux, psfFluxSigma, ra, decl FROM   Source JOIN   Filter USING (filterId) WHERE  objectId = 402412665835716 AND filterName = 'r'", // case01/queries/0004_lightCurve.sql.FIXME
    "SELECT offset, mjdRef, drift FROM LeapSeconds where offset = 10", // case01/queries/0005_nonReplicatedTable.sql
    //"SELECT v.objectId, v.ra, v.decl FROM   Object v, Object o WHERE  o.objectId = :objectId AND spDist(v.ra, v.decl, o.ra, o.decl, :dist) AND v.variability > 0.8 AND o.extendedParameter > 0.8", // case01/queries/0006_transientVarObjNearGalaxy.sql.FIXME
    "SELECT sourceId, objectId FROM Source WHERE objectId = 386942193651348 ORDER BY sourceId;", // case01/queries/0007.1_fetchSourceByObjId.sql
    //"SELECT sourceId, objectId, blobField FROM Source WHERE objectId = 386942193651348 ORDER BY sourceId;", // case01/queries/0007.2_fetchSourceByObjIdSelectBLOB.sql.FIXME
    "SELECT sourceId, objectId FROM Source WHERE objectId = 386942193651348 ORDER BY sourceId;", // case01/queries/0007_fetchSourceByObjId.sql
    "SELECT sourceId, objectId FROM Source WHERE objectId IN (1234) ORDER BY sourceId;", // case01/queries/0008.1_fetchSourceByObjIdIN_noRes.sql
    "SELECT sourceId, objectId FROM Source WHERE objectId IN (386942193651348) ORDER BY sourceId;", // case01/queries/0008.2_fetchSourceByObjIdIN_withRes.sql
    "select COUNT(*) AS N FROM Source WHERE objectId IN (386950783579546, 386942193651348)", // case01/queries/0008.3_fetchSourceByObjIdIN.sql
    "select COUNT(*) AS N FROM Source WHERE objectId BETWEEN 386942193651348 AND 386950783579546", // case01/queries/0008.4_fetchSourceByObjIdBETWEEN.sql
    "SELECT sourceId, objectId FROM Source WHERE objectId IN (386942193651348) ORDER BY sourceId;", // case01/queries/0008_fetchSourceByObjIdIN_withRes.sql
    //"SELECT offset, mjdRef, drift FROM LeapSeconds WHERE whenUtc = ( SELECT MAX(whenUtc) FROM LeapSeconds WHERE whenUtc <=  NAME_CONST('nsecs_',39900600000000000000000000) )", // case01/queries/0010_leapSec.sql.FIXME
    //"SELECT sdqa_metricId FROM   sdqa_Metric WHERE  metricName = NAME_CONST('metricName_',_latin1'ip.isr.numSaturatedPixels' COLLATE 'latin1_swedish_ci')", // case01/queries/0011_sdqaMetric.sql.FIXME
    "SELECT sce.filterId, sce.filterName FROM   Science_Ccd_Exposure AS sce WHERE  (sce.visit = 887404831) AND (sce.raftName = '3,3') AND (sce.ccdName LIKE '%') ORDER BY filterId", // case01/queries/0012.1_raftAndCcd.sql
    "SELECT sce.filterId, sce.filterName FROM   Science_Ccd_Exposure AS sce WHERE  (sce.visit = 887404831) AND (sce.raftName = '3,3') AND (sce.ccdName LIKE '%') ORDER BY filterId LIMIT 5", // case01/queries/0012.2_raftAndCcd.sql
//    "SELECT sce.filterId, sce.filterName FROM   Science_Ccd_Exposure AS sce WHERE  (sce.visit = 887404831) AND (sce.raftName = '3,3') AND (sce.ccdName LIKE '%')", // case01/queries/0012_raftAndCcd.sql
//    "SELECT COUNT(*) as OBJ_COUNT FROM   Object WHERE qserv_areaspec_box(0.1, -6, 4, 6) AND scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24 AND scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) BETWEEN 0.1 AND 0.9 AND scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS) BETWEEN 0.1 AND 1.0", // case01/queries/1002_coneMagColor.sql
//    "SELECT COUNT(*) FROM   Object WHERE qserv_areaspec_box(0.1, -6, 4, 6) AND scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24 AND scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) BETWEEN 0.1 AND 0.9 AND scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS) BETWEEN 0.1 AND 1.0", // case01/queries/1002_coneMagColor_noalias.sql
//    "SELECT COUNT(*) as OBJ_COUNT FROM   Object WHERE qserv_areaspec_box(0, -6, 4, -5) AND scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24 AND scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) BETWEEN 0.1 AND 0.2 AND scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS) BETWEEN 0.1 AND 0.2", // case01/queries/1003_coneMagColorEmptyRes.sql
//    "SELECT objectId, AVG(ra_PS) as ra FROM   Object WHERE qserv_areaspec_box(0, 0, 3, 10) GROUP BY objectId ORDER BY ra", // case01/queries/1004.1_varObjects.sql
//    "SELECT objectId FROM   Object WHERE qserv_areaspec_box(0, 0, 3, 10) ORDER BY objectId", // case01/queries/1004_varObjects.sql
//    //"SELECT objectId FROM   Object WHERE qserv_areaspec_box(:raMin, :declMin, :raMax, :declMax) AND    extendedParameter > 0.8", // case01/queries/1005_allGalaxiesInArea.sql.FIXME
//    "SELECT objectId FROM   Source s JOIN   Science_Ccd_Exposure sce USING (scienceCcdExposureId) WHERE  sce.visit IN (885449631,886257441,886472151) ORDER BY objectId LIMIT 10", // case01/queries/1011_objectsForExposure.sql
//    //"SELECT objectId, iE1_SG, ABS(iE1_SG) FROM Object WHERE iE1_SG between -0.1 and 0.1 ORDER BY ABS(iE1_SG);", // case01/queries/1012_orderByClause.sql.FIXME
//    //"SELECT objectId, ROUND(iE1_SG, 3), ROUND(ABS(iE1_SG), 3) FROM Object WHERE iE1_SG between -0.1 and 0.1 ORDER BY ROUND(ABS(iE1_SG), 3);", // case01/queries/1013_orderByClauseRounded.sql.FIXME
//    "SELECT objectId, taiMidPoint, scisql_fluxToAbMag(psfFlux) FROM   Source JOIN   Object USING(objectId) JOIN   Filter USING(filterId) WHERE qserv_areaspec_box(355, 0, 360, 20) AND filterName = 'g' ORDER BY objectId, taiMidPoint ASC", // case01/queries/1030_timeSeries.sql
//    //"SELECT objectId FROM   Alert JOIN   _Alert2Type USING (alertId) JOIN   AlertType USING (alertTypeId) WHERE  alertTypeDescr = 'newTransients' AND  Alert.timeGenerated BETWEEN :timeMin AND :timeMax", // case01/queries/1031_newTransientsForEpoch.sql.FIXME
//    "SELECT o1.objectId AS objId1, o2.objectId AS objId2, scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_PS) AS distance FROM Object o1, Object o2 WHERE qserv_areaspec_box(0, 0, 0.2, 1) AND scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_PS) < 0.016 AND o1.objectId <> o2.objectId", // case01/queries/1051_nn.sql
//    //"SELECT DISTINCT o1.objectId, o2.objectId FROM   Object o1, Object o2 WHERE  scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_PS) < 1 AND  o1.objectId <> o2.objectId AND  ABS( (scisql_fluxToAbMag(o1.gFlux_PS)-scisql_fluxToAbMag(o1.rFlux_PS)) - (scisql_fluxToAbMag(o2.gFlux_PS)-scisql_fluxToAbMag(o2.rFlux_PS)) ) < 1 AND  ABS( (scisql_fluxToAbMag(o1.rFlux_PS)-scisql_fluxToAbMag(o1.iFlux_PS)) - (scisql_fluxToAbMag(o2.rFlux_PS)-scisql_fluxToAbMag(o2.iFlux_PS)) ) < 1 AND  ABS( (scisql_fluxToAbMag(o1.iFlux_PS)-scisql_fluxToAbMag(o1.zFlux_PS)) - (scisql_fluxToAbMag(o2.iFlux_PS)-scisql_fluxToAbMag(o2.zFlux_PS)) ) < 1", // case01/queries/1052_nnSimilarColors.sql.FIXME
//    "SELECT scienceCcdExposureId, hex(poly) as hexPoly FROM Science_Ccd_Exposure;", // case01/queries/1060_selectPoly.sql
//    //"SET @poly = scisql_s2CPolyToBin(300, 2, 0.01, 2, 0.03, 2.6,  359.9, 2.6); CALL scisql.scisql_s2CPolyRegion(@poly, 20); SELECT refObjectId, isStar, ra, decl, rMag FROM SimRefObject AS sro INNER JOIN scisql.Region AS r ON (sro.htmId20 BETWEEN r.htmMin AND r.htmMax) WHERE scisql_s2PtInCPoly(ra, decl, @poly) = 1;", // case01/queries/1070_areaUsingPoly.sql.FIXME
//    //"SELECT sce.visit, sce.raftName, sce.ccdName, sro.gMag, sro.ra, sro.decl, sro.isStar, sro.refObjectId, rom.nSrcMatches, s.sourceId,s.ra,s.decl,s.xAstrom,s.yAstrom,s.psfFlux,s.psfFluxSigma, s.apFlux,s.apFluxSigma,s.flux_ESG,s.flux_ESG_Sigma,s.flux_Gaussian, s.flux_Gaussian_Sigma,s.ixx,s.iyy,s.ixy,s.psfIxx,s.psfIxxSigma, s.psfIyy,s.psfIyySigma,s.psfIxy,s.psfIxySigma,s.resolution_SG, s.e1_SG,s.e1_SG_Sigma,s.e2_SG,s.e2_SG_Sigma,s.shear1_SG,s.shear1_SG_Sigma, s.shear2_SG,s.shear2_SG_Sigma,s.sourceWidth_SG,s.sourceWidth_SG_Sigma, s.flagForDetection FROM Source AS s, Science_Ccd_Exposure AS sce, RefSrcMatch AS rom, SimRefObject AS sro WHERE (s.scienceCcdExposureId = sce.scienceCcdExposureId) AND (s.sourceId = rom.sourceId) AND (rom.refObjectId = sro.refObjectId) AND (sce.visit = 888241840) AND (sce.raftName = '1,0') AND (sce.ccdName like '%')", // case01/queries/1080_refMatch1.sql.FIXME
//    //"SELECT count(*) FROM   Object o INNER JOIN RefObjMatch o2t ON (o.objectId = o2t.objectId) LEFT  JOIN SimRefObject t ON (o2t.refObjectId = t.refObjectId) WHERE  closestToObj = 1 OR closestToObj is NULL", // case01/queries/1081_refMatch2.sql.FIXME
//    //"select objectId, sro.*, (sro.refObjectId-1)/2%pow(2,10) typeId from Source s join RefObjMatch rom using (objectId) join SimRefObject sro using (refObjectId) where isStar =1 limit 10", // case01/queries/1083_refMatch3.sql.FIXME
//    //"SELECT objectId, scisql_fluxToAbMag(uFlux_PS), scisql_fluxToAbMag(gFlux_PS), scisql_fluxToAbMag(rFlux_PS), scisql_fluxToAbMag(iFlux_PS), scisql_fluxToAbMag(zFlux_PS), scisql_fluxToAbMag(yFlux_PS), ra_PS, decl_PS FROM   Object WHERE  ( scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) > 0.7 OR scisql_fluxToAbMag(gFlux_PS) > 22.3 ) AND    scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) > 0.1 AND    ( scisql_fluxToAbMag(rFlux_PS)-scisql_fluxToAbMag(iFlux_PS) < (0.08 + 0.42 * (scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) - 0.96)) OR scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) > 1.26 ) AND    scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS) < 0.8 ORDER BY objectId", // case01/queries/2001_fullObjectScan.sql.FIXME
//    //"SELECT  objectId FROM    Object WHERE   extendedParameter > 0.8 -- a star AND   uMag BETWEEN 1 AND 27  -- magnitudes are reasonable AND   gMag BETWEEN 1 AND 27 AND   rMag BETWEEN 1 AND 27 AND   iMag BETWEEN 1 AND 27 AND   zMag BETWEEN 1 AND 27 AND   yMag BETWEEN 1 AND 27 AND (                           -- and one of the colors is  different. uAmplitude > .1 + ABS(uMagSigma) OR gAmplitude > .1 + ABS(gMagSigma) OR rAmplitude > .1 + ABS(rMagSigma) OR iAmplitude > .1 + ABS(iMagSigma) OR zAmplitude > .1 + ABS(zMagSigma) OR yAmplitude > .1 + ABS(yMagSigma))", // case01/queries/2002_findStarsWithMultiMeasAndMagVariation.sql.FIXME
//    //"SELECT * FROM   Object WHERE  variability > 0.8 -- variable object AND uTimescale < :timescaleMax AND gTimescale < :timescaleMax AND rTimescale < :timescaleMax AND iTimescale < :timescaleMax AND zTimescale < :timescaleMax AND yTimescale < :timescaleMax OR primaryPeriod BETWEEN :periodMin AND :periodMax OR uAmplitude > :amplitudeMin OR gAmplitude > :amplitudeMin OR rAmplitude > :amplitudeMin OR iAmplitude > :amplitudeMin OR zAmplitude > :amplitudeMin OR yAmplitude > :amplitudeMin", // case01/queries/2003_objectWithVariabilityOrPeriodOrMag.sql.FIXME
//    //"SELECT  COUNT(*)                                               AS totalCount, SUM(CASE WHEN (typeId=3) THEN 1 ELSE 0 END)            AS galaxyCount, SUM(CASE WHEN (typeId=6) THEN 1 ELSE 0 END)            AS starCount, SUM(CASE WHEN (typeId NOT IN (3,6)) THEN 1 ELSE 0 END) AS otherCount FROM    Object JOIN    _Object2Type USING(objectId) WHERE  (uMag-gMag > 2.0 OR uMag > 22.3) AND iMag BETWEEN 0 AND 19 AND gMag - rMag > 1.0 AND ( (rMag-iMag < 0.08 + 0.42 * (gMag-rMag - 0.96)) OR (gMag-rMag > 2.26 ) ) AND iMag-zMag < 0.25", // case01/queries/2004_objectsSimilarToQuasarsWithRedshift.sql.FIXME
//    //"SELECT objectId FROM   Object JOIN   _ObjectToType USING(objectId) JOIN   ObjectType USING (typeId) WHERE  description = 'Supernova' AND  variability > 0.8 AND  probability > 0.8", // case01/queries/2005_varObjectsOfOneType.sql.FIXME
//    //"SELECT fluxToAbMag(uFlux_PS), fluxToAbMag(gFlux_PS), fluxToAbMag(rFlux_PS), fluxToAbMag(iFlux_PS), fluxToAbMag(zFlux_PS), fluxToAbMag(yFlux_PS) FROM   Object WHERE  (objectId % 100 ) = :percentage", // case01/queries/2006_randomSample.sql.FIXME
//    //"SELECT CASE gid WHEN 1 THEN 'pipeline shutdowns seen' WHEN 2 THEN 'CCDs attempted' WHEN 3 THEN 'src writes' WHEN 4 THEN 'calexp writes' END AS descr, COUNT(*) FROM ( SELECT CASE WHEN COMMENT LIKE 'Processing job:% visit=0 %' THEN 1 WHEN COMMENT LIKE 'Processing job:%' AND COMMENT NOT LIKE '% visit=0 %' THEN 2 WHEN COMMENT LIKE 'Ending write to BoostStorage%/src%' THEN 3 WHEN COMMENT LIKE 'Ending write to FitsStorage%/calexp%' THEN 4 ELSE 0 END AS gid FROM Logs ) AS stats WHERE gid > 0 GROUP BY gid", // case01/queries/2010_logs.sql.FIXME
//    //"SELECT count(*) AS n, AVG(ra_PS), AVG(decl_PS), chunkId FROM Object GROUP BY chunkId;", // case01/queries/2100_groupByChunkId.sql.FIXME
//    //"SET @poly = scisql_s2CPolyToBin(359.5, -5.0, 0.05, -5.0, 0.05, 3.5, 359.5, 3.5); SELECT count(*) FROM Object where scisql_s2PtInCPoly(ra_PS, decl_PS, @poly) = 1 ;", // case01/queries/3001_SelectInPoly.sql.FIXME
//    //"SELECT o1.ra_PS,o2.ra_PS FROM Object o1, Object o2 WHERE o1.objectid = 402391191015221 AND o2.objectid = 390030275138483 ;", // case01/queries/3002_sameColumnName.sql.FIXME
//    //"SELECT o.ra_PS,o.decl_PS,o.ra_PS FROM Object o WHERE o.objectid = 402391191015221 ;", // case01/queries/3003_SameColumnTwice.sql.FIXME
//    //"SELECT o.foobar FROM Object o WHERE o.objectid = 402391191015221 ;", // case01/queries/3004_nonExistingColumn.sql.FIXME
//    //"SELECT * FROM Object WHERE qserv_areaspec_box(0.,1.,0.,1.) ORDER BY ra_PS", // case01/queries/3005_orderByRA.sql.FIXME
//    "SELECT ra_PS AS ra, decl_PS AS decl FROM Object WHERE ra_PS BETWEEN 0. AND 1. AND decl_PS BETWEEN 0. AND 1. ORDER BY ra, decl;", // case01/queries/3006.1_selectAs.sql
//    "SELECT ra_PS AS ra FROM Object WHERE ra_PS BETWEEN 0. AND 1. AND decl_PS BETWEEN 0. AND 1. ORDER BY ra;", // case01/queries/3006_selectAs.sql
//    //"SELECT count(src.sourceId), avg(o.ra_PS), avg(o.decl_PS) FROM Object o, Source src WHERE ra_PS  BETWEEN 0. AND 1. AND decl_PS BETWEEN 0. AND 1. GROUP BY src.objectId ;", // case01/queries/3007_countGroupBy.sql.FIXME
//    //"SHOW COLUMNS FROM Object;", // case01/queries/3008_showColumns.sql.FIXME
//    //"SELECT src.sourceId FROM Source src WHERE src.objectId IN ( SELECT objectId FROM Object o WHERE ra_PS  BETWEEN 0. AND 1. AND decl_PS BETWEEN 0. AND 1. ) ;", // case01/queries/3009_subquery.sql.FIXME
//    //"SELECT o.objectId,src.*,src.sourceId%pow(2,10) FROM Object o, Source src WHERE o.ra_PS  BETWEEN 0. AND 1. AND o.decl_PS BETWEEN 0. AND 1. AND o.objectId = src.objectId ;", // case01/queries/3010_selectWithComputation.sql.FIXME
//    //"SELECT uFlux_PS, gFlux_PS, rFlux_PS, iFlux_PS, zFlux_PS, yFlux_PS FROM   Object WHERE  (objectId % 100 ) = 57 ;", // case01/queries/3011_selectSample.sql.FIXME
//    //"SELECT o1.objectId, o2.objectId FROM   Object o1, Object o2 WHERE  scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_PS) < 1 AND  o1.objectId <> o2.objectId AND  ABS( (scisql_fluxToAbMag(o1.gFlux_PS)-scisql_fluxToAbMag(o1.rFlux_PS)) - (scisql_fluxToAbMag(o2.gFlux_PS)-scisql_fluxToAbMag(o2.rFlux_PS)) ) < 1 AND  ABS( (scisql_fluxToAbMag(o1.rFlux_PS)-scisql_fluxToAbMag(o1.iFlux_PS)) - (scisql_fluxToAbMag(o2.rFlux_PS)-scisql_fluxToAbMag(o2.iFlux_PS)) ) < 1 AND  ABS( (scisql_fluxToAbMag(o1.iFlux_PS)-scisql_fluxToAbMag(o1.zFlux_PS)) - (scisql_fluxToAbMag(o2.iFlux_PS)-scisql_fluxToAbMag(o2.zFlux_PS)) ) < 1", // case01/queries/3012_similarObject.sql.FIXME
//    //"select count(*) from Sources;", // case01/queries/3013_nonexistantTable.sql.FIXME
//    //"SELECT objectId FROM Object WHERE qserv_areaspec_box(0.1, -6, 4, 6) LIMIT 10", // case01/queries/3014_limitAfterAreaspec.sql.FIXME
//    //"SELECT objectID FROM Object WHERE scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24 ORDER BY (scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS))", // case01/queries/3015_orderByFunction.sql.FIXME
//    //"SELECT o1.objectId, o2.objectId FROM Object o1, Object o2 WHERE   qserv_areaspec_box(0.04, -3., 5., 3.) AND scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_Ps) < 5.", // case01/queries/3016_selectAllPairsWithinDistance.sql.FIXME
//    //"SELECT count(*) FROM Object WHERE qserv_areaSpec_box(35, 6, 35 .1, 6.0001);", // case01/queries/8001_badLiteral.sql.FIXME
//    //"SELECT count(*) FROM Object WHERE qserv_areaSpec_box(35, 6, 35. 1, 6.0001);", // case01/queries/8002_badLiteral.sql.FIXME
//    //"SELECT COUNT(*) FROM   Object WHERE qserv_areaspec_box(355, 0, 356, 1) LIMIT 10", // case01/queries/8003_areaWithLimitClause.sql.FIXME
//    //"SELECT o1.objectId, o2.objectId FROM Object o1, Object o2 WHERE   qserv_areaspec_box(0.04, 5., -3., 3.) AND scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_Ps) < 5.", // case01/queries/8004_badArea.sql.FIXME
//    "SELECT objectId FROM   Object WHERE QsErV_ArEaSpEc_BoX(0, 0, 3, 10) ORDER BY objectId", // case01/queries/9000_caseinsensitiveUDF.sql
//    "SELECT objectId, iauId, ra_PS, ra_PS_Sigma, decl_PS, decl_PS_Sigma, radecl_PS_Cov, ra_SG, ra_SG_Sigma, decl_SG, decl_SG_Sigma, radecl_SG_Cov, raRange, declRange, muRa_PS, muRa_PS_Sigma, muDecl_PS, muDecl_PS_Sigma, muRaDecl_PS_Cov, parallax_PS, parallax_PS_Sigma, canonicalFilterId, extendedness, varProb, earliestObsTime, latestObsTime, flags, uNumObs, uExtendedness, uVarProb, uRaOffset_PS, uRaOffset_PS_Sigma, uDeclOffset_PS, uDeclOffset_PS_Sigma, uRaDeclOffset_PS_Cov, uRaOffset_SG, uRaOffset_SG_Sigma, uDeclOffset_SG, uDeclOffset_SG_Sigma, uRaDeclOffset_SG_Cov, uLnL_PS, uLnL_SG, uFlux_PS, uFlux_PS_Sigma, uFlux_SG, uFlux_SG_Sigma, uFlux_CSG, uFlux_CSG_Sigma, uTimescale, uEarliestObsTime, uLatestObsTime, uSersicN_SG, uSersicN_SG_Sigma, uE1_SG, uE1_SG_Sigma, uE2_SG, uE2_SG_Sigma, uRadius_SG, uRadius_SG_Sigma, uFlags, gNumObs, gExtendedness, gVarProb, gRaOffset_PS, gRaOffset_PS_Sigma, gDeclOffset_PS, gDeclOffset_PS_Sigma, gRaDeclOffset_PS_Cov, gRaOffset_SG, gRaOffset_SG_Sigma, gDeclOffset_SG, gDeclOffset_SG_Sigma, gRaDeclOffset_SG_Cov, gLnL_PS, gLnL_SG, gFlux_PS, gFlux_PS_Sigma, gFlux_SG, gFlux_SG_Sigma, gFlux_CSG, gFlux_CSG_Sigma, gTimescale, gEarliestObsTime, gLatestObsTime, gSersicN_SG, gSersicN_SG_Sigma, gE1_SG, gE1_SG_Sigma, gE2_SG, gE2_SG_Sigma, gRadius_SG, gRadius_SG_Sigma, gFlags, rNumObs, rExtendedness, rVarProb, rRaOffset_PS, rRaOffset_PS_Sigma, rDeclOffset_PS, rDeclOffset_PS_Sigma, rRaDeclOffset_PS_Cov, rRaOffset_SG, rRaOffset_SG_Sigma, rDeclOffset_SG, rDeclOffset_SG_Sigma, rRaDeclOffset_SG_Cov, rLnL_PS, rLnL_SG, rFlux_PS, rFlux_PS_Sigma, rFlux_SG, rFlux_SG_Sigma, rFlux_CSG, rFlux_CSG_Sigma, rTimescale, rEarliestObsTime, rLatestObsTime, rSersicN_SG, rSersicN_SG_Sigma, rE1_SG, rE1_SG_Sigma, rE2_SG, rE2_SG_Sigma, rRadius_SG, rRadius_SG_Sigma, rFlags, iNumObs, iExtendedness, iVarProb, iRaOffset_PS, iRaOffset_PS_Sigma, iDeclOffset_PS, iDeclOffset_PS_Sigma, iRaDeclOffset_PS_Cov, iRaOffset_SG, iRaOffset_SG_Sigma, iDeclOffset_SG, iDeclOffset_SG_Sigma, iRaDeclOffset_SG_Cov, iLnL_PS, iLnL_SG, iFlux_PS, iFlux_PS_Sigma, iFlux_SG, iFlux_SG_Sigma, iFlux_CSG, iFlux_CSG_Sigma, iTimescale, iEarliestObsTime, iLatestObsTime, iSersicN_SG, iSersicN_SG_Sigma, iE1_SG, iE1_SG_Sigma, iE2_SG, iE2_SG_Sigma, iRadius_SG, iRadius_SG_Sigma, iFlags, zNumObs, zExtendedness, zVarProb, zRaOffset_PS, zRaOffset_PS_Sigma, zDeclOffset_PS, zDeclOffset_PS_Sigma, zRaDeclOffset_PS_Cov, zRaOffset_SG, zRaOffset_SG_Sigma, zDeclOffset_SG, zDeclOffset_SG_Sigma, zRaDeclOffset_SG_Cov, zLnL_PS, zLnL_SG, zFlux_PS, zFlux_PS_Sigma, zFlux_SG, zFlux_SG_Sigma, zFlux_CSG, zFlux_CSG_Sigma, zTimescale, zEarliestObsTime, zLatestObsTime, zSersicN_SG, zSersicN_SG_Sigma, zE1_SG, zE1_SG_Sigma, zE2_SG, zE2_SG_Sigma, zRadius_SG, zRadius_SG_Sigma, zFlags, yNumObs, yExtendedness, yVarProb, yRaOffset_PS, yRaOffset_PS_Sigma, yDeclOffset_PS, yDeclOffset_PS_Sigma, yRaDeclOffset_PS_Cov, yRaOffset_SG, yRaOffset_SG_Sigma, yDeclOffset_SG, yDeclOffset_SG_Sigma, yRaDeclOffset_SG_Cov, yLnL_PS, yLnL_SG, yFlux_PS, yFlux_PS_Sigma, yFlux_SG, yFlux_SG_Sigma, yFlux_CSG, yFlux_CSG_Sigma, yTimescale, yEarliestObsTime, yLatestObsTime, ySersicN_SG, ySersicN_SG_Sigma, yE1_SG, yE1_SG_Sigma, yE2_SG, yE2_SG_Sigma, yRadius_SG, yRadius_SG_Sigma, yFlags FROM   Object WHERE  objectId = 433327840428032", // case02/queries/0001_fetchObjectById.sql
//    "SELECT * FROM   Object WHERE  objectId = 430213989000", // case02/queries/0002_fetchObjectByIdNoResult.sql
//    "SELECT s.ra, s.decl, o.raRange, o.declRange FROM   Object o JOIN   Source s USING (objectId) WHERE  o.objectId = 433327840428032", // case02/queries/0003_selectMetadataForOneGalaxy_withUSING.sql
//    "SELECT sourceId, scienceCcdExposureId, filterId, objectId, movingObjectId, procHistoryId, ra, raErrForDetection, raErrForWcs, decl, declErrForDetection, declErrForWcs, xFlux, xFluxErr, yFlux, yFluxErr, raFlux, raFluxErr, declFlux, declFluxErr, xPeak, yPeak, raPeak, declPeak, xAstrom, xAstromErr, yAstrom, yAstromErr, raAstrom, raAstromErr, declAstrom, declAstromErr, raObject, declObject, taiMidPoint, taiRange, psfFlux, psfFluxErr, apFlux, apFluxErr, modelFlux, modelFluxErr, petroFlux, petroFluxErr, instFlux, instFluxErr, nonGrayCorrFlux, nonGrayCorrFluxErr, atmCorrFlux, atmCorrFluxErr, apDia, Ixx, IxxErr, Iyy, IyyErr, Ixy, IxyErr, snr, chi2, sky, skyErr, extendedness, flux_PS, flux_PS_Sigma, flux_SG, flux_SG_Sigma, sersicN_SG, sersicN_SG_Sigma, e1_SG, e1_SG_Sigma, e2_SG, e2_SG_Sigma, radius_SG, radius_SG_Sigma, flux_flux_SG_Cov, flux_e1_SG_Cov, flux_e2_SG_Cov, flux_radius_SG_Cov, flux_sersicN_SG_Cov, e1_e1_SG_Cov, e1_e2_SG_Cov, e1_radius_SG_Cov, e1_sersicN_SG_Cov, e2_e2_SG_Cov, e2_radius_SG_Cov, e2_sersicN_SG_Cov, radius_radius_SG_Cov, radius_sersicN_SG_Cov, sersicN_sersicN_SG_Cov, flagForAssociation, flagForDetection, flagForWcs FROM   Source WHERE  sourceId = 2867930096075697", // case02/queries/0004_fetchSourceById.sql
//    //"SELECT v.objectId, v.ra, v.decl FROM   Object v, Object o WHERE  o.objectId = :objectId AND spDist(v.ra, v.decl, o.ra, o.decl, :dist) AND v.variability > 0.8 AND o.extendedParameter > 0.8", // case02/queries/0006_transientVarObjNearGalaxy.sql.FIXME
//    "SELECT COUNT(*) AS OBJ_COUNT FROM   Object WHERE qserv_areaspec_box(0.1, -6, 4, 6) AND scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24 AND scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) BETWEEN 0.1 AND 0.9 AND scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS) BETWEEN 0.1 AND 1.0", // case02/queries/1002.1_coneMagColor.sql
//    "SELECT COUNT(*) AS OBJ_COUNT FROM   Object WHERE qserv_areaspec_circle(1.2, 3.2, 0.5) AND scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24 AND scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) BETWEEN 0.1 AND 0.6 AND scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS) BETWEEN 0.1 AND 0.6", // case02/queries/1002.2_coneMagColor.sql
//    "SELECT COUNT(*) AS OBJ_COUNT FROM   Object WHERE qserv_areaspec_ellipse(1.2, 3.2, 6000, 5000, 0.2) AND scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24 AND scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) BETWEEN 0.1 AND 0.6 AND scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS) BETWEEN 0.1 AND 0.6", // case02/queries/1002.3_coneMagColor.sql
//    "SELECT COUNT(*) AS OBJ_COUNT FROM   Object WHERE qserv_areaspec_poly(1.0, 3.0, 1.5, 2.0, 2.0, 4.0) AND scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24 AND scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) BETWEEN 0.1 AND 0.6 AND scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS) BETWEEN 0.1 AND 0.6", // case02/queries/1002.4_coneMagColor.sql
//    "SELECT COUNT(*) AS OBJ_COUNT FROM   Object WHERE qserv_areaspec_box(0, -6, 4, -5) AND scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24 AND scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) BETWEEN 0.1 AND 0.2 AND scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS) BETWEEN 0.1 AND 0.2", // case02/queries/1003_coneMagColorEmptyRes.sql
//    "SELECT objectId, ra_PS, decl_PS FROM   Object WHERE qserv_areaspec_box(0, 0, 3, 10) ORDER BY objectId, ra_PS, decl_PS", // case02/queries/1004.1_varObjects.sql
//    "SELECT objectId FROM   Object WHERE qserv_areaspec_circle(1.5, 3, 1) ORDER BY objectId", // case02/queries/1004.2_varObjects.sql
//    "SELECT objectId FROM   Object WHERE qserv_areaspec_ellipse(1.5, 3, 3500, 200, 0.5) ORDER BY objectId", // case02/queries/1004.3_varObjects.sql
//    "SELECT objectId FROM   Object WHERE qserv_areaspec_poly(0, 0, 3, 10, 0, 5, 3, 1) ORDER BY objectId", // case02/queries/1004.4_varObjects.sql
//    "SELECT objectId FROM   Object WHERE qserv_areaspec_box(0, 0, 3, 10) ORDER BY objectId", // case02/queries/1004_varObjects.sql
//    //"SELECT objectId FROM   Object WHERE qserv_areaspec_box(:raMin, :declMin, :raMax, :declMax) AND    extendedParameter > 0.8", // case02/queries/1005_allGalaxiesInArea.sql.FIXME
//    //"SELECT objectId FROM   Source s JOIN   Science_Ccd_Exposure sce USING (scienceCcdExposureId) WHERE  sce.visit IN (885449631,886257441,886472151) ORDER BY objectId LIMIT 10", // case02/queries/1011_objectsForExposure.sql.FIXME
//    //"SELECT objectId, iE1_SG, ABS(iE1_SG) FROM Object WHERE iE1_SG between -0.1 and 0.1 ORDER BY ABS(iE1_SG);", // case02/queries/1012_orderByClause.sql.FIXME
//    //"SELECT objectId, ROUND(iE1_SG, 3), ROUND(ABS(iE1_SG), 3) FROM Object WHERE iE1_SG between -0.1 and 0.1 ORDER BY ROUND(ABS(iE1_SG), 3);", // case02/queries/1013_orderByClauseRounded.sql.FIXME
//    //"SELECT objectId, taiMidPoint, scisql_fluxToAbMag(psfFlux) FROM   Source JOIN   Object USING(objectId) JOIN   Filter USING(filterId) WHERE qserv_areaspec_box(355, 0, 360, 20) AND filterName = 'g' ORDER BY objectId, taiMidPoint ASC", // case02/queries/1030_timeSeries.sql.FIXME
//    //"SELECT objectId FROM   Alert JOIN   _Alert2Type USING (alertId) JOIN   AlertType USING (alertTypeId) WHERE  alertTypeDescr = 'newTransients' AND  Alert.timeGenerated BETWEEN :timeMin AND :timeMax", // case02/queries/1031_newTransientsForEpoch.sql.FIXME
//    "SELECT o1.objectId AS objId1, o2.objectId AS objId2, scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_PS) AS distance FROM Object o1, Object o2 WHERE qserv_areaspec_box(1.2, 3.3, 1.3, 3.4) AND scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_PS) < 0.016 AND o1.objectId <> o2.objectId", // case02/queries/1051_nn.sql
//    //"SELECT DISTINCT o1.objectId, o2.objectId FROM   Object o1, Object o2 WHERE  scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_PS) < 1 AND  o1.objectId <> o2.objectId AND  ABS( (scisql_fluxToAbMag(o1.gFlux_PS)-scisql_fluxToAbMag(o1.rFlux_PS)) - (scisql_fluxToAbMag(o2.gFlux_PS)-scisql_fluxToAbMag(o2.rFlux_PS)) ) < 1 AND  ABS( (scisql_fluxToAbMag(o1.rFlux_PS)-scisql_fluxToAbMag(o1.iFlux_PS)) - (scisql_fluxToAbMag(o2.rFlux_PS)-scisql_fluxToAbMag(o2.iFlux_PS)) ) < 1 AND  ABS( (scisql_fluxToAbMag(o1.iFlux_PS)-scisql_fluxToAbMag(o1.zFlux_PS)) - (scisql_fluxToAbMag(o2.iFlux_PS)-scisql_fluxToAbMag(o2.zFlux_PS)) ) < 1", // case02/queries/1052_nnSimilarColors.sql.FIXME
//    //"SET @poly = scisql_s2CPolyToBin(300, 2, 0.01, 2, 0.03, 2.6,  359.9, 2.6); CALL scisql.scisql_s2CPolyRegion(@poly, 20); SELECT refObjectId, isStar, ra, decl, rMag FROM SimRefObject AS sro INNER JOIN scisql.Region AS r ON (sro.htmId20 BETWEEN r.htmMin AND r.htmMax) WHERE scisql_s2PtInCPoly(ra, decl, @poly) = 1;", // case02/queries/1070_areaUsingPoly.sql.FIXME
//    //"SELECT sce.visit, sce.raftName, sce.ccdName, sro.gMag, sro.ra, sro.decl, sro.isStar, sro.refObjectId, rom.nSrcMatches, s.sourceId,s.ra,s.decl,s.xAstrom,s.yAstrom,s.psfFlux,s.psfFluxSigma, s.apFlux,s.apFluxSigma,s.flux_ESG,s.flux_ESG_Sigma,s.flux_Gaussian, s.flux_Gaussian_Sigma,s.ixx,s.iyy,s.ixy,s.psfIxx,s.psfIxxSigma, s.psfIyy,s.psfIyySigma,s.psfIxy,s.psfIxySigma,s.resolution_SG, s.e1_SG,s.e1_SG_Sigma,s.e2_SG,s.e2_SG_Sigma,s.shear1_SG,s.shear1_SG_Sigma, s.shear2_SG,s.shear2_SG_Sigma,s.sourceWidth_SG,s.sourceWidth_SG_Sigma, s.flagForDetection FROM Source AS s, Science_Ccd_Exposure AS sce, RefSrcMatch AS rom, SimRefObject AS sro WHERE (s.scienceCcdExposureId = sce.scienceCcdExposureId) AND (s.sourceId = rom.sourceId) AND (rom.refObjectId = sro.refObjectId) AND (sce.visit = 888241840) AND (sce.raftName = '1,0') AND (sce.ccdName like '%')", // case02/queries/1080_refMatch1.sql.FIXME
//    //"SELECT count(*) FROM   Object o INNER JOIN RefObjMatch o2t ON (o.objectId = o2t.objectId) LEFT  JOIN SimRefObject t ON (o2t.refObjectId = t.refObjectId) WHERE  closestToObj = 1 OR closestToObj is NULL", // case02/queries/1081_refMatch2.sql.FIXME
//    //"select objectId, sro.*, (sro.refObjectId-1)/2%pow(2,10) typeId from Source s join RefObjMatch rom using (objectId) join SimRefObject sro using (refObjectId) where isStar =1 limit 10", // case02/queries/1083_refMatch3.sql.FIXME
//    //"SELECT objectId, scisql_fluxToAbMag(uFlux_PS), scisql_fluxToAbMag(gFlux_PS), scisql_fluxToAbMag(rFlux_PS), scisql_fluxToAbMag(iFlux_PS), scisql_fluxToAbMag(zFlux_PS), scisql_fluxToAbMag(yFlux_PS), ra_PS, decl_PS FROM   Object WHERE  ( scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) > 0.7 OR scisql_fluxToAbMag(gFlux_PS) > 22.3 ) AND    scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) > 0.1 AND    ( scisql_fluxToAbMag(rFlux_PS)-scisql_fluxToAbMag(iFlux_PS) < (0.08 + 0.42 * (scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) - 0.96)) OR scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) > 1.26 ) AND    scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS) < 0.8", // case02/queries/2001_fullObjectScan.sql.FIXME
//    //"SELECT  objectId FROM    Object WHERE   extendedParameter > 0.8 -- a star AND   uMag BETWEEN 1 AND 27  -- magnitudes are reasonable AND   gMag BETWEEN 1 AND 27 AND   rMag BETWEEN 1 AND 27 AND   iMag BETWEEN 1 AND 27 AND   zMag BETWEEN 1 AND 27 AND   yMag BETWEEN 1 AND 27 AND (                           -- and one of the colors is  different. uAmplitude > .1 + ABS(uMagSigma) OR gAmplitude > .1 + ABS(gMagSigma) OR rAmplitude > .1 + ABS(rMagSigma) OR iAmplitude > .1 + ABS(iMagSigma) OR zAmplitude > .1 + ABS(zMagSigma) OR yAmplitude > .1 + ABS(yMagSigma))", // case02/queries/2002_findStarsWithMultiMeasAndMagVariation.sql.FIXME
//    //"SELECT * FROM   Object WHERE  variability > 0.8 -- variable object AND uTimescale < :timescaleMax AND gTimescale < :timescaleMax AND rTimescale < :timescaleMax AND iTimescale < :timescaleMax AND zTimescale < :timescaleMax AND yTimescale < :timescaleMax OR primaryPeriod BETWEEN :periodMin AND :periodMax OR uAmplitude > :amplitudeMin OR gAmplitude > :amplitudeMin OR rAmplitude > :amplitudeMin OR iAmplitude > :amplitudeMin OR zAmplitude > :amplitudeMin OR yAmplitude > :amplitudeMin", // case02/queries/2003_objectWithVariabilityOrPeriodOrMag.sql.FIXME
//    //"SELECT  COUNT(*)                                               AS totalCount, SUM(CASE WHEN (typeId=3) THEN 1 ELSE 0 END)            AS galaxyCount, SUM(CASE WHEN (typeId=6) THEN 1 ELSE 0 END)            AS starCount, SUM(CASE WHEN (typeId NOT IN (3,6)) THEN 1 ELSE 0 END) AS otherCount FROM    Object JOIN    _Object2Type USING(objectId) WHERE  (uMag-gMag > 2.0 OR uMag > 22.3) AND iMag BETWEEN 0 AND 19 AND gMag - rMag > 1.0 AND ( (rMag-iMag < 0.08 + 0.42 * (gMag-rMag - 0.96)) OR (gMag-rMag > 2.26 ) ) AND iMag-zMag < 0.25", // case02/queries/2004_objectsSimilarToQuasarsWithRedshift.sql.FIXME
//    //"SELECT objectId FROM   Object JOIN   _ObjectToType USING(objectId) JOIN   ObjectType USING (typeId) WHERE  description = 'Supernova' AND  variability > 0.8 AND  probability > 0.8", // case02/queries/2005_varObjectsOfOneType.sql.FIXME
//    //"SELECT fluxToAbMag(uFlux_PS), fluxToAbMag(gFlux_PS), fluxToAbMag(rFlux_PS), fluxToAbMag(iFlux_PS), fluxToAbMag(zFlux_PS), fluxToAbMag(yFlux_PS) FROM   Object WHERE  (objectId % 100 ) = :percentage", // case02/queries/2006_randomSample.sql.FIXME
//    //"SELECT CASE gid WHEN 1 THEN 'pipeline shutdowns seen' WHEN 2 THEN 'CCDs attempted' WHEN 3 THEN 'src writes' WHEN 4 THEN 'calexp writes' END AS descr, COUNT(*) FROM ( SELECT CASE WHEN COMMENT LIKE 'Processing job:% visit=0 %' THEN 1 WHEN COMMENT LIKE 'Processing job:%' AND COMMENT NOT LIKE '% visit=0 %' THEN 2 WHEN COMMENT LIKE 'Ending write to BoostStorage%/src%' THEN 3 WHEN COMMENT LIKE 'Ending write to FitsStorage%/calexp%' THEN 4 ELSE 0 END AS gid FROM Logs ) AS stats WHERE gid > 0 GROUP BY gid", // case02/queries/2010_logs.sql.FIXME
//    "SELECT  objectId FROM    Object WHERE   scisql_fluxToAbMag(uFlux_PS)-scisql_fluxToAbMag(gFlux_PS) <  2.0 AND  scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) <  0.1 AND  scisql_fluxToAbMag(rFlux_PS)-scisql_fluxToAbMag(iFlux_PS) > -0.8 AND  scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS) <  1.4", // case02/queries/3001_query_035.sql
//    //"SELECT objectId, ra_PS, decl_PS FROM   Object WHERE  ( scisql_fluxToAbMag(rFlux_PS)-scisql_fluxToAbMag(iFlux_PS) - (scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS))/4 - 0.18 ) BETWEEN -0.2 AND 0.2 AND  ( ( (scisql_fluxToAbMag(rFlux_PS)-scisql_fluxToAbMag(iFlux_PS)) - (scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS))/4 - 0.18 ) > (0.45 - 4*(scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS))) )", // case02/queries/3002_query_030.sql.FIXME
//    //"SELECT DISTINCT o1.objectId, o1.ra_PS, o1.decl_PS, o2.iauId FROM   Object o1, Object o2 WHERE  ABS(o2.ra_PS   - o1.ra_PS  ) < o2.raRange/(2*COS(RADIANS(o1.decl_PS))) AND ABS(o2.decl_PS - o1.decl_PS) < o2.declRange/2 AND ( SELECT COUNT(o3.objectId) FROM   Object o3 WHERE  o1.objectId <> o3.objectId AND  ABS(o1.ra_PS   - o3.ra_PS  ) < 0.1/COS(RADIANS(o3.decl_PS)) AND  ABS(o1.decl_PS - o3.decl_PS) < 0.1 ) > 1000", // case02/queries/3003_query_025.sql.FIXME
//    //"SELECT o1.objectId AS objId1, o2.objectId AS objId2, scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_PS) AS distance FROM   Object o1, Object o2 WHERE  o1.ra_PS BETWEEN 1.28 AND 1.38 AND  o1.decl_PS BETWEEN 3.18 AND 3.34 AND  scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_PS) < 1 AND  o1.objectId <> o2.objectId", // case02/queries/3004_query_022.sql.FIXME
//    "SELECT count(*) AS OBJ_COUNT FROM Object", // case02/queries/3005_objectCount.sql
//    "SELECT count(*) AS OBJ_COUNT FROM   Object WHERE ra_PS BETWEEN 1.28 AND 1.38 AND decl_PS BETWEEN 3.18 AND 3.34 AND scisql_fluxToAbMag(zFlux_PS) BETWEEN 21 AND 21.5 AND scisql_fluxToAbMag(gFlux_PS) - scisql_fluxToAbMag(rFlux_PS) BETWEEN 0.3 AND 0.4 AND scisql_fluxToAbMag(iFlux_PS) - scisql_fluxToAbMag(zFlux_PS) BETWEEN 0.1 AND 0.12", // case02/queries/3006_selectIntervalMagnitudes.sql
//    "SELECT COUNT(*) AS OBJ_COUNT FROM Object WHERE gFlux_PS>1e-25", // case02/queries/3007_countObjectWithColorFluxGreaterThan.sql
//    "SELECT objectId, ra_PS, decl_PS, uFlux_PS, gFlux_PS, rFlux_PS, iFlux_PS, zFlux_PS, yFlux_PS FROM Object WHERE scisql_fluxToAbMag(iFlux_PS) - scisql_fluxToAbMag(zFlux_PS) > 0.08", // case02/queries/3008_selectObjectWithColorMagnitudeGreaterThan.sql
//    "SELECT count(*) AS OBJ_COUNT FROM Object WHERE ra_PS BETWEEN 1.28 AND 1.38 AND  decl_PS BETWEEN 3.18 AND 3.34 AND scisql_fluxToAbMag(zFlux_PS) BETWEEN 21 and 21.5", // case02/queries/3009_countObjectInRegionWithZFlux.sql
//    //"SELECT count(*) AS n, AVG(ra_PS), AVG(decl_PS), objectId, chunkId FROM Object GROUP BY chunkId", // case02/queries/3010_countObjectPerChunks.sql.FIXME
//    "SELECT objectId, ra_PS, decl_PS, scisql_fluxToAbMag(zFlux_PS) AS fluxToAbMag FROM Object WHERE scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24", // case02/queries/3011_selectObjectWithMagnitudes.sql
//    "SELECT objectId, ra_PS, decl_PS, scisql_fluxToAbMag(zFlux_PS) FROM Object WHERE scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24", // case02/queries/3011_selectObjectWithMagnitudes_noalias.sql
//    "SELECT count(*) AS OBJ_COUNT FROM Object WHERE scisql_angSep(ra_PS, decl_PS, 1.2, 3.2) < 0.2", // case02/queries/3012_selectObjectInCircularRegion.sql
//    "SELECT objectId FROM Source JOIN Object USING(objectId) WHERE ra_PS BETWEEN 1.28 AND 1.38 AND  decl_PS BETWEEN 3.18 AND 3.34", // case02/queries/3013_joinObjectSourceInRegion_withUSING.sql
//    "SELECT s.ra, s.decl FROM   Object o JOIN   Source s USING (objectId) WHERE  o.objectId = 433327840429024 AND    o.latestObsTime BETWEEN s.taiMidPoint - 300 AND s.taiMidPoint + 300", // case02/queries/3014_joinObjectSourceTimeInterval_withUSING.sql
//    //"SELECT o1.objectId AS objId1, o2.objectId AS objId2, scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_PS) AS distance FROM   Object o1, Object o2 WHERE o1.ra_PS BETWEEN 1.28 AND 1.38 AND o1.decl_PS BETWEEN 3.18 AND 3.34 AND o2.ra_PS BETWEEN 1.28 AND 1.38 AND o2.decl_PS BETWEEN 3.18 AND 3.34 AND o1.objectId <> o2.objectId", // case02/queries/3015_selectAllPairsWithDistanceInRegion.sql.FIXME
//    //"SELECT * FROM Object qserv_areaspec_box(1,3,2,4) LIMIT 10", // case02/queries/3020_selectObjectWithLimit.sql.FIXME
//    //"SELECT * FROM Object WHERE qserv_areaspec_box(1.28,1.38,3.18,3.34) ORDER BY ra_PS", // case02/queries/3021_selectObjectSortedByRA.sql.FIXME
//    //"SELECT o1.objectId, o2.objectId FROM Object o1, Object o2 WHERE   qserv_areaspec_box(0.04, 5., -3., 3.) AND scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_Ps) < 5.", // case02/queries/3022_selectAllPairsWithinSomeDistanceOfPointsInRegion.sql.FIXME
//    //"SELECT objectId, taiMidPoint, fluxToAbMag(psfMag) FROM   Source JOIN   Object USING(objectId) JOIN   Filter USING(filterId) qserv_areaspec_box(1,3,2,4) AND  filterName = 'u' AND  variability BETWEEN 0 AND 2 ORDER BY objectId, taiMidPoint", // case02/queries/3023_joinObjectSourceFilter.sql.FIXME
//    //"SELECT count(*) FROM Object WHERE qserv_areaSpec_box(35, 6, 35 .1, 6.0001);", // case02/queries/8001_badLiteral.sql.FIXME
//    //"SELECT count(*) FROM Object WHERE qserv_areaSpec_box(35, 6, 35. 1, 6.0001);", // case02/queries/8002_badLiteral.sql.FIXME
//    //"SELECT COUNT(*) FROM   Object WHERE qserv_areaspec_box(355, 0, 356, 1) LIMIT 10", // case02/queries/8003_areaWithLimitClause.sql.FIXME
//    //"SHOW COLUMNS FROM DeepSource;", // case03/queries/0001_showColumnsFromSource.sql.FIXME
//    "SELECT sce.filterName, sce.field, sce.camcol, sce.run FROM   Science_Ccd_Exposure AS sce WHERE  sce.filterName like '%' AND sce.field = 535 AND sce.camcol like '%' AND sce.run = 94;", // case03/queries/0002.1_fetchRunAndFieldById.sql
//    //"SELECT sce.filterName, sce.field, sce.camcol, sce.run FROM   Science_Ccd_Exposure AS sce WHERE  (sce.filterName like '%') AND (sce.field = 535) AND (sce.camcol like '%') AND (sce.run = 94);", // case03/queries/0002.1_fetchRunAndFieldById.sql.FIXME
//    "SELECT sce.scienceCcdExposureId, sce.filterName, sce.field, sce.camcol, sce.run, sce.filterId, sce.ra, sce.decl, sce.crpix1, sce.crpix2, sce.crval1, sce.crval2, sce.cd1_1, sce.cd1_2, sce.cd2_1, sce.cd2_2, sce.fluxMag0, sce.fluxMag0Sigma, sce.fwhm FROM   Science_Ccd_Exposure AS sce WHERE  sce.filterName = 'g' AND sce.field = 535 AND sce.camcol = 1 AND sce.run = 94;", // case03/queries/0002.2_fetchRunAndFieldById.sql
//    //"SELECT sce.scienceCcdExposureId, sce.filterName, sce.field, sce.camcol, sce.run, sce.filterId, sce.ra, sce.decl, sce.crpix1, sce.crpix2, sce.crval1, sce.crval2, sce.cd1_1, sce.cd1_2, sce.cd2_1, sce.cd2_2, sce.fluxMag0, sce.fluxMag0Sigma, sce.fwhm FROM   Science_Ccd_Exposure AS sce WHERE  (sce.filterName = 'g') AND (sce.field = 535) AND (sce.camcol = 1) AND (sce.run = 94);", // case03/queries/0002.2_fetchRunAndFieldById.sql.FIXME
//    //"SELECT distinct run, field FROM   Science_Ccd_Exposure WHERE  (run = 94) AND (field = 535);", // case03/queries/0002_fetchRunAndFieldById.sql.FIXME
//    //"SELECT sce.filterName, sce.field, sce.camcol, sce.run, s.deepForcedSourceId, s.ra, s.decl, s.x, s.y, s.psfFlux, s.psfFluxSigma, s.apFlux, s.apFluxSigma, s.modelFlux, s.modelFluxSigma, s.instFlux, s.instFluxSigma, s.shapeIxx, s.shapeIyy, s.shapeIxy, s.flagPixInterpCen, s.flagNegative, s.flagPixEdge, s.flagBadCentroid, s.flagPixSaturCen, s.extendedness FROM   DeepForcedSource AS s, Science_Ccd_Exposure AS sce WHERE  (s.scienceCcdExposureId = sce.scienceCcdExposureId)", // case03/queries/0003.1_SourcesForGivenExposure.sql.FIXME
//    //"SELECT sce.filterName, sce.field, sce.camcol, sce.run, s.deepForcedSourceId, s.ra, s.decl, s.x, s.y, s.psfFlux, s.psfFluxSigma, s.apFlux, s.apFluxSigma, s.modelFlux, s.modelFluxSigma, s.instFlux, s.instFluxSigma, s.shapeIxx, s.shapeIyy, s.shapeIxy, s.flagPixInterpCen, s.flagNegative, s.flagPixEdge, s.flagBadCentroid, s.flagPixSaturCen, s.extendedness FROM   DeepForcedSource AS s, Science_Ccd_Exposure AS sce WHERE  (s.scienceCcdExposureId = sce.scienceCcdExposureId) AND (sce.filterName = 'g') AND (sce.field = 535) AND (sce.camcol = 1) AND (sce.run = 94);", // case03/queries/0003_SourcesForGivenExposure.sql.FIXME
//    //"SELECT sce.filterName, sce.field, sce.camcol, sce.run, s.deepForcedSourceId, s.ra, s.decl, s.x, s.y, s.psfFlux, s.psfFluxSigma, s.apFlux, s.apFluxSigma, s.modelFlux, s.modelFluxSigma, s.instFlux, s.instFluxSigma, s.shapeIxx, s.shapeIyy, s.shapeIxy, s.flagPixInterpCen, s.flagNegative, s.flagPixEdge, s.flagBadCentroid, s.flagPixSaturCen, s.extendedness FROM   DeepForcedSource AS s, Science_Ccd_Exposure AS sce WHERE  (s.scienceCcdExposureId = sce.scienceCcdExposureId) AND (sce.filterName = 'g') AND (sce.field = 793) AND (sce.camcol = 1) AND (sce.run = 5924) ;", // case03/queries/0004_SourceExposureWithFilternameAndField.sql.FIXME
//    //"SHOW COLUMNS FROM RefObject;", // case03/queries/0005_showColumnsFromRefObject.sql.FIXME
//    "SELECT sce.filterName, sce.field, sce.camcol, sce.run FROM   Science_Ccd_Exposure AS sce WHERE  sce.filterName = 'g' AND sce.field = 670 AND sce.camcol = 2 AND sce.run = 7202 ;", // case03/queries/0006_selectExposure.sql
//    //"SELECT sce.filterName, sce.field, sce.camcol, sce.run FROM   Science_Ccd_Exposure AS sce WHERE  (sce.filterName = 'g') AND (sce.field = 670) AND (sce.camcol = 2) AND (sce.run = 7202) ;", // case03/queries/0006_selectExposure.sql.FIXME
//    //"SELECT scisql_s2CPolyToBin(54.96, -0.64, 55.12, -0.64, 55.12, -0.41, 54.96, -0.41) FROM   Science_Ccd_Exposure AS sce WHERE  (sce.filterName = 'g') AND (sce.field = 670) AND (sce.camcol = 2) AND (sce.run = 7202) INTO @poly;", // case03/queries/0007_selectExposureWithPoly.sql.FIXME
//    //"SET @poly = scisql_s2CPolyToBin(54.9, -1.25, 55.0, -1.25, 55.0, -0.75, 54.9, -0.75); SELECT sro.refObjectId, sro.isStar, sro.ra, sro.decl, sro.uMag, sro.gMag, sro.rMag, sro.iMag, sro.zMag FROM   RefObject AS sro WHERE  (scisql_s2PtInCPoly(sro.ra, sro.decl, @poly) = 1);", // case03/queries/0008_selectRefObjectInPoly.sql.FIXME
//    "SELECT sce.filterId, sce.filterName FROM   Science_Ccd_Exposure AS sce WHERE  sce.filterName = 'g' AND sce.field = 670 AND sce.camcol = 2 AND sce.run = 7202 ;", // case03/queries/0009_selectCCDExposure.sql
//    //"SELECT sce.filterId, sce.filterName FROM   Science_Ccd_Exposure AS sce WHERE  (sce.filterName = 'g') AND (sce.field = 670) AND (sce.camcol = 2) AND (sce.run = 7202) ;", // case03/queries/0009_selectCCDExposure.sql.FIXME
//    //"SELECT sce.filterName, sce.field, sce.camcol, sce.run, sro.gMag, sro.isStar, sro.refObjectId, s.deepForcedSourceId,  rom.nSrcMatches,s.ra, s.decl, s.x, s.y, s.psfFlux, s.psfFluxSigma, s.apFlux, s.apFluxSigma, s.modelFlux, s.modelFluxSigma, s.instFlux, s.instFluxSigma, s.shapeIxx, s.shapeIyy, s.shapeIxy, s.flagPixInterpCen, s.flagNegative, s.flagPixEdge, s.flagBadCentroid, s.flagPixSaturCen, s.extendedness FROM   DeepForcedSource AS s, Science_Ccd_Exposure AS sce, RefDeepSrcMatch AS rom, RefObject AS sro WHERE  (s.scienceCcdExposureId = sce.scienceCcdExposureId) AND (s.deepForcedSourceId = rom.deepSourceId) AND (rom.refObjectId = sro.refObjectId) AND (sce.filterName = 'g') AND (sce.field = 670) AND (sce.camcol = 2) AND (sce.run = 7202) ;", // case03/queries/0010_selectSource_RefSrcMatch_RefObject.sql.FIXME
//    "SELECT DISTINCT tract,patch,filterName FROM DeepCoadd ;", // case03/queries/0011_selectDeepCoadd.sql
//    "SELECT DISTINCT tract, patch, filterName FROM   DeepCoadd WHERE  tract = 0 AND patch = '159,2' AND filterName = 'r';", // case03/queries/0012_selectDistinctDeepCoaddWithGivenTractPatchFiltername.sql
//    //"SELECT DISTINCT tract, patch, filterName FROM   DeepCoadd WHERE  (tract = 0) AND (patch = '159,2') AND (filterName = 'r');", // case03/queries/0012_selectDistinctDeepCoaddWithGivenTractPatchFiltername.sql.FIXME
//    "SELECT sce.filterName, sce.tract, sce.patch FROM   DeepCoadd AS sce WHERE  sce.filterName = 'r' AND sce.tract = 0 AND sce.patch = '159,3';", // case03/queries/0013_selectDeepCoadd2.sql
//    //"SELECT sce.filterName, sce.tract, sce.patch FROM   DeepCoadd AS sce WHERE  (sce.filterName = 'r') AND (sce.tract = 0) AND (sce.patch = '159,3');", // case03/queries/0013_selectDeepCoadd2.sql.FIXME
//    //"SELECT sce.DeepCoaddId, sce.filterName, sce.tract, sce.patch, sce.filterId, sce.filterName, sce.ra, sce.decl, sce.crpix1, sce.crpix2, sce.crval1, sce.crval2, sce.cd1_1, sce.cd1_2, sce.cd2_1, sce.cd2_2, sce.fluxMag0, sce.fluxMag0Sigma, sce.measuredFwhm FROM   DeepCoadd AS sce WHERE  (sce.filterName = 'r') AND (sce.tract = 0) AND (sce.patch = '159,2');", // case03/queries/0014.1_selectDeepCoadd3.sql.FIXME
//    "SELECT sce.DeepCoaddId, sce.filterName, sce.tract, sce.patch, sce.filterId, sce.ra, sce.decl, sce.crpix1, sce.crpix2, sce.crval1, sce.crval2, sce.cd1_1, sce.cd1_2, sce.cd2_1, sce.cd2_2, sce.fluxMag0, sce.fluxMag0Sigma, sce.measuredFwhm FROM   DeepCoadd AS sce WHERE  sce.filterName = 'r' AND sce.tract = 0 AND sce.patch = '159,2';", // case03/queries/0014_selectDeepCoadd3.sql
//    //"SELECT sce.DeepCoaddId, sce.filterName, sce.tract, sce.patch, sce.filterId, sce.ra, sce.decl, sce.crpix1, sce.crpix2, sce.crval1, sce.crval2, sce.cd1_1, sce.cd1_2, sce.cd2_1, sce.cd2_2, sce.fluxMag0, sce.fluxMag0Sigma, sce.measuredFwhm FROM   DeepCoadd AS sce WHERE  (sce.filterName = 'r') AND (sce.tract = 0) AND (sce.patch = '159,2');", // case03/queries/0014_selectDeepCoadd3.sql.FIXME
//    //"SELECT sce.filterName, sce.tract, sce.patch, s.deepSourceId, s.ra, s.decl, s.x, s.y, s.psfFlux, s.psfFluxSigma, s.apFlux, s.apFluxSigma, s.modelFlux, s.modelFluxSigma, s.instFlux, s.instFluxSigma, s.shapeIxx, s.shapeIyy, s.shapeIxy, s.flagPixInterpCen, s.flagNegative, s.flagPixEdge, s.flagBadCentroid, s.flagPixSaturCen, s.extendedness FROM   DeepSource AS s, DeepCoadd AS sce WHERE  (s.deepCoaddId = sce.deepCoaddId) AND (sce.filterName = 'r') AND (sce.tract = 0) AND (sce.patch = '159,2');", // case03/queries/0015_selectDeepSourceDeepCoadd.sql.FIXME
//    //"SELECT scisql_s2CPolyToBin(sce.corner1Ra, sce.corner1Decl, sce.corner2Ra, sce.corner2Decl, sce.corner3Ra, sce.corner3Decl, sce.corner4Ra, sce.corner4Decl) FROM   DeepCoadd AS sce WHERE  (sce.filterName = 'g') AND (sce.tract = 0) AND (sce.patch = '159,1') INTO @poly;", // case03/queries/0016_selectDeepCoaddInPoly.sql.FIXME
//    //"SELECT sro.refObjectId, sro.isStar, sro.ra, sro.decl, sro.uMag, sro.gMag, sro.rMag, sro.iMag, sro.zMag FROM   RefObject AS sro WHERE  (scisql_s2PtInCPoly(sro.ra, sro.decl, @poly) = 1);", // case03/queries/0017_selectRefObjectInPoly.sql.FIXME
//    "SELECT sce.filterId, sce.filterName FROM   DeepCoadd AS sce WHERE  sce.filterName = 'r' AND sce.tract = 0 AND sce.patch = '159,1';", // case03/queries/0018_selectDeepCoaddWithGivenTractPatchFiltername.sql
//    //"SELECT sce.filterId, sce.filterName FROM   DeepCoadd AS sce WHERE  (sce.filterName = 'r') AND (sce.tract = 0) AND (sce.patch = '159,1');", // case03/queries/0018_selectDeepCoaddWithGivenTractPatchFiltername.sql.FIXME
//    //"SELECT sce.filterName, sce.tract, sce.patch, sro.gMag, sro.ra, sro.decl, sro.isStar, sro.refObjectId, s.id,  rom.nSrcMatches, s.flags_pixel_interpolated_center, s.flags_negative, s.flags_pixel_edge, s.centroid_sdss_flags, s.flags_pixel_saturated_center FROM   RunDeepSource AS s, DeepCoadd AS sce, RefDeepSrcMatch AS rom, RefObject AS sro WHERE  (s.coadd_id = sce.deepCoaddId) AND (s.id = rom.deepSourceId) AND (rom.refObjectId = sro.refObjectId) AND (sce.filterName = 'r') AND (sce.tract = 0) AND (sce.patch = '159,3') AND (s.id = 1398582280194457) ORDER BY s.id", // case03/queries/0019.1.0_selectRunDeepSourceDeepcoaddDeepsrcmatchRefobject.sql.FIXME
//    "SELECT sce.filterName, sce.tract, sce.patch, sro.gMag, sro.ra, sro.decl, sro.isStar, sro.refObjectId, s.id,  rom.nSrcMatches, s.flags_pixel_interpolated_center, s.flags_negative, s.flags_pixel_edge, s.centroid_sdss_flags, s.flags_pixel_saturated_center FROM   RunDeepSource AS s, DeepCoadd AS sce, RefDeepSrcMatch AS rom, RefObject AS sro WHERE  (s.coadd_id = sce.deepCoaddId) AND (s.id = rom.deepSourceId) AND (rom.refObjectId = sro.refObjectId) AND (sce.filterName = 'r') AND (sce.tract = 0) AND (sce.patch = '159,3') AND (s.id IN (1398582280195495, 1398582280195498, 1398582280195256))", // case03/queries/0019.1_selectRunDeepSourceDeepcoaddDeepsrcmatchRefobject.sql
//    "SELECT sce.filterName, sce.tract, sce.patch, sro.gMag, sro.ra, sro.decl, sro.isStar, sro.refObjectId, s.id as sourceId,  rom.nSrcMatches, s.flags_pixel_interpolated_center, s.flags_negative, s.flags_pixel_edge, s.centroid_sdss_flags, s.flags_pixel_saturated_center FROM   RunDeepSource AS s, DeepCoadd AS sce, RefDeepSrcMatch AS rom, RefObject AS sro WHERE  (s.coadd_id = sce.deepCoaddId) AND (s.id = rom.deepSourceId) AND (rom.refObjectId = sro.refObjectId) AND (sce.filterName = 'r') AND (sce.tract = 0) AND (sce.patch = '159,3') AND (s.id = 1398582280194457) ORDER BY sourceId", // case03/queries/0019.2_selectRunDeepSourceDeepcoaddDeepsrcmatchRefobject.sql
//    //"SELECT sce.filterName, sce.tract, sce.patch, sro.gMag, sro.ra, sro.decl, sro.isStar, sro.refObjectId, s.deepSourceId,  rom.nSrcMatches,s.ra, s.decl, s.x, s.y, s.psfFlux, s.psfFluxSigma, s.apFlux, s.apFluxSigma, s.modelFlux, s.modelFluxSigma, s.instFlux, s.instFluxSigma, s.shapeIxx, s.shapeIyy, s.shapeIxy, s.flagPixInterpCen, s.flagNegative, s.flagPixEdge, s.flagBadCentroid, s.flagPixSaturCen, s.extendedness FROM   DeepSource AS s, DeepCoadd AS sce, RefDeepSrcMatch AS rom, RefObject AS sro WHERE  (s.deepCoaddId = sce.deepCoaddId) AND (s.deepSourceId = rom.deepSourceId) AND (rom.refObjectId = sro.refObjectId) AND (sce.filterName = 'r') AND (sce.tract = 0) AND (sce.patch = '159,3');", // case03/queries/0019_selectDeepsourceDeepcoaddDeepsrcmatchRefobject.sql.FIXME
//    //"SHOW COLUMNS FROM DeepForcedSource;", // case03/queries/0020_showColumnsFromDeepforcedsource.sql.FIXME
//    //"SELECT distinct run, field FROM   Science_Ccd_Exposure WHERE  (run = 94) AND (field = 535);", // case03/queries/0021_selectScienceCCDExposure.sql.FIXME
//    "SELECT sce.filterName, sce.field, sce.camcol, sce.run FROM   Science_Ccd_Exposure AS sce WHERE  sce.filterName like '%' AND sce.field = 535 AND sce.camcol like '%' AND sce.run = 94;", // case03/queries/0022_selectScienceCCDExposureWithFilternameFieldCamcolRun.sql
//    //"SELECT sce.filterName, sce.field, sce.camcol, sce.run FROM   Science_Ccd_Exposure AS sce WHERE  (sce.filterName like '%') AND (sce.field = 535) AND (sce.camcol like '%') AND (sce.run = 94);", // case03/queries/0022_selectScienceCCDExposureWithFilternameFieldCamcolRun.sql.FIXME
//    "SELECT sce.scienceCcdExposureId, sce.field, sce.camcol, sce.run, sce.filterId, sce.filterName, sce.ra, sce.decl, sce.crpix1, sce.crpix2, sce.crval1, sce.crval2, sce.cd1_1, sce.cd1_2, sce.cd2_1, sce.cd2_2, sce.fluxMag0, sce.fluxMag0Sigma, sce.fwhm FROM   Science_Ccd_Exposure AS sce WHERE  sce.filterName = 'g' AND sce.field = 535 AND sce.camcol = 1 AND sce.run = 94;", // case03/queries/0023_selectScienceCCDExposureWithFilternameFieldCamcolRun.sql
//    //"SELECT sce.scienceCcdExposureId, sce.field, sce.camcol, sce.run, sce.filterId, sce.filterName, sce.ra, sce.decl, sce.crpix1, sce.crpix2, sce.crval1, sce.crval2, sce.cd1_1, sce.cd1_2, sce.cd2_1, sce.cd2_2, sce.fluxMag0, sce.fluxMag0Sigma, sce.fwhm FROM   Science_Ccd_Exposure AS sce WHERE  (sce.filterName = 'g') AND (sce.field = 535) AND (sce.camcol = 1) AND (sce.run = 94);", // case03/queries/0023_selectScienceCCDExposureWithFilternameFieldCamcolRun.sql.FIXME
//    //"SELECT sce.filterName, sce.field, sce.camcol, sce.run, s.deepSourceId, s.ra, s.decl, s.x, s.y, s.psfFlux, s.psfFluxSigma, s.apFlux, s.apFluxSigma, s.modelFlux, s.modelFluxSigma, s.instFlux, s.instFluxSigma, s.shapeIxx, s.shapeIyy, s.shapeIxy, s.flagPixInterpCen, s.flagNegative, s.flagPixEdge, s.flagBadCentroid, s.flagPixSaturCen, s.extendedness FROM   DeepForcedSource AS s, Science_Ccd_Exposure AS sce WHERE  (s.scienceCcdExposureId = sce.scienceCcdExposureId) AND (sce.filterName = 'g') AND (sce.field = 535) AND (sce.camcol = 1) AND (sce.run = 94);", // case03/queries/0024_selectDeepForcedSourceScienceCCDExposureWithFilternameFieldCamcolRun.sql.FIXME
//    "SELECT sce.filterName, sce.field, sce.camcol, sce.run FROM   Science_Ccd_Exposure AS sce WHERE  sce.filterName = 'g' AND sce.field = 535 AND sce.camcol = 1 AND sce.run = 94;", // case03/queries/0025_selectScienceCCDExposureWithFilternameFieldCamcolRun.sql
//    //"SELECT sce.filterName, sce.field, sce.camcol, sce.run FROM   Science_Ccd_Exposure AS sce WHERE  (sce.filterName = 'g') AND (sce.field = 535) AND (sce.camcol = 1) AND (sce.run = 94);", // case03/queries/0025_selectScienceCCDExposureWithFilternameFieldCamcolRun.sql.FIXME
//    //"SELECT scisql_s2CPolyToBin(sce.corner1Ra, sce.corner1Decl, sce.corner2Ra, sce.corner2Decl, sce.corner3Ra, sce.corner3Decl, sce.corner4Ra, sce.corner4Decl) FROM   Science_Ccd_Exposure AS sce WHERE  (sce.filterName = 'g') AND (sce.field = 535) AND (sce.camcol = 1) AND (sce.run = 94) INTO @poly;", // case03/queries/0026_selectScienceCCDExposureInPoly.sql.FIXME
//    //"SET @poly = scisql_s2CPolyToBin(54.9, -1.25, 55.0, -1.25, 55.0, -0.75, 54.9, -0.75); SELECT sro.refObjectId, sro.isStar, sro.ra, sro.decl, sro.uMag, sro.gMag, sro.rMag, sro.iMag, sro.zMag FROM   RefObject AS sro WHERE  (scisql_s2PtInCPoly(sro.ra, sro.decl, @poly) = 1) ;", // case03/queries/0027_selectRefObjectInPoly.sql.FIXME
//    "SELECT sce.filterId, sce.filterName FROM   Science_Ccd_Exposure AS sce WHERE  sce.filterName = 'g' AND sce.field = 535 AND sce.camcol = 1 AND sce.run = 94;", // case03/queries/0028_selectScienceCCDExposure.sql
//    //"SELECT sce.filterId, sce.filterName FROM   Science_Ccd_Exposure AS sce WHERE  (sce.filterName = 'g') AND (sce.field = 535) AND (sce.camcol = 1) AND (sce.run = 94);", // case03/queries/0028_selectScienceCCDExposure.sql.FIXME
//    //"SELECT sce.filterName, sce.field, sce.camcol, sce.run, sro.gMag, sro.ra, sro.decl, sro.isStar, sro.refObjectId, s.deepSourceId,  rom.nSrcMatches,s.ra, s.decl, s.x, s.y, s.psfFlux, s.psfFluxSigma, s.apFlux, s.apFluxSigma, s.modelFlux, s.modelFluxSigma, s.instFlux, s.instFluxSigma, s.shapeIxx, s.shapeIyy, s.shapeIxy, s.flagPixInterpCen, s.flagNegative, s.flagPixEdge, s.flagBadCentroid, s.flagPixSaturCen, s.extendedness FROM   DeepForcedSource AS s, Science_Ccd_Exposure AS sce use index(), RefDeepSrcMatch AS rom, RefObject AS sro WHERE  (s.scienceCcdExposureId = sce.scienceCcdExposureId) AND (s.deepSourceId = rom.deepSourceId) AND (rom.refObjectId = sro.refObjectId) AND (sce.filterName = 'g') AND (sce.field = 794) AND (sce.camcol = 1) AND (sce.run = 5924);", // case03/queries/0029_selectDeepforcedsourceScienceCCDExposureRefdeepsrcmatchRefobject.sql.FIXME
//    //"SELECT distinct run, field FROM   Science_Ccd_Exposure WHERE  (run = 94) AND (field = 536);", // case03/queries/0030_selectScienceCCDExposureByRunField.sql.FIXME
//    "SELECT * FROM Science_Ccd_Exposure_Metadata WHERE scienceCcdExposureId=7202320671 AND stringValue=''", // case03/queries/0031_selectEmptyString.sql
//    //"SHOW COLUMNS FROM DeepSource;", // case04/queries/0001_showColumnsFromSource.sql.FIXME
//    "SELECT sce.filterName, sce.field, sce.camcol, sce.run, s.deepForcedSourceId, s.ra, s.decl, s.x, s.y, s.psfFlux, s.psfFluxSigma, s.apFlux, s.apFluxSigma, s.modelFlux, s.modelFluxSigma, s.instFlux, s.instFluxSigma, s.shapeIxx, s.shapeIyy, s.shapeIxy, s.flagPixInterpCen, s.flagNegative, s.flagPixEdge, s.flagBadCentroid, s.flagPixSaturCen, s.extendedness FROM   DeepForcedSource AS s, Science_Ccd_Exposure AS sce WHERE  (s.scienceCcdExposureId = sce.scienceCcdExposureId) AND (sce.filterName = 'g') AND (sce.field = 535) AND (sce.camcol = 1) AND (sce.run = 94);", // case04/queries/0003_SourcesForGivenExposure.sql
//    //"SELECT DISTINCT tract,patch,filterName FROM DeepCoadd ;", // case04/queries/0011_selectDeepCoadd.sql.FIXME
//    "SELECT sce.filterName, sce.tract, sce.patch, s.deepSourceId, s.ra, s.decl, s.x, s.y, s.psfFlux, s.psfFluxSigma, s.apFlux, s.apFluxSigma, s.modelFlux, s.modelFluxSigma, s.instFlux, s.instFluxSigma, s.shapeIxx, s.shapeIyy, s.shapeIxy, s.flagPixInterpCen, s.flagNegative, s.flagPixEdge, s.flagBadCentroid, s.flagPixSaturCen, s.extendedness FROM   DeepSource AS s, DeepCoadd AS sce WHERE  (s.deepCoaddId = sce.deepCoaddId) AND (sce.filterName = 'r') AND (sce.tract = 0) AND (sce.patch = '159,2');", // case04/queries/0015_selectDeepSourceDeepCoadd.sql
//    "SELECT sce.filterId, sce.filterName FROM   DeepCoadd AS sce WHERE  sce.filterName = 'r' AND sce.tract = 0 AND sce.patch = '159,1';", // case04/queries/0018_selectDeepCoaddWithGivenTractPatchFiltername.sql
//    "SHOW COLUMNS FROM DeepForcedSource;", // case04/queries/0020_showColumnsFromDeepForcedSource.sql
//    "SELECT deepForcedSourceId, scienceCcdExposureId, filterId, deepSourceId, timeMid, expTime, ra, decl, raVar, declVar, radeclCov, htmId20, x, y, xVar, yVar, xyCov, psfFlux, psfFluxSigma, apFlux, apFluxSigma, modelFlux, modelFluxSigma, instFlux, instFluxSigma, apCorrection, apCorrectionSigma, shapeIx, shapeIy, shapeIxVar, shapeIyVar, shapeIxIyCov, shapeIxx, shapeIyy, shapeIxy, shapeIxxVar, shapeIyyVar, shapeIxyVar, shapeIxxIyyCov, shapeIxxIxyCov, shapeIyyIxyCov, extendedness, flagNegative, flagBadMeasCentroid, flagPixEdge, flagPixInterpAny, flagPixInterpCen, flagPixSaturAny, flagPixSaturCen, flagBadPsfFlux, flagBadApFlux, flagBadModelFlux, flagBadInstFlux, flagBadCentroid, flagBadShape, raDeepSource, declDeepSource FROM DeepForcedSource ORDER BY deepForcedSourceId;", // case04/queries/0030_largeResult.sql
//    "SELECT objectId, iauId, ra_PS, ra_PS_Sigma, decl_PS, decl_PS_Sigma, radecl_PS_Cov, ra_SG, ra_SG_Sigma, decl_SG, decl_SG_Sigma, radecl_SG_Cov, raRange, declRange, muRa_PS, muRa_PS_Sigma, muDecl_PS, muDecl_PS_Sigma, muRaDecl_PS_Cov, parallax_PS, parallax_PS_Sigma, canonicalFilterId, extendedness, varProb, earliestObsTime, latestObsTime, flags, uNumObs, uExtendedness, uVarProb, uRaOffset_PS, uRaOffset_PS_Sigma, uDeclOffset_PS, uDeclOffset_PS_Sigma, uRaDeclOffset_PS_Cov, uRaOffset_SG, uRaOffset_SG_Sigma, uDeclOffset_SG, uDeclOffset_SG_Sigma, uRaDeclOffset_SG_Cov, uLnL_PS, uLnL_SG, uFlux_PS, uFlux_PS_Sigma, uFlux_SG, uFlux_SG_Sigma, uFlux_CSG, uFlux_CSG_Sigma, uTimescale, uEarliestObsTime, uLatestObsTime, uSersicN_SG, uSersicN_SG_Sigma, uE1_SG, uE1_SG_Sigma, uE2_SG, uE2_SG_Sigma, uRadius_SG, uRadius_SG_Sigma, uFlags, gNumObs, gExtendedness, gVarProb, gRaOffset_PS, gRaOffset_PS_Sigma, gDeclOffset_PS, gDeclOffset_PS_Sigma, gRaDeclOffset_PS_Cov, gRaOffset_SG, gRaOffset_SG_Sigma, gDeclOffset_SG, gDeclOffset_SG_Sigma, gRaDeclOffset_SG_Cov, gLnL_PS, gLnL_SG, gFlux_PS, gFlux_PS_Sigma, gFlux_SG, gFlux_SG_Sigma, gFlux_CSG, gFlux_CSG_Sigma, gTimescale, gEarliestObsTime, gLatestObsTime, gSersicN_SG, gSersicN_SG_Sigma, gE1_SG, gE1_SG_Sigma, gE2_SG, gE2_SG_Sigma, gRadius_SG, gRadius_SG_Sigma, gFlags, rNumObs, rExtendedness, rVarProb, rRaOffset_PS, rRaOffset_PS_Sigma, rDeclOffset_PS, rDeclOffset_PS_Sigma, rRaDeclOffset_PS_Cov, rRaOffset_SG, rRaOffset_SG_Sigma, rDeclOffset_SG, rDeclOffset_SG_Sigma, rRaDeclOffset_SG_Cov, rLnL_PS, rLnL_SG, rFlux_PS, rFlux_PS_Sigma, rFlux_SG, rFlux_SG_Sigma, rFlux_CSG, rFlux_CSG_Sigma, rTimescale, rEarliestObsTime, rLatestObsTime, rSersicN_SG, rSersicN_SG_Sigma, rE1_SG, rE1_SG_Sigma, rE2_SG, rE2_SG_Sigma, rRadius_SG, rRadius_SG_Sigma, rFlags, iNumObs, iExtendedness, iVarProb, iRaOffset_PS, iRaOffset_PS_Sigma, iDeclOffset_PS, iDeclOffset_PS_Sigma, iRaDeclOffset_PS_Cov, iRaOffset_SG, iRaOffset_SG_Sigma, iDeclOffset_SG, iDeclOffset_SG_Sigma, iRaDeclOffset_SG_Cov, iLnL_PS, iLnL_SG, iFlux_PS, iFlux_PS_Sigma, iFlux_SG, iFlux_SG_Sigma, iFlux_CSG, iFlux_CSG_Sigma, iTimescale, iEarliestObsTime, iLatestObsTime, iSersicN_SG, iSersicN_SG_Sigma, iE1_SG, iE1_SG_Sigma, iE2_SG, iE2_SG_Sigma, iRadius_SG, iRadius_SG_Sigma, iFlags, zNumObs, zExtendedness, zVarProb, zRaOffset_PS, zRaOffset_PS_Sigma, zDeclOffset_PS, zDeclOffset_PS_Sigma, zRaDeclOffset_PS_Cov, zRaOffset_SG, zRaOffset_SG_Sigma, zDeclOffset_SG, zDeclOffset_SG_Sigma, zRaDeclOffset_SG_Cov, zLnL_PS, zLnL_SG, zFlux_PS, zFlux_PS_Sigma, zFlux_SG, zFlux_SG_Sigma, zFlux_CSG, zFlux_CSG_Sigma, zTimescale, zEarliestObsTime, zLatestObsTime, zSersicN_SG, zSersicN_SG_Sigma, zE1_SG, zE1_SG_Sigma, zE2_SG, zE2_SG_Sigma, zRadius_SG, zRadius_SG_Sigma, zFlags, yNumObs, yExtendedness, yVarProb, yRaOffset_PS, yRaOffset_PS_Sigma, yDeclOffset_PS, yDeclOffset_PS_Sigma, yRaDeclOffset_PS_Cov, yRaOffset_SG, yRaOffset_SG_Sigma, yDeclOffset_SG, yDeclOffset_SG_Sigma, yRaDeclOffset_SG_Cov, yLnL_PS, yLnL_SG, yFlux_PS, yFlux_PS_Sigma, yFlux_SG, yFlux_SG_Sigma, yFlux_CSG, yFlux_CSG_Sigma, yTimescale, yEarliestObsTime, yLatestObsTime, ySersicN_SG, ySersicN_SG_Sigma, yE1_SG, yE1_SG_Sigma, yE2_SG, yE2_SG_Sigma, yRadius_SG, yRadius_SG_Sigma, yFlags FROM   Object WHERE  objectId = 433327840428032", // case05/queries/0001_fetchObjectById.sql
//    "SELECT * FROM   Object WHERE  objectId = 430213989000", // case05/queries/0002_fetchObjectByIdNoResult.sql
//    "SELECT s.ra, s.decl, o.raRange, o.declRange FROM   Object o JOIN   Source s USING (objectId) WHERE  o.objectId = 433327840428032", // case05/queries/0003_selectMetadataForOneGalaxy_withUSING.sql
//    "SELECT sourceId, scienceCcdExposureId, filterId, objectId, movingObjectId, procHistoryId, ra, raErrForDetection, raErrForWcs, decl, declErrForDetection, declErrForWcs, xFlux, xFluxErr, yFlux, yFluxErr, raFlux, raFluxErr, declFlux, declFluxErr, xPeak, yPeak, raPeak, declPeak, xAstrom, xAstromErr, yAstrom, yAstromErr, raAstrom, raAstromErr, declAstrom, declAstromErr, raObject, declObject, taiMidPoint, taiRange, psfFlux, psfFluxErr, apFlux, apFluxErr, modelFlux, modelFluxErr, petroFlux, petroFluxErr, instFlux, instFluxErr, nonGrayCorrFlux, nonGrayCorrFluxErr, atmCorrFlux, atmCorrFluxErr, apDia, Ixx, IxxErr, Iyy, IyyErr, Ixy, IxyErr, snr, chi2, sky, skyErr, extendedness, flux_PS, flux_PS_Sigma, flux_SG, flux_SG_Sigma, sersicN_SG, sersicN_SG_Sigma, e1_SG, e1_SG_Sigma, e2_SG, e2_SG_Sigma, radius_SG, radius_SG_Sigma, flux_flux_SG_Cov, flux_e1_SG_Cov, flux_e2_SG_Cov, flux_radius_SG_Cov, flux_sersicN_SG_Cov, e1_e1_SG_Cov, e1_e2_SG_Cov, e1_radius_SG_Cov, e1_sersicN_SG_Cov, e2_e2_SG_Cov, e2_radius_SG_Cov, e2_sersicN_SG_Cov, radius_radius_SG_Cov, radius_sersicN_SG_Cov, sersicN_sersicN_SG_Cov, flagForAssociation, flagForDetection, flagForWcs FROM   Source WHERE  sourceId = 2867930096075697", // case05/queries/0004_fetchSourceById.sql
//    //"SELECT v.objectId, v.ra, v.decl FROM   Object v, Object o WHERE  o.objectId = :objectId AND spDist(v.ra, v.decl, o.ra, o.decl, :dist) AND v.variability > 0.8 AND o.extendedParameter > 0.8", // case05/queries/0006_transientVarObjNearGalaxy.sql.FIXME
//    "SELECT COUNT(*) AS OBJ_COUNT FROM   Object WHERE qserv_areaspec_box(70, 3, 75, 3.5) AND scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24 AND scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) BETWEEN 0.1 AND 0.9 AND scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS) BETWEEN 0.1 AND 1.0", // case05/queries/1002.1_coneMagColor.sql
//    "SELECT COUNT(*) AS OBJ_COUNT FROM   Object WHERE qserv_areaspec_circle(72.5, 3.25, 0.6) AND scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24 AND scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) BETWEEN 0.1 AND 0.9 AND scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS) BETWEEN 0.1 AND 1.0", // case05/queries/1002.2_coneMagColor.sql
//    "SELECT COUNT(*) AS OBJ_COUNT FROM   Object WHERE qserv_areaspec_ellipse(72.5, 3.25, 6000, 1700, 0.2) AND scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24 AND scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) BETWEEN 0.1 AND 0.9 AND scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS) BETWEEN 0.1 AND 1.0", // case05/queries/1002.3_coneMagColor.sql
//    "SELECT COUNT(*) AS OBJ_COUNT FROM   Object WHERE qserv_areaspec_poly( 70, 3, 75, 3.5, 70, 4.0) AND scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24 AND scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) BETWEEN 0.1 AND 0.9 AND scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS) BETWEEN 0.1 AND 1.0", // case05/queries/1002.4_coneMagColor.sql
//    "SELECT COUNT(*) AS OBJ_COUNT FROM   Object WHERE qserv_areaspec_box(0, -6, 4, -5) AND scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24 AND scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) BETWEEN 0.1 AND 0.2 AND scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS) BETWEEN 0.1 AND 0.2", // case05/queries/1003_coneMagColorEmptyRes.sql
//    "SELECT objectId FROM   Object WHERE qserv_areaspec_box(0, 0, 3, 10) ORDER BY objectId", // case05/queries/1004_varObjects.sql
//    //"SELECT objectId FROM   Object WHERE qserv_areaspec_box(:raMin, :declMin, :raMax, :declMax) AND    extendedParameter > 0.8", // case05/queries/1005_allGalaxiesInArea.sql.FIXME
//    //"SELECT objectId FROM   Source s JOIN   Science_Ccd_Exposure sce USING (scienceCcdExposureId) WHERE  sce.visit IN (885449631,886257441,886472151) ORDER BY objectId LIMIT 10", // case05/queries/1011_objectsForExposure.sql.FIXME
//    //"SELECT objectId, iE1_SG, ABS(iE1_SG) FROM Object WHERE iE1_SG between -0.1 and 0.1 ORDER BY ABS(iE1_SG);", // case05/queries/1012_orderByClause.sql.FIXME
//    //"SELECT objectId, ROUND(iE1_SG, 3), ROUND(ABS(iE1_SG), 3) FROM Object WHERE iE1_SG between -0.1 and 0.1 ORDER BY ROUND(ABS(iE1_SG), 3);", // case05/queries/1013_orderByClauseRounded.sql.FIXME
//    //"SELECT objectId, taiMidPoint, scisql_fluxToAbMag(psfFlux) FROM   Source JOIN   Object USING(objectId) JOIN   Filter USING(filterId) WHERE qserv_areaspec_box(355, 0, 360, 20) AND filterName = 'g' ORDER BY objectId, taiMidPoint ASC", // case05/queries/1030_timeSeries.sql.FIXME
//    //"SELECT objectId FROM   Alert JOIN   _Alert2Type USING (alertId) JOIN   AlertType USING (alertTypeId) WHERE  alertTypeDescr = 'newTransients' AND  Alert.timeGenerated BETWEEN :timeMin AND :timeMax", // case05/queries/1031_newTransientsForEpoch.sql.FIXME
//    //"SELECT o1.objectId AS objId1, o2.objectId AS objId2, scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_PS) AS distance FROM Object o1, Object o2 WHERE qserv_areaspec_box(0, 0, 0.2, 1) AND scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_PS) < 1 AND o1.objectId <> o2.objectId", // case05/queries/1051_nn.sql.FIXME
//    //"SELECT DISTINCT o1.objectId, o2.objectId FROM   Object o1, Object o2 WHERE  scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_PS) < 1 AND  o1.objectId <> o2.objectId AND  ABS( (scisql_fluxToAbMag(o1.gFlux_PS)-scisql_fluxToAbMag(o1.rFlux_PS)) - (scisql_fluxToAbMag(o2.gFlux_PS)-scisql_fluxToAbMag(o2.rFlux_PS)) ) < 1 AND  ABS( (scisql_fluxToAbMag(o1.rFlux_PS)-scisql_fluxToAbMag(o1.iFlux_PS)) - (scisql_fluxToAbMag(o2.rFlux_PS)-scisql_fluxToAbMag(o2.iFlux_PS)) ) < 1 AND  ABS( (scisql_fluxToAbMag(o1.iFlux_PS)-scisql_fluxToAbMag(o1.zFlux_PS)) - (scisql_fluxToAbMag(o2.iFlux_PS)-scisql_fluxToAbMag(o2.zFlux_PS)) ) < 1", // case05/queries/1052_nnSimilarColors.sql.FIXME
//    //"SET @poly = scisql_s2CPolyToBin(300, 2, 0.01, 2, 0.03, 2.6,  359.9, 2.6); CALL scisql.scisql_s2CPolyRegion(@poly, 20); SELECT refObjectId, isStar, ra, decl, rMag FROM SimRefObject AS sro INNER JOIN scisql.Region AS r ON (sro.htmId20 BETWEEN r.htmMin AND r.htmMax) WHERE scisql_s2PtInCPoly(ra, decl, @poly) = 1;", // case05/queries/1070_areaUsingPoly.sql.FIXME
//    //"SELECT sce.visit, sce.raftName, sce.ccdName, sro.gMag, sro.ra, sro.decl, sro.isStar, sro.refObjectId, rom.nSrcMatches, s.sourceId,s.ra,s.decl,s.xAstrom,s.yAstrom,s.psfFlux,s.psfFluxSigma, s.apFlux,s.apFluxSigma,s.flux_ESG,s.flux_ESG_Sigma,s.flux_Gaussian, s.flux_Gaussian_Sigma,s.ixx,s.iyy,s.ixy,s.psfIxx,s.psfIxxSigma, s.psfIyy,s.psfIyySigma,s.psfIxy,s.psfIxySigma,s.resolution_SG, s.e1_SG,s.e1_SG_Sigma,s.e2_SG,s.e2_SG_Sigma,s.shear1_SG,s.shear1_SG_Sigma, s.shear2_SG,s.shear2_SG_Sigma,s.sourceWidth_SG,s.sourceWidth_SG_Sigma, s.flagForDetection FROM Source AS s, Science_Ccd_Exposure AS sce, RefSrcMatch AS rom, SimRefObject AS sro WHERE (s.scienceCcdExposureId = sce.scienceCcdExposureId) AND (s.sourceId = rom.sourceId) AND (rom.refObjectId = sro.refObjectId) AND (sce.visit = 888241840) AND (sce.raftName = '1,0') AND (sce.ccdName like '%')", // case05/queries/1080_refMatch1.sql.FIXME
//    //"SELECT count(*) FROM   Object o INNER JOIN RefObjMatch o2t ON (o.objectId = o2t.objectId) LEFT  JOIN SimRefObject t ON (o2t.refObjectId = t.refObjectId) WHERE  closestToObj = 1 OR closestToObj is NULL", // case05/queries/1081_refMatch2.sql.FIXME
//    //"select objectId, sro.*, (sro.refObjectId-1)/2%pow(2,10) typeId from Source s join RefObjMatch rom using (objectId) join SimRefObject sro using (refObjectId) where isStar =1 limit 10", // case05/queries/1083_refMatch3.sql.FIXME
//    //"SELECT objectId, scisql_fluxToAbMag(uFlux_PS), scisql_fluxToAbMag(gFlux_PS), scisql_fluxToAbMag(rFlux_PS), scisql_fluxToAbMag(iFlux_PS), scisql_fluxToAbMag(zFlux_PS), scisql_fluxToAbMag(yFlux_PS), ra_PS, decl_PS FROM   Object WHERE  ( scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) > 0.7 OR scisql_fluxToAbMag(gFlux_PS) > 22.3 ) AND    scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) > 0.1 AND    ( scisql_fluxToAbMag(rFlux_PS)-scisql_fluxToAbMag(iFlux_PS) < (0.08 + 0.42 * (scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) - 0.96)) OR scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) > 1.26 ) AND    scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS) < 0.8", // case05/queries/2001_fullObjectScan.sql.FIXME
//    //"SELECT  objectId FROM    Object WHERE   extendedParameter > 0.8 -- a star AND   uMag BETWEEN 1 AND 27  -- magnitudes are reasonable AND   gMag BETWEEN 1 AND 27 AND   rMag BETWEEN 1 AND 27 AND   iMag BETWEEN 1 AND 27 AND   zMag BETWEEN 1 AND 27 AND   yMag BETWEEN 1 AND 27 AND (                           -- and one of the colors is  different. uAmplitude > .1 + ABS(uMagSigma) OR gAmplitude > .1 + ABS(gMagSigma) OR rAmplitude > .1 + ABS(rMagSigma) OR iAmplitude > .1 + ABS(iMagSigma) OR zAmplitude > .1 + ABS(zMagSigma) OR yAmplitude > .1 + ABS(yMagSigma))", // case05/queries/2002_findStarsWithMultiMeasAndMagVariation.sql.FIXME
//    //"SELECT * FROM   Object WHERE  variability > 0.8 -- variable object AND uTimescale < :timescaleMax AND gTimescale < :timescaleMax AND rTimescale < :timescaleMax AND iTimescale < :timescaleMax AND zTimescale < :timescaleMax AND yTimescale < :timescaleMax OR primaryPeriod BETWEEN :periodMin AND :periodMax OR uAmplitude > :amplitudeMin OR gAmplitude > :amplitudeMin OR rAmplitude > :amplitudeMin OR iAmplitude > :amplitudeMin OR zAmplitude > :amplitudeMin OR yAmplitude > :amplitudeMin", // case05/queries/2003_objectWithVariabilityOrPeriodOrMag.sql.FIXME
//    //"SELECT  COUNT(*)                                               AS totalCount, SUM(CASE WHEN (typeId=3) THEN 1 ELSE 0 END)            AS galaxyCount, SUM(CASE WHEN (typeId=6) THEN 1 ELSE 0 END)            AS starCount, SUM(CASE WHEN (typeId NOT IN (3,6)) THEN 1 ELSE 0 END) AS otherCount FROM    Object JOIN    _Object2Type USING(objectId) WHERE  (uMag-gMag > 2.0 OR uMag > 22.3) AND iMag BETWEEN 0 AND 19 AND gMag - rMag > 1.0 AND ( (rMag-iMag < 0.08 + 0.42 * (gMag-rMag - 0.96)) OR (gMag-rMag > 2.26 ) ) AND iMag-zMag < 0.25", // case05/queries/2004_objectsSimilarToQuasarsWithRedshift.sql.FIXME
//    //"SELECT objectId FROM   Object JOIN   _ObjectToType USING(objectId) JOIN   ObjectType USING (typeId) WHERE  description = 'Supernova' AND  variability > 0.8 AND  probability > 0.8", // case05/queries/2005_varObjectsOfOneType.sql.FIXME
//    //"SELECT fluxToAbMag(uFlux_PS), fluxToAbMag(gFlux_PS), fluxToAbMag(rFlux_PS), fluxToAbMag(iFlux_PS), fluxToAbMag(zFlux_PS), fluxToAbMag(yFlux_PS) FROM   Object WHERE  (objectId % 100 ) = :percentage", // case05/queries/2006_randomSample.sql.FIXME
//    //"SELECT CASE gid WHEN 1 THEN 'pipeline shutdowns seen' WHEN 2 THEN 'CCDs attempted' WHEN 3 THEN 'src writes' WHEN 4 THEN 'calexp writes' END AS descr, COUNT(*) FROM ( SELECT CASE WHEN COMMENT LIKE 'Processing job:% visit=0 %' THEN 1 WHEN COMMENT LIKE 'Processing job:%' AND COMMENT NOT LIKE '% visit=0 %' THEN 2 WHEN COMMENT LIKE 'Ending write to BoostStorage%/src%' THEN 3 WHEN COMMENT LIKE 'Ending write to FitsStorage%/calexp%' THEN 4 ELSE 0 END AS gid FROM Logs ) AS stats WHERE gid > 0 GROUP BY gid", // case05/queries/2010_logs.sql.FIXME
//    "SELECT  objectId FROM    Object WHERE   scisql_fluxToAbMag(uFlux_PS)-scisql_fluxToAbMag(gFlux_PS) <  2.0 AND  scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) <  0.1 AND  scisql_fluxToAbMag(rFlux_PS)-scisql_fluxToAbMag(iFlux_PS) > -0.8 AND  scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS) <  1.4", // case05/queries/3001_query_035.sql
//    //"SELECT objectId, ra_PS, decl_PS FROM   Object WHERE  ( scisql_fluxToAbMag(rFlux_PS)-scisql_fluxToAbMag(iFlux_PS) - (scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS))/4 - 0.18 ) BETWEEN -0.2 AND 0.2 AND  ( ( (scisql_fluxToAbMag(rFlux_PS)-scisql_fluxToAbMag(iFlux_PS)) - (scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS))/4 - 0.18 ) > (0.45 - 4*(scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS))) )", // case05/queries/3002_query_030.sql.FIXME
//    //"SELECT DISTINCT o1.objectId, o1.ra_PS, o1.decl_PS, o2.iauId FROM   Object o1, Object o2 WHERE  ABS(o2.ra_PS   - o1.ra_PS  ) < o2.raRange/(2*COS(RADIANS(o1.decl_PS))) AND ABS(o2.decl_PS - o1.decl_PS) < o2.declRange/2 AND ( SELECT COUNT(o3.objectId) FROM   Object o3 WHERE  o1.objectId <> o3.objectId AND  ABS(o1.ra_PS   - o3.ra_PS  ) < 0.1/COS(RADIANS(o3.decl_PS)) AND  ABS(o1.decl_PS - o3.decl_PS) < 0.1 ) > 1000", // case05/queries/3003_query_025.sql.FIXME
//    //"SELECT o1.objectId AS objId1, o2.objectId AS objId2, scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_PS) AS distance FROM   Object o1, Object o2 WHERE  o1.ra_PS BETWEEN 1.28 AND 1.38 AND  o1.decl_PS BETWEEN 3.18 AND 3.34 AND  scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_PS) < 1 AND  o1.objectId <> o2.objectId", // case05/queries/3004_query_022.sql.FIXME
//    "SELECT count(*) AS OBJ_COUNT FROM Object", // case05/queries/3005_objectCount.sql
//    "SELECT count(*) AS OBJ_COUNT FROM   Object WHERE ra_PS BETWEEN 1.28 AND 1.38 AND decl_PS BETWEEN 3.18 AND 3.34 AND scisql_fluxToAbMag(zFlux_PS) BETWEEN 21 AND 21.5 AND scisql_fluxToAbMag(gFlux_PS) - scisql_fluxToAbMag(rFlux_PS) BETWEEN 0.3 AND 0.4 AND scisql_fluxToAbMag(iFlux_PS) - scisql_fluxToAbMag(zFlux_PS) BETWEEN 0.1 AND 0.12", // case05/queries/3006_selectIntervalMagnitudes.sql
//    "SELECT COUNT(*) AS OBJ_COUNT FROM Object WHERE gFlux_PS>1e-25", // case05/queries/3007_countObjectWithColorFluxGreaterThan.sql
//    "SELECT objectId, ra_PS, decl_PS, uFlux_PS, gFlux_PS, rFlux_PS, iFlux_PS, zFlux_PS, yFlux_PS FROM Object WHERE scisql_fluxToAbMag(iFlux_PS) - scisql_fluxToAbMag(zFlux_PS) > 0.08", // case05/queries/3008_selectObjectWithColorMagnitudeGreaterThan.sql
//    "SELECT count(*) AS OBJ_COUNT FROM Object WHERE ra_PS BETWEEN 1.28 AND 1.38 AND  decl_PS BETWEEN 3.18 AND 3.34 AND scisql_fluxToAbMag(zFlux_PS) BETWEEN 21 and 21.5", // case05/queries/3009_countObjectInRegionWithZFlux.sql
//    //"SELECT count(*) AS n, AVG(ra_PS), AVG(decl_PS), objectId, chunkId FROM Object GROUP BY chunkId", // case05/queries/3010_countObjectPerChunks.sql.FIXME
//    "SELECT objectId, ra_PS, decl_PS, scisql_fluxToAbMag(zFlux_PS) AS fluxToAbMag FROM Object WHERE scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24", // case05/queries/3011_selectObjectWithMagnitudes.sql
//    "SELECT objectId, ra_PS, decl_PS, scisql_fluxToAbMag(zFlux_PS) FROM Object WHERE scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24", // case05/queries/3011_selectObjectWithMagnitudes_noalias.sql
//    "SELECT count(*) AS OBJ_COUNT FROM Object WHERE scisql_angSep(ra_PS, decl_PS, 0., 0.) < 0.2", // case05/queries/3012_selectObjectInCircularRegion.sql
//    "SELECT objectId FROM Source JOIN Object USING(objectId) WHERE ra_PS BETWEEN 1.28 AND 1.38 AND  decl_PS BETWEEN 3.18 AND 3.34", // case05/queries/3013_joinObjectSourceInRegion_withUSING.sql
//    "SELECT s.ra, s.decl FROM   Object o JOIN   Source s USING (objectId) WHERE  o.objectId = 433327840429024 AND    o.latestObsTime BETWEEN s.taiMidPoint - 300 AND s.taiMidPoint + 300", // case05/queries/3014_joinObjectSourceTimeInterval_withUSING.sql
//    //"SELECT o1.objectId AS objId1, o2.objectId AS objId2, scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_PS) AS distance FROM   Object o1, Object o2 WHERE o1.ra_PS BETWEEN 1.28 AND 1.38 AND o1.decl_PS BETWEEN 3.18 AND 3.34 AND o2.ra_PS BETWEEN 1.28 AND 1.38 AND o2.decl_PS BETWEEN 3.18 AND 3.34 AND o1.objectId <> o2.objectId", // case05/queries/3015_selectAllPairsWithDistanceInRegion.sql.FIXME
//    //"SELECT * FROM Object qserv_areaspec_box(1,3,2,4) LIMIT 10", // case05/queries/3020_selectObjectWithLimit.sql.FIXME
//    //"SELECT * FROM Object WHERE qserv_areaspec_box(1.28,1.38,3.18,3.34) ORDER BY ra_PS", // case05/queries/3021_selectObjectSortedByRA.sql.FIXME
//    //"SELECT o1.objectId, o2.objectId FROM Object o1, Object o2 WHERE   qserv_areaspec_box(0.04, 5., -3., 3.) AND scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_Ps) < 5.", // case05/queries/3022_selectAllPairsWithinSomeDistanceOfPointsInRegion.sql.FIXME
//    //"SELECT objectId, taiMidPoint, fluxToAbMag(psfMag) FROM   Source JOIN   Object USING(objectId) JOIN   Filter USING(filterId) qserv_areaspec_box(1,3,2,4) AND  filterName = 'u' AND  variability BETWEEN 0 AND 2 ORDER BY objectId, taiMidPoint", // case05/queries/3023_joinObjectSourceFilter.sql.FIXME
//    //"SELECT count(*) FROM Object WHERE qserv_areaSpec_box(35, 6, 35 .1, 6.0001);", // case05/queries/8001_badLiteral.sql.FIXME
//    //"SELECT count(*) FROM Object WHERE qserv_areaSpec_box(35, 6, 35. 1, 6.0001);", // case05/queries/8002_badLiteral.sql.FIXME
//    //"SELECT COUNT(*) FROM   Object WHERE qserv_areaspec_box(355, 0, 356, 1) LIMIT 10", // case05/queries/8003_areaWithLimitClause.sql.FIXME
};


BOOST_DATA_TEST_CASE(antlr_compare, QUERIES, query) {
    auto a2SelectStatement = ccontrol::UserQueryFactory::antlr2NewSelectStmt(query);
    BOOST_REQUIRE(a2SelectStatement != nullptr);
    std::ostringstream a2QueryStr;
    a2QueryStr << a2SelectStatement->getQueryTemplate();
    BOOST_TEST_MESSAGE("the antlr2 selectStmt structure:" << a2SelectStatement);

    auto a4SelectStatement = ccontrol::a4NewUserQuery(query);
    BOOST_REQUIRE(a4SelectStatement != nullptr);
    std::ostringstream a4QueryStr;
    a4QueryStr << a4SelectStatement->getQueryTemplate();

    if (a4QueryStr.str() != a2QueryStr.str()) {
        BOOST_TEST_MESSAGE("antlr4 selectStmt does not match antlr2 selectStmt...");
        BOOST_TEST_MESSAGE("antlr4 selectStmt structure:" << a4SelectStatement);
    }
    BOOST_REQUIRE_EQUAL(a4QueryStr.str(), a2QueryStr.str());
}


BOOST_AUTO_TEST_CASE(testUserQueryType) {
    using lsst::qserv::ccontrol::UserQueryType;

    BOOST_CHECK(UserQueryType::isSelect("SELECT 1"));
    BOOST_CHECK(UserQueryType::isSelect("SELECT\t1"));
    BOOST_CHECK(UserQueryType::isSelect("SELECT\n\r1"));

    BOOST_CHECK(UserQueryType::isSelect("select 1"));
    BOOST_CHECK(UserQueryType::isSelect("SeLeCt 1"));

    BOOST_CHECK(not UserQueryType::isSelect("unselect X"));
    BOOST_CHECK(not UserQueryType::isSelect("DROP SELECT;"));

    std::string stripped;
    BOOST_CHECK(UserQueryType::isSubmit("SUBMIT SELECT", stripped));
    BOOST_CHECK_EQUAL("SELECT", stripped);
    BOOST_CHECK(UserQueryType::isSubmit("submit\tselect  ", stripped));
    BOOST_CHECK_EQUAL("select  ", stripped);
    BOOST_CHECK(UserQueryType::isSubmit("SubMiT \n SelEcT", stripped));
    BOOST_CHECK_EQUAL("SelEcT", stripped);
    BOOST_CHECK(not UserQueryType::isSubmit("submit", stripped));
    BOOST_CHECK(not UserQueryType::isSubmit("submit ", stripped));
    BOOST_CHECK(not UserQueryType::isSubmit("unsubmit select", stripped));
    BOOST_CHECK(not UserQueryType::isSubmit("submitting select", stripped));

    struct {
        const char* query;
        const char* db;
        const char* table;
    } drop_table_ok[] = {
        {"DROP TABLE DB.TABLE", "DB", "TABLE"},
        {"DROP TABLE DB.TABLE;", "DB", "TABLE"},
        {"DROP TABLE DB.TABLE ;", "DB", "TABLE"},
        {"DROP TABLE `DB`.`TABLE` ", "DB", "TABLE"},
        {"DROP TABLE \"DB\".\"TABLE\"", "DB", "TABLE"},
        {"DROP TABLE TABLE", "", "TABLE"},
        {"DROP TABLE `TABLE`", "", "TABLE"},
        {"DROP TABLE \"TABLE\"", "", "TABLE"},
        {"drop\ttable\nDB.TABLE ;", "DB", "TABLE"}
    };

    for (auto test: drop_table_ok) {
        std::string db, table;
        BOOST_CHECK(UserQueryType::isDropTable(test.query, db, table));
        BOOST_CHECK_EQUAL(db, test.db);
        BOOST_CHECK_EQUAL(table, test.table);
    }

    const char* drop_table_fail[] = {
        "DROP DATABASE DB",
        "DROP TABLE",
        "DROP TABLE TABLE; DROP IT;",
        "DROP TABLE 'DB'.'TABLE'",
        "DROP TABLE db%.TABLE",
        "UNDROP TABLE X"
    };
    for (auto test: drop_table_fail) {
        std::string db, table;
        BOOST_CHECK(not UserQueryType::isDropTable(test, db, table));
    }

    struct {
        const char* query;
        const char* db;
    } drop_db_ok[] = {
        {"DROP DATABASE DB", "DB"},
        {"DROP SCHEMA DB ", "DB"},
        {"DROP DATABASE DB;", "DB"},
        {"DROP SCHEMA DB ; ", "DB"},
        {"DROP DATABASE `DB` ", "DB"},
        {"DROP SCHEMA \"DB\"", "DB"},
        {"drop\tdatabase\nd_b ;", "d_b"}
    };
    for (auto test: drop_db_ok) {
        std::string db;
        BOOST_CHECK(UserQueryType::isDropDb(test.query, db));
        BOOST_CHECK_EQUAL(db, test.db);
    }

    const char* drop_db_fail[] = {
        "DROP TABLE DB",
        "DROP DB",
        "DROP DATABASE",
        "DROP DATABASE DB;;",
        "DROP SCHEMA DB; DROP IT;",
        "DROP SCHEMA DB.TABLE",
        "DROP SCHEMA 'DB'",
        "DROP DATABASE db%",
        "UNDROP DATABASE X",
        "UN DROP DATABASE X"
    };
    for (auto test: drop_db_fail) {
        std::string db;
        BOOST_CHECK(not UserQueryType::isDropDb(test, db));
    }

    struct {
        const char* query;
        const char* db;
    } flush_empty_ok[] = {
        {"FLUSH QSERV_CHUNKS_CACHE", ""},
        {"FLUSH QSERV_CHUNKS_CACHE\t ", ""},
        {"FLUSH QSERV_CHUNKS_CACHE;", ""},
        {"FLUSH QSERV_CHUNKS_CACHE ; ", ""},
        {"FLUSH QSERV_CHUNKS_CACHE FOR DB", "DB"},
        {"FLUSH QSERV_CHUNKS_CACHE FOR `DB`", "DB"},
        {"FLUSH QSERV_CHUNKS_CACHE FOR \"DB\"", "DB"},
        {"FLUSH QSERV_CHUNKS_CACHE FOR DB ; ", "DB"},
        {"flush qserv_chunks_cache for `d_b`", "d_b"},
        {"flush\nqserv_chunks_CACHE\tfor \t d_b", "d_b"},
    };
    for (auto test: flush_empty_ok) {
        std::string db;
        BOOST_CHECK(UserQueryType::isFlushChunksCache(test.query, db));
        BOOST_CHECK_EQUAL(db, test.db);
    }

    const char* flush_empty_fail[] = {
        "FLUSH QSERV CHUNKS CACHE",
        "UNFLUSH QSERV_CHUNKS_CACHE",
        "FLUSH QSERV_CHUNKS_CACHE DB",
        "FLUSH QSERV_CHUNKS_CACHE FOR",
        "FLUSH QSERV_CHUNKS_CACHE FROM DB",
        "FLUSH QSERV_CHUNKS_CACHE FOR DB.TABLE",
    };
    for (auto test: flush_empty_fail) {
        std::string db;
        BOOST_CHECK(not UserQueryType::isFlushChunksCache(test, db));
    }

    const char* show_proclist_ok[] = {
        "SHOW PROCESSLIST",
        "show processlist",
        "show    PROCESSLIST",
    };
    for (auto test: show_proclist_ok) {
        bool full;
        BOOST_CHECK(UserQueryType::isShowProcessList(test, full));
        BOOST_CHECK(not full);
    }

    const char* show_full_proclist_ok[] = {
        "SHOW FULL PROCESSLIST",
        "show full   processlist",
        "show FULL PROCESSLIST",
    };
    for (auto test: show_full_proclist_ok) {
        bool full;
        BOOST_CHECK(UserQueryType::isShowProcessList(test, full));
        BOOST_CHECK(full);
    }

    const char* show_proclist_fail[] = {
        "show PROCESS",
        "SHOW PROCESS LIST",
        "show fullprocesslist",
        "show full process list",
    };
    for (auto test: show_proclist_fail) {
        bool full;
        BOOST_CHECK(not UserQueryType::isShowProcessList(test, full));
    }

    struct {
        const char* db;
        const char* table;
    } proclist_table_ok[] = {
        {"INFORMATION_SCHEMA", "PROCESSLIST"},
        {"information_schema", "processlist"},
        {"Information_Schema", "ProcessList"},
    };
    for (auto test: proclist_table_ok) {
        BOOST_CHECK(UserQueryType::isProcessListTable(test.db, test.table));
    }

    struct {
        const char* db;
        const char* table;
    } proclist_table_fail[] = {
        {"INFORMATIONSCHEMA", "PROCESSLIST"},
        {"information_schema", "process_list"},
        {"Information Schema", "Process List"},
    };
    for (auto test: proclist_table_fail) {
        BOOST_CHECK(not UserQueryType::isProcessListTable(test.db, test.table));
    }

    struct {
        const char* query;
        int id;
    } kill_query_ok[] = {
        {"KILL 100", 100},
        {"KilL 101  ", 101},
        {"kill   102  ", 102},
        {"KILL QUERY 100", 100},
        {"kill\tquery   100   ", 100},
        {"KILL CONNECTION 100", 100},
        {"KILL \t CONNECTION   100  ", 100},
    };
    for (auto test: kill_query_ok) {
        int threadId;
        BOOST_CHECK(UserQueryType::isKill(test.query, threadId));
        BOOST_CHECK_EQUAL(threadId, test.id);
    }

    const char* kill_query_fail[] = {
        "NOT KILL 100",
        "KILL SESSION 100 ",
        "KILL QID100",
        "KILL 100Q ",
        "KILL QUIERY=100 ",
    };
    for (auto test: kill_query_fail) {
        int threadId;
        BOOST_CHECK(not UserQueryType::isKill(test, threadId));
    }

    struct {
        const char* query;
        QueryId id;
    } cancel_ok[] = {
        {"CANCEL 100", 100},
        {"CAnCeL 101  ", 101},
        {"cancel \t  102  ", 102},
    };
    for (auto test: cancel_ok) {
        QueryId queryId;
        BOOST_CHECK(UserQueryType::isCancel(test.query, queryId));
        BOOST_CHECK_EQUAL(queryId, test.id);
    }

    const char* cancel_fail[] = {
        "NOT CANCLE 100",
        "CANCEL QUERY 100 ",
        "CANCEL q100",
        "cancel 100Q ",
        "cancel QUIERY=100 ",
    };
    for (auto test: cancel_fail) {
        QueryId queryId;
        BOOST_CHECK(not UserQueryType::isCancel(test, queryId));
    }

}

BOOST_AUTO_TEST_SUITE_END()
