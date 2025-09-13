-- Find an object with a particular object id
-- http://dev.lsstcorp.org/trac/wiki/dbQuery009

-- note that the data for mysql version is specially
-- precooked for this object to include chunkId and
-- subchunkId

-- pragma sortresult
SELECT objectId,iauId,ra_PS,ra_PS_Sigma,decl_PS,decl_PS_Sigma,radecl_PS_Cov,htmId20,ra_SG,ra_SG_Sigma,decl_SG,decl_SG_Sigma,
       radecl_SG_Cov,raRange,declRange,muRa_PS,muRa_PS_Sigma,muDecl_PS,muDecl_PS_Sigma,muRaDecl_PS_Cov,parallax_PS,
	   parallax_PS_Sigma,canonicalFilterId,extendedness,varProb,earliestObsTime,latestObsTime,meanObsTime,flags,uNumObs,
	   uExtendedness,uVarProb,uRaOffset_PS,uRaOffset_PS_Sigma,uDeclOffset_PS,uDeclOffset_PS_Sigma,uRaDeclOffset_PS_Cov,
	   uRaOffset_SG,uRaOffset_SG_Sigma,uDeclOffset_SG,uDeclOffset_SG_Sigma,uRaDeclOffset_SG_Cov,uLnL_PS,uLnL_SG,uFlux_PS,
	   uFlux_PS_Sigma,uFlux_ESG,uFlux_ESG_Sigma,uFlux_Gaussian,uFlux_Gaussian_Sigma,uTimescale,uEarliestObsTime,uLatestObsTime,
	   uSersicN_SG,uSersicN_SG_Sigma,uE1_SG,uE1_SG_Sigma,uE2_SG,uE2_SG_Sigma,uRadius_SG,uRadius_SG_Sigma,uFlags,gNumObs,
	   gExtendedness,gVarProb,gRaOffset_PS,gRaOffset_PS_Sigma,gDeclOffset_PS,gDeclOffset_PS_Sigma,gRaDeclOffset_PS_Cov,
	   gRaOffset_SG,gRaOffset_SG_Sigma,gDeclOffset_SG,gDeclOffset_SG_Sigma,gRaDeclOffset_SG_Cov,gLnL_PS,gLnL_SG,gFlux_PS,
	   gFlux_PS_Sigma,gFlux_ESG,gFlux_ESG_Sigma,gFlux_Gaussian,gFlux_Gaussian_Sigma,gTimescale,gEarliestObsTime,
	   gLatestObsTime,gSersicN_SG,gSersicN_SG_Sigma,gE1_SG,gE1_SG_Sigma,gE2_SG,gE2_SG_Sigma,gRadius_SG,gRadius_SG_Sigma,
	   gFlags,rNumObs,rExtendedness,rVarProb,rRaOffset_PS,rRaOffset_PS_Sigma,rDeclOffset_PS,rDeclOffset_PS_Sigma,
	   rRaDeclOffset_PS_Cov,rRaOffset_SG,rRaOffset_SG_Sigma,rDeclOffset_SG,rDeclOffset_SG_Sigma,rRaDeclOffset_SG_Cov,rLnL_PS,
	   rLnL_SG,rFlux_PS,rFlux_PS_Sigma,rFlux_ESG,rFlux_ESG_Sigma,rFlux_Gaussian,rFlux_Gaussian_Sigma,rTimescale,
	   rEarliestObsTime,rLatestObsTime,rSersicN_SG,rSersicN_SG_Sigma,rE1_SG,rE1_SG_Sigma,rE2_SG,rE2_SG_Sigma,rRadius_SG,
	   rRadius_SG_Sigma,rFlags,iNumObs,iExtendedness,iVarProb,iRaOffset_PS,iRaOffset_PS_Sigma,iDeclOffset_PS,
	   iDeclOffset_PS_Sigma,iRaDeclOffset_PS_Cov,iRaOffset_SG,iRaOffset_SG_Sigma,iDeclOffset_SG,iDeclOffset_SG_Sigma,
	   iRaDeclOffset_SG_Cov,iLnL_PS,iLnL_SG,iFlux_PS,iFlux_PS_Sigma,iFlux_ESG,iFlux_ESG_Sigma,iFlux_Gaussian,
	   iFlux_Gaussian_Sigma,iTimescale,iEarliestObsTime,iLatestObsTime,iSersicN_SG,iSersicN_SG_Sigma,iE1_SG,iE1_SG_Sigma,
	   iE2_SG,iE2_SG_Sigma,iRadius_SG,iRadius_SG_Sigma,iFlags,zNumObs,zExtendedness,zVarProb,zRaOffset_PS,zRaOffset_PS_Sigma,
	   zDeclOffset_PS,zDeclOffset_PS_Sigma,zRaDeclOffset_PS_Cov,zRaOffset_SG,zRaOffset_SG_Sigma,zDeclOffset_SG,
	   zDeclOffset_SG_Sigma,zRaDeclOffset_SG_Cov,zLnL_PS,zLnL_SG,zFlux_PS,zFlux_PS_Sigma,zFlux_ESG,zFlux_ESG_Sigma,
	   zFlux_Gaussian,zFlux_Gaussian_Sigma,zTimescale,zEarliestObsTime,zLatestObsTime,zSersicN_SG,zSersicN_SG_Sigma,zE1_SG,
	   zE1_SG_Sigma,zE2_SG,zE2_SG_Sigma,zRadius_SG,zRadius_SG_Sigma,zFlags,yNumObs,yExtendedness,yVarProb,yRaOffset_PS,
	   yRaOffset_PS_Sigma,yDeclOffset_PS,yDeclOffset_PS_Sigma,yRaDeclOffset_PS_Cov,yRaOffset_SG,yRaOffset_SG_Sigma,
	   yDeclOffset_SG,yDeclOffset_SG_Sigma,yRaDeclOffset_SG_Cov,yLnL_PS,yLnL_SG,yFlux_PS,yFlux_PS_Sigma,yFlux_ESG,
	   yFlux_ESG_Sigma,yFlux_Gaussian,yFlux_Gaussian_Sigma,yTimescale,yEarliestObsTime,yLatestObsTime,ySersicN_SG,
	   ySersicN_SG_Sigma,yE1_SG,yE1_SG_Sigma,yE2_SG,yE2_SG_Sigma,yRadius_SG,yRadius_SG_Sigma,yFlags,
	   varBinaryField
FROM   Object
WHERE  objectId = 430213989148129
