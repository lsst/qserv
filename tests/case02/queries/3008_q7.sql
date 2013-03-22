SELECT objectId, ra_PS, decl_PS,
        uFlux_PS, gFlux_PS, rFlux_PS, 
        iFlux_PS, zFlux_PS, yFlux_PS 
 FROM Object
 WHERE scisql_fluxToAbMag(iFlux_PS)
       - scisql_fluxToAbMag(zFlux_PS) > 4
