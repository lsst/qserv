SELECT uFlux_PS, gFlux_PS, rFlux_PS, 
       iFlux_PS, zFlux_PS, yFlux_PS
FROM   Object 
WHERE  (objectId % 100 ) = 57
ORDER BY uFlux_PS, gFlux_PS, rFlux_PS,
         iFlux_PS, zFlux_PS, yFlux_PS;
