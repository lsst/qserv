delimiter //
create function spdist(ra1 double, dec1 double, ra2 double, dec2 double)
returns double
begin
   declare dra  double;
   declare ddec double;
   declare a double;
   declare b double;
   declare c double;
   set dra  = radians(0.5*(ra2 - ra1));
   set ddec = radians(0.5*(dec2 - dec1));
   set a    = pow(sin(ddec), 2) + cos(radians(dec1))*cos(radians(dec2))*pow(sin(dra), 2);
   set b    = sqrt(a);
   set c    = if(b > 1, 1, b);
   return degrees(2.0*asin(c));
end
//
delimiter ;
