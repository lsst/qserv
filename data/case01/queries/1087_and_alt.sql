-- test for &&

-- pragma sortresult

select objectId, iRadius_SG, ra_PS, decl_PS from Object where iRadius_SG > .5 && ra_PS < 2 && decl_PS < 3;

