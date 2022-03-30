-- test for the modulo operator

-- pragma sortresult

select objectId, ra_PS, decl_PS from Object where ra_PS % 3 > 1.5

