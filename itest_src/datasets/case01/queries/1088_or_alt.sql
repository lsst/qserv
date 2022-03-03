-- test for ||

-- pragma sortresult

select objectId from Object where objectId < 400000000000000 || objectId > 430000000000000 ORDER BY objectId;

