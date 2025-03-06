-- find columns whose value is NULL using the null safe
-- equals operator, `<=>`


-- pragma sortresult

select objectId from Object where raRange <=> NULL;

