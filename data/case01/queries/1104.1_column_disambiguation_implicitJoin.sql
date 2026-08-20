-- Variant of 1104_column_disambiguation.sql using an implicit join condition.

-- pragma sortresult
SELECT Object.htmId20, Source.htmId20
FROM Object
JOIN Source
WHERE Object.objectId = Source.objectId;
