-- Select the same column name from different tables
-- Verifies that the columns are disambiguated to coexist in the
-- results table, and re-ambiguated so that only the column name
-- appears in the results returned to the user (as is normal
-- mysql behavior)

-- pragma sortresult

SELECT Object.htmId20, Source.htmId20 FROM Object JOIN Source ON Object.objectId = Source.objectId;
