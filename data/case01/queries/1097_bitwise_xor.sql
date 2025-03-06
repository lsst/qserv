-- test for the ^ operator

SELECT filterId,filterName,photClam,photBW FROM Filter WHERE filterId ^ 3 != 0;
