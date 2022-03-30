-- Used to reproduce DM-15491: Parenthesis are ignored in the WHERE clause of qserv queries

select objectId, ra_PS 
from Object 
where ra_PS > 359.5 and (objectId = 417853073271391 or  objectId = 399294519599888)

