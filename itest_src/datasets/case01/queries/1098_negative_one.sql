-- regression test for DM-16613
-- tests constant value -1 in e.g. 'WHERE x > -1'
-- -1 and -2 were parsing differently than other decimal integers because of 
-- placeholder lexer values TWO_DECIMAL, ONE_DECIMAL, AND ZERO_DECIMAL, which
-- are necessary for cases where the grammar uses decimal integers in quotes,
-- for example 'boolValue=('0' | '1')'. This was fixed with prioritization of
-- ONE_DECIMAL and the others with respect to DECIMAL_LITERAL in the lexer.

-- pragma sortresult

select objectId from Object where ra_PS > -1;
