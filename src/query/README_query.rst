==============
Module "query"
==============

Overview
========
The query module contains the class definitions and implementation
required to represent a SQL query. It should be possible to faithfully
represent any Qserv-supported SQL query using only classes in this
module. 

The query module does not depend on the parser, so you should be able
to construct and manipulate query representations without dependence on
ANTLR. One exception to this is the re-use of parse-token types. Some
query constructs include a field that references tokenids in the
parser space--instead of creating a token ID space in parallel with
the one needed for ANTLR's parse vocabulary, we have reused the ANTLR
vocabulary to produce token ID values for use in the query
representation. We have placed the vocabulary definition in this
package (query): the parser makes use of a copy of this definition,
and the query module (and its clients) make use of a C++ header
produced from this definition.


QueryContext
============
Classes in query do not generally represent any persistent state. The
QueryContext class exists to remember user query context, such as
incoming query scope restrictions, and current working database. 

Right now, it also stores information used for query analysis,
dispatch, and execution, but we are planning to split off those
fields.
