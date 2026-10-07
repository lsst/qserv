==============
Module "query"
==============

Overview
========
The query module contains the class definitions and implementation
required to represent a SQL query. It should be possible to faithfully
represent any Qserv-supported SQL query using only classes in this
module. Currently, this is restricted to `SELECT` queries.

The query module is independent of the SQL parser. We currently use the Hyrise
SQL parser to handle initial parsing and then convert its AST to Qserv IR via 
HyriseAdapter in the ccontrol module.


QueryContext
============
The QueryContext class exists to remember user query context, such as:
* current working database (default db)
* metadata / schema access through CSS
* resolved table / expression references
* spatial / secondary-index restrictions
* templates for generating chunk-specific SQL, required chunks
* scan information
* result merging
