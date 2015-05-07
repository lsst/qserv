// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2015 LSST Corporation.
 *
 * This product includes software developed by the
 * LSST Project (http://www.lsst.org/).
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the LSST License Statement and
 * the GNU General Public License along with this program.  If not,
 * see <http://www.lsstcorp.org/LegalNotices/>.
 */
#ifndef LSST_QSERV_PARSER_WHEREFACTORY_H
#define LSST_QSERV_PARSER_WHEREFACTORY_H
/**
  * @file
  *
  * @brief WhereFactory constructs a WhereClause that maintains parse state of
  * the WHERE clause for future interrogation, manipulation, and
  * reconstruction.
  *
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <memory>

// Third-party headers
#include <antlr/AST.hpp>

// Forward declarations
class SqlSQL2Parser;
namespace lsst {
namespace qserv {
namespace parser {
    class SelectFactory;
    class ValueExprFactory;
}
namespace query {
    class WhereClause;
}}} // End of forward declarations


namespace lsst {
namespace qserv {
namespace parser {

/// WhereFactory is a factory for WhereClause parsed elements.
class WhereFactory {
public:
    friend class SelectFactory;
    class WhereCondH;
    friend class WhereCondH;

    explicit WhereFactory(std::shared_ptr<ValueExprFactory> vf);

    std::shared_ptr<query::WhereClause> getProduct();
    static std::shared_ptr<query::WhereClause> newEmpty();
private:
    void attachTo(SqlSQL2Parser& p);
    void _import(antlr::RefAST a);
    void _addQservRestrictor(antlr::RefAST a);
    void _addOrSibs(antlr::RefAST a);

    // Fields
    std::shared_ptr<query::WhereClause> _clause;
    std::shared_ptr<ValueExprFactory> _vf;
};

}}} // namespace lsst::qserv::parser

#endif // LSST_QSERV_PARSER_WHEREFACTORY_H

