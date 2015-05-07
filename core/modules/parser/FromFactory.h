// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2014 LSST Corporation.
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
// FromFactory constructs a FromList that maintains parse state of
// the FROM clause for future interrogation, manipulation, and
// reconstruction.

#ifndef LSST_QSERV_PARSER_FROMFACTORY_H
#define LSST_QSERV_PARSER_FROMFACTORY_H
/**
  * @file
  *
  * @brief FromFactory is a factory for constructing FromLists from
  * ANTLR nodes.
  *
  * @author Daniel L. Wang, SLAC
  */

// Third-party headers
#include <antlr/AST.hpp>
#include <memory>


// Forward declarations
class SqlSQL2Parser;
namespace lsst {
namespace qserv {
namespace query {
    class FromList;
}
namespace parser {
    class BoolTermFactory;
    class ParseAliasMap;
    class ValueExprFactory;
}}} // End of forward declarations


namespace lsst {
namespace qserv {
namespace parser {

class FromFactory {
public:
    friend class SelectFactory;
    class TableRefListH;
    class TableRefAuxH;
    friend class TableRefListH;
    class RefGenerator;

    FromFactory(std::shared_ptr<ParseAliasMap> aliases,
                std::shared_ptr<ValueExprFactory> vf);

    std::shared_ptr<query::FromList> getProduct();
private:
    void attachTo(SqlSQL2Parser& p);
    void _import(antlr::RefAST a);

    std::shared_ptr<ParseAliasMap> _aliases;
    std::shared_ptr<BoolTermFactory> _bFactory;
    std::shared_ptr<query::FromList> _list;
};

}}} // namespace lsst::qserv::parser

#endif // LSST_QSERV_PARSER_FROMFACTORY_H

