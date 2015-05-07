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
#ifndef LSST_QSERV_PARSER_MODFACTORY_H
#define LSST_QSERV_PARSER_MODFACTORY_H
/**
  * @file
  *
  * @brief ModFactory constructs representations of misc. modifier clauses in
  * SQL such as ORDER BY, GROUP BY, LIMIT, and HAVING. LIMIT is assumed to only
  * permit unsigned integers.
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
namespace query {
    class GroupByClause;
    class HavingClause;
    class OrderByClause;
    class SelectFactory;
}
namespace parser {
    class ValueExprFactory;
}}} // End of forward declarations


namespace lsst {
namespace qserv {
namespace parser {

class ModFactory {
public:
    // ANTLR handlers
    friend class SelectFactory;
    class GroupByH;
    friend class GroupByH;
    class OrderByH;
    friend class OrderByH;
    class LimitH;
    friend class LimitH;
    class HavingH;
    friend class HavingH;

    ModFactory(std::shared_ptr<ValueExprFactory> vf);

    int getLimit() { return _limit; } // -1: not specified.
    std::shared_ptr<query::OrderByClause> getOrderBy() { return _orderBy; }
    std::shared_ptr<query::GroupByClause> getGroupBy() { return _groupBy; }
    std::shared_ptr<query::HavingClause> getHaving() { return _having; }

private:
    void attachTo(SqlSQL2Parser& p);
    void _importLimit(antlr::RefAST a);
    void _importOrderBy(antlr::RefAST a);
    void _importGroupBy(antlr::RefAST a);
    void _importHaving(antlr::RefAST a);

    // Fields
    std::shared_ptr<ValueExprFactory> _vFactory;
    int _limit;
    std::shared_ptr<query::OrderByClause> _orderBy;
    std::shared_ptr<query::GroupByClause> _groupBy;
    std::shared_ptr<query::HavingClause> _having;
};

}}} // namespace lsst::qserv::parser

#endif // LSST_QSERV_PARSER_MODFACTORY_H
