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
#ifndef LSST_QSERV_QUERY_SELECTLIST_H
#define LSST_QSERV_QUERY_SELECTLIST_H
/**
  * @file SelectList.h
  *
  * @author Daniel L. Wang, SLAC
  */
#include <list>
#include <map>
#include <deque>
#include <boost/shared_ptr.hpp>

#include "query/ColumnRef.h"
#include "query/ValueExpr.h"

namespace lsst {
namespace qserv {

namespace parser {
    // Forward
    class SelectListFactory;
}

namespace query {

// Forward
class ColumnRefNodeMap;
class ColumnAliasMap;
class QueryTemplate;
class BoolTerm;
class GroupByClause;

/// SelectList is the SELECT... portion of a SELECT...FROM...
/// SelectList contains a list of the ValueExprs that are representative of the
/// columns in the SELECT query's result.
class SelectList {
public:
    typedef boost::shared_ptr<SelectList> Ptr;

    SelectList()
        : _valueExprList(new ValueExprList())
        {}
    ~SelectList() {}
    void addStar(std::string const& table);
    void dbgPrint(std::ostream& os) const;

    std::string getGenerated();
    void renderTo(QueryTemplate& qt) const;
    boost::shared_ptr<SelectList> clone();
    boost::shared_ptr<SelectList> copySyntax();

    // non-const accessor for query manipulation.
    boost::shared_ptr<ValueExprList> getValueExprList()
        { return _valueExprList; }

    friend class parser::SelectListFactory;
private:
    friend std::ostream& operator<<(std::ostream& os, SelectList const& sl);
    boost::shared_ptr<ValueExprList> _valueExprList;
    boost::shared_ptr<ColumnRefNodeMap const> _aliasMap;
};

}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_SELECTLIST_H
