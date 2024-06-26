// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2016 LSST Corporation.
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
/**
 * @file
 *
 * @author Daniel L. Wang, SLAC
 */

#ifndef LSST_QSERV_QUERY_SELECTLIST_H
#define LSST_QSERV_QUERY_SELECTLIST_H

// System headers
#include <memory>
#include <vector>

// Local headers
#include "global/stringTypes.h"
#include "query/ColumnRef.h"
#include "query/ValueExpr.h"

// Forward declarations
namespace lsst::qserv::query {
class ColumnRefNodeMap;
class ColumnAliasMap;
class QueryTemplate;
class BoolTerm;
class GroupByClause;
}  // namespace lsst::qserv::query

namespace lsst::qserv::query {

/// SelectList is the SELECT... portion of a SELECT...FROM...
/// SelectList contains a list of the ValueExprs that are representative of the
/// columns in the SELECT query's result.
class SelectList {
public:
    typedef std::shared_ptr<SelectList> Ptr;

    SelectList() : _valueExprList(std::make_shared<ValueExprPtrVector>()) {}

    // Construct a SelectList that owns the given vector of ValueExpr.
    SelectList(std::shared_ptr<ValueExprPtrVector> valueExprList) : _valueExprList(valueExprList) {}

    ~SelectList() {}

    void addStar(std::string const& table);

    void addValueExpr(ValueExprPtr const& valueExpr);

    std::string getGenerated();

    void renderTo(QueryTemplate& qt) const;

    std::shared_ptr<SelectList> clone() const;

    std::shared_ptr<SelectList> copySyntax();

    // non-const accessor for query manipulation.
    std::shared_ptr<ValueExprPtrVector> getValueExprList() const { return _valueExprList; }

    bool operator==(const SelectList& rhs);

private:
    friend std::ostream& operator<<(std::ostream& os, SelectList const& sl);
    friend std::ostream& operator<<(std::ostream& os, SelectList const* sl);

    std::shared_ptr<ValueExprPtrVector> _valueExprList;
};

}  // namespace lsst::qserv::query

#endif  // LSST_QSERV_QUERY_SELECTLIST_H
