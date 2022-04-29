// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2019 LSST Corporation.
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
 * @brief WhereClause is a parsed SQL WHERE; AreaRestrictor a spatial restrictor.
 *
 * @author Daniel L. Wang, SLAC
 */

#ifndef LSST_QSERV_QUERY_WHERECLAUSE_H
#define LSST_QSERV_QUERY_WHERECLAUSE_H

// System headers
#include <iostream>
#include <memory>
#include <stack>
#include <vector>

// Qserv headers
#include "query/typedefs.h"

// Forward declarations
namespace lsst { namespace qserv {
namespace parser {
class WhereFactory;
}
namespace query {
class AndTerm;
class AreaRestrictor;
class BoolTerm;
class ColumnRef;
class LogicalTerm;
class OrTerm;
class QueryTemplate;
class ValueExpr;
}  // namespace query
}}  // namespace lsst::qserv

namespace lsst { namespace qserv { namespace query {

/// WhereClause is a SQL WHERE containing AreaRestrictors and a BoolTerm tree.
class WhereClause {
public:
    WhereClause() = default;

    // Construct a WhereClause that has the given OrTerm as its root term.
    WhereClause(std::shared_ptr<query::OrTerm> rootOrTerm, AreaRestrictorVecPtr areaRestrictor = nullptr)
            : _rootOrTerm(rootOrTerm), _restrs(areaRestrictor) {}

    ~WhereClause() = default;

    AreaRestrictorVecPtr getRestrs() const { return _restrs; }

    /**
     * @brief Determine if there are any AreaRestrictors in the WHERE clause.
     *
     * If there are restrictors, this guarantees that getRestrs() will return a valid pointer to a vector
     * that has one or more objects.
     *
     * @return true if there are restrictors.
     * @return false if there are not restrictors.
     */
    bool hasRestrs() const { return nullptr != _restrs && not _restrs->empty(); }

    std::shared_ptr<OrTerm>& getRootTerm() { return _rootOrTerm; }

    // Set the root term of the where clause. If `term` is an OrTerm, this will be the root term. If not then
    // an OrTerm that contains term will be the root term.
    void setRootTerm(std::shared_ptr<LogicalTerm> const& term);

    void addAreaRestrictor(std::shared_ptr<AreaRestrictor> const& areaRestrictor);

    std::shared_ptr<std::vector<std::shared_ptr<ColumnRef>> const> getColumnRefs() const;
    std::shared_ptr<AndTerm> getRootAndTerm() const;

    std::string getGenerated() const;
    void renderTo(QueryTemplate& qt) const;
    std::shared_ptr<WhereClause> clone() const;
    std::shared_ptr<WhereClause> copySyntax();

    void findValueExprs(std::vector<std::shared_ptr<ValueExpr>>& list) const;
    void findValueExprRefs(ValueExprPtrRefVector& list);

    void resetRestrs();
    void prependAndTerm(std::shared_ptr<BoolTerm> t);

    bool operator==(WhereClause const& rhs) const;

private:
    std::shared_ptr<AndTerm> _addRootAndTerm();

    friend std::ostream& operator<<(std::ostream& os, WhereClause const& wc);
    friend std::ostream& operator<<(std::ostream& os, WhereClause const* wc);

    friend class parser::WhereFactory;

    std::shared_ptr<OrTerm> _rootOrTerm;
    AreaRestrictorVecPtr _restrs{std::make_shared<AreaRestrictorVec>()};
};

}}}  // namespace lsst::qserv::query

#endif  // LSST_QSERV_QUERY_WHERECLAUSE_H
