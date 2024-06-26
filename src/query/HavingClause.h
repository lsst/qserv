// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2015 LSST Corporation.
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

#ifndef LSST_QSERV_QUERY_HAVINGCLAUSE_H
#define LSST_QSERV_QUERY_HAVINGCLAUSE_H

// System headers
#include <memory>

// Local headers
#include "query/BoolTerm.h"

// Forward declarations
namespace lsst::qserv::query {
class BoolTerm;
class QueryTemplate;
}  // namespace lsst::qserv::query

namespace lsst::qserv::query {

/// HavingClause: a representation of SQL HAVING. Support for this construct is
/// incomplete.
class HavingClause {
public:
    HavingClause() {}
    ~HavingClause() {}

    HavingClause(std::shared_ptr<BoolTerm> const& tree) : _tree(tree) {}

    std::string getGenerated() const;
    void renderTo(QueryTemplate& qt) const;
    std::shared_ptr<HavingClause> clone() const;
    std::shared_ptr<HavingClause> copySyntax();
    void findValueExprs(ValueExprPtrVector& list) const;
    void findValueExprRefs(ValueExprPtrRefVector& list);

    bool operator==(const HavingClause& rhs) const;

private:
    friend std::ostream& operator<<(std::ostream& os, HavingClause const& h);
    friend std::ostream& operator<<(std::ostream& os, HavingClause const* h);
    std::shared_ptr<BoolTerm> _tree;
};

}  // namespace lsst::qserv::query

#endif  // LSST_QSERV_QUERY_HAVINGCLAUSE_H
