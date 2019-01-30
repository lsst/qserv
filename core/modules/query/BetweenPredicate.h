// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2019 LSST Corporation.
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


#ifndef LSST_QSERV_QUERY_BETWEENPREDICATE_H
#define LSST_QSERV_QUERY_BETWEENPREDICATE_H


// Local headers
#include "query/Predicate.h"


// Forward declarations
namespace lsst {
namespace qserv {
namespace query {
    class ColumnRef;
    class ValueExpr;
    class QueryTemplate;
}}} // End of forward declarations


namespace lsst {
namespace qserv {
namespace query {


/// BetweenPredicate is a Predicate comparing a row value to a range
class BetweenPredicate : public Predicate {
public:
    typedef std::shared_ptr<BetweenPredicate> Ptr;

    BetweenPredicate() : hasNot(false) {}

    BetweenPredicate(std::shared_ptr<ValueExpr>& iValue, std::shared_ptr<ValueExpr>& iMinValue,
                     std::shared_ptr<ValueExpr>& iMaxValue, bool iHasNot)
    : value(iValue), minValue(iMinValue), maxValue(iMaxValue), hasNot(iHasNot) {}

    ~BetweenPredicate() override = default;

    char const* getName() const override { return "BetweenPredicate"; }

    void findValueExprs(std::vector<std::shared_ptr<ValueExpr>>& vector) const override;

    void findColumnRefs(std::vector<std::shared_ptr<ColumnRef>>& vector) const override;

    std::ostream& putStream(std::ostream& os) const override;

    void renderTo(QueryTemplate& qt) const override;

    BoolFactorTerm::Ptr clone() const override;

    BoolFactorTerm::Ptr copySyntax() const override { return clone(); }

    bool operator==(BoolFactorTerm const& rhs) const override;

    // FIXME these members should be private, or at least protected. Jira issue DM-17306
     std::shared_ptr<ValueExpr> value;
    std::shared_ptr<ValueExpr> minValue;
    std::shared_ptr<ValueExpr> maxValue;
    bool hasNot;

protected:
    void dbgPrint(std::ostream& os) const override;
};


}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_BETWEENPREDICATE_H
