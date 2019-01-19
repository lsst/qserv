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


#ifndef LSST_QSERV_QUERY_LIKEPREDICATE_H
#define LSST_QSERV_QUERY_LIKEPREDICATE_H


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


/// LikePredicate is a Predicate involving a row value compared to a pattern
/// (pattern is a char-valued value expression
class LikePredicate : public Predicate {
public:
    typedef std::shared_ptr<LikePredicate> Ptr;

    ~LikePredicate()  override = default;

    char const* getName() const override { return "LikePredicate"; }
    void findValueExprs(std::vector<std::shared_ptr<ValueExpr>>& vector) const override;
    void findColumnRefs(std::vector<std::shared_ptr<ColumnRef>>& vector) const override;
    std::ostream& putStream(std::ostream& os) const override;
    void renderTo(QueryTemplate& qt) const override;
    BoolFactorTerm::Ptr clone() const override;
    BoolFactorTerm::Ptr copySyntax() const override { return clone(); }
    bool operator==(const BoolFactorTerm& rhs) const override;

    std::shared_ptr<ValueExpr> value;
    std::shared_ptr<ValueExpr> charValue;
    bool hasNot;

protected:
    void dbgPrint(std::ostream& os) const override;
};


}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_LIKEPREDICATE_H
