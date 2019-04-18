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


#ifndef LSST_QSERV_QUERY_COMPPREDICATE_H
#define LSST_QSERV_QUERY_COMPPREDICATE_H


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


/// CompPredicate is a Predicate involving a row value compared to another row value.
/// (literals can be row values)
class CompPredicate : public Predicate {
public:
    enum OpType {
        EQUALS_OP,                  // =
        NULL_SAFE_EQUALS_OP,        // <=>
        NOT_EQUALS_OP,              // <>
        LESS_THAN_OP,               // <
        GREATER_THAN_OP,            // >
        LESS_THAN_OR_EQUALS_OP,     // <=
        GREATER_THAN_OR_EQUALS_OP,  // >=
        NOT_EQUALS_OP_ALT,          // !=
    };

    typedef std::shared_ptr<CompPredicate> Ptr;

    CompPredicate() = default;

    /// Construct a CompPredicate that owns the given args and uses them for its expression.
    CompPredicate(std::shared_ptr<ValueExpr> const& iLeft, OpType iOp,
            std::shared_ptr<ValueExpr> const& iRight)
        : left(iLeft), op(iOp), right(iRight) {}

    ~CompPredicate() override = default;

    char const* getName() const override { return "CompPredicate"; }
    void findValueExprs(std::vector<std::shared_ptr<ValueExpr>>& vector) const override;
    void findValueExprRefs(ValueExprPtrRefVector& vector) override;
    void findColumnRefs(std::vector<std::shared_ptr<ColumnRef>>& vector) const override;
    std::ostream& putStream(std::ostream& os) const override;
    void renderTo(QueryTemplate& qt) const override;
    BoolFactorTerm::Ptr clone() const override;
    BoolFactorTerm::Ptr copySyntax() const override { return clone(); }
    bool operator==(BoolFactorTerm const& rhs) const override;

    static OpType lookupOp(char const* op);

    static const char* opTypeToStr(CompPredicate::OpType op);
    static const char* opTypeToEnumStr(CompPredicate::OpType op);

    std::shared_ptr<ValueExpr> left;
    OpType op; // Parser token type of operator
    std::shared_ptr<ValueExpr> right;

protected:
    void dbgPrint(std::ostream& os) const override;
};


}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_COMPPREDICATE_H
