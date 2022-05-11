/*
 * This file is part of qserv.
 *
 * Developed for the LSST Data Management System.
 * This product includes software developed by the LSST Project
 * (https://www.lsst.org).
 * See the COPYRIGHT file at the top-level directory of this distribution
 * for details of code ownership.
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
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

#ifndef LSST_QSERV_VALUE_EXPR_PREDICATE_H
#define LSST_QSERV_VALUE_EXPR_PREDICATE_H

// System headers
#include <memory>
#include <string>

// Local headers
#include "query/Predicate.h"

namespace lsst::qserv::query {
class ValueExpr;
}

namespace lsst::qserv::query {

class ValueExprPredicate : public Predicate {
public:
    typedef std::shared_ptr<ValueExprPredicate> Ptr;

    ValueExprPredicate(std::shared_ptr<ValueExpr> const& ve) : _valueExpr(ve) {}

    ~ValueExprPredicate() override = default;

    char const* getName() const override { return "ValueExprPredicate"; }

    friend std::ostream& operator<<(std::ostream& os, ValueExprPredicate const& bt);

    /// Make a deep copy of this term.
    BoolFactorTerm::Ptr clone() const override;

    /// Make a shallow copy of this term.
    BoolFactorTerm::Ptr copySyntax() const override;

    /// Write a human-readable version of this instance to the ostream for debug output.
    std::ostream& putStream(std::ostream& os) const override;

    /// Serialze this instance as SQL to the QueryTemplate.
    void renderTo(QueryTemplate& qt) const override;

    /// Get a vector of the ValueExprs this contains.
    void findValueExprs(std::vector<std::shared_ptr<ValueExpr>>& vector) const override;

    /// Get a vector of references to pointers to the ValueExprs this contains.
    void findValueExprRefs(ValueExprPtrRefVector& vector) override;

    /// Get a vector of the ColumnRefs this contains.
    void findColumnRefs(std::vector<std::shared_ptr<ColumnRef>>& vector) const override;

    bool operator==(BoolFactorTerm const& rhs) const override;

private:
    void dbgPrint(std::ostream& os) const;

    std::shared_ptr<ValueExpr> _valueExpr;
};

}  // namespace lsst::qserv::query

#endif  // LSST_QSERV_VALUE_EXPR_PREDICATE_H
