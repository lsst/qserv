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

#ifndef LSST_QSERV_QUERY_BOOLFACTOR_H
#define LSST_QSERV_QUERY_BOOLFACTOR_H

// System headers
#include <vector>

// Qserv headers
#include "query/BoolTerm.h"

// Forward declarations
namespace lsst { namespace qserv { namespace query {
class BoolFactorTerm;
class ColumnRef;
class ValueExpr;
}}}  // namespace lsst::qserv::query

namespace lsst { namespace qserv { namespace query {

/// BoolFactor is a plain factor in a BoolTerm.
class BoolFactor : public BoolTerm {
public:
    BoolFactor() = default;

    BoolFactor(std::vector<std::shared_ptr<BoolFactorTerm>> const& terms, bool hasNot = false)
            : _terms(terms), _hasNot(hasNot) {}

    BoolFactor(std::shared_ptr<BoolFactorTerm> const& term, bool hasNot = false)
            : _terms({term}), _hasNot(hasNot) {}

    typedef std::shared_ptr<BoolFactor> Ptr;

    /// Get the class name.
    char const* getName() const override { return "BoolFactor"; }

    /// Get the operator precidence for this class.
    OpPrecedence getOpPrecedence() const override { return OTHER_PRECEDENCE; }

    /// Add a BoolFactorTerm.
    void addBoolFactorTerm(std::shared_ptr<BoolFactorTerm> boolFactorTerm);

    /// Get a vector of the ValueExprs this contains.
    void findValueExprs(std::vector<std::shared_ptr<ValueExpr>>& vector) const override;

    /// Get a vector of references to pointers to the ValueExprs this contains.
    void findValueExprRefs(ValueExprPtrRefVector& list) override;

    /// Get a vector of the ColumnRefs this contains.
    void findColumnRefs(std::vector<std::shared_ptr<ColumnRef>>& vector) const override;

    /// Set if this term is 'not', as in `NOT <something>` in SQL.
    void setHasNot(bool hasNot) { _hasNot = hasNot; }

    /// Get the reduced form of this BoolFactor, or null if no reduction is possible.
    std::shared_ptr<BoolTerm> getReduced() override;

    /// Write a human-readable version of this instance to the ostream for debug output.
    std::ostream& putStream(std::ostream& os) const override;

    /// Serialze this instance as SQL to the QueryTemplate.
    void renderTo(QueryTemplate& qt) const override;

    /// Make a deep copy of this term.
    std::shared_ptr<BoolTerm> clone() const override;

    /// Make a shallow copy of this term.
    std::shared_ptr<BoolTerm> copySyntax() const override;

    bool operator==(const BoolTerm& rhs) const override;

    // prepend _terms with an open parenthesis PassTerm and append it with a close parenthesis PassTerm.
    void addParenthesis();

    // FIXME these members should be private, or at least protected. Jira issue DM-17306
    std::vector<std::shared_ptr<BoolFactorTerm>> _terms;
    bool _hasNot;

protected:
    /// Serialize this instance to os for debug output.
    void dbgPrint(std::ostream& os) const override;

private:
    /// Perform term reduction for `getReduced`
    bool _reduceTerms(std::vector<std::shared_ptr<BoolFactorTerm>>& newTerms,
                      std::vector<std::shared_ptr<BoolFactorTerm>>& oldTerms);

    /**
     * @brief Check if parenthesis can be removed from the vector of BoolFactorTerms.
     *
     * @param terms[in] the vector of BoolFactorTerms to check
     * @return true if the first item is an open paren, the last item is a close paren, and it's allowed to
     *         remove parens from the middle item. Otherwise, false.
     */
    bool _checkParen(std::vector<std::shared_ptr<BoolFactorTerm>>& terms);
};

}}}  // namespace lsst::qserv::query

#endif  // LSST_QSERV_QUERY_BOOLFACOTR_H
