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


#include <vector>


#include "query/BoolTerm.h"


namespace lsst {
namespace qserv {
namespace query {


class BoolFactorTerm;
class ColumnRef;
class ValueExpr;


/// BoolFactor is a plain factor in a BoolTerm
class BoolFactor : public BoolTerm {
public:
    BoolFactor() = default;

    BoolFactor(std::vector<std::shared_ptr<BoolFactorTerm>> const & terms, bool hasNot=false)
        : _terms(terms), _hasNot(hasNot) {}

    BoolFactor(std::shared_ptr<BoolFactorTerm> const & term, bool hasNot=false)
        : _terms({term}), _hasNot(hasNot) {}

    typedef std::shared_ptr<BoolFactor> Ptr;
    virtual char const* getName() const { return "BoolFactor"; }
    virtual OpPrecedence getOpPrecedence() const { return OTHER_PRECEDENCE; }

    void addBoolFactorTerm(std::shared_ptr<BoolFactorTerm> boolFactorTerm);

    virtual void findValueExprs(std::vector<std::shared_ptr<ValueExpr>>& vector) const;

    virtual void findColumnRefs(std::vector<std::shared_ptr<ColumnRef>>& vector) const;

    void setHasNot(bool hasNot) { _hasNot = hasNot; }

    virtual std::shared_ptr<BoolTerm> getReduced();

    virtual std::ostream& putStream(std::ostream& os) const;
    virtual void renderTo(QueryTemplate& qt) const;
    virtual std::shared_ptr<BoolTerm> clone() const;
    virtual std::shared_ptr<BoolTerm> copySyntax() const;

    bool operator==(const BoolTerm& rhs) const;

    // prepend _terms with an open parenthesis PassTerm and append it with a close parenthesis PassTerm.
    void addParenthesis();

    std::vector<std::shared_ptr<BoolFactorTerm>> _terms;
    bool _hasNot;

protected:
    void dbgPrint(std::ostream& os) const override;

private:
    bool _reduceTerms(std::vector<std::shared_ptr<BoolFactorTerm>>& newTerms,
                      std::vector<std::shared_ptr<BoolFactorTerm>>& oldTerms);
    bool _checkParen(std::vector<std::shared_ptr<BoolFactorTerm>>& terms);
};


}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_BOOLFACOTR_H
