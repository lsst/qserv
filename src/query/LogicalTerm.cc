// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2019 AURA/LSST.
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

// Class header
#include "query/LogicalTerm.h"

// Qserv headers
#include "query/BoolFactor.h"
#include "query/ColumnRef.h"
#include "query/QueryTemplate.h"

namespace lsst { namespace qserv { namespace query {

std::ostream& LogicalTerm::putStream(std::ostream& os) const { return QueryTemplate::renderDbg(os, *this); }

std::shared_ptr<BoolTerm> LogicalTerm::getReduced() {
    // Can I eliminate myself?
    if (_terms.size() == 1) {
        std::shared_ptr<BoolTerm> reduced = _terms.front()->getReduced();
        if (reduced) {
            return reduced;
        } else {
            return _terms.front();
        }
    } else {  // Get reduced versions of my children.
        // FIXME: Apply reduction on each term.
        // If reduction was successful on any child, construct a new LogicalTerm of the same subclass type
        // (AndTerm, OrTerm, etc).
    }
    return std::shared_ptr<BoolTerm>();
}

void LogicalTerm::addBoolTerm(std::shared_ptr<BoolTerm> boolTerm) { _terms.push_back(boolTerm); }

void LogicalTerm::setBoolTerms(std::vector<std::shared_ptr<BoolTerm>> const& terms) { _terms = terms; }

void LogicalTerm::setBoolTerms(std::vector<std::shared_ptr<BoolFactor>> const& terms) {
    std::copy(terms.begin(), terms.end(), std::back_inserter(_terms));
}

void LogicalTerm::findValueExprs(std::vector<std::shared_ptr<ValueExpr>>& vector) const {
    for (auto&& boolTerm : _terms) {
        if (boolTerm) {
            boolTerm->findValueExprs(vector);
        }
    }
}

void LogicalTerm::findValueExprRefs(ValueExprPtrRefVector& vector) {
    for (auto&& boolTerm : _terms) {
        if (boolTerm) {
            boolTerm->findValueExprRefs(vector);
        }
    }
}

void LogicalTerm::findColumnRefs(std::vector<std::shared_ptr<ColumnRef>>& vector) const {
    for (auto&& boolTerm : _terms) {
        if (boolTerm) {
            boolTerm->findColumnRefs(vector);
        }
    }
}

}}}  // namespace lsst::qserv::query
