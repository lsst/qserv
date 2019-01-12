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


#include "query/BoolFactor.h"


#include "query/BoolFactorTerm.h"
#include "query/BoolTermFactor.h"
#include "query/ColumnRef.h"
#include "query/CopyTerms.h"
#include "query/PassTerm.h"
#include "query/QueryTemplate.h"
#include "query/ValueExpr.h"
#include "util/IterableFormatter.h"


namespace lsst {
namespace qserv {
namespace query {


std::ostream& BoolFactor::putStream(std::ostream& os) const {
    return QueryTemplate::renderDbg(os, *this);
}


void BoolFactor::renderTo(QueryTemplate& qt) const {
    std::string s;
    if (_hasNot) {
        qt.append("NOT");
    }
    renderList(qt, _terms, s);
}


bool BoolFactor::_reduceTerms(std::vector<std::shared_ptr<BoolFactorTerm>>& newTerms,
                              std::vector<std::shared_ptr<BoolFactorTerm>>& oldTerms) {
    bool hasReduction = false;
    for (auto&& term : oldTerms) {
        auto btf = std::dynamic_pointer_cast<BoolTermFactor>(term);
        if (btf) {
            if (btf->_term) {
                std::shared_ptr<BoolTerm> reduced = btf->_term->getReduced();
                if (reduced) {
                    auto f = std::dynamic_pointer_cast<BoolFactor>(reduced);
                    if (f) {
                        // factor in a term in a factor --> factor
                        newTerms.insert(newTerms.end(), f->_terms.begin(), f->_terms.end());
                        hasReduction = true;
                    } else {
                        // still a reduction in the term, replace
                        std::shared_ptr<BoolTermFactor> newBtf;
                        newBtf = std::make_shared<BoolTermFactor>();
                        newBtf->_term = reduced;
                        newTerms.push_back(newBtf);
                        hasReduction = true;
                    }
                } else {
                    // The bfterm's term couldn't be reduced, so just add it.
                    newTerms.push_back(term);
                }
            } else {
                // Term-less bool term factor. Ignore.
                hasReduction = true;
            }
        } else {
            // add old bfterm
            newTerms.push_back(term);
        }
    }
    return hasReduction;
}


bool BoolFactor::_checkParen(std::vector<std::shared_ptr<BoolFactorTerm>>& terms) {
    if (terms.size() != 3) { return false; }

    PassTerm* pt = dynamic_cast<PassTerm*>(terms.front().get());
    if (!pt || (pt->_text != "(")) { return false; }

    pt = dynamic_cast<PassTerm*>(terms.back().get());
    if (!pt || (pt->_text != ")")) { return false; }

    auto boolTermFactorPtr = std::dynamic_pointer_cast<BoolTermFactor>(terms[1]);
    if (nullptr == boolTermFactorPtr) {
        return true;
    }
    auto logicalTermPtr = std::dynamic_pointer_cast<LogicalTerm>(boolTermFactorPtr->_term);
    if (nullptr != logicalTermPtr) {
        return false; // don't remove parens from an AND or an OR.
    }

    return true;
}


std::shared_ptr<BoolTerm> BoolFactor::getReduced() {
    // Get reduced versions of my children.
    BoolFactorTerm::PtrVector newTerms;
    bool hasReduction = false;
    hasReduction = _reduceTerms(newTerms, _terms);
    // Parentheses reduction
    if (_checkParen(newTerms)) {
        newTerms.erase(newTerms.begin());
        newTerms.pop_back();
        hasReduction = true;
    }
    if (hasReduction) {
        return std::make_shared<BoolFactor>(newTerms, _hasNot);
    } else {
        return std::shared_ptr<BoolTerm>();
    }
}


std::shared_ptr<BoolTerm> BoolFactor::clone() const {
    std::shared_ptr<BoolFactor> t = std::make_shared<BoolFactor>();
    t->_hasNot = _hasNot;
    copyTerms<BoolFactorTerm::PtrVector, deepCopy>(t->_terms, _terms);
    return t;
}


std::shared_ptr<BoolTerm> BoolFactor::copySyntax() const {
    std::shared_ptr<BoolFactor> bf = std::make_shared<BoolFactor>();
    bf->_hasNot = _hasNot;
    copyTerms<BoolFactorTerm::PtrVector, syntaxCopy>(bf->_terms, _terms);
    return bf;
}


void BoolFactor::dbgPrint(std::ostream& os) const {
    os << "BoolFactor(" << util::printable(_terms);
    if (_hasNot) {
        os << ", has NOT";
    }
    os << ")";
}


void BoolFactor::addBoolFactorTerm(std::shared_ptr<BoolFactorTerm> boolFactorTerm) {
    _terms.push_back(boolFactorTerm);
}


void BoolFactor::findValueExprs(std::vector<std::shared_ptr<ValueExpr>>& vector) const {
    for (auto&& boolFactorTerm : _terms) {
        if (boolFactorTerm) {
            boolFactorTerm->findValueExprs(vector);
        }
    }
}


void BoolFactor::findColumnRefs(std::vector<std::shared_ptr<ColumnRef>>& vector) const {
    for (auto&& boolFactorTerm : _terms) {
        if (boolFactorTerm) {
            boolFactorTerm->findColumnRefs(vector);
        }
    }
}


bool BoolFactor::operator==(const BoolTerm& rhs) const {
    auto rhsBoolFactor = dynamic_cast<const BoolFactor*>(&rhs);
    if (nullptr == rhsBoolFactor) {
        return false;
    }
    if (_hasNot != rhsBoolFactor->_hasNot) {
        return false;
    }
    return util::vectorPtrCompare<BoolFactorTerm>(_terms, rhsBoolFactor->_terms);
}


// prepend _terms with an open parenthesis PassTerm and append it with a close parenthesis PassTerm.
void BoolFactor::addParenthesis() {
    auto leftParen = std::make_shared<PassTerm>("(");
    auto rightParen = std::make_shared<PassTerm>(")");
    _terms.insert(_terms.begin(), leftParen);
    _terms.push_back(rightParen);
}


}}} // namespace lsst::qserv::query
