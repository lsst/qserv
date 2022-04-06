// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2017 AURA/LSST.
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
  * @brief WhereClause is a parse element construct for SQL WHERE.
  *
  * @author Daniel L. Wang, SLAC
  */


// Class header
#include "query/WhereClause.h"

// System headers
#include <algorithm>
#include <iostream>
#include <memory>
#include <stdexcept>

// Qserv headers
#include "query/AndTerm.h"
#include "query/AreaRestrictor.h"
#include "query/BoolTerm.h"
#include "query/BoolFactor.h"
#include "query/ColumnRef.h"
#include "query/LogicalTerm.h"
#include "query/OrTerm.h"
#include "query/Predicate.h"
#include "query/QueryTemplate.h"
#include "query/typedefs.h"
#include "util/Bug.h"
#include "util/PointerCompare.h"
#include "util/IterableFormatter.h"


namespace lsst {
namespace qserv {
namespace query {


std::ostream&
operator<<(std::ostream& os, WhereClause const& wc) {
    os << "WhereClause(" << wc._rootOrTerm;
    if (nullptr != wc._restrs && !wc._restrs->empty()) {
        os << ", " << util::ptrPrintable(wc._restrs, "", "");
    }
    os << ")";
    return os;
}


std::ostream&
operator<<(std::ostream& os, WhereClause const* wc) {
    (nullptr == wc) ? os << "nullptr" : os << *wc;
    return os;
}


void findColumnRefs(std::shared_ptr<BoolFactor> f, ColumnRef::Vector& vector) {
    if (f) {
        f->findColumnRefs(vector);
    }
}


void findColumnRefs(std::shared_ptr<BoolTerm> t, ColumnRef::Vector& vector) {
    if (!t) { return; }
    BoolTerm::PtrVector::iterator i = t->iterBegin();
    BoolTerm::PtrVector::iterator e = t->iterEnd();
    if (i == e) { // Leaf.
        // Bool factor?
        std::shared_ptr<BoolFactor> bf = std::dynamic_pointer_cast<BoolFactor>(t);
        if (bf) {
            findColumnRefs(bf, vector);
        } else {
            std::ostringstream os;
            t->putStream(os);
            throw util::Bug(ERR_LOC, std::string("Unexpected non BoolFactor in BoolTerm(")
                      + t->getName()
                      + "): " + os.str());
        }
    } else {
        for(; i != e; ++i) {
            findColumnRefs(*i, vector); // Recurse
        }
    }
}


void WhereClause::setRootTerm(std::shared_ptr<LogicalTerm> const& term) {
    auto orTerm = std::dynamic_pointer_cast<OrTerm>(term);
    if (nullptr == orTerm) {
        orTerm = std::make_shared<OrTerm>(term);
    }
    _rootOrTerm = orTerm;
}


void WhereClause::addAreaRestrictor(std::shared_ptr<AreaRestrictor> const& areaRestrictor) {
    _restrs->push_back(areaRestrictor);
}


std::shared_ptr<AndTerm> WhereClause::getRootAndTerm() const
{
    // Find the global AND. If an OR term is root and has multiple terms, there is no global AND which means
    // we should return NULL.
    if (nullptr == _rootOrTerm) {
        return nullptr;
    }
    if (_rootOrTerm->_terms.size() != 1) {
        return nullptr;
    }
    auto andTerm = std::dynamic_pointer_cast<AndTerm>(_rootOrTerm->_terms.front());
    return andTerm;
}


void
WhereClause::prependAndTerm(std::shared_ptr<BoolTerm> t) {
    // Find the global AndTerm and add the new BoolTerm to its terms. If the new BoolTerm is an instance of
    // AndTerm, merge its terms instead of adding it to the AndTerm's terms.
    // If a global AndTerm can not be found then throw; this query can not be handled.
    if (nullptr == _rootOrTerm) {
        _rootOrTerm = std::make_shared<OrTerm>();
    }

    std::shared_ptr<AndTerm> andTerm;
    if (_rootOrTerm->_terms.size() == 0) {
        andTerm = std::make_shared<AndTerm>();
        _rootOrTerm->addBoolTerm(andTerm);
    } else if (_rootOrTerm->_terms.size() == 1) {
        andTerm = std::dynamic_pointer_cast<AndTerm>(_rootOrTerm->_terms[0]);
        if (nullptr == andTerm) {
            throw std::logic_error("Term of first OR term is not an AND term; there is no global AND term");
        }
    }
    else {
        throw std::logic_error("There is more than term in the root OR term; can't pick a global AND term");
    }

    if (!andTerm->merge(*t, AndTerm::PREPEND)) {
        andTerm->_terms.insert(andTerm->_terms.begin(), t);
    }
}


std::shared_ptr<ColumnRef::Vector const>
WhereClause::getColumnRefs() const {
    std::shared_ptr<ColumnRef::Vector> vector = std::make_shared<ColumnRef::Vector>();

    // Idea: Walk the expression tree and add all column refs to the
    // list. We will walk in depth-first order, but the interface spec
    // doesn't require any particular order.
    findColumnRefs(_rootOrTerm, *vector);

    return vector;
}


void WhereClause::findValueExprs(ValueExprPtrVector& vector) const {
    if (_rootOrTerm) { _rootOrTerm->findValueExprs(vector); }
}


void WhereClause::findValueExprRefs(ValueExprPtrRefVector& list) {
    if (_rootOrTerm) { _rootOrTerm->findValueExprRefs(list); }
}


std::string
WhereClause::getGenerated() const {
    QueryTemplate qt;
    renderTo(qt);
    return qt.sqlFragment();
}


void WhereClause::renderTo(QueryTemplate& qt) const {
    if (_restrs != nullptr) {
        for (auto& restrictor : *_restrs) {
            restrictor->renderTo(qt);
        }
    }
    if (nullptr != _rootOrTerm) {
        _rootOrTerm->renderTo(qt);
    }
}


std::shared_ptr<WhereClause> WhereClause::clone() const {
    // FIXME
    std::shared_ptr<WhereClause> newC = std::make_shared<WhereClause>(*this);
    // Shallow copy of expr list is okay.
    if (nullptr != _rootOrTerm) {
        newC->_rootOrTerm = _rootOrTerm->copy();
    }
    if (nullptr != _restrs) {
        newC->_restrs = std::make_shared<AreaRestrictorVec>(*_restrs);
    }
    // For the other fields, default-copied versions are okay.
    return newC;

}


std::shared_ptr<WhereClause> WhereClause::copySyntax() {
    std::shared_ptr<WhereClause> newC = std::make_shared<WhereClause>(*this);
    // Shallow copy of expr list is okay.
    if (nullptr != _rootOrTerm) {
        newC->_rootOrTerm = _rootOrTerm->copy();
    }
    // For the other fields, default-copied versions are okay.
    return newC;
}


std::shared_ptr<AndTerm> WhereClause::_addRootAndTerm() {
    if (nullptr == _rootOrTerm) {
        _rootOrTerm = std::make_shared<OrTerm>();
    } else if (_rootOrTerm->_terms.size() != 0) {
        throw std::logic_error("Can not add root AND term."); // expected 0 or 1 items in _terms
    }
    auto andTerm = std::make_shared<AndTerm>();
    _rootOrTerm->addBoolTerm(andTerm);
    return andTerm;
}


bool WhereClause::operator==(WhereClause const& rhs) const {
    return (util::ptrCompare<BoolTerm>(_rootOrTerm, rhs._rootOrTerm) &&
            util::ptrVectorPtrCompare<AreaRestrictor>(_restrs, rhs._restrs));
}


void WhereClause::resetRestrs() {
    _restrs = std::make_shared<AreaRestrictorVec>();
}


}}} // namespace lsst::qserv::query
