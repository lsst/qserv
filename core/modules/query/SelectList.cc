// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2015 AURA/LSST.
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
  * @brief Implementation of a SelectList
  *
  * @author Daniel L. Wang, SLAC
  */
// SelectList design notes:
// Idea was to have this as an intermediate query tree representation.
// This might be practical through the use of factories to hide enough
// of the ANTLR-specific parts. Because we have inserted nodes in the
// ANTLR tree, node navigation should be sensible enough that the
// ANTLR-specific complexity can be minimized to only a dependence on
// the tree node structure.

// Should we keep a hash table when column refs are detected, so we can
// map them?
// For now, just build the syntax tree without evaluating.

// Class header
#include "query/SelectList.h"

// System headers
#include <algorithm>
#include <iostream>
#include <iterator>
#include <stdexcept>
#include <string>

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "query/QueryTemplate.h"
#include "query/typedefs.h"
#include "query/ValueFactor.h"

namespace lsst {
namespace qserv {
namespace query {

LOG_LOGGER SelectList::_logger = LOG_GET("lsst.qserv.query.SelectList");

template <typename T>
struct renderWithSep {
    renderWithSep(QueryTemplate& qt_, std::string const& sep_)
        : qt(qt_),sep(sep_),count(0) {}
    void operator()(T const& t) {
        if(++count > 1) qt.append(sep);
    }
    QueryTemplate& qt;
    std::string sep;
    int count;

};

void
SelectList::addStar(std::string const& table) {
    if(!_valueExprList) {
        throw std::logic_error("Corrupt SelectList object");
    }

    ValueExprPtr ve;
    ve = ValueExpr::newSimple(ValueFactor::newStarFactor(table));
    _valueExprList->push_back(ve);
}

void
SelectList::dbgPrint(std::ostream& os) const {
    if(!_valueExprList) {
        throw std::logic_error("Corrupt SelectList object");
    }
    for(ValueExprPtrVectorConstIter viter = _valueExprList->begin(),
        e = _valueExprList->end();
        viter != e;
        ++viter) {
        (*viter)->dbgPrint(os);
    }
}

std::ostream&
operator<<(std::ostream& os, SelectList const& sl) {
    os << "SELECT ";
    std::copy(sl._valueExprList->begin(), sl._valueExprList->end(),
                  std::ostream_iterator<ValueExprPtr>(os,", "));
    os << "(FIXME)";
    return os;
}

std::string
SelectList::getGenerated() {
    QueryTemplate qt;
    renderTo(qt);
    return qt.toString();
}

void
SelectList::renderTo(QueryTemplate& qt) const {
    std::for_each(_valueExprList->begin(), _valueExprList->end(),
                  ValueExpr::render(qt, true));

}
std::shared_ptr<SelectList> SelectList::clone() const {
    std::shared_ptr<SelectList> newS = std::make_shared<SelectList>(*this);
    newS->_valueExprList = std::make_shared<ValueExprPtrVector>();
    cloneValueExprPtrVector(*(newS->_valueExprList), *_valueExprList);
    // For the other fields, default-copied versions are okay.
    return newS;
}

std::shared_ptr<SelectList> SelectList::copySyntax() {
    std::shared_ptr<SelectList> newS = std::make_shared<SelectList>(*this);
    // Shallow copy of expr list is okay.
    newS->_valueExprList = std::make_shared<ValueExprPtrVector>(*_valueExprList);
    // For the other fields, default-copied versions are okay.
    return newS;
}

}}} // namespace lsst::qserv::query
