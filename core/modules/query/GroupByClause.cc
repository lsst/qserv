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
  * @brief Implementation of GroupByClause and GroupByTerm
  *
  * @author Daniel L. Wang, SLAC
  */

// Class header
#include "query/GroupByClause.h"

// System headers
#include <iostream>
#include <iterator>

// Third-party headers
#include "boost/make_shared.hpp"

// Qserv headers
#include "query/QueryTemplate.h"
#include "query/ValueExpr.h"

namespace lsst {
namespace qserv {
namespace query {

class GroupByTerm::render {
public:
    render(QueryTemplate& qt) : _vr(qt, true), _count(0) {}
    void operator()(GroupByTerm const& t) {
        if (_count++ > 0) { _vr._qt.append(","); }
        _vr(t._expr);
    }
    ValueExpr::render _vr;
    int _count;
};

////////////////////////////////////////////////////////////////////////
// GroupByTerm
////////////////////////////////////////////////////////////////////////
GroupByTerm GroupByTerm::cloneValue() const {
    GroupByTerm t;
    if(_expr) { t._expr = _expr->clone(); }
    t._collate = _collate;
    return t;
}

GroupByTerm& GroupByTerm::operator=(GroupByTerm const& gb) {
    if(this != &gb) {
        if(gb._expr) { _expr = gb._expr->clone(); }
        _collate = gb._collate;
    }
    return *this;
}

std::ostream& operator<<(std::ostream& os, GroupByTerm const& t) {
    os << *(t._expr);
    if(!t._collate.empty()) os << " COLLATE " << t._collate;
    return os;
}

////////////////////////////////////////////////////////////////////////
// GroupByClause
////////////////////////////////////////////////////////////////////////
std::ostream& operator<<(std::ostream& os, GroupByClause const& c) {
    if(c._terms.get()) {
        os << "GROUP BY ";
        std::copy(c._terms->begin(),c._terms->end(),
              std::ostream_iterator<GroupByTerm>(os,", "));
    }
    return os;
}

std::string GroupByClause::getGenerated() {
    QueryTemplate qt;
    renderTo(qt);
    return qt.dbgStr();
}

void GroupByClause::renderTo(QueryTemplate& qt) const {
   if(_terms.get() && _terms->size() > 0) {
        List const& terms = *_terms;
        std::for_each(terms.begin(), terms.end(), GroupByTerm::render(qt));
    }
}

namespace {
GroupByTerm callClone(GroupByTerm const& t) {
    return t.cloneValue();
}
}

boost::shared_ptr<GroupByClause> GroupByClause::clone() const {
    GroupByClause::Ptr p = boost::make_shared<GroupByClause>();
    std::transform(_terms->begin(), _terms->end(),
                   std::back_inserter(*p->_terms), callClone);
    return p;
}

boost::shared_ptr<GroupByClause> GroupByClause::copySyntax() {
    return boost::make_shared<GroupByClause>(*this);
}

void GroupByClause::findValueExprs(ValueExprList& list) {
    for (List::iterator i = _terms->begin(), e = _terms->end(); i != e; ++i) {
        list.push_back(i->getExpr());
    }
}

}}} // namespace lsst::qserv::query
