/*
 * LSST Data Management System
 * Copyright 2012-2013 LSST Corporation.
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
  * @file GroupByClause.cc
  *
  * @brief Implementation of GroupByClause and GroupByTerm
  *
  * @author Daniel L. Wang, SLAC
  */
#include "query/GroupByClause.h"
#include <iostream>
#include <iterator>
#include <boost/make_shared.hpp>
#include "query/QueryTemplate.h"
#include "query/ValueExpr.h"

namespace lsst {
namespace qserv {
namespace query {

class GroupByTerm::render {
public:
    render(QueryTemplate& qt) : vr(qt, true) {}
    void operator()(GroupByTerm const& t) {
        vr(t._expr);
    }
    ValueExpr::render vr;
};
////////////////////////////////////////////////////////////////////////
// GroupByTerm
////////////////////////////////////////////////////////////////////////
std::ostream&
operator<<(std::ostream& os, GroupByTerm const& t) {
    os << *(t._expr);
    if(!t._collate.empty()) os << " COLLATE " << t._collate;
    return os;
}

////////////////////////////////////////////////////////////////////////
// GroupByClause
////////////////////////////////////////////////////////////////////////
std::ostream&
operator<<(std::ostream& os, GroupByClause const& c) {
    if(c._terms.get()) {
        os << "GROUP BY ";
        std::copy(c._terms->begin(),c._terms->end(),
              std::ostream_iterator<GroupByTerm>(os,", "));
    }
    return os;
}
std::string
GroupByClause::getGenerated() {
    QueryTemplate qt;
    renderTo(qt);
    return qt.dbgStr();
}

void
GroupByClause::renderTo(QueryTemplate& qt) const {
   if(_terms.get() && _terms->size() > 0) {
        List const& terms = *_terms;
        std::for_each(terms.begin(), terms.end(), GroupByTerm::render(qt));
    }
}

boost::shared_ptr<GroupByClause>
GroupByClause::copyDeep() {
    return boost::make_shared<GroupByClause>(*this); // FIXME
}

boost::shared_ptr<GroupByClause>
GroupByClause::copySyntax() {
    return boost::make_shared<GroupByClause>(*this);
}

}}} // namespace lsst::qserv::query
