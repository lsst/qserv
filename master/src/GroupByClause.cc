/* 
 * LSST Data Management System
 * Copyright 2012 LSST Corporation.
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
// X houses the implementation of 
#include "lsst/qserv/master/GroupByClause.h"
#include <iostream>
#include <iterator>
#include <boost/make_shared.hpp>
#include "lsst/qserv/master/QueryTemplate.h"
#include "lsst/qserv/master/ValueExpr.h"


namespace qMaster=lsst::qserv::master;
using lsst::qserv::master::GroupByTerm;
using lsst::qserv::master::GroupByClause;

namespace { // File-scope helpers
}

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
qMaster::operator<<(std::ostream& os, qMaster::GroupByTerm const& t) {
    os << *(t._expr);
    if(!t._collate.empty()) os << " COLLATE " << t._collate;
    return os;
}

////////////////////////////////////////////////////////////////////////
// GroupByClause
////////////////////////////////////////////////////////////////////////
std::ostream& 
qMaster::operator<<(std::ostream& os, qMaster::GroupByClause const& c) {
    if(c._terms.get()) {
        os << "GROUP BY ";
        std::copy(c._terms->begin(),c._terms->end(),
              std::ostream_iterator<qMaster::GroupByTerm>(os,", "));
    }
    return os;
}
std::string
qMaster::GroupByClause::getGenerated() {
    QueryTemplate qt;
    renderTo(qt);
    return qt.dbgStr();
}

void
qMaster::GroupByClause::renderTo(qMaster::QueryTemplate& qt) const {
   if(_terms.get() && _terms->size() > 0) {
        List const& terms = *_terms;
        std::for_each(terms.begin(), terms.end(), GroupByTerm::render(qt));
    } 
}
#if 0
    std::stringstream ss;
    if(_terms.get()) {
        std::copy(_terms->begin(), _terms->end(),
              std::ostream_iterator<qMaster::GroupByTerm>(ss,", "));
    }
    
    qt.append(ss.str()); // FIXME
}
#endif
boost::shared_ptr<GroupByClause> GroupByClause::copyDeep() {
    return boost::make_shared<GroupByClause>(*this); // FIXME
}
boost::shared_ptr<GroupByClause> GroupByClause::copySyntax() {
    return boost::make_shared<GroupByClause>(*this);
}
