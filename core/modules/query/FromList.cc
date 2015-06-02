// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2015 AURA/LSST.
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
  * @brief Implementation of FromList
  *
  * @author Daniel L. Wang, SLAC
  */

// Class header
#include "query/FromList.h"

// System headers
#include <algorithm>
#include <iterator>

// Third-party headers

namespace lsst {
namespace qserv {
namespace query {

std::ostream&
operator<<(std::ostream& os, FromList const& fl) {
    os << "FROM ";
    if(fl._tableRefs.get() && fl._tableRefs->size() > 0) {
        TableRefList const& refList = *(fl._tableRefs);
        std::copy(refList.begin(), refList.end(),
                  std::ostream_iterator<TableRef::Ptr>(os,", "));
    } else {
        os << "(empty)";
    }
    return os;
}

bool
FromList::isJoin() const {
    if(_tableRefs) {
        int count = 0;
        typedef TableRefList::const_iterator Iter;
        for(Iter i=_tableRefs->begin(), e=_tableRefs->end();
            i != e;
            ++i) {

            if(*i) {
                if((**i).isSimple()) { ++count; }
            } else {
                count += 2;
            }
            if(count > 1) { return true; }
        }
    }
    return false;
}

std::vector<DbTablePair>
FromList::computeResolverTables() const {
    struct Memo : public TableRef::FuncC {
        virtual void operator()(TableRef const& t) {
            vec.push_back(DbTablePair(t.getDb(), t.getTable()));
        }
        std::vector<DbTablePair> vec;
    };
    Memo m;
    typedef TableRefList::const_iterator Iter;
    for(Iter i=_tableRefs->begin(), e= _tableRefs->end();
        i != e; ++i) {
        (**i).apply(m);
    }
    return m.vec;
}

std::string
FromList::getGenerated() {
    QueryTemplate qt;
    renderTo(qt);
    return qt.toString();
}

void
FromList::renderTo(QueryTemplate& qt) const {
    if(_tableRefs.get() && _tableRefs->size() > 0) {
        TableRefList const& refList = *_tableRefs;
        std::for_each(refList.begin(), refList.end(), TableRef::render(qt));
    }
}

std::shared_ptr<FromList>
FromList::copySyntax() {
    std::shared_ptr<FromList> newL = std::make_shared<FromList>(*this);
    // Shallow copy of expr list is okay.
    newL->_tableRefs  = std::make_shared<TableRefList>(*_tableRefs);
    // For the other fields, default-copied versions are okay.
    return newL;
}

std::shared_ptr<FromList>
FromList::clone() const {
    typedef TableRefList::const_iterator Iter;
    std::shared_ptr<FromList> newL = std::make_shared<FromList>(*this);

    newL->_tableRefs = std::make_shared<TableRefList>();

    for(Iter i=_tableRefs->begin(), e=_tableRefs->end(); i != e; ++ i) {
        newL->_tableRefs->push_back((*i)->clone());
    }
    return newL;
}

}}} // namespace lsst::qserv::query

