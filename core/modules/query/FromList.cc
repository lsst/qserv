/*
 * LSST Data Management System
 * Copyright 2013-2014 LSST Corporation.
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

#include "query/FromList.h"

// System headers
#include <algorithm>
#include <iterator>


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
    return qt.dbgStr();
}

void
FromList::renderTo(QueryTemplate& qt) const {
    if(_tableRefs.get() && _tableRefs->size() > 0) {
        TableRefList const& refList = *_tableRefs;
        std::for_each(refList.begin(), refList.end(), TableRef::render(qt));
    }
}

boost::shared_ptr<FromList>
FromList::copySyntax() {
    boost::shared_ptr<FromList> newL(new FromList(*this));
    // Shallow copy of expr list is okay.
    newL->_tableRefs.reset(new TableRefList(*_tableRefs));
    // For the other fields, default-copied versions are okay.
    return newL;
}

boost::shared_ptr<FromList>
FromList::clone() const {
    typedef TableRefList::const_iterator Iter;
    boost::shared_ptr<FromList> newL(new FromList(*this));

    newL->_tableRefs.reset(new TableRefList());

    for(Iter i=_tableRefs->begin(), e=_tableRefs->end(); i != e; ++ i) {
        newL->_tableRefs->push_back(*i);
    }
    return newL;
}

typedef std::list<TableRefListPtr> ListList;
/// @param i, e : bounds iterating over possibilities at each level
/// @param soFar : accumulated TableRefList representing the traversal
/// path and an incomplete TableRefList permutation
/// @param finals : list of computed permutations (return value)
void
permuteHelper(ListList::iterator i, ListList::iterator e,
              TableRefListPtr soFar, ListList& finals) {
    // Idea: Walk through the possible permutations in a depth-first manner.
    // Each call iterates through the possibilities at its level and
    // recurses to fill the lower levels.

    if(i == e) {
        // Finished traversal, add finished permutation to the output.
        finals.push_back(soFar);
    }
    TableRefList& slotList = **i; // List of possibilities for this
                                  // slot (level)
    ++i;
    if(slotList.size() > 1) { // More than one slot?
        // Iterate through possibilities
        typedef TableRefList::iterator Iter;
        for(Iter j=slotList.begin(), e2=slotList.end(); j != e2; ++j) {
            // Could clone the list, but in practice, permutations
            // don't need to vary independently.
            TableRefListPtr nSoFar(new TableRefList(*soFar));
            // TableRef, similarly, need not be cloned.
            nSoFar->push_back(*j);
            permuteHelper(i, e, nSoFar, finals);
        }
    } else { // singleton for this slot-->no need to copy soFar
        soFar->push_back(slotList.front());
        permuteHelper(i, e, soFar, finals);
    }
}

FromList::PtrList
FromList::computePermutations(TableRef::PermuteFunc& f) {
    PtrList pList;
    ListList combos;
    typedef TableRefList::const_iterator Iter;

    // For each TableRef, compute its possible TableRefs
    for(Iter i=_tableRefs->begin(), e=_tableRefs->end();
        i != e; ++i) {
        combos.push_back(TableRefListPtr(new TableRefList((**i).permute(f))));
    }
    // Now combos.size() == _tableRefs->size()
    // To compute a permutation, choose 1 TableRef from each element in combos.
    // Total permutations = combos[0].size() * combos[1].size() *...
    ListList finals;
    permuteHelper(combos.begin(), combos.end(), TableRefListPtr(new TableRefList), finals);

    // Now compute a new FromList for each permutation in finals.
    typedef ListList::iterator Liter;
    for(Liter i=finals.begin(), e=finals.end(); i != e; ++i) {
        pList.push_back(Ptr(new FromList(*i)));
    }

    return pList;
}

}}} // namespace lsst::qserv::query

