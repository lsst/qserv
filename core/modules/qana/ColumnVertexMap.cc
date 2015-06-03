// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2015 AURA/LSST.
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

/// \file
/// \brief Implementation of column to table reference resolution

// Class header
#include "qana/ColumnVertexMap.h"

// System headers
#include <algorithm>
#include <utility>

// Qserv headers
#include "qana/QueryNotEvaluableError.h"
#include "qana/RelationGraph.h"

#include "query/QueryTemplate.h"


namespace lsst {
namespace qserv {
namespace qana {

ColumnVertexMap::ColumnVertexMap(Vertex& v) {
    std::vector<ColumnRefConstPtr> c = v.info->makeColumnRefs(v.tr.getAlias());
    _init(v, c.begin(), c.end());
}

std::vector<Vertex*> const& ColumnVertexMap::find(
    query::ColumnRef const& c) const
{
    typedef std::vector<Entry>::const_iterator Iter;
    std::vector<Vertex*> static const NONE;

    std::pair<Iter,Iter> p = std::equal_range(
        _entries.begin(), _entries.end(), c, ColumnRefLt());
    if (p.first == p.second) {
        return NONE;
    } else if (p.first->isAmbiguous()) {
        query::QueryTemplate qt;
        c.renderTo(qt);
        throw QueryNotEvaluableError("Column reference " + qt.toString() +
                                     " is ambiguous");
    }
    return p.first->vertices;
}

void ColumnVertexMap::fuse(ColumnVertexMap& m,
                           bool natural,
                           std::vector<std::string> const& cols)
{
    typedef std::vector<Entry>::iterator EntryIter;
    typedef std::vector<std::string>::const_iterator StringIter;

    std::vector<Entry>::size_type s = _entries.size();
    // Reserve required space up front, then swap default constructed
    // entries with entries from m.
    _entries.resize(s + m._entries.size());
    EntryIter middle = _entries.begin() + s;
    for (EntryIter i = m._entries.begin(), e = m._entries.end(), o = middle;
         i != e; ++i, ++o) {
        o->swap(*i);
    }
    m._entries.clear();
    // Merge-sort the two sorted runs of entries
    std::inplace_merge(_entries.begin(), middle, _entries.end(), ColumnRefLt());
    // Duplicate column references are now adjacent to eachother in _entries -
    // remove the duplicates by merging them.
    if (!_entries.empty()) {
        ColumnRefEq eq;
        EntryIter cur = _entries.begin(); // Current entry
        EntryIter i = cur; // Entry to compare to *cur
        EntryIter end = _entries.end();
        StringIter firstCol = cols.begin();
        StringIter endCol = cols.end();
        while (++i != end) {
            if (eq(*cur, *i)) {
                // Found an entry with the same column reference as the
                // currrent entry
                if (!cur->cr->table.empty() || (!natural &&
                    std::find(firstCol, endCol, cur->cr->column) == endCol)) {
                    // The column reference is qualified or is not a natural
                    // join or using column, so mark it as ambiguous.
                    cur->markAmbiguous();
                } else if (cur->isAmbiguous() || i->isAmbiguous()) {
                    throw QueryNotEvaluableError(
                        "Join column " + cur->cr->column + " is ambiguous");
                } else {
                    // Add the vertices from i to the current entry (for
                    // natural join and using columns).
                    cur->vertices.insert(cur->vertices.end(),
                                         i->vertices.begin(),
                                         i->vertices.end());
                }
            } else {
                ++cur;
                if (cur != i) {
                    *cur = *i;
                }
            }
        }
        // The entries following cur are duplicates that have been merged into
        // entries at or before cur - erase them.
        _entries.erase(++cur, _entries.end());
    }
}

std::vector<std::string> const ColumnVertexMap::computeCommonColumns(
    ColumnVertexMap const& m) const
{
    typedef std::vector<Entry>::const_iterator EntryIter;
    std::vector<std::string> cols;
    // The entries for this map and m are both sorted, so we can find
    // identical unqualified column references in linear time with a
    // coordinated scan over both entry lists.
    ColumnRefLt lt;
    EntryIter i = _entries.begin(), iend = _entries.end();
    EntryIter j = m._entries.begin(), jend = m._entries.end();
    while (i != iend && j != jend) {
        if (lt(*i, *j)) {
            ++i;
        } else if (lt(*j, *i)) {
            ++j;
        } else {
            // Found a pair of identical column references.
            if (i->cr->table.empty()) {
                // If the reference is unqualified and unambiguous in
                // both entry lists, add it to the list of common columns.
                if (i->isAmbiguous() || j->isAmbiguous()) {
                    throw QueryNotEvaluableError(
                        "Join column " + i->cr->column + " is ambiguous");
                }
                cols.push_back(i->cr->column);
            }
            ++i;
            ++j;
        }
    }
    return cols;
}

}}} // namespace lsst::qserv::qana
