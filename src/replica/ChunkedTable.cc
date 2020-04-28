/*
 * LSST Data Management System
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
#include "replica/ChunkedTable.h"

// System headers
#include <regex>
#include <stdexcept>

using namespace std;

namespace lsst {
namespace qserv {
namespace replica {

ChunkedTable::ChunkedTable(string const& baseName,
                           unsigned int chunk,
                           bool overlap)
    :   _baseName(baseName),
        _chunk(chunk),
        _overlap(overlap),
        _name(baseName + (overlap ? "FullOverlap" : "") + "_" + to_string(chunk)) {
    _assertValid();
}


ChunkedTable::ChunkedTable(string const& name) {
    
    // The algorithm tries two regular impressions to avoid dealing with
    // "greedy" behavior of the RegExp match for the base name of a table.
    // Otherwise 'FullOverlap' would always be made a part of the table name.
    // Besides, the two-step algorithm treats tables like 'FullOverlap__<chunk>'
    // as perfectly valid non-overlap tables.

    regex const reFullOverlapTable("^(.+)FullOverlap_([0-9]+)$", regex::extended);
    regex const reTable("^(.+)_([0-9]+)$", regex::extended);
    smatch match;

    if (regex_search(name, match, reFullOverlapTable) and match.size() == 3) {
        _overlap  = true;
    } else if (regex_search(name, match, reTable) and match.size() == 3) {
        _overlap  = false;
    } else {
        throw invalid_argument(
                "ChunkedTable::" + string(__func__)
                + "failed to parse the table name '" + name
                + "' as a valid full name of a table. A syntax for the allowed names"
                " is: <base-name>_<chunk> or <table-base-name>FullOverlap_<chunk>");
    }
    _baseName = match[1].str();
    _chunk    = stoul(match[2].str());
    _name     = name;
}


string const& ChunkedTable::baseName() const {
    _assertValid();
    return _baseName;
}


unsigned int ChunkedTable::chunk() const {
    _assertValid();
    return _chunk;
}


bool ChunkedTable::overlap() const {
    _assertValid();
    return _overlap;
}


string const& ChunkedTable::name() const {
    _assertValid();
    return _name;
}


void ChunkedTable::_assertValid() const {
    if (_baseName.empty()) {
        throw invalid_argument(
            "ChunkedTable::" + string(__func__) + "  object is not in the valid state");
    }
}

}}} // namespace lsst::qserv::replica
