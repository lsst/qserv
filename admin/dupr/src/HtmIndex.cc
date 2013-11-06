/*
 * LSST Data Management System
 * Copyright 2013 LSST Corporation.
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

#include "HtmIndex.h"

#include <algorithm>
#include <iomanip>
#include <stdexcept>

#include "boost/scoped_array.hpp"

#include "Constants.h"
#include "FileUtils.h"
#include "Geometry.h"
#include "Hash.h"

using std::ostream;
using std::pair;
using std::runtime_error;
using std::setw;
using std::sort;
using std::vector;

namespace fs = boost::filesystem;


namespace lsst {
namespace qserv {
namespace admin {
namespace dupr {

HtmIndex::HtmIndex(int level) :
    _numRecords(0),
    _map(),
    _keys(),
    _level(level)
{
    if (level < 0 || level > HTM_MAX_LEVEL) {
        throw runtime_error("Invalid HTM subdivision level.");
    }
}

HtmIndex::HtmIndex(fs::path const & path) :
    _numRecords(0),
    _map(),
    _keys(),
    _level(-1)
{
    _read(path);
}

HtmIndex::HtmIndex(vector<fs::path> const & paths) :
    _numRecords(0),
    _map(),
    _keys(),
    _level(-1)
{
    typedef vector<fs::path>::const_iterator Iter;
    if (paths.empty()) {
        throw runtime_error("Empty HTM index file list.");
    }
    for (Iter i = paths.begin(), e = paths.end(); i != e; ++i) {
        _read(*i);
    }
}

HtmIndex::HtmIndex(HtmIndex const & idx) :
    _numRecords(idx._numRecords),
    _map(idx._map),
    _keys(idx._keys),
    _level(idx._level)
{ }

HtmIndex::~HtmIndex() { }

uint32_t HtmIndex::mapToNonEmpty(uint32_t id) const {
    if (_map.empty()) {
        throw runtime_error("HTM index is empty.");
    }
    Map::const_iterator i = _map.find(id);
    if (i != _map.end()) {
        return i->first;
    }
    if (_keys.empty()) {
        // Build sorted list of non-empty HTM triangle IDs.
        _keys.reserve(_map.size());
        for (Map::const_iterator i = _map.begin(), e = _map.end(); i != e; ++i) {
            _keys.push_back(i->first);
        }
        sort(_keys.begin(), _keys.end());
    }
    return _keys[hash(id) % _keys.size()];
}

void HtmIndex::write(fs::path const & path, bool truncate) const {
    size_t const numBytes = _map.size()*ENTRY_SIZE;
    boost::scoped_array<uint8_t> buf(new uint8_t[numBytes]);
    uint8_t * b = buf.get();
    // Write out array of (HTM ID, record count) pairs.
    for (Map::const_iterator i = _map.begin(), e = _map.end(); i != e; ++i) {
        b = encode(b, i->first);
        b = encode(b, i->second);
    }
    OutputFile f(path, truncate);
    f.append(buf.get(), numBytes);
}

void HtmIndex::write(ostream & os) const {
    typedef vector<pair<uint32_t, uint64_t> >::const_iterator Iter;
    // Extract non-empty triangles and sort them by HTM ID.
    vector<pair<uint32_t, uint64_t> > tris;
    tris.reserve(_map.size());
    for (Map::const_iterator i = _map.begin(), e = _map.end(); i != e; ++i) {
        tris.push_back(pair<uint32_t, uint64_t>(i->first, i->second));
    }
    sort(tris.begin(), tris.end());
    // Pretty-print the index in JSON format.
    os << "{\n"
          "\"nrec\":      " << _numRecords << ",\n"
          "\"triangles\": [\n";
    for (Iter b = tris.begin(), e = tris.end(), i = b; i != e; ++i) {
        if (i != b) {
            os << ",\n";
        }
        os << "\t{\"id\":"   << setw(10) << i->first
           << ", \"nrec\":"  << setw(8)  << i->second
           << "}";
    }
    os << "\n]\n}";
}

void HtmIndex::add(uint32_t id, uint64_t numRecords) {
    if (htmLevel(id) != _level) {
        throw runtime_error("HTM ID is invalid or has an inconsistent "
                            "subdivision level.");
    }
    if (numRecords > 0) {
        _keys.clear();
        _map[id] += numRecords;
        _numRecords += numRecords;
    }
}

void HtmIndex::merge(HtmIndex const & idx) {
    if (this == &idx) {
        return;
    }
    if (idx._level != _level) {
        throw runtime_error("HTM index subdivision levels do not match.");
    }
    _keys.clear();
    for (Map::const_iterator i = idx._map.begin(), e = idx._map.end();
         i != e; ++i) {
        _map[i->first] += i->second;
        _numRecords += i->second;
    }
}

void HtmIndex::clear() {
    _numRecords = 0;
    _map.clear();
    _keys.clear();
}

void HtmIndex::swap(HtmIndex & idx) {
    using std::swap;
    if (this != &idx) {
        swap(_numRecords, idx._numRecords);
        swap(_map, idx._map);
        swap(_keys, idx._keys);
        swap(_level, idx._level);
    }
}

void HtmIndex::_read(fs::path const & path) {
    InputFile f(path);
    if (f.size() == 0 || f.size() % ENTRY_SIZE != 0) {
        throw runtime_error("Invalid HTM index file.");
    }
    boost::scoped_array<uint8_t> data(new uint8_t[f.size()]);
    f.read(data.get(), 0, f.size());
    uint8_t const * b = data.get();
    off_t const numTriangles = f.size()/ENTRY_SIZE;
    _keys.clear();
    // Read array of (HTM ID, record count) pairs.
    for (off_t i = 0; i < numTriangles; ++i, b += ENTRY_SIZE) {
        uint32_t id = decode<uint32_t>(b);
        uint64_t numRecords = decode<uint64_t>(b + 4);
        int level = htmLevel(id);
        if (level < 0 || level > HTM_MAX_LEVEL) {
            throw runtime_error("Invalid HTM index file.");
        }
        if (_level < 0) {
            _level = level;
        } else if (level != _level) {
            throw runtime_error("HTM index subdivision levels do not match.");
        }
        if (numRecords == 0) {
            throw runtime_error("HTM index file contains an empty triangle.");
        }
        _map[id] += numRecords;
        _numRecords += numRecords;
    }
}

}}}} // namespace lsst::qserv::admin::dupr
