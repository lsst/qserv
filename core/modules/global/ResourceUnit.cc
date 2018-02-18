// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2015 LSST Corporation.
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

#include "global/ResourceUnit.h"

// System headers
#include <cassert>
#include <cstdlib>
#include <iostream>
#include <sstream>

namespace lsst {
namespace qserv {

//////////////////////////////////////////////////////////////////////
// lsst::qserv::ResourceUnit::Tokenizer
// A simple class to tokenize paths.
//////////////////////////////////////////////////////////////////////
class ResourceUnit::Tokenizer {
public:
    Tokenizer(std::string const& s, char sep='/')
        : _cursor(0), _next(0), _s(s), _sep(sep) {
        _seek();
    }

    std::string token() {
        return _s.substr(_cursor, _next - _cursor);
    }

    int tokenAsInt() {
        int num;
        std::istringstream csm(token());
        csm >> num;
        return num;
    }

    void next() { assert(!done()); _cursor = _next + 1; _seek(); }

    bool done() { return _next == std::string::npos; }

private:
    void _seek() { _next = _s.find_first_of(_sep, _cursor); }

    std::string::size_type _cursor;
    std::string::size_type _next;
    std::string const _s;
    char const _sep;
};

//////////////////////////////////////////////////////////////////////
ResourceUnit::ResourceUnit(std::string const& path)
    : _unitType(GARBAGE), _chunk(-1) {
    _setFromPath(path);
}

std::string
ResourceUnit::path() const {
    std::stringstream ss;
    ss << _pathSep << prefix(_unitType);
    switch(_unitType) {
    case GARBAGE:
        return "/GARBAGE";
    case DBCHUNK: // For now, DBCHUNK is handled the same as CQUERY
    case CQUERY:
        ss << _pathSep << _db;
        if (_chunk != -1) {
            ss << _pathSep << _chunk;
        }
        break;
    case UNKNOWN:
        ss << _pathSep << "UNKNOWN_RESOURCE_UNIT";
        break;
    case RESULT:
    case WORKER:
        ss << _hashName;
        break;
    default:
        ::abort();
        break;
    }
    return ss.str();
}

std::string
ResourceUnit::var(std::string const& key) const {
    VarMap::const_iterator ci = _vars.find(key);
    if (ci != _vars.end()) {
        return ci->second;
    }
    return std::string();
}

std::string
ResourceUnit::prefix(UnitType const& r) {
    switch(r) {
    case DBCHUNK:
        return "chk";
    case CQUERY:
        return "q";
    case UNKNOWN:
        return "UNKNOWN";
    case RESULT:
        return "result";
    case WORKER:
        return "worker";
    case GARBAGE:
    default:
        return "GARBAGE";
    }
}

std::string ResourceUnit::makePath(int chunk, std::string const& db) {
    return "/" + prefix(UnitType::DBCHUNK) + "/" + db + "/" + std::to_string(chunk);
}
std::string ResourceUnit::makeWorkerPath(std::string const& id) {
    return "/" + prefix(UnitType::WORKER) + "/" + id;
}

void
ResourceUnit::setAsDbChunk(std::string const& db, int chunk) {
    _unitType = DBCHUNK;
    _db = db;
    _chunk = chunk;
}

void
ResourceUnit::setAsCquery(std::string const& db, int chunk) {
    _unitType = CQUERY;
    _db = db;
    _chunk = chunk;
}

bool ResourceUnit::_markGarbageIfDone(Tokenizer& t) {
    if (t.done()) {
        _unitType = GARBAGE;
        return true;
    }
    return false;
}

void
ResourceUnit::_setFromPath(std::string const& path) {
    std::string rTypeString;
    Tokenizer t(path, _pathSep);
    if (!t.token().empty()) { // Expect leading separator (should start with /)
        _unitType = UNKNOWN;
        return;
    }
    if (_markGarbageIfDone(t)) { return; } // Consider using GOTO structure.
    t.next();
    rTypeString = t.token();
    if (rTypeString == prefix(DBCHUNK)) {
        // XrdSsi query
        if (_markGarbageIfDone(t)) { return; }
        _unitType = DBCHUNK;
        t.next();
        _db = t.token();
        if (_db.empty()) {
            _unitType = GARBAGE;
            return;
        }
        if (_markGarbageIfDone(t)) { return; }
        t.next();
        if (t.token().empty()) { _unitType = GARBAGE; return; }
        _chunk = t.tokenAsInt();
        _ingestLeafAndKeys(t.token());
    } else if (rTypeString == prefix(CQUERY)) {
        // Import as chunk query
        _unitType = CQUERY;
        if (_markGarbageIfDone(t)) { return; }
        t.next();
        _db = t.token();
        if (_db.empty()) {
            _unitType = GARBAGE;
            return;
        }
        if (_markGarbageIfDone(t)) { return; }
        t.next();
        if (t.token().empty()) { _unitType = GARBAGE; return; }
        _chunk = t.tokenAsInt();
        _ingestLeafAndKeys(t.token());

    } else if (rTypeString == prefix(RESULT)) {
        _unitType = RESULT;
        if (_markGarbageIfDone(t)) { return; }
        t.next();
        _hashName = t.token();
        if (_hashName.empty()) { _unitType = GARBAGE; return; }
    } else if (rTypeString == prefix(WORKER)) {
        _unitType = WORKER;
        if (_markGarbageIfDone(t)) { return; }
        t.next();
        _hashName = t.token();
        if (_hashName.empty()) { _unitType = GARBAGE; return; }
    } else {
        _unitType = GARBAGE;
    }
}

/// Ingest key-value pairs from a string including the last portion of the path,
/// e.g., somenumber?key1=val1&key2=val2
void
ResourceUnit::_ingestLeafAndKeys(std::string const& leafPlusKeys) {
    std::string::size_type start;
    start = leafPlusKeys.find_first_of(_varSep, 0);
    _vars.clear();

    if (start == std::string::npos) { // No keys found
        return;
    }
    ++start;
    Tokenizer t(leafPlusKeys.substr(start), _varDelim);
    for(std::string defn = t.token(); !defn.empty(); t.next()) {
        _ingestKeyStr(defn);
    }
}

/// Ingest key-value pairs from a packed key-value representation.
/// e.g., key1=val1&key2=val2
void
ResourceUnit::_ingestKeyStr(std::string const& keyStr) {
    std::string::size_type equalsPos;
    equalsPos = keyStr.find_first_of('=');
    if (equalsPos == std::string::npos) { // No = clause, value-less key.
        _vars[keyStr] = std::string(); // empty insert.
    } else {
        _vars[keyStr.substr(0,equalsPos)] = keyStr.substr(equalsPos+1);
    }
}

std::ostream& operator<<(std::ostream& os, ResourceUnit const& ru) {
    return os << "Resource(" << ru.path() << ")";
}

}} // namespace lsst::qserv
