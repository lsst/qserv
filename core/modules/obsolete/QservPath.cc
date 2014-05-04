/*
 * LSST Data Management System
 * Copyright 2008, 2009, 2010 LSST Corporation.
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

#include "QservPath.h"

// System headers
#include <iostream>
#include <sstream>

namespace lsst {
namespace qserv {
namespace obsolete {

//////////////////////////////////////////////////////////////////////
// qsrv::QservPath::Tokenizer
// A simple class to tokenize paths.
//////////////////////////////////////////////////////////////////////
class QservPath::Tokenizer {
public:
    Tokenizer(std::string s, char sep)
        : _cursor(0), _next(0), _s(s), _sep(sep) {
        _seek();
    }
    std::string token() { return _s.substr(_cursor, _next-_cursor); }
    int tokenAsInt() {
        int num;
        std::stringstream csm(token());
        csm >> num;
        return num;
    }
    void next() { _cursor = _next + 1; _seek(); }
private:
    void _seek() { _next = _s.find_first_of(_sep, _cursor); }

    std::string::size_type _cursor;
    std::string::size_type _next;
    std::string const _s;
    char _sep;
};

//////////////////////////////////////////////////////////////////////
QservPath::QservPath(std::string const& path)
    : _chunk(-1) {
    _setFromPath(path);
}

std::string
QservPath::path() const {
    std::stringstream ss;
    ss << _pathSep << prefix(_requestType);
    if(_requestType == CQUERY) {
        ss << _pathSep << _db;
        if(_chunk != -1) {
            ss << _pathSep << _chunk;
        }
    }
    return ss.str();
}

std::string
QservPath::var(std::string const& key) const {
    VarMap::const_iterator ci = _vars.find(key);
    if(ci != _vars.end()) {
        return ci->second;
    }
    return std::string();
}

std::string
QservPath::prefix(RequestType const& r) {
    switch(r) {
    case CQUERY:
        return "q";
    case UNKNOWN:
        return "UNKNOWN";
    case OLDQ1:
        return "query";
    case OLDQ2:
        return "query2";
    case RESULT:
        return "result";
    case GARBAGE:
    default:
        return "GARBAGE";
    }
}

void
QservPath::setAsCquery(std::string const& db, int chunk) {
    _requestType = CQUERY;
    _db = db;
    _chunk = chunk;
}

void
QservPath::setAsCquery(std::string const& db) {
    _requestType = CQUERY;
    _db = db;
}

void
QservPath::_setFromPath(std::string const& path) {
    std::string rTypeString;
    Tokenizer t(path, _pathSep);
    if(!t.token().empty()) { // Expect leading separator (should start with /)
        _requestType = UNKNOWN;
        return;
    }
    t.next();
    rTypeString = t.token();
    if(rTypeString == prefix(CQUERY)) {
        // Import as chunk query
        _requestType = CQUERY;
        t.next();
        _db = t.token();
        if(_db.empty()) {
            _requestType = GARBAGE;
            return;
        }
        t.next();
        _chunk = t.tokenAsInt();
    } else if(rTypeString == prefix(RESULT)) {
        _requestType = RESULT;
        t.next();
        _hashName = t.token();
    } else if(rTypeString == prefix(OLDQ1)) {
        _requestType = OLDQ1;
        t.next();
        _chunk = t.tokenAsInt();
    } else if(rTypeString == prefix(OLDQ2)) {
        _requestType = OLDQ2;
        t.next();
        _chunk = t.tokenAsInt();
    } else {
        _requestType = GARBAGE;
    }
}

void
QservPath::_ingestKeys(std::string const& leafPlusKeys) {
    std::string::size_type start;
    start = leafPlusKeys.find_first_of(_varSep, 0);
    _vars.clear();

    if(start == std::string::npos) { // No keys found
        return; //leafPlusKeys;
    }
    ++start;
    Tokenizer t(leafPlusKeys.substr(start), _varDelim);
    for(std::string defn = t.token(); !defn.empty(); t.next()) {
        _ingestKeyStr(defn);
    }
}

void
QservPath::_ingestKeyStr(std::string const& keyStr) {
    std::string::size_type equalsPos;
    equalsPos = keyStr.find_first_of('=');
    if(equalsPos == std::string::npos) { // No = clause, value-less key.
        _vars[keyStr] = std::string(); // empty insert.
    } else {
        _vars[keyStr.substr(0,equalsPos)] = keyStr.substr(equalsPos+1);
    }
}

}}} // namespace lsst::qserv::obsolete
