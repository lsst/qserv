// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014 LSST Corporation.
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

#ifndef LSST_QSERV_RESOURCEUNIT_H
#define LSST_QSERV_RESOURCEUNIT_H

// System headers
#include <map>
#include <string>

namespace lsst {
namespace qserv {

// ResourceUnit contains a name for an XrdSsi-resolvable resource unit.
// Not sure this belongs in global, but czar, worker both need it.
// Other components may as well.
class ResourceUnit {
public:
    class Checker;
    enum UnitType {GARBAGE, DBCHUNK, CQUERY, UNKNOWN, RESULT};

    ResourceUnit() : _chunk(-1) {}

    explicit ResourceUnit(std::string const& path);

    /// @return the constructed path.
    std::string path() const;

    // Retrieve elements of the path.
    UnitType unitType() const {return _unitType;}
    std::string db() const {return _db;}
    int chunk() const {return _chunk;}
    std::string hashName() const { return _hashName; }

    std::string var(std::string const& key) const;

    /// @return the path prefix element for a given request type.
    static std::string prefix(UnitType const& r);

    // Setup a path of a certain type.
    void setAsDbChunk(std::string const& db, int chunk);

    // Compatibility types
    void setAsCquery(std::string const& db, int chunk);
    void setAsCquery(std::string const& db);
    void setAsResult(std::string const& hashName);

    // Optional specifiers may not be supported by XrdSsi
    // Add optional specifiers ?foo&bar=1&bar2=2
    void addKey(std::string const& key);
    void addKey(std::string const& key, int val);

private:
    class Tokenizer;
    void _setFromPath(std::string const& path);
    void _ingestKeys(std::string const& leafPlusKeys);
    void _ingestKeyStr(std::string const& keyStr);

    UnitType _unitType;
    std::string _db;
    int _chunk;
    std::string _hashName;
    typedef std::map<std::string, std::string> VarMap;
    VarMap _vars;
    static char const _pathSep = '/';
    static char const _varSep = '?';
    static char const _varDelim = '&';

    friend std::ostream& operator<<(std::ostream& os, ResourceUnit const& ru);
};

class ResourceUnit::Checker {
public:
    virtual bool operator()(ResourceUnit const& ru) = 0;
};

}} // namespace lsst::qserv

#endif // LSST_QSERV_RESOURCEUNIT_H
