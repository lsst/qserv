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

#ifndef LSST_QSERV_RESOURCEUNIT_H
#define LSST_QSERV_RESOURCEUNIT_H

// System headers
#include <map>
#include <string>

// Qserv headers
#include "global/constants.h" // For DUMMY_CHUNK

namespace lsst {
namespace qserv {

/// ResourceUnit contains a name for an XrdSsi-resolvable resource unit.
////
/// Not sure this belongs in global, but czar, worker both need it.
/// Other components may as well.
////
/// Note that while key-value specifiers are parsed from the path string at
/// construction, the code for generating a path that includes the key-value
/// portion is not implemented. It is unclear whether we need the generation
/// capability, now that key-value pairs can be packed in protobufs messages.
class ResourceUnit {
public:
    class Checker;
    enum UnitType {GARBAGE, DBCHUNK, CQUERY, UNKNOWN, RESULT, WORKER};

    ResourceUnit() : _unitType(GARBAGE), _chunk(-1) {}

    explicit ResourceUnit(std::string const& path);

    /// @return the constructed path.
    std::string path() const;

    // Retrieve elements of the path.
    UnitType unitType() const {return _unitType;}
    std::string db() const {return _db;}
    int chunk() const {return _chunk;}
    std::string hashName() const { return _hashName; }

    /// Lookup extended path variables (?k=val syntax)
    std::string var(std::string const& key) const;

    /// @return the path prefix element for a given request type.
    static std::string prefix(UnitType const& r);

    /// @return the path of the database/chunk resource
    static std::string makePath(int chunk, std::string const& db);

    /// @return the path of the worker-specific resource
    static std::string makeWorkerPath(std::string const& id);

    // Setup a path of a certain type.
    void setAsDbChunk(std::string const& db, int chunk=DUMMY_CHUNK);

    // Compatibility types
    void setAsCquery(std::string const& db, int chunk=DUMMY_CHUNK);
    void setAsResult(std::string const& hashName);

    // Optional specifiers may not be supported by XrdSsi
    // Add optional specifiers ?foo&bar=1&bar2=2
    void addKey(std::string const& key);
    void addKey(std::string const& key, int val);

private:
    class Tokenizer;
    void _setFromPath(std::string const& path);
    void _ingestLeafAndKeys(std::string const& leafPlusKeys);
    void _ingestKeyStr(std::string const& keyStr);
    bool _markGarbageIfDone(Tokenizer& t);

    UnitType _unitType; //< Type of unit
    std::string _db; //< for CQUERY and DBCHUNK types
    int _chunk; //< for CQUERY and DBCHUNK types
    std::string _hashName; //< for RESULT and WORKER types

    typedef std::map<std::string, std::string> VarMap;
    VarMap _vars; //< Key-value specifiers

    static char const _pathSep = '/';
    static char const _varSep = '?';
    static char const _varDelim = '&';

    friend std::ostream& operator<<(std::ostream& os, ResourceUnit const& ru);
};

class ResourceUnit::Checker {
public:
    virtual ~Checker() {}
    virtual bool operator()(ResourceUnit const& ru) = 0;
};

}} // namespace lsst::qserv

#endif // LSST_QSERV_RESOURCEUNIT_H
