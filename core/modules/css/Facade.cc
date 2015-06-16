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

/**
  * @file
  *
  * @brief A facade to the Central State System used by all Qserv core modules.
  *
  * @Author Jacek Becla, SLAC
  *
  */

// Class header
#include "css/Facade.h"

// System headers
#include <fstream>
#include <iostream>

// Third-party headers
#include "boost/lexical_cast.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "css/constants.h"
#include "css/CssError.h"
#include "css/EmptyChunks.h"
#include "css/KvInterfaceImplMem.h"
#include "global/stringTypes.h"

using std::endl;
using std::map;
using std::string;
using std::vector;

namespace lsst {
namespace qserv {
namespace css {


Facade::Facade() = default;    

/** Creates a new Facade over metadata in an in-memory key-value store.
  *
  * @param mapStream An input stream to data dumped using
  *                  ./admin/bin/qserv-admin.py
  * @param emptyChunkPath Path to a directory containing: empty_<dbname>.txt
  */
Facade::Facade(std::istream& mapStream,
               std::string const& emptyChunkPath)
    : _kvI(new KvInterfaceImplMem(mapStream)) {
    _versionCheck();
    if (!emptyChunkPath.empty()) {
        _emptyChunks.reset(new EmptyChunks(emptyChunkPath));
    } // empty str: no empty chunks available.
}

/** Creates a new Facade over metadata in an in-memory key-value store.
  *
  * @param kv A pre-built KvInterface on top of the Facade will use
  *           for querying.
  * @param emptyChunkPath Path to a directory containing: empty_<dbname>.txt
  */
Facade::Facade(std::shared_ptr<KvInterface> kv,
               std::string const& emptyChunkPath)
    : _kvI(kv) {
    if (_kvI) _versionCheck();
    if (!emptyChunkPath.empty()) {
        _emptyChunks.reset(new EmptyChunks(emptyChunkPath));
    } // empty str: no empty chunks available.
}

Facade::~Facade() = default;    

/** Returns true if the given database exists.
  */
bool
Facade::containsDb(string const& dbName) const {
    if (dbName.empty()) {
        LOGF_DEBUG("Empty database name passed.");
        return false;
    }
    string p = _prefix + "/DBS/" + dbName;
    bool ret =  _kvI->exists(p);
    LOGF_DEBUG("containsDb(%1%): %2%" % dbName % ret);
    return ret;
}

/** Returns true if the given table exists. Throws an exception if the given
  * database does not exist.
  */
bool
Facade::containsTable(string const& dbName, string const& tableName) const {
    LOGF_DEBUG("containsTable(%1%, %2%)" % dbName % tableName);
    _throwIfNotDbExists(dbName);
    return _containsTable(dbName, tableName);
}

/** Returns true if the given table is chunked. Throws an exception if the
  * table or its database does not exist.
  */
bool
Facade::tableIsChunked(string const& dbName, string const& tableName) const {
    _throwIfNotDbTbExists(dbName, tableName);
    bool ret = _tableIsChunked(dbName, tableName);
    LOGF_DEBUG("Table %1%.%2% %3% chunked"
               % dbName % tableName % (ret?"is":"is not"));
    return ret;
}

/** Returns true if the given table is sub-chunked. Throws an exception if the
  * table or its database does not exist.
  */
bool
Facade::tableIsSubChunked(string const& dbName,
                          string const& tableName) const {
    _throwIfNotDbTbExists(dbName, tableName);
    bool ret = _tableIsSubChunked(dbName, tableName);
    LOGF_DEBUG("Table %1%.%2% %3% subChunked"
               % dbName % tableName % (ret?"is":"is not"));
    return ret;
}

/** Returns true if the given table is a match table; that is, if it
  * relates two director tables. Throws an exception if the table or
  * its database does not exist.
  */
bool
Facade::isMatchTable(std::string const& dbName,
                     std::string const& tableName) const {
    LOGF_DEBUG("isMatchTable(%1%.%2%)" % dbName % tableName);
    _throwIfNotDbTbExists(dbName, tableName);
    string k = _prefix + "/DBS/" + dbName + "/TABLES/" + tableName + "/match";
    string v = _kvI->get(k, "0");
    bool m = (v == "1");
    LOGF_DEBUG("%1%.%2% is %3% a match table" % dbName % tableName % (m?"":"not "));
    return m;
}

/** Returns the names of all allowed databases (those that are configured
  * for Qserv).
  */
vector<string>
Facade::getAllowedDbs() const {
    string p = _prefix + "/DBS";
    return _kvI->getChildren(p);
}

/** Returns the names of all chunked tables in a given database.
  * An exception is thrown if the given database does not exist.
  */
vector<string>
Facade::getChunkedTables(string const& dbName) const {
    LOGF_DEBUG("getChunkedTables(%1%)" % dbName);
    _throwIfNotDbExists(dbName);
    string p = _prefix + "/DBS/" + dbName + "/TABLES";
    vector<string> ret, v = _kvI->getChildren(p);
    vector<string>::const_iterator itr;
    for (itr = v.begin() ; itr != v.end(); ++itr) {
        if (tableIsChunked(dbName, *itr)) {
            LOGF_DEBUG("getChunkedTables: %1%" % *itr);
            ret.push_back(*itr);
        }
    }
    return ret;
}

/** Returns the names of all sub-chunked tables in a given database.
  * An exception is thrown if the given database does not exist.
  */
vector<string>
Facade::getSubChunkedTables(string const& dbName) const {
    LOGF_DEBUG("getSubChunkedTables(%1%)" % dbName);
    _throwIfNotDbExists(dbName);
    string p = _prefix + "/DBS/" + dbName + "/TABLES";
    vector<string> ret, v = _kvI->getChildren(p);
    vector<string>::const_iterator itr;
    for (itr = v.begin() ; itr != v.end(); ++itr) {
        if (_tableIsSubChunked(dbName, *itr)) {
            LOGF_DEBUG("getSubChunkedTables: %1%" % *itr);
            ret.push_back(*itr);
        }
    }
    return ret;
}

/** Returns the partitioning columns for the given table. This is a
  * 3-element vector containing the longitude, latitude, and secondary
  * index column name for that table. An empty string indicates
  * that a column is not available. An exception is thrown if the given
  * database or table does not exist.
  */
vector<string>
Facade::getPartitionCols(string const& dbName, string const& tableName) const {
    LOGF_DEBUG("getPartitionCols(%1%, %2%)" % dbName % tableName);
    _throwIfNotDbTbExists(dbName, tableName);
    string p = _prefix + "/DBS/" + dbName + "/TABLES/" +
               tableName + "/partitioning/";
    vector<string> v;
    v.push_back(_kvI->get(p + "lonColName", ""));
    v.push_back(_kvI->get(p + "latColName", ""));
    v.push_back(_kvI->get(p + "dirColName", ""));
    LOGF_DEBUG("getPartitionCols: %1%, %2%, %3%" % v[0] % v[1] % v[2]);
    return v;
}

/** Returns the chunk level for a table. This is 0 for replicated
  * tables, 1 for chunked tables, and 2 for sub-chunked tables.
  */
int
Facade::getChunkLevel(string const& dbName, string const& tableName) const {
    LOGF_DEBUG("getChunkLevel(%1%, %2%)" % dbName % tableName);
    _throwIfNotDbTbExists(dbName, tableName);
    bool isChunked = _tableIsChunked(dbName, tableName);
    bool isSubChunked = _tableIsSubChunked(dbName, tableName);
    if (isSubChunked) {
        LOGF_DEBUG("getChunkLevel returns 2");
        return 2;
    }
    if (isChunked) {
        LOGF_DEBUG("getChunkLevel returns 1");
        return 1;
    }
    LOGF_DEBUG("getChunkLevel returns 0");
    return 0;
}

/** Returns the name of the director database name for the given table if there
  * is one and an empty string otherwise. Throws an exception if the database
  * or table does not exist.
  */
string
Facade::getDirDb(string const& dbName, string const& tableName) const {
    LOGF_DEBUG("getDirDb(%1%, %2%)" % dbName % tableName);
    _throwIfNotDbTbExists(dbName, tableName);
    string p = _prefix + "/DBS/" + dbName + "/TABLES/" + tableName +
        "/partitioning/dirDb";
    string ret = _kvI->get(p, "");
    LOGF_DEBUG("getDirDb returns %1%" % ret);
    return ret;
}

/** Returns the name of the director table for the given table if there
  * is one and an empty string otherwise. Throws an exception if the database
  * or table does not exist.
  */
string
Facade::getDirTable(string const& dbName, string const& tableName) const {
    LOGF_DEBUG("getDirTable(%1%, %2%)" % dbName % tableName);
    _throwIfNotDbTbExists(dbName, tableName);
    string p = _prefix + "/DBS/" + dbName + "/TABLES/" + tableName +
        "/partitioning/dirTable";
    string ret = _kvI->get(p, "");
    LOGF_DEBUG("getDirTable returns %1%" % ret);
    return ret;
}

/** Returns the name of the director column for the given table if there
  * is one and an empty string otherwise. Throws an exception if the database
  * or table does not exist.
  */
string
Facade::getDirColName(string const& dbName, string const& tableName) const {
    LOGF_DEBUG("getDirColName(%1%.%2%)" % dbName % tableName);
    _throwIfNotDbTbExists(dbName, tableName);
    string p = _prefix + "/DBS/" + dbName + "/TABLES/" + tableName +
        "/partitioning/dirColName";
    string ret = _kvI->get(p, "");
    LOGF_DEBUG("getDirColName, returning: '%1%'" % ret);
    return ret;
}

/** Returns the names of all secondary index columns for the given table.
  * Throws an exception if the database or table does not exist.
  */
vector<string>
Facade::getSecIndexColNames(string const& dbName, string const& tableName) const {
    LOGF_DEBUG("getSecIndexColNames(%1%.%2%)" % dbName % tableName);
    _throwIfNotDbTbExists(dbName, tableName);
    // TODO: we don't actually support multiple secondary indexes yet. So
    // the list of secondary index columnns is either empty, or contains
    // just the director column. See DM-2916
    string p = _prefix + "/DBS/" + dbName + "/TABLES/" + tableName +
                    "/partitioning/dirColName";
    string dc = _kvI->get(p, "");
    vector<string> ret;
    if (!dc.empty()) {
        ret.push_back(dc);
    }
    LOGF_DEBUG("getSecIndexColNames, returning: [%1%]" % dc);
    return ret;
}

/** Retrieves the # of stripes and sub-stripes for a database. Throws an
  * exception if the database does not exist. Returns (0,0) for unpartitioned
  * databases.
  */
StripingParams
Facade::getDbStriping(string const& dbName) const {
    LOGF_DEBUG("getDbStriping(%1%)" % dbName);
    _throwIfNotDbExists(dbName);
    StripingParams striping;
    string v = _kvI->get(_prefix + "/DBS/" + dbName + "/partitioningId", "");
    if (v.empty()) {
        return striping;
    }
    string p = _prefix + "/PARTITIONING/_" + v + "/";
    striping.stripes = _getIntValue(p+"nStripes", 0);
    striping.subStripes = _getIntValue(p+"nSubStripes", 0);
    striping.partitioningId = boost::lexical_cast<int>(v);
    return striping;
}

/** Retrieves the partition overlap in degrees for a database. Throws an
  * exception if the database does not exist. Returns 0 for unpartitioned
  * databases.
  */
double
Facade::getOverlap(string const& dbName) const {
    LOGF_DEBUG("getOverlap(%1%)" % dbName);
    _throwIfNotDbExists(dbName);
    string v = _kvI->get(_prefix + "/DBS/" + dbName + "/partitioningId", "");
    if (v == "") {
        return 0.0;
    }
    v = _kvI->get(_prefix + "/PARTITIONING/_" + v + "/overlap", "");
    if (v == "") {
        return 0.0;
    }
    return boost::lexical_cast<double>(v);
}

/** Retrieves match-table specific metadata for a table. Throws an
  * exception if the database and/or table does not exist, and returns
  * a MatchTableParams object containing only empty strings if the given
  * table is not a match table.
  */
MatchTableParams
Facade::getMatchTableParams(std::string const& dbName,
                            std::string const& tableName) const {
    LOGF_DEBUG("getMatchTableParams(%1%.%2%)" % dbName % tableName);
    _throwIfNotDbTbExists(dbName, tableName);
    MatchTableParams p;
    string k = _prefix + "/DBS/" + dbName + "/TABLES/" + tableName + "/match";
    string v = _kvI->get(k, "0");
    if (v == "1") {
        try {
            p.dirTable1 = _kvI->get(k + "/dirTable1");
            p.dirColName1 = _kvI->get(k + "/dirColName1");
            p.dirTable2 = _kvI->get(k + "/dirTable2");
            p.dirColName2 = _kvI->get(k + "/dirColName2");
            p.flagColName = _kvI->get(k + "/flagColName");
        } catch (NoSuchKey& ex) {
            throw CssError(
                string("Invalid match-table metadata for table ") +
                dbName + "." + tableName);
        }
    }
    return p;
}

/*
 *  Returns current compiled-in version number of CSS data structures.
 *  This is not normally useful for clients but can be used by various tests.
 */
int
Facade::cssVersion() {
    return lsst::qserv::css::VERSION;
}

EmptyChunks const&
Facade::getEmptyChunks() const {
    assert(_emptyChunks.get());
    return *_emptyChunks;
}

void
Facade::_versionCheck() const {
    const string& vstr = _kvI->get(lsst::qserv::css::VERSION_KEY, string());
    if (vstr.empty()) {
        throw VersionMissingError(lsst::qserv::css::VERSION_KEY);
    }
    if (vstr != lsst::qserv::css::VERSION_STR) {
        throw VersionMismatchError(lsst::qserv::css::VERSION_STR, vstr);
    }
}

int
Facade::_getIntValue(string const& key, int defaultValue) const {
    string s = boost::lexical_cast<string>(defaultValue);
    string v = _kvI->get(key, s);
    return v == s ? defaultValue : boost::lexical_cast<int>(v);
}

/** Throws an exception if the given database does not exist.
  */
void
Facade::_throwIfNotDbExists(string const& dbName) const {
    if (!containsDb(dbName)) {
        LOGF_DEBUG("Db '%1%' not found." % dbName);
        throw NoSuchDb(dbName);
    }
}

/** Throws an exception if the given  table does not exist.
  * Database existence is not checked.
  */
void
Facade::_throwIfNotTbExists(string const& dbName, string const& tableName) const {
    if (!containsTable(dbName, tableName)) {
        LOGF_DEBUG("Table %1%.%2% not found." % dbName % tableName);
        throw NoSuchTable(dbName+"."+tableName);
    }
}

/** Throws an exception if the given database or table does not exist.
  */
void
Facade::_throwIfNotDbTbExists(string const& dbName, string const& tableName) const {
    _throwIfNotDbExists(dbName);
    _throwIfNotTbExists(dbName, tableName);
}

/** Returns true if the given database contains the given table.
  * Database existence is not checked.
  */
bool
Facade::_containsTable(string const& dbName, string const& tableName) const {
    string p = _prefix + "/DBS/" + dbName + "/TABLES/" + tableName;
    bool ret = _kvI->exists(p);
    LOGF_DEBUG("containsTable returns: %1%" % ret);
    return ret;
}

/** Returns true if the given table is chunked.
  * Database/table existence is not checked.
  */
bool
Facade::_tableIsChunked(string const& dbName, string const& tableName) const{
    string p = _prefix + "/DBS/" + dbName + "/TABLES/" +
               tableName + "/partitioning";
    bool ret = _kvI->exists(p);
    LOGF_DEBUG("%1%.%2% %3% chunked." % dbName % tableName % (ret?"is":"is NOT"));
    return ret;
}

/** Returns true if the given table is sub-chunked.
  * Database/table existence is not checked.
  */
bool
Facade::_tableIsSubChunked(string const& dbName,
                           string const& tableName) const {
    string p = _prefix + "/DBS/" + dbName + "/TABLES/" +
               tableName + "/partitioning/" + "subChunks";
    string retS = _kvI->get(p, "0");
    bool retV = (retS == "1");
    LOGF_DEBUG("%1%.%2% %3% subChunked."
               % dbName % tableName % (retV?"is":"is NOT"));
    return retV;
}

std::shared_ptr<Facade>
FacadeFactory::createMemFacade(string const& mapPath,
                               std::string const& emptyChunkPath) {
    std::ifstream f(mapPath.c_str());
    if(f.fail()) {
        throw ConnError();
    }
    return FacadeFactory::createMemFacade(f, emptyChunkPath);
}

std::shared_ptr<Facade>
FacadeFactory::createMemFacade(std::istream& mapStream,
                               std::string const& emptyChunkPath) {
    std::shared_ptr<css::Facade> cssFPtr(new css::Facade(
                                           mapStream, emptyChunkPath));
    return cssFPtr;
}

std::shared_ptr<Facade>
FacadeFactory::createCacheFacade(std::shared_ptr<KvInterface> kv,
                                 std::string const& emptyChunkPath) {
    std::shared_ptr<css::Facade> facade(new Facade(kv, emptyChunkPath));
    return facade;
}
}}} // namespace lsst::qserv::css
