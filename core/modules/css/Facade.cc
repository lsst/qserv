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

/**
  * @file
  *
  * @brief A facade to the Central State System used by all Qserv core modules.
  *
  * @Author Jacek Becla, SLAC
  *
  */

#include "css/Facade.h"

// System headers
#include <fstream>
#include <iostream>

// Third-party headers
#include <boost/lexical_cast.hpp>

// Local headers
#include "css/CssError.h"
#include "css/KvInterfaceImplMem.h"
#include "css/KvInterfaceImplZoo.h"
#include "log/Logger.h"

using std::endl;
using std::map;
using std::string;
using std::vector;

namespace lsst {
namespace qserv {
namespace css {

/** Creates a new Facade over metadata in a Zookeeper key-value store.
  *
  * @param connInfo A comma separated list of host:port pairs, each
  *                 each corresponding to a Zookeeper server.
  * @param timeout  connection timeout in msec.
  */
Facade::Facade(string const& connInfo, int timeout_msec) {
    _kvI = new KvInterfaceImplZoo(connInfo, timeout_msec);
}

/** Creates a new Facade over metadata in a Zookeeper key-value store.
  * The given prefix will be used on all key names and can be used to
  * avoid polluting the production namespace.
  *
  * @param connInfo A comma separated list of host:port pairs, each
  *                 each corresponding to a Zookeeper server.
  * @param prefix   Key name prefix. If non-empty, should be of the
  *                 form "/XXX".
  */
Facade::Facade(string const& connInfo, int timeout_msec, string const& prefix) :
    _prefix(prefix) {
    _kvI = new KvInterfaceImplZoo(connInfo, timeout_msec);
}

/** Creates a new Facade over metadata in an in-memory key-value store.
  *
  * @param mapStream An input stream to data dumped using
  *                  ./admin/bin/qserv-admin.py
  */
Facade::Facade(std::istream& mapStream) {
    _kvI = new KvInterfaceImplMem(mapStream);
}

Facade::~Facade() {
    delete _kvI;
}

/** Returns true if the given database exists.
  */
bool
Facade::containsDb(string const& dbName) const {
    if (dbName.empty()) {
        LOGGER_INF << "Empty database name passed." << endl;
        throw NoSuchDb("<empty>");
    }
    string p = _prefix + "/DBS/" + dbName;
    bool ret =  _kvI->exists(p);
    LOGGER_INF << "*** containsDb(" << dbName << "): " << ret << endl;
    return ret;
}

/** Returns true if the given table exists. Throws an exception if the given
  * database does not exist.
  */
bool
Facade::containsTable(string const& dbName, string const& tableName) const {
    LOGGER_INF << "*** containsTable(" << dbName << ", " << tableName
               << ")" << endl;
    _throwIfNotDbExists(dbName);
    return _containsTable(dbName, tableName);
}

/** Returns true if the given table is chunked. Throws an exception if the
  * table or its database does not exist.
  */
bool
Facade::tableIsChunked(string const& dbName, string const& tableName) const {
    LOGGER_INF << "tableIsChunked " << dbName << " " << tableName << endl;
    _throwIfNotDbTbExists(dbName, tableName);
    return _tableIsChunked(dbName, tableName);
}

/** Returns true if the given table is sub-chunked. Throws an exception if the
  * table or its database does not exist.
  */
bool
Facade::tableIsSubChunked(string const& dbName,
                          string const& tableName) const {
    LOGGER_INF << "tableIsSubChunked(" << dbName << ", " << tableName << ")" << endl;
    _throwIfNotDbTbExists(dbName, tableName);
    return _tableIsSubChunked(dbName, tableName);
}

/** Returns true if the given table is a match table; that is, if it
  * relates two director tables. Throws an exception if the table or
  * its database does not exist.
  */
bool
Facade::isMatchTable(std::string const& dbName,
                     std::string const& tableName) const {
    LOGGER_INF << "isMatchTable(" << dbName << ", " << tableName << ")" << endl;
    _throwIfNotDbTbExists(dbName, tableName);
    string k = _prefix + "/DBS/" + dbName + "/TABLES/" + tableName + "/match";
    string v = _kvI->get(k, "0");
    bool m = (v == "1");
    LOGGER_INF << "*** " << dbName << "." << tableName << " is "
               << (m ? "" : "not ") << " a match table.";
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
    LOGGER_INF << "*** getChunkedTables(" << dbName << ")" << endl;
    _throwIfNotDbExists(dbName);
    string p = _prefix + "/DBS/" + dbName + "/TABLES";
    vector<string> ret, v = _kvI->getChildren(p);
    vector<string>::const_iterator itr;
    for (itr = v.begin() ; itr != v.end(); ++itr) {
        if (tableIsChunked(dbName, *itr)) {
            LOGGER_INF << "*** getChunkedTables: " << *itr << endl;
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
    LOGGER_INF << "*** getSubChunkedTables(" << dbName << ")" << endl;
    _throwIfNotDbExists(dbName);
    string p = _prefix + "/DBS/" + dbName + "/TABLES";
    vector<string> ret, v = _kvI->getChildren(p);
    vector<string>::const_iterator itr;
    for (itr = v.begin() ; itr != v.end(); ++itr) {
        if (_tableIsSubChunked(dbName, *itr)) {
            LOGGER_INF << "*** getSubChunkedTables: " << *itr << endl;
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
    LOGGER_INF << "*** getPartitionCols(" << dbName << ", " << tableName << ")"
               << endl;
    _throwIfNotDbTbExists(dbName, tableName);
    string p = _prefix + "/DBS/" + dbName + "/TABLES/" +
               tableName + "/partitioning/";
    vector<string> v;
    v.push_back(_kvI->get(p + "lonColName", ""));
    v.push_back(_kvI->get(p + "latColName", ""));
    v.push_back(_kvI->get(p + "dirColName", ""));
    LOGGER_INF << "*** getPartitionCols: "
               << v[0] << ", " << v[1] << ", " << v[2] << endl;
    return v;
}

/** Returns the chunk level for a table. This is 0 for replicated
  * tables, 1 for chunked tables, and 2 for sub-chunked tables.
  */
int Facade::getChunkLevel(string const& dbName, string const& tableName) const {
    LOGGER_INF << "getChunkLevel(" << dbName << ", " << tableName << ")" << endl;
    _throwIfNotDbTbExists(dbName, tableName);
    bool isChunked = _tableIsChunked(dbName, tableName);
    bool isSubChunked = _tableIsSubChunked(dbName, tableName);
    if (isSubChunked) {
        LOGGER_INF << "getChunkLevel returns 2" << endl;
        return 2;
    }
    if (isChunked) {
        LOGGER_INF << "getChunkLevel returns 1" << endl;
        return 1;
    }
    LOGGER_INF << "getChunkLevel returns 0" << endl;
    return 0;
}

/** Returns the name of the director table for the given table if there
  * is one and an empty string otherwise. Throws an exception if the database
  * or table does not exist.
  */
string
Facade::getDirTable(string const& dbName, string const& tableName) const {
    LOGGER_INF << "*** Facade::getDirTable(" << dbName << ", " << tableName
               << ")" << endl;
    _throwIfNotDbTbExists(dbName, tableName);
    string p = _prefix + "/DBS/" + dbName + "/TABLES/" + tableName +
                    "/partitioning/dirTable";
    string ret = _kvI->get(p, "");
    LOGGER_INF << "Facade::getDirTable, returning: " << ret << endl;
    return ret;
}

/** Returns the name of the director column for the given table if there
  * is one and an empty string otherwise. Throws an exception if the database
  * or table does not exist.
  */
string
Facade::getDirColName(string const& dbName, string const& tableName) const {
    LOGGER_INF << "*** Facade::getDirColName(" << dbName << ", " << tableName
               << ")" << endl;
    _throwIfNotDbTbExists(dbName, tableName);
    string p = _prefix + "/DBS/" + dbName + "/TABLES/" + tableName +
                    "/partitioning/dirColName";
    string ret = _kvI->get(p, "");
    LOGGER_INF << "Facade::getDirColName, returning: " << ret << endl;
    return ret;
}

/** Returns the names of all secondary index columns for the given table.
  * Throws an exception if the database or table does not exist.
  */
vector<string>
Facade::getSecIndexColNames(string const& dbName, string const& tableName) const {
    LOGGER_INF << "*** Facade::getSecIndexColNames(" << dbName << ", " << tableName
               << ")" << endl;
    _throwIfNotDbTbExists(dbName, tableName);
    // TODO: we don't actually support multiple secondary indexes yet. So
    // the list of secondary index columnns is either empty, or contains
    // just the director column.
    string p = _prefix + "/DBS/" + dbName + "/TABLES/" + tableName +
                    "/partitioning/dirColName";
    string dc = _kvI->get(p, "");
    vector<string> ret;
    if (!dc.empty()) {
        ret.push_back(dc);
    }
    LOGGER_INF << "Facade::getSecIndexColNames, returning: [" << dc << "]" << endl;
    return ret;
}

/** Retrieves the # of stripes and sub-stripes for a database. Throws an
  * exception if the database does not exist. Returns (0,0) for unpartitioned
  * databases.
  */
StripingParams
Facade::getDbStriping(string const& dbName) const {
    LOGGER_INF << "*** getDbStriping(" << dbName << ")" << endl;
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
    LOGGER_INF << "*** getOverlap(" << dbName << ")" << endl;
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
    LOGGER_INF << "*** getMatchTableParams(" << dbName << ", "
               << tableName << ")" << endl;
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
        LOGGER_INF << "Db '" << dbName << "' not found." << endl;
        throw NoSuchDb(dbName);
    }
}

/** Throws an exception if the given  table does not exist.
  * Database existence is not checked.
  */
void
Facade::_throwIfNotTbExists(string const& dbName, string const& tableName) const {
    if (!containsTable(dbName, tableName)) {
        LOGGER_INF << "table " << dbName << "." << tableName << " not found"
                   << endl;
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
    LOGGER_INF << "*** containsTable returns: " << ret << endl;
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
    LOGGER_INF << "*** " << dbName << "." << tableName << " is ";
    if (!ret) LOGGER_INF << "NOT ";
    LOGGER_INF << "chunked." << endl;
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
    LOGGER_INF << "*** " << dbName << "." << tableName << " is "
               << (retV ? "" : "NOT ") << "subchunked.";
    return retV;
}

boost::shared_ptr<Facade>
FacadeFactory::createZooFacade(string const& connInfo, int timeout_msec) {
    boost::shared_ptr<css::Facade> cssFPtr(new css::Facade(connInfo, timeout_msec));
    return cssFPtr;
}

boost::shared_ptr<Facade>
FacadeFactory::createMemFacade(string const& mapPath) {
    std::ifstream f(mapPath.c_str());
    if(f.fail()) {
        throw ConnError();
    }
    return FacadeFactory::createMemFacade(f);
}

boost::shared_ptr<Facade>
FacadeFactory::createMemFacade(std::istream& mapStream) {
    boost::shared_ptr<css::Facade> cssFPtr(new css::Facade(mapStream));
    return cssFPtr;
}

boost::shared_ptr<Facade>
FacadeFactory::createZooTestFacade(string const& connInfo,
                                   int timeout_msec,
                                   string const& prefix) {
    boost::shared_ptr<css::Facade> cssFPtr(
                        new css::Facade(connInfo, timeout_msec, prefix));
    return cssFPtr;
}

}}} // namespace lsst::qserv::css
