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
// MySqlExportMgr implementation.
#include "wpublish/MySqlExportMgr.h"
#include <sstream>
#include <iostream>
#include <boost/regex.hpp>
#include "sql/SqlConnection.h"
#include "wconfig/Config.h"
#include "wlog/WLogger.h"

using namespace lsst::qserv::worker;
using namespace lsst::qserv;

namespace { // File-scope helpers

inline std::string getTableNameDbListing(std::string const& instanceName) {
    return "qservw_" + instanceName + "." + "Dbs";
}

template <class C>
void getDbs(WLogger& log,
            std::string const& instanceName,
            SqlConnection& sc,
            C& dbs) {
    // get list of tables
    // Assume table has schema that includes char column named "db"
    SqlErrorObject sqlErrorObject;
    std::string tableNameDbListing = getTableNameDbListing(instanceName);
    std::string listq = "SELECT db FROM " + tableNameDbListing;
    log.warn("Launching query : " + listq);
    boost::shared_ptr<SqlResultIter> resultP = sc.getQueryIter(listq);
    assert(resultP.get());
    bool nothing = true;
    for(; !resultP->done(); ++(*resultP)) {
        dbs.push_back((**resultP)[0]);
        nothing = false;
    }
    if(nothing) log.warn("TEST : No databases found to export."+listq);
}

/// Functor to be called per-table name
class doTable {
public:
    doTable(boost::regex& regex, MySqlExportMgr::ChunkMap& chunkMap)
        : _regex(regex), _chunkMap(chunkMap) {}
    void operator()(std::string const& tableName) {
        boost::smatch what;
        if(boost::regex_match(tableName, what, _regex)) {
            //std::cout << "Found chunk table: " << what[1]
            //<< "(" << what[2] << ")" << std::endl;
            // Get chunk# slot. Append/set table name.
            std::string chunkStr = what[2];
            int chunk = std::atoi(chunkStr.c_str());
            MySqlExportMgr::StringSet& ss = _chunkMap[chunk];
            ss.insert(what[1]);
        }
    }
private:
    boost::regex _regex;
    MySqlExportMgr::ChunkMap& _chunkMap;
};

/// Functor for iterating over a ChunkMap and printing out its contents.
struct printChunk {
    printChunk(std::ostream& os) : _os(os) {}
    void operator()(MySqlExportMgr::ChunkMap::value_type const& tuple) {
        std::stringstream ss;
        int chunkId = tuple.first;
        MySqlExportMgr::StringSet const& s = tuple.second;
        ss << chunkId << "(";
        std::copy(s.begin(), s.end(),
                  std::ostream_iterator<std::string>(ss, ","));
        ss << ")";
        _os << ss.str() << "\n";
    }
    std::ostream& _os;
};

/// Functor for iterating over a ChunkMap and updating a StringSet.
struct addDbItem {
    addDbItem(std::string const& dbName, MySqlExportMgr::StringSet& stringSet)
        : _dbName(dbName), _stringSet(stringSet) {}
    void operator()(MySqlExportMgr::ChunkMap::value_type const& tuple) {
        // Ignore tuple.second (list of tables for this chunk)
        _stringSet.insert(MySqlExportMgr::makeKey(_dbName, tuple.first));
    }
    std::string const& _dbName;
    MySqlExportMgr::StringSet& _stringSet;
};

/// Functor to load db
class doDb {
public:
    doDb(SqlConnection& conn,
         boost::regex& regex,
         MySqlExportMgr::ExistMap& existMap)
        : _conn(conn),
          _regex(regex),
          _existMap(existMap)
        {}

    void operator()(std::string const& dbName) {
        // get list of tables
        std::vector<std::string> tables;
        SqlErrorObject sqlErrorObject;
        bool ok = _conn.listTables(tables,  sqlErrorObject, "", dbName);
        if(!ok) {
            std::cout << "SQL error: " << sqlErrorObject.errMsg() << std::endl;
            assert(ok);
        }
        MySqlExportMgr::ChunkMap& chunkMap = _existMap[dbName];
        chunkMap.clear(); // Clear out stale entries to avoid mixing.
        std::for_each(tables.begin(), tables.end(),
                      doTable(_regex, chunkMap));

        //std::for_each(chunkMap.begin(), chunkMap.end(), printChunk(std::cout));
        // TODO: Sanity check: do all tables have the same chunks represented?
    }
private:
    MySqlExportMgr::ExistMap& _existMap;
    SqlConnection& _conn;
    boost::regex& _regex;
};

} // anonymous namespace


void MySqlExportMgr::_init() {
    std::string chunkedForm("(\\w+)_(\\d+)");
    boost::regex regex(chunkedForm);
    // Check metadata for databases to track

    SqlConnection sc(getConfig().getSqlConfig(), true);

    std::deque<std::string> dbs;

    getDbs(_log, _name, sc, dbs);
    // If we want to merge in the fs-level files/dirs, we will need the
    // export path (from getenv(XRDLCLROOT))
    // std::string exportRoot("/tmp/testExport");

    // get chunkList
    // SHOW TABLES IN db;
    std::deque<std::string> chunks;
    std::for_each(dbs.begin(), dbs.end(), doDb(sc, regex, _existMap));
    //
}
void MySqlExportMgr::fillDbChunks(MySqlExportMgr::StringSet& s) {
    s.clear();
    for(ExistMap::const_iterator ei=_existMap.begin();
        ei != _existMap.end(); ++ei) {
        std::string dbName = ei->first;
        ChunkMap const& cm = ei->second;
        std::for_each(cm.begin(), cm.end(), addDbItem(dbName, s));
        //std::cout << "loaded chunks for " << dbName << std::endl;
    }
}


