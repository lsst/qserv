// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2016 AURA/LSST.
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
// ChunkInventory implementation.

// Class header
#include "wpublish/ChunkInventory.h"

// System headers
#include <cassert>
#include <exception>
#include <iostream>
#include <sstream>

// Third-party headers
#include "boost/regex.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "global/constants.h"
#include "sql/SqlConnection.h"

namespace { // File-scope helpers

LOG_LOGGER _log = LOG_GET("lsst.qserv.wpublish.ChunkInventory");

using lsst::qserv::sql::SqlConnection;
using lsst::qserv::sql::SqlErrorObject;
using lsst::qserv::sql::SqlResultIter;
using lsst::qserv::wpublish::ChunkInventory;

class CorruptDbError : public std::exception {
public:
    CorruptDbError(std::string const& msg) : _msg(msg) {}
    virtual ~CorruptDbError() throw() {}
    virtual const char* what() const throw() {
        try {
            return _msg.c_str();
        } catch(...) {} // Silence any exceptions
        return "";
    }

    std::string _msg;
};

inline std::string getTableNameDbListing(std::string const& instanceName) {
    return "qservw_" + instanceName + "." + "Dbs";
}

template <class C>
void fetchDbs(std::string const& instanceName,
              SqlConnection& sc,
              C& dbs) {

    // get list of tables
    // Assume table has schema that includes char column named "db"
    SqlErrorObject sqlErrorObject;
    std::string tableNameDbListing = getTableNameDbListing(instanceName);

    std::string listq = "SELECT db FROM " + tableNameDbListing;
    LOGS(_log, LOG_LVL_DEBUG, "Launching query: " << listq);
    std::shared_ptr<SqlResultIter> resultP = sc.getQueryIter(listq);
    assert(resultP.get());
    if (resultP->getErrorObject().isSet()) {
        SqlErrorObject& seo = resultP->getErrorObject();
        LOGS(_log, LOG_LVL_ERROR, "ChunkInventory can't get list of publishable dbs.");
        LOGS(_log, LOG_LVL_ERROR, seo.printErrMsg());
        return;
    }
    bool nothing = true;
    for(; !resultP->done(); ++(*resultP)) {
        dbs.push_back((**resultP)[0]);
        nothing = false;
    }
    if (nothing) {
        LOGS(_log, LOG_LVL_WARN, "TEST: No databases found to export: " << listq);
    }
}

/// Functor to be called per-table name
class doTable {
public:
    doTable(boost::regex& regex, ChunkInventory::ChunkMap& chunkMap)
        : _regex(regex), _chunkMap(chunkMap) {}
    void operator()(std::string const& tableName) {
        boost::smatch what;
        if (boost::regex_match(tableName, what, _regex)) {
            //std::cout << "Found chunk table: " << what[1]
            //<< "(" << what[2] << ")" << std::endl;
            // Get chunk# slot. Append/set table name.
            std::string chunkStr = what[2];
            int chunk = std::atoi(chunkStr.c_str());
            ChunkInventory::StringSet& ss = _chunkMap[chunk];
            ss.insert(what[1]);
        }
    }
private:
    boost::regex _regex;
    ChunkInventory::ChunkMap& _chunkMap;
};

/// Functor for iterating over a ChunkMap and printing out its contents.
struct printChunk {
    printChunk(std::ostream& os) : _os(os) {}
    void operator()(ChunkInventory::ChunkMap::value_type const& tuple) {
        std::stringstream ss;
        int chunkId = tuple.first;
        ChunkInventory::StringSet const& s = tuple.second;
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
    addDbItem(std::string const& dbName, ChunkInventory::StringSet& stringSet)
        : _dbName(dbName), _stringSet(stringSet) {}
    void operator()(ChunkInventory::ChunkMap::value_type const& tuple) {
        // Ignore tuple.second (list of tables for this chunk)
        _stringSet.insert(ChunkInventory::makeKey(_dbName, tuple.first));
    }
    std::string const& _dbName;
    ChunkInventory::StringSet& _stringSet;
};

/// Functor to load db
class doDb {
public:
    doDb(SqlConnection& conn,
         boost::regex& regex,
         ChunkInventory::ExistMap& existMap)
        : _conn(conn),
          _regex(regex),
          _existMap(existMap)
        {}

    void operator()(std::string const& dbName) {
        // get list of tables
        std::vector<std::string> tables;
        SqlErrorObject sqlErrorObject;
        bool ok = _conn.listTables(tables,  sqlErrorObject, "", dbName);
        if (!ok) {
            LOGS(_log, LOG_LVL_ERROR, "SQL error: " << sqlErrorObject.errMsg());
            assert(ok);
        }
        ChunkInventory::ChunkMap& chunkMap = _existMap[dbName];
        chunkMap.clear(); // Clear out stale entries to avoid mixing.
        std::for_each(tables.begin(), tables.end(),
                      doTable(_regex, chunkMap));
        // All databases get a dummy chunk.
        // Partitioned databases should already have acceptable dummy chunk
        // partitioned tables (e.g., Object_1234567890, Source_1234567890)
        // Non-partitioned databases need a dummy chunk anyway.
        if (chunkMap.empty()) {
            // No partitioned tables in this db. Publish an empty chunk anyway.
            chunkMap[lsst::qserv::DUMMY_CHUNK];
        } else {
            // Verify that there is a dummy chunk entry
            if (chunkMap.find(lsst::qserv::DUMMY_CHUNK) == chunkMap.end()) {
                LOGS(_log, LOG_LVL_ERROR, "Missing dummy chunk for db=" << dbName);

                // FIXME enable once loader/installer can ensure that the
                // dummy chunk exists exactly when appropriate

                // std::string msg = "Missing dummy chunk for db=" + dbName;
                // throw CorruptDbError(msg);
            }
        }
        //std::for_each(chunkMap.begin(), chunkMap.end(), printChunk(std::cout));
        // TODO: Sanity check: do all tables have the same chunks represented?
        }
private:
    SqlConnection& _conn;
    boost::regex& _regex;
    ChunkInventory::ExistMap& _existMap;
};

class Validator : public lsst::qserv::ResourceUnit::Checker {
public:
    Validator(lsst::qserv::wpublish::ChunkInventory& c) : chunkInventory(c) {}
    virtual bool operator()(lsst::qserv::ResourceUnit const& ru) {
        return chunkInventory.has(ru.db(), ru.chunk());
    }
    lsst::qserv::wpublish::ChunkInventory& chunkInventory;
};
} // anonymous namespace

namespace lsst {
namespace qserv {
namespace wpublish {

ChunkInventory::ChunkInventory(std::string const& name,
                               std::shared_ptr<SqlConnection> sc)
    : _name(name) {
    _init(*sc);
}

void ChunkInventory::init(std::string const& name, mysql::MySqlConfig const& mySqlConfig) {
    _name = name;
    SqlConnection sc(mySqlConfig, true);
    _init(sc);
}

bool ChunkInventory::has(std::string const& db, int chunk,
                         std::string table) const {
    ExistMap::const_iterator di = _existMap.find(db);
    if (di == _existMap.end()) { return false; }

    ChunkMap const& cm = di->second;
    ChunkMap::const_iterator ci = cm.find(chunk);
    if (ci == cm.end()) { return false; }

    if (table.empty()) {
        return true;
    } else {
        StringSet const& si = ci->second;
        return si.find(table) != si.end();
    }
}

std::shared_ptr<ResourceUnit::Checker> ChunkInventory::newValidator() {
    return std::shared_ptr<ResourceUnit::Checker>(new Validator(*this));
}

void ChunkInventory::dbgPrint(std::ostream& os) {
    os << "ChunkInventory(";
    ExistMap::const_iterator i,e;
    bool firstDb = true;
    for(i=_existMap.begin(), e=_existMap.end();
        i != e; ++i) {
        if (!firstDb) {
            os << std::endl;
            firstDb = false;
        }
        os << "db: " << i->first << " chunks=";
        ChunkMap::const_iterator ci, ce;
        bool firstChunk = true;
        for(ci=i->second.begin(), ce=i->second.end();
            ci != ce; ++ci) {
            if (!firstChunk) {
                os << "; ";
                firstChunk = false;
            }
            os << ci->first << " [";
            std::copy(ci->second.begin(), ci->second.end(),
                      std::ostream_iterator<std::string>(os, ","));
            os << "] \t";
        }
    }
    os << ")";
}

void ChunkInventory::_init(SqlConnection& sc) {
    std::string chunkedForm("(\\w+)_(\\d+)");
    boost::regex regex(chunkedForm);
    // Check metadata for databases to track

    std::deque<std::string> dbs;

    fetchDbs(_name, sc, dbs);
    // If we want to merge in the fs-level files/dirs, we will need the
    // export path (from getenv(XRDLCLROOT))
    // std::string exportRoot("/tmp/testExport");

    // get chunkList
    // SHOW TABLES IN db;
    std::deque<std::string> chunks;
    std::for_each(dbs.begin(), dbs.end(), doDb(sc, regex, _existMap));
}

}}} // lsst::qserv::wpublish

