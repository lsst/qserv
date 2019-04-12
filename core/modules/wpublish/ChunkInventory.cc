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
#include <algorithm>
#include <cassert>
#include <deque>
#include <iostream>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "global/constants.h"
#include "sql/SqlConnection.h"

using lsst::qserv::sql::SqlConnection;
using lsst::qserv::sql::SqlErrorObject;
using lsst::qserv::sql::SqlResultIter;
using lsst::qserv::wpublish::ChunkInventory;

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.wpublish.ChunkInventory");



/// get a list of published databases
template <class C>
void fetchDbs(string const& instanceName,
              SqlConnection& sc,
              C& dbs) {

    string const query = "SELECT db FROM qservw_" + instanceName + ".Dbs";

    LOGS(_log, LOG_LVL_DEBUG, "Launching query: " << query);

    shared_ptr<SqlResultIter> resultP = sc.getQueryIter(query);
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
    if (nothing)
        LOGS(_log, LOG_LVL_WARN, "ChunkInventory couldn't find databases to export");
}


/// Fetch a list of chunks published for a database
void fetchChunks(string const& instanceName,
                 string const& db,
                 SqlConnection& sc,
                 ChunkInventory::ChunkMap& chunkMap) {

    string const query = "SELECT db,chunk FROM qservw_" + instanceName + ".Chunks WHERE db='" + db + "'";

    LOGS(_log, LOG_LVL_DEBUG, "Launching query: " << query);

    shared_ptr<SqlResultIter> resultP = sc.getQueryIter(query);
    assert(resultP.get());

    if (resultP->getErrorObject().isSet()) {
        SqlErrorObject& seo = resultP->getErrorObject();
        LOGS(_log, LOG_LVL_ERROR, "ChunkInventory failed to get a list of published chunks for db: " << db);
        LOGS(_log, LOG_LVL_ERROR, seo.printErrMsg());
        return;
    }
    bool nothing = true;
    for(; !resultP->done(); ++(*resultP)) {
        int const chunk = stoi((**resultP)[1]);
        chunkMap.insert(chunk);
        nothing = false;
    }
    if (nothing)
        LOGS(_log, LOG_LVL_WARN, "ChunkInventory couldn't find any published chunks for db: " << db);
}


/// Fetch a unique identifier of a worker
void fetchId(string const& instanceName,
             SqlConnection& sc,
             string& id) {

    // Look for the newest one
    // FIXME: perhaps we should allow multiple identifiers?
    string const query = "SELECT id FROM qservw_" + instanceName + ".Id WHERE `type`='UUID'";

    LOGS(_log, LOG_LVL_DEBUG, "Launching query: " << query);

    shared_ptr<SqlResultIter> resultP = sc.getQueryIter(query);
    assert(resultP.get());

    if (resultP->getErrorObject().isSet()) {
        SqlErrorObject& seo = resultP->getErrorObject();
        LOGS(_log, LOG_LVL_ERROR, "ChunkInventory failed to get a unique identifier of the worker");
        LOGS(_log, LOG_LVL_ERROR, seo.printErrMsg());
        return;
    }
    for(; !resultP->done(); ++(*resultP)) {
        id = (**resultP)[0];
        return;
    }
    LOGS(_log, LOG_LVL_WARN, "ChunkInventory couldn't find any a unique identifier of the worker");
}


class Validator : public lsst::qserv::ResourceUnit::Checker {
public:
    Validator(lsst::qserv::wpublish::ChunkInventory& c) : chunkInventory(c) {}
    virtual bool operator()(lsst::qserv::ResourceUnit const& ru) {
        switch (ru.unitType()) {
            case lsst::qserv::ResourceUnit::DBCHUNK: return chunkInventory.has(ru.db(), ru.chunk());
            case lsst::qserv::ResourceUnit::WORKER:  return chunkInventory.id() == ru.hashName();
            default: return false;
        }
    }
    lsst::qserv::wpublish::ChunkInventory& chunkInventory;
};

} // anonymous namespace



namespace lsst {
namespace qserv {
namespace wpublish {

ChunkInventory::ChunkInventory(string const& name,
                               shared_ptr<SqlConnection> sc)
    : _name(name) {
    _init(*sc);
}


ChunkInventory::ChunkInventory(ChunkInventory::ExistMap const& existMap,
                               string const& name,
                               string const& id)
    :   _existMap(existMap),
        _name(name),
        _id(id) {
}


void ChunkInventory::init(string const& name, mysql::MySqlConfig const& mySqlConfig) {
    _name = name;
    SqlConnection sc(mySqlConfig, true);
    _init(sc);
}


void ChunkInventory::rebuild(string const& name, mysql::MySqlConfig const& mySqlConfig) {
    _name = name;
    SqlConnection sc(mySqlConfig, true);
    _rebuild(sc);
    _init(sc);
}


void ChunkInventory::add(string const& db, int chunk) {

    lock_guard<mutex> lock(_mtx);

    LOGS(_log, LOG_LVL_DEBUG, "ChunkInventory::add()  db: " << db << ", chunk: " << chunk);

    // Adding unconditionally. if the database key doesn't exist then it will
    // be automatically added by this operation.
    _existMap[db].insert(chunk);

}


void ChunkInventory::add(string const& db, int chunk, mysql::MySqlConfig const& mySqlConfig) {

    lock_guard<mutex> lock(_mtx);

    LOGS(_log, LOG_LVL_DEBUG, "ChunkInventory::add()  db: " << db << ", chunk: " << chunk);

    SqlConnection sc(mySqlConfig, true);

    // Validate parameters of the request

    deque<string> dbs;
    ::fetchDbs(_name, sc, dbs);

    if (find(dbs.begin(), dbs.end(), db) == dbs.end()) {
        string const error = "ChunkInventory::add()  invalid database: " + db;
        LOGS(_log, LOG_LVL_ERROR, error);
        throw InvalidParamError(error);
    }

    vector<string> const queries = {
        "DELETE FROM qservw_" + _name + ".Chunks WHERE db='" + db + "' AND chunk=" + to_string(chunk),
        "INSERT INTO qservw_" + _name + ".Chunks (db,chunk) VALUES ('" + db + "'," + to_string(chunk) +")"
    };
    for (auto const& query: queries) {
        LOGS(_log, LOG_LVL_DEBUG, "Launching query:\n" << query);

        SqlErrorObject seo;
        if (not sc.runQuery(query, seo)) {
            string const error = "ChunkInventory failed to add a chunk, error: " + seo.printErrMsg();
            LOGS(_log, LOG_LVL_ERROR, error);
            throw QueryError(error);
        }
    }

    // Adding unconditionally. if the database key doesn't exist then it will
    // be automatically added by this operation.
    _existMap[db].insert(chunk);
}


void ChunkInventory::remove(string const& db, int chunk) {

    lock_guard<mutex> lock(_mtx);

    LOGS(_log, LOG_LVL_DEBUG, "ChunkInventory::remove()  db: " << db << ", chunk: " << chunk);

    // If no such database or a chunk exsist in the map then simply
    // quite and make no fuss about it.

    auto dbItr = _existMap.find(db);
    if (dbItr == _existMap.end()) return;

    auto chunks   = dbItr->second;
    auto chunkItr = chunks.find(chunk);
    if (chunkItr == chunks.end()) return;

    _existMap[db].erase(chunk);
}


void ChunkInventory::remove(string const& db, int chunk, mysql::MySqlConfig const& mySqlConfig) {

    lock_guard<mutex> lock(_mtx);

    LOGS(_log, LOG_LVL_DEBUG, "ChunkInventory::remove()  db: " << db << ", chunk: " << chunk);

    vector<string> const queries = {
        "DELETE FROM qservw_" + _name + ".Chunks WHERE db='" + db + "' AND chunk=" + to_string(chunk)
    };

    SqlConnection sc(mySqlConfig, true);

    for (auto const& query: queries) {
        LOGS(_log, LOG_LVL_DEBUG, "Launching query:\n" << query);

        SqlErrorObject seo;
        if (not sc.runQuery(query, seo)) {
            string const error = "ChunkInventory failed to remove a chunk, error: " + seo.printErrMsg();
            LOGS(_log, LOG_LVL_ERROR, error);
            throw QueryError(error);
        }
    }

    // If no such database or a chunk exsist in the map then simply
    // quite and make no fuss about it.

    auto dbItr = _existMap.find(db);
    if (dbItr == _existMap.end()) return;

    auto chunks   = dbItr->second;
    auto chunkItr = chunks.find(chunk);
    if (chunkItr == chunks.end()) return;

    _existMap[db].erase(chunk);
}


bool ChunkInventory::has(string const& db, int chunk) const {

    lock_guard<mutex> lock(_mtx);

    auto dbItr = _existMap.find(db);
    if (dbItr == _existMap.end()) return false;

    auto const& chunks   = dbItr->second;
    auto        chunkItr = chunks.find(chunk);
    if (chunkItr == chunks.end()) return false;

    return true;
}


shared_ptr<ResourceUnit::Checker> ChunkInventory::newValidator() {
    return shared_ptr<ResourceUnit::Checker>(new Validator(*this));
}


void ChunkInventory::dbgPrint(ostream& os) const {

    lock_guard<mutex> lock(_mtx);

    os << "ChunkInventory(";
    for(auto dbItr      = _existMap.begin(),
             dbItrBegin = _existMap.begin(),
             dbItrEnd   = _existMap.end(); dbItr != dbItrEnd; ++dbItr) {

        if (dbItr != dbItrBegin) os << endl;

        auto const& db     = dbItr->first;
        auto const& chunks = dbItr->second;

        os << "db: " << db << ", chunks: [";
        for (auto chunkItr      = chunks.begin(),
                  chunkItrBegin = chunks.begin(),
                  chunkItrEnd   = chunks.end(); chunkItr != chunkItrEnd; ++chunkItr) {

            if (chunkItr != chunkItrBegin) os << ",";

            auto const& chunk = *chunkItr;
            os << chunk;
        }
        os << "]";
    }
    os << ")";
}


void ChunkInventory::_init(SqlConnection& sc) {

    lock_guard<mutex> lock(_mtx);

    // Check metadata for databases to track

    deque<string> dbs;
    ::fetchDbs(_name, sc, dbs);

    // get chunkList
    _existMap.clear();
    for (string const& db: dbs)
        ::fetchChunks(_name, db, sc, _existMap[db]);

    // get unique identifier of a worker
    ::fetchId(_name, sc, _id);
}


void ChunkInventory::_rebuild(SqlConnection& sc) {

    lock_guard<mutex> lock(_mtx);

    vector<string> const queries = {
        "DELETE FROM qservw_" + _name + ".Chunks",
        "INSERT INTO qservw_" + _name + ".Chunks"
        "  SELECT DISTINCT TABLE_SCHEMA,SUBSTRING_INDEX(TABLE_NAME,'_',-1)"
        "    FROM information_schema.tables"
        "    WHERE TABLE_SCHEMA IN (SELECT db FROM qservw_" + _name + ".Dbs)"
        "          AND TABLE_NAME REGEXP '_[0-9]*$'"
    };

    for (auto const& query: queries) {
        LOGS(_log, LOG_LVL_DEBUG, "Launching query:\n" << query);

        SqlErrorObject seo;
        if (not sc.runQuery(query, seo)) {
            string const error =
                "ChunkInventory failed to rebuild a list of published chunks, error: " +
                seo.printErrMsg();
            LOGS(_log, LOG_LVL_ERROR, error);
            throw QueryError(error);
        }
    }
}


ChunkInventory::ExistMap ChunkInventory::existMap() const {

    ChunkInventory::ExistMap result;

    lock_guard<mutex> lock(_mtx);

    // Make this copy while holding the mutex too guarantee a consistent
    // result o fthe operation.
    result = _existMap;

    return result;
}


ChunkInventory::ExistMap operator-(ChunkInventory const& lhs, ChunkInventory const& rhs) {

    // The comparision will be made based on two self-consistent copies of
    // the maps obtained by calling the thread-safe accessor methods.

    ChunkInventory::ExistMap const lhs_existMap = lhs.existMap();
    ChunkInventory::ExistMap const rhs_existMap = rhs.existMap();
    ChunkInventory::ExistMap result;

    for (auto&& entry: lhs_existMap) {
        string const& db = entry.first;
        ChunkInventory::ChunkMap const& chunks = entry.second;
        if (rhs_existMap.count(db)) {
            for (int chunk: chunks)
                if (not rhs_existMap.at(db).count(chunk))
                    result[db].insert(chunk);   // this chunk was missing
        } else
            result[db] = chunks;    // the whole database was missing
    }
    return result;
}

}}} // lsst::qserv::wpublish
