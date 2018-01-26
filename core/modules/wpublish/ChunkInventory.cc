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
#include <iostream>

// Third-party headers

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


/// get a list of published databases
template <class C>
void fetchDbs(std::string const& instanceName,
              SqlConnection& sc,
              C& dbs) {

    std::string const query = "SELECT db FROM qservw_" + instanceName + ".Dbs";

    LOGS(_log, LOG_LVL_DEBUG, "Launching query: " << query);

    std::shared_ptr<SqlResultIter> resultP = sc.getQueryIter(query);
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
void fetchChunks(std::string const& instanceName,
                 std::string const& db,
                 SqlConnection& sc,
                 ChunkInventory::ChunkMap& chunkMap) {

    std::string const query = "SELECT db,chunk FROM qservw_" + instanceName + ".Chunks WHERE db='" + db + "'";

    LOGS(_log, LOG_LVL_DEBUG, "Launching query: " << query);

    std::shared_ptr<SqlResultIter> resultP = sc.getQueryIter(query);
    assert(resultP.get());

    if (resultP->getErrorObject().isSet()) {
        SqlErrorObject& seo = resultP->getErrorObject();
        LOGS(_log, LOG_LVL_ERROR, "ChunkInventory failed to get a list of published chunks for db: " << db);
        LOGS(_log, LOG_LVL_ERROR, seo.printErrMsg());
        return;
    }
    bool nothing = true;
    for(; !resultP->done(); ++(*resultP)) {
        int const chunk = std::stoi((**resultP)[1]);
        chunkMap.insert(chunk);
        nothing = false;
    }
    if (nothing)
        LOGS(_log, LOG_LVL_WARN, "ChunkInventory couldn't find any published chunks for db: " << db);
}

/// Fetch a unique identifier of a worker
void fetchId(std::string const& instanceName,
             SqlConnection& sc,
             std::string& id) {

    // Look for the newest one
    // FIXME: perhaps we should allow multiple identifiers?
    std::string const query = "SELECT id FROM qservw_" + instanceName + ".Id WHERE `type`='UUID'";

    LOGS(_log, LOG_LVL_DEBUG, "Launching query: " << query);

    std::shared_ptr<SqlResultIter> resultP = sc.getQueryIter(query);
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

bool ChunkInventory::has(std::string const& db, int chunk) const {

    auto dbItr = _existMap.find(db);
    if (dbItr == _existMap.end()) return false;

    auto const& chunks   = dbItr->second;
    auto        chunkItr = chunks.find(chunk);
    if (chunkItr == chunks.end()) return false;

    return true;
}

std::shared_ptr<ResourceUnit::Checker> ChunkInventory::newValidator() {
    return std::shared_ptr<ResourceUnit::Checker>(new Validator(*this));
}

void ChunkInventory::dbgPrint(std::ostream& os) {

    os << "ChunkInventory(";
    for(auto dbItr      = _existMap.begin(),
             dbItrBegin = _existMap.begin(),
             dbItrEnd   = _existMap.end(); dbItr != dbItrEnd; ++dbItr) {

        if (dbItr != dbItrBegin) os << std::endl;

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

    // Check metadata for databases to track

    std::deque<std::string> dbs;
    ::fetchDbs(_name, sc, dbs);

    // get chunkList
    _existMap.clear();
    for (std::string const& db: dbs)
        ::fetchChunks(_name, db, sc, _existMap[db]);

    // get unique identifier of a worker
    ::fetchId(_name, sc, _id);
}

}}} // lsst::qserv::wpublish

