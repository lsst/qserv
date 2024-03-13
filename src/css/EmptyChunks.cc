// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015-2016 AURA/LSST.
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

// Class header
#include "css/EmptyChunks.h"

// System headers
#include <iterator>
#include <functional>
#include <memory>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "css/CssError.h"
#include "css/DbInterfaceMySql.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.css.EmptyChunks");

}  // anonymous namespace

namespace lsst::qserv::css {

EmptyChunks::EmptyChunks(map<string, set<int>> const& database2chunks) {
    for (auto const& [db, chunks] : database2chunks) {
        auto newSet = make_shared<set<int>>();
        *newSet = chunks;
        _sets.insert({db, newSet});
    }
}

shared_ptr<set<int> const> EmptyChunks::getEmpty(string const& db) {
    lock_guard<mutex> lock(_mtx);
    if (auto itr = _sets.find(db); itr != _sets.end()) {
        return itr->second;
    }
    auto newSet = make_shared<set<int>>();
    if (_databaseInterface != nullptr) {
        *newSet = _populate(db);
        _sets.insert({db, newSet});
    }
    return shared_ptr<set<int> const>(newSet);
}

bool EmptyChunks::isEmpty(string const& db, int chunk) {
    auto const s = getEmpty(db);
    return s->end() != s->find(chunk);
}

void EmptyChunks::clearCache(string const& db) {
    lock_guard<mutex> lock(_mtx);
    if (db.empty()) {
        LOGS(_log, LOG_LVL_DEBUG, "Clearing empty chunks cache for all databases");
        _sets.clear();
    } else {
        LOGS(_log, LOG_LVL_DEBUG, "Clearing empty chunks cache for database " << db);
        _sets.erase(db);
    }
}

set<int> EmptyChunks::_populate(string const& db) {
    LOGS(_log, LOG_LVL_DEBUG, "populate " << db);
    try {
        return _databaseInterface->getEmptyChunks(db);
    } catch (CssError const& e) {
        string const eMsg("Failed to read empty chunks from table. Trying file. " + db + " " + e.what());
        LOGS(_log, LOG_LVL_ERROR, eMsg);
        throw CssError(ERR_LOC, eMsg);
    }
}

}  // namespace lsst::qserv::css
