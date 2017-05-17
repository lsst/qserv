// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2016 AURA/LSST.
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
#include "wdb/SQLBackend.h"

// System headers
#include <iostream>

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "global/constants.h"
#include "sql/SqlResults.h"
#include "wbase/Base.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.wdb.ChunkResource");

} // anonymous namespace


namespace lsst {
namespace qserv {
namespace wdb {


std::ostream& operator<<(std::ostream& os, ScTable const& st) {
    return os << SUBCHUNKDB_PREFIX << st.dbTable.db << "_" << st.chunkId << "."
              << st.dbTable.table << "_" << st.subChunkId;
}


bool SQLBackend::load(ScTableVector const& v, sql::SqlErrorObject& err) {
    using namespace lsst::qserv::wbase;
    memLockRequireOwnership();
    for(ScTableVector::const_iterator i=v.begin(), e=v.end();
            i != e; ++i) {
        std::string const* createScript = nullptr;
        if (i->chunkId == DUMMY_CHUNK) {
            createScript = &CREATE_DUMMY_SUBCHUNK_SCRIPT;
        } else {
            createScript = &CREATE_SUBCHUNK_SCRIPT;
        }
        std::string create = (boost::format(*createScript)
            % i->dbTable.db % i->dbTable.table % SUB_CHUNK_COLUMN
                % i->chunkId % i->subChunkId).str();

        if (!_sqlConn.runQuery(create, err)) {
            _discard(v.begin(), i);
            return false;
        }
    }
    return true;
}


void SQLBackend::discard(ScTableVector const& v) {
    _discard(v.begin(), v.end());
}


void SQLBackend::memLockRequireOwnership() {
    if (_memLockStatus() != LOCKED_OURS) {
        _exitDueToConflict("memLockRequireOwnership could not verify this program owned the memory table lock, Exiting.");
    }
}


void SQLBackend::_discard(ScTableVector::const_iterator begin,
              ScTableVector::const_iterator end) {
    memLockRequireOwnership();
    for(ScTableVector::const_iterator i=begin, e=end; i != e; ++i) {
        std::string discard = (boost::format(lsst::qserv::wbase::CLEANUP_SUBCHUNK_SCRIPT)
                % i->dbTable.db % i->dbTable.table % i->chunkId % i->subChunkId).str();
        sql::SqlErrorObject err;
        if (!_sqlConn.runQuery(discard, err)) {
            throw err;
        }
    }
}

/// Run the 'query'. If it fails, terminate the program.
void SQLBackend::_execLockSql(std::string const& query) {
    LOGS(_log, LOG_LVL_DEBUG, "execLockSql " << query);
    sql::SqlErrorObject err;
    if (!_sqlConn.runQuery(query, err)) {
        _exitDueToConflict("Lock failed, exiting. query=" + query + " err=" + err.printErrMsg());
    }
}

/// Return the status of the lock on the in memory tables.
SQLBackend::LockStatus SQLBackend::_memLockStatus() {
    std::string sql = "SELECT uid FROM " + _lockDbTbl + " WHERE keyId = 1";
    sql::SqlResults results;
    sql::SqlErrorObject err;
    if (!_sqlConn.runQuery(sql, results, err)) {
        // Assuming UNLOCKED should be safe as either it must be LOCKED_OURS to continue
        // or we are about to try to lock. Failure to lock will cause the program to exit.
        LOGS(_log, LOG_LVL_WARN, "memLockStatus query failed, assuming UNLOCKED. " << sql << " err=" << err.printErrMsg());
        return UNLOCKED;
    }
    std::string uidStr;
    if (!results.extractFirstValue(uidStr, err)) {
        LOGS(_log, LOG_LVL_WARN, "memLockStatus unexpected results, assuming LOCKED_OTHER. err=" << err.printErrMsg());
        return LOCKED_OTHER;
    }
    int uid = atoi(uidStr.c_str());
    if (uid != _uid) {
        LOGS(_log, LOG_LVL_WARN, "memLockStatus LOCKED_OTHER wrong uid. Expected "
             << _uid << " got " << uid << " err=" << err.printErrMsg());
        return LOCKED_OTHER;
    }
    return LOCKED_OURS;
}

/// Attempt to acquire the memory table lock, terminate this program if the lock is not acquired.
// This must be run before any other operations on in memory tables.
void SQLBackend::_memLockAcquire() {
    _lockDb = MEMLOCKDB;
    _lockTbl = MEMLOCKTBL;
    _lockDbTbl = _lockDb + "." + _lockTbl;
    LockStatus mls = _memLockStatus();
    if (mls != UNLOCKED) {
        LOGS(_log, LOG_LVL_WARN, "Memory tables were not released cleanly! LockStatus=" << mls);
    }

    // Lock the memory tables.
    std::string sql = "CREATE DATABASE IF NOT EXISTS " + _lockDb + ";";
    sql += "CREATE TABLE IF NOT EXISTS " + _lockDbTbl + " ( keyId INT UNIQUE, uid INT ) ENGINE = MEMORY;";
    _execLockSql(sql);
    // The following 2 lines will cause the new worker to always take the lock.
    sql = "TRUNCATE TABLE " + _lockDbTbl;
    _execLockSql(sql);
    std::ostringstream insert;
    insert << "INSERT INTO " << _lockDbTbl << " (keyId, uid) VALUES(1, " << _uid << " )";
    _execLockSql(insert.str());
    _lockAquired = true;

    // Delete any old in memory databases. They could be empty or otherwise wrong.
    // Empty tables would prevent new tables from being created.
    std::string subChunkPrefix = SUBCHUNKDB_PREFIX;
    sql = "SHOW DATABASES LIKE '" + subChunkPrefix + "%'";
    sql::SqlResults results;
    sql::SqlErrorObject err;
    if (!_sqlConn.runQuery(sql, results, err)) {
        _exitDueToConflict("SQLBackend query failed, exiting. " + sql + " err=" + err.printErrMsg());
    }
    std::vector<std::string> databases;
    results.extractFirstColumn(databases, err);
    for (auto iter=databases.begin(), end=databases.end(); iter != end;) {
        // Delete in blocks of 50 to save time.
        std::string dropDb = "";
        int count = 0;
        while (iter != end && count < 50) {
            std::string db = *iter;
            ++iter;
            // Check that the name is actually a match to subChunkPrefix and not a wild card match.
            if (db.compare(0, subChunkPrefix.length(), subChunkPrefix)==0 ) {
                dropDb += "DROP DATABASE " + db + ";";
                ++count;
            }
        }
        if (count > 0) {
            _execLockSql(dropDb);
        }
    }
}

/// Delete the memory lock database and everything in it.
void SQLBackend::_memLockRelease() {
    LOGS(_log, LOG_LVL_DEBUG, "memLockRelease");
    if (_lockAquired && !_lockConflict) {
        // Only attempt to release tables if the lock on the db was aquired.
        LOGS(_log, LOG_LVL_DEBUG, "memLockRelease releasing lock.");
        std::string sql = "DROP DATABASE " + _lockDb + ";";
        _execLockSql(sql);
    }
}

/// Exit the program immediately to reduce minimize possible problems.
void SQLBackend::_exitDueToConflict(const std::string& msg) {
    _lockConflict = true;
    LOGS(_log, LOG_LVL_ERROR, msg);
    exit(EXIT_FAILURE);
}


bool FakeBackend::load(ScTableVector const& v, sql::SqlErrorObject& err) {
    using namespace lsst::qserv::wbase;
    std::ostringstream os;
    os << "Pretending to load:";
    std::copy(v.begin(), v.end(),
            std::ostream_iterator<ScTable>(os, ","));
    os << std::endl;
    LOGS(_log, LOG_LVL_DEBUG, os.str());
    for (auto& scTbl : v) {
        std::string key = makeFakeKey(scTbl);
        fakeSet.insert(key);
    }
    return true;
}


void FakeBackend::discard(ScTableVector const& v) {
    for(auto const& scTbl : v) {
        fakeSet.erase(makeFakeKey(scTbl));
    }
    _discard(v.begin(), v.end());
}


void FakeBackend::_discard(ScTableVector::const_iterator begin,
              ScTableVector::const_iterator end) {
    std::ostringstream os;
    os << "Pretending to discard:";
    std::copy(begin, end, std::ostream_iterator<ScTable>(os, ","));
    LOGS(_log, LOG_LVL_DEBUG, os.str());
}

}}} // namespace lsst::qserv::wdb

