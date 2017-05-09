// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2016 LSST Corporation.
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


#ifndef LSST_QSERV_WDB_SQLBACKEND_H
#define LSST_QSERV_WDB_SQLBACKEND_H

// System headers
#include <atomic>
#include <set>
#include <string>
#include <sys/types.h>
#include <unistd.h>

// Qserv headers
#include "global/DbTable.h"
#include "sql/SqlConnection.h"
#include "sql/SqlErrorObject.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace mysql {
    class MySqlConfig;
}}} // end forward declarations


namespace lsst {
namespace qserv {
namespace wdb {


struct ScTable {
    ScTable(int chunkId_, DbTable const& dbTable_, int subChunkId_)
        : chunkId(chunkId_), dbTable(dbTable_), subChunkId(subChunkId_) {
    }

    int chunkId;
    DbTable dbTable;
    int subChunkId;
};


typedef std::vector<ScTable> ScTableVector;


/// This class maintains a connection to the database for making temporary in-memory tables
/// for subchunks.
/// It is important at startup that any tables from a previous run are deleted. This happens
/// in the SQLBackend constructor call to SQLBackend::_memLockAcquire(). The reason it is so important
/// is that the in-memory tables have their schema written to disk but no data, so they are
/// just a bunch of empty tables when the program starts up.
class SQLBackend {
public:
    using Ptr=std::shared_ptr<SQLBackend>;

    SQLBackend(mysql::MySqlConfig const& mc)
        : _sqlConn(mc), _uid(getpid()) {
        _memLockAcquire();
    }

    virtual ~SQLBackend() {
        _memLockRelease();
    }

    virtual bool load(ScTableVector const& v, sql::SqlErrorObject& err);

    virtual void discard(ScTableVector const& v);

    enum LockStatus {UNLOCKED, LOCKED_OTHER, LOCKED_OURS};

    virtual void memLockRequireOwnership();

protected:
    SQLBackend() : _uid(getpid()) {};

    /// Construct a fake instance
    SQLBackend(char) : _uid(getpid()) {}

    virtual void _discard(ScTableVector::const_iterator begin, ScTableVector::const_iterator end);

    /// Run the 'query'. If it fails, terminate the program.
    void _execLockSql(std::string const& query);

    /// Return the status of the lock on the in memory tables.
    LockStatus _memLockStatus();

    /// Attempt to acquire the memory table lock, terminate this program if the lock is not acquired.
    // This must be run before any other operations on in memory tables.
    void _memLockAcquire();

    /// Delete the memory lock database and everything in it.
    void _memLockRelease();
    /// Exit the program immediately to reduce minimize possible problems.
    void _exitDueToConflict(const std::string& msg);

    sql::SqlConnection _sqlConn;

    // Memory lock table members.
    std::atomic<bool> _lockConflict{false};
    std::atomic<bool> _lockAquired{false};
    std::string _lockDb;
    std::string _lockTbl;
    std::string _lockDbTbl;
    int _uid;
};


/// Mock for unit testing other classes.
class FakeBackend : public SQLBackend {
public:
    using Ptr=std::shared_ptr<FakeBackend>;

    FakeBackend() {}

    virtual ~FakeBackend() {}

    bool load(ScTableVector const& v, sql::SqlErrorObject& err) override;

    void discard(ScTableVector const& v) override;

    void memLockRequireOwnership() override {}; ///< Do nothing for fake version.

    /// For unit tests only.
    static std::string makeFakeKey(ScTable const& sctbl) {
        std::string str = sctbl.dbTable.db + ":" + std::to_string(sctbl.chunkId) + ":"
                + sctbl.dbTable.table + ":" + std::to_string(sctbl.subChunkId);
        return str;
    }
    std::set<std::string> fakeSet; // set of strings for tracking unique tables.

private:
    void _discard(ScTableVector::const_iterator begin, ScTableVector::const_iterator end) override;
};

}}} // namespace lsst::qserv::wdb

#endif // LSST_QSERV_WDB_SQLBACKEND_H
