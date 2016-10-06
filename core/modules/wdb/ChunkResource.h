// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015 LSST Corporation.
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

#ifndef LSST_QSERV_WDB_CHUNKRESOURCE_H
#define LSST_QSERV_WDB_CHUNKRESOURCE_H
 /**
  * @file
  *
  * @brief ChunkResource tracks which chunks are needed. Right now, it is used
  * to manage subchunk creation.
  *
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <deque>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

// Third-party headers
#include "boost/utility.hpp"

// Qserv headers
#include "global/intTypes.h"
#include "global/stringTypes.h"
#include "sql/SqlConnection.h"
#include "sql/SqlErrorObject.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace mysql {
    class MySqlConfig;
}
namespace proto {
    class TaskMsg_Fragment;
}
namespace wdb {
    class Task;
}}} // End of forward declarations


namespace lsst {
namespace qserv {
namespace wdb {

class ChunkResourceMgr;

/// ChunkResources are reservations on data resources. Releases its resource
/// when it dies. If you make a copy, the copy holds its own reservation on the
/// same resource.
class ChunkResource {
public:
    class Info; // Internal metadata for the resource.
    ~ChunkResource();

    std::string const& getDb() const;
    int getChunkId() const;
    StringVector const& getTables() const;
    IntVector const& getSubChunkIds() const;

    friend class ChunkResourceMgr;
    ChunkResource(ChunkResource const& cr);
    ChunkResource& operator=(ChunkResource const& cr);
private:
    ChunkResource(ChunkResourceMgr* mgr);
    ChunkResource(ChunkResourceMgr* mgr, Info* info);

    ChunkResourceMgr *_mgr; ///< Do not delete, not owner.
    std::unique_ptr<Info> _info;
};


struct ScTable {
    ScTable(std::string const& db_, int chunkId_,
            std::string const& table_, int subChunkId_)
        : db(db_), chunkId(chunkId_), table(table_), subChunkId(subChunkId_) {
    }

    std::string db;
    int chunkId;
    std::string table;
    int subChunkId;
};


typedef std::vector<ScTable> ScTableVector;


/// This class maintains a connection to the database for making temporary in memory tables
/// for subchunks.
/// It is important at startup that any tables from a previous run are deleted. This happens
/// in the Backend constructor call to Backend::_memLockAcquire(). The reason it is so important
/// is that the in memory tables have their schema written to disk but no data, so they are
/// just a bunch of empty tables when the program starts up.
class Backend {
public:
    virtual ~Backend() {
        _memLockRelease();
    }
    typedef std::shared_ptr<Backend> Ptr;
    bool load(ScTableVector const& v, sql::SqlErrorObject& err);

    void discard(ScTableVector const& v);

    enum LockStatus {UNLOCKED, LOCKED_OTHER, LOCKED_OURS};

    void memLockRequireOwnership();

    static std::shared_ptr<Backend> newInstance(mysql::MySqlConfig const& mc) {
        return std::shared_ptr<Backend>(new Backend(mc));
    }
    static std::shared_ptr<Backend> newFakeInstance() {
        return std::shared_ptr<Backend>(new Backend('f'));
    }

    /// For unit tests only.
    static std::string makeFakeKey(ScTable const& sctbl) {
        std::string str = sctbl.db + ":" + std::to_string(sctbl.chunkId) + ":"
                + sctbl.table + ":" + std::to_string(sctbl.subChunkId);
        return str;
    }
    std::set<std::string> fakeSet; ///< For unit tests only.


private:
    Backend(mysql::MySqlConfig const& mc)
        : _isFake(false), _sqlConn(mc), _lockConflict(false), _uid(getpid()) {
        _memLockAcquire();
    }

    /// Construct a fake instance
    Backend(char) : _isFake(true), _lockConflict(false), _uid(getpid()) {}

    void _discard(ScTableVector::const_iterator begin,
                  ScTableVector::const_iterator end);

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

    bool _isFake;
    sql::SqlConnection _sqlConn;

    // Memory lock table members.
    bool _lockConflict;
    std::string _lockDb;
    std::string _lockTbl;
    std::string _lockDbTbl;
    int _uid;
};


/// ChunkResourceMgr is a lightweight manager for holding reservations on
/// subchunks.
class ChunkResourceMgr {
public:
    using Ptr = std::shared_ptr<ChunkResourceMgr>;

    /// Factory
    static Ptr newMgr(mysql::MySqlConfig const& c);
    static Ptr newFakeMgr();
    virtual ~ChunkResourceMgr() {}

    /// Reserve a chunk. Currently, this does not result in any explicit chunk
    /// loading.
    /// @return a ChunkResource which should be used for releasing the
    /// reservation.
    virtual ChunkResource acquire(std::string const& db, int chunkId,
                                  StringVector const& tables) = 0;
    /// Reserve a list of subchunks for a chunk. If they are not yet available,
    /// block until they are.
    /// @return a ChunkResource which should be used for releasing the
    /// reservation.
    virtual ChunkResource acquire(std::string const& db, int chunkId,
                                  StringVector const& tables,
                                  IntVector const& subChunks) = 0;

    /// Release a reservation. Currently, block until the resource has been
    /// released if the resource is no longer needed by anyone.
    /// Clients should not need to call this explicitly-- ChunkResource
    /// instances are implicit references and will release upon their
    /// destruction.
    virtual void release(ChunkResource::Info const& i) = 0;

    /// Acquire a reservation. Block until it is available if it is not
    /// already. Clients should not need to call this explicitly.
    virtual void acquireUnit(ChunkResource::Info const& i) = 0;

    /// @return the reference count for the database and chunkId.
    virtual int getRefCount(std::string const& db, int chunkId) = 0;

    virtual Backend::Ptr getBackend() const = 0;
private:
    class Impl; // Nested to share friend access to ChunkResource
};

}}} // namespace lsst::qserv::wdb

#endif // LSST_QSERV_WDB_CHUNKRESOURCE_H
