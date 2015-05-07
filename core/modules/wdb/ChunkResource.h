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
#include <ostream>
#include <string>
#include <vector>

// Third-party headers
#include <memory>
#include "boost/utility.hpp"

// Qserv headers
#include "global/intTypes.h"
#include "global/stringTypes.h"

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
    ChunkResource(ChunkResourceMgr& mgr);
    ChunkResource(ChunkResourceMgr& mgr, Info* info);

    ChunkResourceMgr& _mgr;
    std::auto_ptr<Info> _info;
};

/// ChunkResourceMgr is a lightweight manager for holding reservations on
/// subchunks.
class ChunkResourceMgr {
public:
    /// Factory
    static std::shared_ptr<ChunkResourceMgr> newMgr(mysql::MySqlConfig const& c);
    static std::shared_ptr<ChunkResourceMgr> newFakeMgr();
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
private:
    class Impl; // Nested to share friend access to ChunkResource
};
}}} // namespace lsst::qserv::wdb

#endif // LSST_QSERV_WDB_CHUNKRESOURCE_H
