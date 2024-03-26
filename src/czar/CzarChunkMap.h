// -*- LSST-C++ -*-
/*
 * LSST Data Management System
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

#ifndef LSST_QSERV_CZAR_CZARCHUNKMAP_H
#define LSST_QSERV_CZAR_CZARCHUNKMAP_H

// System headers
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <sstream>

// Third party headers
#include <nlohmann/json.hpp>

// Qserv headers
#include "global/clock_defs.h"
#include "util/Issue.h"

namespace lsst::qserv::qmeta {
class QMeta;
}

namespace lsst::qserv::czar {

class ChunkMapException : public util::Issue {
public:
    ChunkMapException(Context const& ctx, std::string const& msg) : util::Issue(ctx, msg) {}
};

/// This class is used to organize worker chunk table information so that it
/// can be used to send jobs to the appropriate worker and inform workers
/// what chunks they can expect to handle in shared scans.
/// The data for the maps is provided by the Replicator and stored in QMeta.
/// When the data is changed, there is a timestamp that is updated, which
/// will cause new maps to be made by this class.
///
/// The maps generated are constant objects stored with shared pointers. As
/// such, it should be possible for numerous threads to use each map
/// simultaneously provided they have their own pointers to the maps.
/// The pointers to the maps are mutex protected to safely allow map updates.
///
/// The czar is expected to heavily use the
///    `getMaps() -> WorkerChunkMap -> getSharedScanChunkMap()`
/// to send jobs to workers, as that gets an ordered list of all chunks
/// the worker should handle during a shared scan.
///    `getMaps() -> ChunkMap` is expected to be more useful if there is a
///  failure and a chunk query needs to go to a different worker.
class CzarChunkMap {
public:
    using Ptr = std::shared_ptr<CzarChunkMap>;

    CzarChunkMap() = delete;

    static Ptr create(std::shared_ptr<qmeta::QMeta> const& qmeta) { return Ptr(new CzarChunkMap(qmeta)); }

    class WorkerChunksData;

    /// Essentially a structure for storing data about which tables and workers are associated with this
    /// chunk.
    class ChunkData {
    public:
        using Ptr = std::shared_ptr<ChunkData>;
        ChunkData(int chunkId_) : _chunkId(chunkId_) {}

        int64_t getChunkId() const { return _chunkId; }

        int64_t getTotalBytes() const { return _totalBytes; }

        std::weak_ptr<WorkerChunksData> getPrimaryScanWorker() const { return _primaryScanWorker; }

        /// Add up the bytes in each table for this chunk to get `_totalBytes`
        void calcTotalBytes();

        /// Add `worker` to the `_workerHasThisMap` to indicate that worker has a copy
        /// of this chunk.
        void addToWorkerHasThis(std::shared_ptr<WorkerChunksData> const& worker);

        std::string dump() const;

        friend CzarChunkMap;

    private:
        int64_t const _chunkId;   ///< The Id number for this chunk.
        int64_t _totalBytes = 0;  ///< The total number of bytes used by all tables in this chunk.
        std::weak_ptr<WorkerChunksData> _primaryScanWorker;  ///< The worker to be used to shared scans.

        /// Key is databaseName+tableName, value is size in bytes.
        std::map<std::pair<std::string, std::string>, int64_t> _dbTableMap;

        /// Map of workers that have this chunk
        std::map<std::string, std::weak_ptr<WorkerChunksData>> _workerHasThisMap;
    };

    /// Essentially a structure for storing which chunks are associated with a worker.
    class WorkerChunksData {
    public:
        using Ptr = std::shared_ptr<WorkerChunksData>;
        WorkerChunksData(std::string const& wId) : _workerId(wId) {}

        /// Return the worker's id string.
        std::string const& getWorkerId() const { return _workerId; }

        /// Return the number of bytes contained in all chunks/tables to be
        /// accessed in a full table scan on this worker.
        int64_t getSharedScanTotalSize() const { return _sharedScanTotalSize; }

        /// Return a reference to `_sharedScanChunkMap`. A copy of the pointer
        /// to this class (or the containing map) should be held to ensure the reference.
        std::map<int, ChunkData::Ptr> const& getSharedScanChunkMap() const { return _sharedScanChunkMap; }

        std::string dump() const;

        friend CzarChunkMap;

    private:
        std::string const _workerId;

        /// Map of all chunks found on the worker where key is chunkId
        std::map<int, ChunkData::Ptr> _chunkDataMap;

        /// Map of chunks this worker will handle during shared scans.
        /// Since scans are done in order of chunk id numbers, it helps
        /// to have this in chunk id number order.
        /// At some point, thus should be sent to workers so they
        /// can make more accurate time estimates for chunk completion.
        std::map<int, ChunkData::Ptr> _sharedScanChunkMap;

        /// The total size (in bytes) of all chunks on this worker that
        /// are to be used in shared scans.
        int64_t _sharedScanTotalSize = 0;
    };

    using WorkerChunkMap = std::map<std::string, WorkerChunksData::Ptr>;
    using ChunkMap = std::map<int, ChunkData::Ptr>;
    using ChunkVector = std::vector<ChunkData::Ptr>;

    /// Sort the chunks in `chunksSortedBySize` in descending order by total size in bytes.
    static void sortChunks(ChunkVector& chunksSortedBySize);

    /// Insert the chunk table described into the correct locations in
    /// `wcMap` and `chunkMap`.
    /// @param `wcMap` - WorkerChunkMap being constructed.
    /// @param `chunkMap` - ChunkMap being constructed.
    /// @param `workerId` - worker id where this table was found.
    /// @param `dbName` - database name for the table being inserted.
    /// @param `tableName` - table name for the table being inserted.
    /// @param `chunkIdNum` - chunk id number for the table being inserted.
    /// @param `sz` - size in bytes of the table being inserted.
    static void insertIntoChunkMap(WorkerChunkMap& wcMap, ChunkMap& chunkMap, std::string const& workerId,
                                   std::string const& dbName, std::string const& tableName,
                                   int64_t chunkIdNum, int64_t sz);

    /// Calculate the total bytes in each chunk and then sort the resulting ChunkVector by chunk size,
    /// descending.
    static void calcChunkMap(ChunkMap& chunkMap, ChunkVector& chunksSortedBySize);

    /// Make new ChunkMap and WorkerChunkMap from the data in `jsChunks`.
    static std::pair<std::shared_ptr<CzarChunkMap::ChunkMap>, std::shared_ptr<CzarChunkMap::WorkerChunkMap>>
    makeNewMaps(nlohmann::json const& jsChunks);

    /// Verify that all chunks belong to at least one worker and that all chunks are represented in shared
    /// scans.
    /// @throws ChunkMapException
    static void verify(ChunkMap const& chunkMap, WorkerChunkMap const& wcMap);

    static std::string dumpChunkMap(ChunkMap const& chunkMap);

    static std::string dumpWorkerChunkMap(WorkerChunkMap const& wcMap);

    /// Return shared pointers to `_chunkMap` and `_workerChunkMap`, which should be held until
    /// finished with the data.
    std::pair<std::shared_ptr<CzarChunkMap::ChunkMap const>,
              std::shared_ptr<CzarChunkMap::WorkerChunkMap const>>
    getMaps() const {
        std::lock_guard<std::mutex> lck(_mapMtx);
        return {_chunkMap, _workerChunkMap};
    }

private:
    /// Try to `_read` values for maps from `qmeta`.
    CzarChunkMap(std::shared_ptr<qmeta::QMeta> const& qmeta);

    /// Read the json worker list from the database and update the maps if there's a new
    /// version since the `_lastUpdateTime`.
    /// @throws `qmeta::QMetaError`
    bool _read();

    std::shared_ptr<qmeta::QMeta> _qmeta;  ///< Database connection to collect json worker list.

    /// Map of all workers and which chunks they contain.
    std::shared_ptr<WorkerChunkMap const> _workerChunkMap;

    /// Map of all chunks in the system with chunkId number as the key and the values contain
    /// information about the tables in those chunks and which worker is responsible for
    /// handling the chunk in a shared scan.
    std::shared_ptr<ChunkMap const> _chunkMap;

    /// The last time the maps were updated with information from the replicator.
    TIMEPOINT _lastUpdateTime;  // initialized to 0;

    mutable std::mutex _mapMtx;  ///< protects _workerChunkMap, _chunkMap, _timeStamp, and _qmeta.
};

}  // namespace lsst::qserv::czar

#endif  // LSST_QSERV_CZAR_CZARCHUNKMAP_H
