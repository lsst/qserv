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
#include "util/Issue.h"

namespace lsst::qserv::qmeta {
class QMeta;
}

namespace lsst::qserv::czar {

class ChunkMapException : public util::Issue {
public:
    ChunkMapException(Context const& ctx, std::string const& msg) : util::Issue(ctx, msg) {}
};

class CzarChunkMap {
public:
    using Ptr = std::shared_ptr<CzarChunkMap>;

    CzarChunkMap() = delete;

    static Ptr create(std::shared_ptr<qmeta::QMeta> const& qmeta) {
        return Ptr(new CzarChunkMap(qmeta));
    }

    class WorkerChunksData;

    class ChunkData {
    public:
        using Ptr = std::shared_ptr<ChunkData>;
        ChunkData(int chunkId_) : _chunkId(chunkId_) {}
        int64_t const _chunkId;
        int64_t _totalBytes = 0;
        std::weak_ptr<WorkerChunksData> _primaryScanWorker;

        /// &&& doc
        void calcTotalBytes();

        /// &&& doc
        void addToWorkerHasThis(std::shared_ptr<WorkerChunksData> const& worker);

        /// Key is databaseName+tableName, value is size in bytes.
        std::map<std::pair<std::string, std::string>, int64_t> _dbTableMap;

        /// Map of workers that have this chunk
        std::map<std::string, std::weak_ptr<WorkerChunksData>> _workerHasThisMap;

        std::string dump() const;
    };

    class WorkerChunksData {
    public:
        using Ptr = std::shared_ptr<WorkerChunksData>;
        WorkerChunksData(std::string const& wId) : _workerId(wId) {}

        std::string dump() const;

        std::string const _workerId;

        /// Map of all chunks found on the worker
        std::map<int, ChunkData::Ptr> _chunkDataMap; ///< key is chunkId // &&& needed?

        /// Map of chunks this worker will handle during shared scans.
        std::map<int, ChunkData::Ptr> _sharedScanChunkMap;
        int64_t _sharedScanTotalSize = 0;
    };

    using WorkerChunkMap = std::map<std::string, WorkerChunksData::Ptr>;
    using ChunkMap = std::map<int, ChunkData::Ptr>;
    using ChunkVector = std::vector<ChunkData::Ptr>;

    /// &&& doc
    static void sortChunks(ChunkVector& chunksSortedBySize);

    /// &&& doc
    /// @throws ChunkMapException
    static void verify(ChunkMap const& chunkMap, WorkerChunkMap const& wcMap);

    static std::string dumpChunkMap(ChunkMap const& chunkMap);

    static std::string dumpWorkerChunkMap(WorkerChunkMap const& wcMap);

private:
    /// Try to retrieve
    CzarChunkMap(std::shared_ptr<qmeta::QMeta> const& qmeta);

    /// &&& doc
    /// @throws `qmeta::QMetaError`
    void _read();

    /// &&& doc
    void _insertIntoChunkMap(WorkerChunkMap& wcMap, ChunkMap& chunkMap, std::string const& workerId, std::string const& dbName, std::string const& tableName, int64_t chunkIdNum, int64_t sz);

    /// &&& doc
    void _calcChunkMap(ChunkMap& chunkMap, ChunkVector& chunksSortedBySize);

    std::shared_ptr<qmeta::QMeta> _qmeta; ///< Database connection to collect json work list.

    /// &&& doc
    std::shared_ptr<WorkerChunkMap> _workerChunksMap;

    /// &&& doc
    std::shared_ptr<ChunkMap> _ChunksMap;

    /// List of chunks sorted by the total size of all tables in the chunk.
    std::shared_ptr<ChunkVector> _chunksSortedBySize;

};


}  // namespace lsst::qserv::czar

#endif  // LSST_QSERV_CZAR_CZARCHUNKMAP_H
