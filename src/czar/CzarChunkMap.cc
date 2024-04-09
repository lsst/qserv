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

// Class header
#include "czar/CzarChunkMap.h"

// System headers
#include <sstream>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "qmeta/Exceptions.h"
#include "util/Bug.h"
#include "util/TimeUtils.h"

using namespace std;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.czar.CzarChunkMap");
}  // namespace

namespace lsst::qserv::czar {

CzarChunkMap::CzarChunkMap(std::shared_ptr<qmeta::QMeta> const& qmeta) : _qmeta(qmeta) {
    try {
        auto mapsSet = _read();
        if (!mapsSet) {
            throw ChunkMapException(ERR_LOC, "CzarChunkMap maps were not set in contructor");
        }
    } catch (qmeta::QMetaError const& qExc) {
        LOGS(_log, LOG_LVL_ERROR, __func__ << " CzarChunkMap could not read DB " << qExc.what());
        throw ChunkMapException(ERR_LOC, string(" CzarChunkMap constructor failed read ") + qExc.what());
    }
}

bool CzarChunkMap::_read() {
    LOGS(_log, LOG_LVL_TRACE, "CzarChunkMap::_read() start");
    // If replacing the map, this may take a bit of time, but it's probably
    // better to wait for new maps if something changed.
    std::lock_guard gLock(_mapMtx);
    qmeta::QMeta::ChunkMap qChunkMap = _qmeta->getChunkMap();
    if (_lastUpdateTime >= qChunkMap.updateTime) {
        LOGS(_log, LOG_LVL_DEBUG,
             __func__ << " CzarChunkMap no need to read "
                      << util::TimeUtils::timePointToDateTimeString(_lastUpdateTime)
                      << " db=" << util::TimeUtils::timePointToDateTimeString(qChunkMap.updateTime));
        return false;
    }

    // Make the new maps.
    auto [chunkMapPtr, wcMapPtr] = makeNewMaps(qChunkMap);

    verify(*chunkMapPtr, *wcMapPtr);
    LOGS(_log, LOG_LVL_DEBUG, " chunkMap=" << dumpChunkMap(*chunkMapPtr));
    LOGS(_log, LOG_LVL_DEBUG, " workerChunkMap=" << dumpWorkerChunkMap(*wcMapPtr));

    _workerChunkMap = wcMapPtr;
    _chunkMap = chunkMapPtr;
    _lastUpdateTime = qChunkMap.updateTime;

    LOGS(_log, LOG_LVL_TRACE, "CzarChunkMap::_read() end");
    return true;
}

pair<shared_ptr<CzarChunkMap::ChunkMap>, shared_ptr<CzarChunkMap::WorkerChunkMap>> CzarChunkMap::makeNewMaps(
        qmeta::QMeta::ChunkMap const& qChunkMap) {
    // Create new maps.
    auto wcMapPtr = make_shared<WorkerChunkMap>();
    auto chunkMapPtr = make_shared<ChunkMap>();

    // Workers -> Databases map
    for (auto const& [workerId, dbs] : qChunkMap.workers) {
        // Databases -> Tables map
        for (auto const& [dbName, tables] : dbs) {
            // Tables -> Chunks map
            for (auto const& [tableName, chunks] : tables) {
                // vector of ChunkInfo
                for (qmeta::QMeta::ChunkMap::ChunkInfo const& chunkInfo : chunks) {
                    try {
                        int64_t chunkNum = chunkInfo.chunk;
                        SizeT sz = chunkInfo.size;
                        LOGS(_log, LOG_LVL_DEBUG,
                             "workerdId=" << workerId << " db=" << dbName << " table=" << tableName
                                          << " chunk=" << chunkNum << " sz=" << sz);
                        insertIntoChunkMap(*wcMapPtr, *chunkMapPtr, workerId, dbName, tableName, chunkNum,
                                           sz);
                    } catch (invalid_argument const& exc) {
                        throw ChunkMapException(
                                ERR_LOC, string(__func__) + " invalid_argument workerdId=" + workerId +
                                                 " db=" + dbName + " table=" + tableName +
                                                 " chunk=" + to_string(chunkInfo.chunk) + " " + exc.what());
                    } catch (out_of_range const& exc) {
                        throw ChunkMapException(
                                ERR_LOC, string(__func__) + " out_of_range workerdId=" + workerId +
                                                 " db=" + dbName + " table=" + tableName +
                                                 " chunk=" + to_string(chunkInfo.chunk) + " " + exc.what());
                    }
                }
            }
        }
    }

    auto chunksSortedBySize = make_shared<ChunkVector>();
    calcChunkMap(*chunkMapPtr, *chunksSortedBySize);

    // At this point we have
    //  - wcMapPtr has a map of workerData by worker id with each worker having a map of ChunkData
    //  - chunkMapPtr has a map of all chunkData by chunk id
    //  - chunksSortedBySize a list of chunks sorted with largest first.
    // From here need to assign shared scan chunk priority
    // Go through the chunksSortedBySize list and assign each chunk to worker that has it with the smallest
    // totalScanSize.
    for (auto&& chunkData : *chunksSortedBySize) {
        SizeT smallest = std::numeric_limits<SizeT>::max();
        WorkerChunksData::Ptr smallestWkr = nullptr;
        for (auto&& [wkrId, wkrDataWeak] : chunkData->_workerHasThisMap) {
            auto wkrData = wkrDataWeak.lock();
            if (wkrData == nullptr) {
                LOGS(_log, LOG_LVL_ERROR, __func__ << " unexpected null weak ptr for " << wkrId);
                continue;  // maybe the next one will be ok.
            }
            LOGS(_log, LOG_LVL_DEBUG,
                 __func__ << " wkrId=" << wkrData << " tsz=" << wkrData->_sharedScanTotalSize
                          << " smallest=" << smallest);
            if (wkrData->_sharedScanTotalSize < smallest) {
                smallestWkr = wkrData;
                smallest = smallestWkr->_sharedScanTotalSize;
            }
        }
        if (smallestWkr == nullptr) {
            throw ChunkMapException(ERR_LOC, string(__func__) + " no smallesWkr found for chunk=" +
                                                     to_string(chunkData->_chunkId));
        }
        smallestWkr->_sharedScanChunkMap[chunkData->_chunkId] = chunkData;
        smallestWkr->_sharedScanTotalSize += chunkData->_totalBytes;
        chunkData->_primaryScanWorker = smallestWkr;
        LOGS(_log, LOG_LVL_DEBUG,
             " chunk=" << chunkData->_chunkId << " assigned to scan on " << smallestWkr->_workerId);
    }

    LOGS(_log, LOG_LVL_TRACE, " chunkMap=" << dumpChunkMap(*chunkMapPtr));
    LOGS(_log, LOG_LVL_TRACE, " workerChunkMap=" << dumpWorkerChunkMap(*wcMapPtr));
    return {chunkMapPtr, wcMapPtr};
}

void CzarChunkMap::insertIntoChunkMap(WorkerChunkMap& wcMap, ChunkMap& chunkMap, string const& workerId,
                                      string const& dbName, string const& tableName, int64_t chunkIdNum,
                                      SizeT sz) {
    // Get or make the worker entry
    WorkerChunksData::Ptr workerChunksData;
    auto iterWC = wcMap.find(workerId);
    if (iterWC == wcMap.end()) {
        workerChunksData = WorkerChunksData::Ptr(new WorkerChunksData(workerId));
        wcMap[workerId] = workerChunksData;
    } else {
        workerChunksData = iterWC->second;
    }

    // Get or make the ChunkData entry in chunkMap
    ChunkData::Ptr chunkData;
    auto iterChunkData = chunkMap.find(chunkIdNum);
    if (iterChunkData == chunkMap.end()) {
        chunkData = ChunkData::Ptr(new ChunkData(chunkIdNum));
        chunkMap[chunkIdNum] = chunkData;
    } else {
        chunkData = iterChunkData->second;
    }

    // Set or verify the table information
    auto iterDT = chunkData->_dbTableMap.find({dbName, tableName});
    if (iterDT == chunkData->_dbTableMap.end()) {
        // doesn't exist so set it up
        chunkData->_dbTableMap[{dbName, tableName}] = sz;
    } else {
        // Verify that it matches other data
        auto const& dbTbl = iterDT->first;
        auto tblSz = iterDT->second;
        auto const& dbN = dbTbl.first;
        auto const& tblN = dbTbl.second;
        if (dbName != dbN || tblN != tableName || tblSz != sz) {
            LOGS(_log, LOG_LVL_ERROR,
                 __func__ << " data mismatch for " << dbName << "." << tableName << "=" << sz << " vs " << dbN
                          << "." << tblN << "=" << tblSz);
        }
    }

    // Link WorkerData the single chunkData instance for the chunkId
    workerChunksData->_chunkDataMap[chunkIdNum] = chunkData;

    // Add worker to the list of workers containing the chunk.
    chunkData->addToWorkerHasThis(workerChunksData);
}

void CzarChunkMap::calcChunkMap(ChunkMap& chunkMap, ChunkVector& chunksSortedBySize) {
    // Calculate total bytes for all chunks.
    for (auto&& [chunkIdNum, chunkData] : chunkMap) {
        chunkData->_calcTotalBytes();
        chunksSortedBySize.push_back(chunkData);
    }

    sortChunks(chunksSortedBySize);
}

void CzarChunkMap::sortChunks(std::vector<ChunkData::Ptr>& chunksSortedBySize) {
    /// Return true if a->_totalBytes > b->_totalBytes
    auto sortBySizeDesc = [](ChunkData::Ptr const& a, ChunkData::Ptr const& b) {
        if (b == nullptr && a != nullptr) return true;
        if (a == nullptr) return false;
        return a->_totalBytes > b->_totalBytes;
    };

    std::sort(chunksSortedBySize.begin(), chunksSortedBySize.end(), sortBySizeDesc);
}

void CzarChunkMap::verify(ChunkMap const& chunkMap, WorkerChunkMap const& wcMap) {
    // Use a set to prevent duplicate ids caused by replication levels > 1.
    set<int64_t> allChunkIds;
    int errorCount = 0;
    for (auto const& [wkr, wkrData] : wcMap) {
        for (auto const& [chunkId, chunkData] : wkrData->_chunkDataMap) {
            allChunkIds.insert(chunkId);
        }
    }

    for (auto const& [chunkId, chunkDataPtr] : chunkMap) {
        if (chunkDataPtr == nullptr) {
            LOGS(_log, LOG_LVL_ERROR, " chunkId=" << chunkId << " had nullptr");
            ++errorCount;
            continue;
        }
        auto primeScanWkr = chunkDataPtr->_primaryScanWorker.lock();
        if (primeScanWkr == nullptr) {
            LOGS(_log, LOG_LVL_ERROR, " chunkId=" << chunkId << " missing primaryScanWorker");
            ++errorCount;
            continue;
        }
        if (primeScanWkr->_sharedScanChunkMap.find(chunkId) == primeScanWkr->_sharedScanChunkMap.end()) {
            LOGS(_log, LOG_LVL_ERROR,
                 " chunkId=" << chunkId << " should have been (and was not) in the sharedScanChunkMap for "
                             << primeScanWkr->_workerId);
            ++errorCount;
            continue;
        }
        auto iter = allChunkIds.find(chunkId);
        if (iter != allChunkIds.end()) {
            allChunkIds.erase(iter);
        } else {
            LOGS(_log, LOG_LVL_ERROR, " chunkId=" << chunkId << " chunkId was not in allChunks list");
            ++errorCount;
            continue;
        }
    }

    auto missing = allChunkIds.size();
    if (missing > 0) {
        string allMissingIds;
        for (auto const& cId : allChunkIds) {
            allMissingIds += to_string(cId) + ",";
        }
        LOGS(_log, LOG_LVL_ERROR,
             " There were " << missing << " missing chunks from the scan list " << allMissingIds);
        ++errorCount;
    }

    if (errorCount > 0) {
        throw ChunkMapException(ERR_LOC, "verification failed with " + to_string(errorCount) + " errors");
    }
}

string CzarChunkMap::dumpChunkMap(ChunkMap const& chunkMap) {
    stringstream os;
    os << "ChunkMap{";
    for (auto const& [cId, cDataPtr] : chunkMap) {
        os << "(cId=" << cId << ":";
        os << ((cDataPtr == nullptr) ? "null" : cDataPtr->dump()) << ")";
    }
    os << "}";
    return os.str();
}

string CzarChunkMap::dumpWorkerChunkMap(WorkerChunkMap const& wcMap) {
    stringstream os;
    os << "WorkerChunkMap{";
    for (auto const& [wId, wDataPtr] : wcMap) {
        os << "(wId=" << wId << ":";
        os << ((wDataPtr == nullptr) ? "null" : wDataPtr->dump()) << ")";
    }
    os << "}";
    return os.str();
}

void CzarChunkMap::ChunkData::_calcTotalBytes() {
    _totalBytes = 0;
    for (auto const& [key, val] : _dbTableMap) {
        _totalBytes += val;
    }
}

void CzarChunkMap::ChunkData::addToWorkerHasThis(std::shared_ptr<WorkerChunksData> const& worker) {
    if (worker == nullptr) {
        throw ChunkMapException(ERR_LOC, string(__func__) + " worker was null");
    }

    _workerHasThisMap[worker->_workerId] = worker;
}

string CzarChunkMap::ChunkData::dump() const {
    stringstream os;
    auto primaryWorker = _primaryScanWorker.lock();
    os << "{ChunkData id=" << _chunkId << " totalBytes=" << _totalBytes;
    os << " primaryWorker=" << ((primaryWorker == nullptr) ? "null" : primaryWorker->_workerId);
    os << " workers{";
    for (auto const& [wId, wData] : _workerHasThisMap) {
        os << "(" << wId << ")";
    }
    os << "} tables{";
    for (auto const& [dbTbl, sz] : _dbTableMap) {
        os << "(" << dbTbl.first << "." << dbTbl.second << " sz=" << sz << ")";
    }
    os << "}}";
    return os.str();
}

string CzarChunkMap::WorkerChunksData::dump() const {
    stringstream os;
    os << "{WorkerChunksData id=" << _workerId << " scanTotalSize=" << _sharedScanTotalSize;
    os << " chunkDataIds{";
    for (auto const& [chunkId, chunkData] : _chunkDataMap) {
        os << "(" << chunkId << ")";
    }
    os << "} sharedScanChunks{";
    for (auto const& [chunkId, chunkData] : _sharedScanChunkMap) {
        os << "(" << chunkId << ")";
    }
    os << "}}";
    return os.str();
}

}  // namespace lsst::qserv::czar
