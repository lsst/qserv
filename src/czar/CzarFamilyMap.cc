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
#include "czar/CzarFamilyMap.h"

// System headers
#include <sstream>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "qmeta/QMeta.h"
#include "cconfig/CzarConfig.h"
#include "czar/Czar.h"
#include "czar/CzarRegistry.h"
#include "qmeta/Exceptions.h"
#include "util/Bug.h"
#include "util/TimeUtils.h"

using namespace std;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.czar.CzarFamilyMap");
}  // namespace

namespace lsst::qserv::czar {

CzarFamilyMap::Ptr CzarFamilyMap::create(std::shared_ptr<qmeta::QMeta> const& qmeta) {
    // There's nothing the czar can do until with user queries until there's been at least
    // one successful read of the database family tables, as the czar doesn't know where to find anything.
    Ptr newPtr = nullptr;
    while (newPtr == nullptr) {
        try {
            newPtr = Ptr(new CzarFamilyMap(qmeta));
        } catch (ChunkMapException const& exc) {
            LOGS(_log, LOG_LVL_WARN, "Could not create CzarFamilyMap, sleep and retry " << exc.what());
        }
        if (newPtr == nullptr) {
            this_thread::sleep_for(10s);
        }
    }

    return newPtr;
}

CzarFamilyMap::CzarFamilyMap(std::shared_ptr<qmeta::QMeta> const& qmeta) : _qmeta(qmeta) {
    try {
        auto mapsSet = _read();
        if (!mapsSet) {
            throw ChunkMapException(ERR_LOC, cName(__func__) + " maps were not set in constructor");
        }
    } catch (qmeta::QMetaError const& qExc) {
        LOGS(_log, LOG_LVL_ERROR, cName(__func__) << " could not read DB " << qExc.what());
        throw ChunkMapException(ERR_LOC, cName(__func__) + " constructor failed read " + qExc.what());
    }
}

bool CzarFamilyMap::read() {
    bool mapsSet = false;
    try {
        mapsSet = _read();
    } catch (qmeta::QMetaError const& qExc) {
        LOGS(_log, LOG_LVL_ERROR, cName(__func__) + " could not read DB " << qExc.what());
    }
    return mapsSet;
}

bool CzarFamilyMap::_read() {
    LOGS(_log, LOG_LVL_TRACE, "CzarFamilyMap::_read() start");
    // If replacing the map, this may take a bit of time, but it's probably
    // better to wait for new maps if something changed.
    std::lock_guard gLock(_familyMapMtx);
    qmeta::QMetaChunkMap qChunkMap = _qmeta->getChunkMap(_lastUpdateTime);
    if (_lastUpdateTime == qChunkMap.updateTime) {
        // If "_lastUpdateTime == qChunkMap.updateTime", qChunkMap is empty.
        LOGS(_log, LOG_LVL_INFO,
             cName(__func__) << " no need to read last="
                             << util::TimeUtils::timePointToDateTimeString(_lastUpdateTime)
                             << " map=" << util::TimeUtils::timePointToDateTimeString(qChunkMap.updateTime));
        return false;
    }

    // Make the new maps.
    auto czConfig = cconfig::CzarConfig::instance();
    bool usingChunkSize = czConfig->getFamilyMapUsingChunkSize();
    shared_ptr<CzarFamilyMap::FamilyMapType> familyMapPtr = makeNewMaps(qChunkMap, usingChunkSize);

    verify(familyMapPtr);

    for (auto const& [fam, ccMap] : *familyMapPtr) {
        LOGS(_log, LOG_LVL_INFO, "{family=" << fam << "{" << ccMap->dumpChunkMap() << "}}");
    }

    _familyMap = familyMapPtr;

    _lastUpdateTime = qChunkMap.updateTime;

    LOGS(_log, LOG_LVL_INFO,
         cName(__func__) << " read and verified "
                         << util::TimeUtils::timePointToDateTimeString(_lastUpdateTime));

    LOGS(_log, LOG_LVL_TRACE, "CzarChunkMap::_read() end");
    return true;
}

std::shared_ptr<CzarFamilyMap::FamilyMapType> CzarFamilyMap::makeNewMaps(
        qmeta::QMetaChunkMap const& qChunkMap, bool usingChunkSize) {
    // Create new maps.
    std::shared_ptr<FamilyMapType> newFamilyMap = make_shared<FamilyMapType>();

    // Workers -> Databases map
    LOGS(_log, LOG_LVL_DEBUG, cName(__func__) << " workers.sz=" << qChunkMap.workers.size());
    for (auto const& [workerId, dbs] : qChunkMap.workers) {
        // Databases -> Tables map
        for (auto const& [dbName, tables] : dbs) {
            // Tables -> Chunks map
            for (auto const& [tableName, chunks] : tables) {
                // vector of ChunkInfo
                for (qmeta::QMetaChunkMap::ChunkInfo const& chunkInfo : chunks) {
                    try {
                        int64_t chunkNum = chunkInfo.chunk;
                        CzarChunkMap::SizeT sz = 1;
                        if (usingChunkSize) {
                            sz = chunkInfo.size;
                        }
                        LOGS(_log, LOG_LVL_DEBUG,
                             cName(__func__) << "workerdId=" << workerId << " db=" << dbName << " table="
                                             << tableName << " chunk=" << chunkNum << " sz=" << sz);
                        _insertIntoMaps(newFamilyMap, workerId, dbName, tableName, chunkNum, sz);
                    } catch (invalid_argument const& exc) {
                        throw ChunkMapException(
                                ERR_LOC, cName(__func__) + " invalid_argument workerdId=" + workerId +
                                                 " db=" + dbName + " table=" + tableName +
                                                 " chunk=" + to_string(chunkInfo.chunk) + " " + exc.what());
                    } catch (out_of_range const& exc) {
                        throw ChunkMapException(
                                ERR_LOC, cName(__func__) + " out_of_range workerdId=" + workerId +
                                                 " db=" + dbName + " table=" + tableName +
                                                 " chunk=" + to_string(chunkInfo.chunk) + " " + exc.what());
                    }
                }
            }
        }
    }

    // This needs to be done for each CzarChunkMap in the family map.
    for (auto&& [familyName, chunkMapPtr] : *newFamilyMap) {
        LOGS(_log, LOG_LVL_DEBUG, cName(__func__) << " working on " << familyName);
        auto missing = chunkMapPtr->organize();
        if (missing != nullptr && !missing->empty()) {
            // TODO:DM-53240 Some element of the dashboard should be made aware of this. Also,
            // TODO:DM-53239 maybe this should check all families before throwing.
            //             There are implications that maybe the replicator should not
            //             tell the czar about families/databases that do not have
            //             at least one copy of each chunk with data loaded on a worker.
            string chunkIdStr;
            for (auto const& chunkData : *missing) {
                chunkIdStr += to_string(chunkData->getChunkId()) + " ";
            }
            throw ChunkMapException(
                    ERR_LOC, cName(__func__) + " family=" + familyName + " is missing chunks " + chunkIdStr);
        }
    }

    return newFamilyMap;
}

void CzarFamilyMap::_insertIntoMaps(std::shared_ptr<FamilyMapType> const& newFamilyMap,
                                    string const& workerId, string const& dbName, string const& tableName,
                                    int64_t chunkIdNum, CzarChunkMap::SizeT sz) {
    // Get the CzarChunkMap for this family
    auto familyName = getFamilyNameFromDbName(dbName);
    LOGS(_log, LOG_LVL_TRACE,
         cName(__func__) << " familyInsrt{w=" << workerId << " fN=" << familyName << " dbN=" << dbName
                         << " tblN=" << tableName << " chunk=" << chunkIdNum << " sz=" << sz << "}");
    auto& nfMap = *newFamilyMap;
    CzarChunkMap::Ptr czarChunkMap;
    auto familyIter = nfMap.find(familyName);
    if (familyIter == nfMap.end()) {
        czarChunkMap = CzarChunkMap::Ptr(new CzarChunkMap());
        nfMap[familyName] = czarChunkMap;
    } else {
        czarChunkMap = familyIter->second;
    }

    auto [chunkMapPtr, wcMapPtr] = czarChunkMap->_getMaps();

    CzarChunkMap::WorkerChunkMap& wcMap = *wcMapPtr;
    CzarChunkMap::ChunkMap& chunkMap = *chunkMapPtr;

    // Get or make the worker entry
    CzarChunkMap::WorkerChunksData::Ptr workerChunksData;
    auto iterWC = wcMap.find(workerId);
    if (iterWC == wcMap.end()) {
        workerChunksData = CzarChunkMap::WorkerChunksData::Ptr(new CzarChunkMap::WorkerChunksData(workerId));
        wcMap[workerId] = workerChunksData;
    } else {
        workerChunksData = iterWC->second;
    }

    // Get or make the ChunkData entry in chunkMap
    CzarChunkMap::ChunkData::Ptr chunkData;
    auto iterChunkData = chunkMap.find(chunkIdNum);
    if (iterChunkData == chunkMap.end()) {
        chunkData = CzarChunkMap::ChunkData::Ptr(new CzarChunkMap::ChunkData(chunkIdNum));
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
                 cName(__func__) << " data mismatch for " << dbName << "." << tableName << "=" << sz << " vs "
                                 << dbN << "." << tblN << "=" << tblSz);
        }
    }

    // Link WorkerData the single chunkData instance for the chunkId
    workerChunksData->_chunkDataMap[chunkIdNum] = chunkData;

    // Add worker to the list of workers containing the chunk.
    chunkData->addToWorkerHasThis(workerChunksData);
}

void CzarFamilyMap::verify(std::shared_ptr<FamilyMapType> const& familyMap) {
    for (auto&& [familyName, czarChunkMapPtr] : *familyMap) {
        czarChunkMapPtr->verify(familyName);
    }
}

}  // namespace lsst::qserv::czar
