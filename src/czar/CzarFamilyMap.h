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

#ifndef LSST_QSERV_CZAR_CZARFAMILYMAP_H
#define LSST_QSERV_CZAR_CZARFAMILYMAP_H

// System headers
#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <sstream>

// Qserv headers
#include "czar/CzarChunkMap.h"

namespace lsst::qserv::qmeta {
class QMeta;
struct QMetaChunkMap;
}  // namespace lsst::qserv::qmeta

namespace lsst::qserv::czar {

/// This class is used to organize worker chunk table information so that it
/// can be used to send jobs to the appropriate worker and inform workers
/// what chunks they can expect to handle in shared scans, focusing at the
/// family level.
/// The data for the maps is provided by the Replicator and stored in the
/// QMeta database.
/// When the data is changed, there is a timestamp that is updated, which
/// will cause new maps to be made by this class.
///
/// The maps generated should be treated as immutable objects stored with
/// shared pointers. As such, it should be possible for numerous threads
/// to use each map simultaneously provided they have their own pointers
/// to the maps.
/// The pointers to the maps are mutex protected to safely allow map updates.
//
// TODO:DM-53239 Currently, each family only has one database and they share
//   a name. Once a table mapping databases to families is available, it needs
//   to be used to map databases to families in this class.
class CzarFamilyMap {
public:
    using Ptr = std::shared_ptr<CzarFamilyMap>;
    typedef std::map<std::string, CzarChunkMap::Ptr> FamilyMapType;
    typedef std::map<std::string, std::string> DbNameToFamilyNameType;

    static Ptr create(std::shared_ptr<qmeta::QMeta> const& qmeta);

    CzarFamilyMap() = delete;
    CzarFamilyMap(CzarFamilyMap const&) = delete;
    CzarFamilyMap& operator=(CzarFamilyMap const&) = delete;

    ~CzarFamilyMap() = default;

    /// For unit testing only
    /// @param dbNameToFamilyNameType - valid map of db to family name for the unit test.
    // TODO::UJ define member instance for `_dbNameToFamilyName`
    CzarFamilyMap(std::shared_ptr<DbNameToFamilyNameType> const& dbNameToFamilyName) {}

    std::string cName(const char* fName) const {
        return std::string("CzarFamilyMap::") + ((fName == nullptr) ? "?" : fName);
    }

    /// Family names are unknown until a table has been added to the database, so
    /// the dbName will be used as the family name until the table exists.
    std::string getFamilyNameFromDbName(std::string const& dbName) const {
        // TODO:DM-53239 use a member instance of std::shared_ptr<DbNameToFamilyNameType>
        //     once info is available in QMeta.
        return dbName;
    }

    /// Return the chunk map for the database `dbName`
    CzarChunkMap::Ptr getChunkMap(std::string const& dbName) const {
        auto familyName = getFamilyNameFromDbName(dbName);
        return _getChunkMap(familyName);
    }

    /// Read the registry information from the database, if not already set.
    bool read();

    /// Make a new FamilyMapType map including ChunkMap and WorkerChunkMap from the data
    /// in `qChunkMap`. Each family has its own ChunkMap and WorkerChunkMap.
    /// @param qChunkMap - data source for the family map
    /// @param usingChunkSize - true if the distribution of chunks will depend on the
    ///                  size of the chunks/
    ///
    /// NOTE: This is likely an expensive operation and should probably only
    ///   be called if new workers have been added or chunks have been moved.
    std::shared_ptr<FamilyMapType> makeNewMaps(qmeta::QMetaChunkMap const& qChunkMap, bool usingChunkSize);

    /// Verify the `familyMap` does not have errors.
    static void verify(std::shared_ptr<FamilyMapType> const& familyMap);

private:
    /// Try to `_read` values for maps from `qmeta`.
    CzarFamilyMap(std::shared_ptr<qmeta::QMeta> const& qmeta);

    /// Read the registry information from the database, stopping if
    /// it hasn't been updated.
    // TODO:DM-53239 add a changed timestamp (similar to the existing updated timestamp)
    //    to the registry database and only update when changed.
    bool _read();

    /// Insert the new element described by the parameters into the `newFamilyMap` as appropriate.
    void _insertIntoMaps(std::shared_ptr<FamilyMapType> const& newFamilyMap, std::string const& workerId,
                         std::string const& dbName, std::string const& tableName, int64_t chunkIdNum,
                         CzarChunkMap::SizeT sz);

    /// Return the chunk map for the `familyName`
    CzarChunkMap::Ptr _getChunkMap(std::string const& familyName) const {
        std::lock_guard<std::mutex> familyLock(_familyMapMtx);
        auto iter = _familyMap->find(familyName);
        return (iter == _familyMap->end()) ? nullptr : iter->second;
    }

    std::shared_ptr<qmeta::QMeta> _qmeta;  ///< Database connection to collect json worker list.

    /// The last time the maps were updated with information from the replicator.
    TIMEPOINT _lastUpdateTime;  // initialized to 0;

    std::shared_ptr<FamilyMapType const> _familyMap{new FamilyMapType()};
    mutable std::mutex _familyMapMtx;  ///< protects _familyMap, _timeStamp, and _qmeta.
};

}  // namespace lsst::qserv::czar

#endif  // LSST_QSERV_CZAR_CZARFAMILYMAP_H
