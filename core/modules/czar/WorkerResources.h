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
#ifndef LSST_QSERV_CZAR_WORKERRESOURCES_H
#define LSST_QSERV_CZAR_WORKERRESOURCES_H

// System headers
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <utility>

// Qserv headers
#include "global/ResourceUnit.h"


// This header declarations
namespace lsst {
namespace qserv {
namespace czar {


/// Store the chunk id numbers for a specific database.
class DbResource {
public:
    using Ptr = std::shared_ptr<DbResource>;

    static std::string getDbNameFromResource(std::string const& chunkResource);

    DbResource(std::string const& dbName) : _dbName(dbName) {};

    /// @return true if it was inserted.
    bool insert(int chunkId);

    /// @return a deque with all the elements in _chunkSet, in order.
    std::deque<int> getDeque();

private:
    std::string const _dbName;  // name of the database
    std::set<int> _chunkSet;
    std::mutex _mtx;
};


/// TODO:UJ These classes are essentiall place holders until it's been
/// determined how this information will be collected and cached.
class WorkerResource {
public:
    using Ptr = std::shared_ptr<WorkerResource>;
    WorkerResource(std::string const& name) : _resourceName(name) {}

    bool insert(std::string const& dbChunkResourceName);

    std::deque<int> getDequeFor(std::string const& dbName);

private:
    std::string _resourceName;
    std::map<std::string, DbResource::Ptr> _dbResources; /// Map of databases on the worker (key is dbName).
    std::mutex _dbMtx;
};


class WorkerResources {
public:

    WorkerResources() = default;
    WorkerResources(WorkerResources const&) = delete;
    WorkerResources& operator=(WorkerResources const&) = delete;

    ~WorkerResources() = default;

    std::pair<WorkerResource::Ptr, bool> insertWorker(std::string const& wResourceName) {
        std::lock_guard<std::mutex> lg(_workerMapMtx);
        return _insertWorker(wResourceName);
    }

    std::map<std::string, std::deque<int>> getDequesFor(std::string const& dbName);

    /// mono-node test functions
    void setMonoNodeTest();
    std::deque<std::string> fillChunkIdSet();

private:
    /// Insert a new worker into the map. Must lock _workerMapMtx before calling
    std::pair<WorkerResource::Ptr, bool> _insertWorker(std::string const& wResourceName) {
        WorkerResource::Ptr  newWr = std::make_shared<WorkerResource>(wResourceName);
        auto result = _workers.emplace(wResourceName, newWr);
        auto iter = result.first;
        return std::pair<WorkerResource::Ptr, bool>(iter->second, result.second);
    }

    std::map<std::string, WorkerResource::Ptr> _workers;
    std::mutex _workerMapMtx;
};



}}} // namespace lsst::qserv::czar
#endif // LSST_QSERV_CZAR_WORKERRESOURCES_H
