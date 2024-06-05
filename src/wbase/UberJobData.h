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

#ifndef LSST_QSERV_WBASE_UBERJOBDATA_H
#define LSST_QSERV_WBASE_UBERJOBDATA_H

// System headers
#include <atomic>
#include <fstream>
#include <memory>
#include <mutex>
#include <vector>

// Third-party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "global/intTypes.h"
#include "qmeta/types.h"
#include "wbase/SendChannel.h"

namespace lsst::qserv::wcontrol {
class Foreman;
}

namespace lsst::qserv::wbase {

class FileChannelShared;
class Task;

// &&&uj doc
/// This class tracks all Tasks associates with the UberJob and reports status to the czar.
class UberJobData {
public:
    using Ptr = std::shared_ptr<UberJobData>;

    UberJobData() = delete;
    UberJobData(UberJobData const&) = delete;

    static Ptr create(UberJobId uberJobId, std::string const& czarName, qmeta::CzarId czarId, std::string const& czarHost, int czarPort,
                      uint64_t queryId, std::string const& workerId,
                      std::shared_ptr<wcontrol::Foreman> const& foreman, std::string const& authKey) {
        return Ptr(new UberJobData(uberJobId, czarName, czarId, czarHost, czarPort, queryId, workerId, foreman, authKey));
    }
    // &&& doc
    void setFileChannelShared(std::shared_ptr<FileChannelShared> const& fileChannelShared);

    UberJobId getUberJobId() const { return _uberJobId; }
    qmeta::CzarId getCzarId() const { return _czarId; }
    std::string getCzarHost() const { return _czarHost; }
    int getCzarPort() const { return _czarPort; }
    uint64_t getQueryId() const { return _queryId; }
    std::string getWorkerId() const { return _workerId; }

    /// &&& doc
    void addTasks(std::vector<std::shared_ptr<wbase::Task>> const& tasks) {
        _ujTasks.insert(_ujTasks.end(), tasks.begin(), tasks.end());
    }

    /// &&& doc
    void fileReadyResponse(std::string const& httpFileUrl, uint64_t rowCount, uint64_t fileSize);

private:
    UberJobData(UberJobId uberJobId, std::string const& czarName, qmeta::CzarId czarId, std::string czarHost, int czarPort,
                uint64_t queryId, std::string const& workerId,
                std::shared_ptr<wcontrol::Foreman> const& foreman, std::string const& authKey);

    UberJobId const _uberJobId;
    std::string const _czarName;
    qmeta::CzarId const _czarId;
    std::string const _czarHost;
    int const _czarPort;
    QueryId const _queryId;
    std::string const _workerId;  //&&&uj should be able to get this from the worker in a reasonable way.
    std::string const _authKey;

    std::shared_ptr<wcontrol::Foreman> const _foreman;

    std::vector<std::shared_ptr<wbase::Task>> _ujTasks;
    std::shared_ptr<FileChannelShared> _fileChannelShared;

    //&&&std::shared_ptr<wcontrol::Foreman> const foreman;
    //&&& std::string const targetWorkerId;  _workerId
    //&&&std::string const czarName;
    //&&&qmeta::CzarId const czarId;
    //&&&std::string const czarHostName;   _czarHost
    //&&& int const czarPort;
    //&&& uint64_t const queryId;
    //&&&uint64_t const uberJobId;


};

}  // namespace lsst::qserv::wbase

#endif  // LSST_QSERV_WBASE_UBERJOBDATA_H
