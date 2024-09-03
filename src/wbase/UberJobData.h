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

namespace lsst::qserv {

namespace util {
class MultiError;
}

namespace wcontrol {
class Foreman;
}
}  // namespace lsst::qserv

namespace lsst::qserv::wbase {

class FileChannelShared;
class Task;

/// This class tracks all Tasks associates with the UberJob on the worker
/// and reports status to the czar.
class UberJobData {
public:
    using Ptr = std::shared_ptr<UberJobData>;

    UberJobData() = delete;
    UberJobData(UberJobData const&) = delete;

    static Ptr create(UberJobId uberJobId, std::string const& czarName, qmeta::CzarId czarId,
                      std::string const& czarHost, int czarPort, uint64_t queryId,
                      std::string const& workerId, std::shared_ptr<wcontrol::Foreman> const& foreman,
                      std::string const& authKey) {
        return Ptr(new UberJobData(uberJobId, czarName, czarId, czarHost, czarPort, queryId, workerId,
                                   foreman, authKey));
    }
    /// Set file channel for this UberJob
    void setFileChannelShared(std::shared_ptr<FileChannelShared> const& fileChannelShared);

    UberJobId getUberJobId() const { return _uberJobId; }
    qmeta::CzarId getCzarId() const { return _czarId; }
    std::string getCzarHost() const { return _czarHost; }
    int getCzarPort() const { return _czarPort; }
    uint64_t getQueryId() const { return _queryId; }
    std::string getWorkerId() const { return _workerId; }

    /// Add the tasks defined in the UberJob to this UberJobData object.
    void addTasks(std::vector<std::shared_ptr<wbase::Task>> const& tasks) {
        std::lock_guard<std::mutex> tLg(_ujTasksMtx);
        _ujTasks.insert(_ujTasks.end(), tasks.begin(), tasks.end());
    }

    /// Let the czar know the result is ready.
    void responseFileReady(std::string const& httpFileUrl, uint64_t rowCount, uint64_t fileSize,
                           uint64_t headerCount);  // TODO:UJ remove headerCount

    /// Let the Czar know there's been a problem.
    bool responseError(util::MultiError& multiErr, std::shared_ptr<Task> const& task, bool cancelled);

    std::string getIdStr() const { return _idStr; }
    std::string cName(std::string const& funcName) { return "UberJobData::" + funcName + " " + getIdStr(); }

    /// &&& doc
    void cancelAllTasks();

private:
    UberJobData(UberJobId uberJobId, std::string const& czarName, qmeta::CzarId czarId, std::string czarHost,
                int czarPort, uint64_t queryId, std::string const& workerId,
                std::shared_ptr<wcontrol::Foreman> const& foreman, std::string const& authKey);

    UberJobId const _uberJobId;
    std::string const _czarName;
    qmeta::CzarId const _czarId;
    std::string const _czarHost;
    int const _czarPort;
    QueryId const _queryId;
    std::string const _workerId;
    std::string const _authKey;

    std::shared_ptr<wcontrol::Foreman> const _foreman;

    std::vector<std::shared_ptr<wbase::Task>> _ujTasks;
    std::shared_ptr<FileChannelShared> _fileChannelShared;

    std::mutex _ujTasksMtx;  ///< Protects _ujTasks.

    std::string const _idStr;
};

}  // namespace lsst::qserv::wbase

#endif  // LSST_QSERV_WBASE_UBERJOBDATA_H
