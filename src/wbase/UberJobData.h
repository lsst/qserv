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
#include "http/Method.h"
#include "qmeta/types.h"
#include "util/QdispPool.h"
#include "wbase/SendChannel.h"
#include "util/InstanceCount.h"

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
class UberJobData : public std::enable_shared_from_this<UberJobData>{
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

    void setScanInteractive(bool scanInteractive) {
        _scanInteractive = scanInteractive;
    }

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

    bool getCancelled() const { return _cancelled; }

    /// &&& doc
    void cancelAllTasks();

private:
    UberJobData(UberJobId uberJobId, std::string const& czarName, qmeta::CzarId czarId, std::string czarHost,
                int czarPort, uint64_t queryId, std::string const& workerId,
                std::shared_ptr<wcontrol::Foreman> const& foreman, std::string const& authKey);

    /// &&& doc
    void _queueUJResponse(http::Method method_, std::vector<std::string> const& headers_, std::string const& url_, std::string const& requestContext_, std::string const& requestStr_);


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

    std::atomic<bool> _scanInteractive; ///< &&& doc

    std::atomic<bool> _cancelled{false}; ///< Set to true if this was cancelled.
};

/// &&& doc
class UJTransmitCmd : public util::PriorityCommand {
public:
    using Ptr = std::shared_ptr<UJTransmitCmd>;

    UJTransmitCmd() = delete;
    ~UJTransmitCmd() override = default;

    std::string cName(const char* funcN) const;

    /* &&&
    static Ptr create(std::shared_ptr<wcontrol::Foreman> const& foreman_, CzarIdType czarId_, QueryId queryId_, UberJobId uberJobId_, http::Method method_, std::vector<std::string> const& headers_, std::string const& url_, std::string const& requestContext_, std::string const& requestStr_) {
        auto ptr = Ptr(new UJTransmitCmd(foreman_, czarId_, queryId_, uberJobId_, method_, headers_, url_, requestContext_, requestStr_));
        ptr->_selfPtr = ptr;
        return ptr;
    }
    */
    static Ptr create(std::shared_ptr<wcontrol::Foreman> const& foreman_, UberJobData::Ptr const& ujData_, http::Method method_, std::vector<std::string> const& headers_, std::string const& url_, std::string const& requestContext_, std::string const& requestStr_) {
        auto ptr = Ptr(new UJTransmitCmd(foreman_, ujData_, method_, headers_, url_, requestContext_, requestStr_));
        ptr->_selfPtr = ptr;
        return ptr;
    }

    /// This is the function that will be run when the queue gets to this command.
    void action(util::CmdData* data) override;

    /// Reset the self pointer so this object can be killed.
    void kill();

    /// &&&
    Ptr duplicate();

private:
    /* &&&
    UJTransmitCmd(std::shared_ptr<wcontrol::Foreman> const& foreman_, CzarIdType czarId_, QueryId queryId_, UberJobId uberJobId_, http::Method method_, std::vector<std::string> const& headers_, std::string const& url_, std::string const& requestContext_, std::string const& requestStr_)
        : PriorityCommand(), _foreman(foreman_), _czarId(czarId_), _queryId(queryId_), _uberJobId(uberJobId_), _method(method_), _headers(headers_), _url(url_), _requestContext(requestContext_), _requestStr(requestStr_) {}
        */
    UJTransmitCmd(std::shared_ptr<wcontrol::Foreman> const& foreman_, UberJobData::Ptr const& ujData_, http::Method method_, std::vector<std::string> const& headers_, std::string const& url_, std::string const& requestContext_, std::string const& requestStr_)
        : PriorityCommand(), _foreman(foreman_), _ujData(ujData_), _czarId(ujData_->getCzarId()), _queryId(ujData_->getQueryId()), _uberJobId(ujData_->getUberJobId()), _method(method_), _headers(headers_), _url(url_), _requestContext(requestContext_), _requestStr(requestStr_) {}

    Ptr _selfPtr; ///< So this object can put itself back on the queue and keep itself alive.
    std::shared_ptr<wcontrol::Foreman> const _foreman;
    std::weak_ptr<UberJobData> const _ujData;
    CzarIdType const _czarId;
    QueryId const _queryId;
    UberJobId const _uberJobId;
    http::Method const _method;
    std::vector<std::string> const _headers;
    std::string const _url;
    std::string const _requestContext;
    std::string const _requestStr;
    int _attemptCount = 0; ///< How many attempts have been made to transmit this.
    util::InstanceCount _ic{cName("&&&")};
};

}  // namespace lsst::qserv::wbase

#endif  // LSST_QSERV_WBASE_UBERJOBDATA_H
