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
#ifndef LSST_QSERV_HTTP_WORKERQUERYSTATUSDATA_H
#define LSST_QSERV_HTTP_WORKERQUERYSTATUSDATA_H

// System headers
#include <chrono>
#include <map>
#include <memory>
#include <set>
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// qserv headers
#include "global/clock_defs.h"
#include "global/intTypes.h"

// This header declarations
namespace lsst::qserv::http {

/// This class just contains the czar id and network contact information.
class CzarContactInfo : public std::enable_shared_from_this<CzarContactInfo> {
public:
    using Ptr = std::shared_ptr<CzarContactInfo>;
    std::string cName(const char* fnc) const { return std::string("CzarContactInfo") + fnc; }

    CzarContactInfo() = delete;
    CzarContactInfo(CzarContactInfo const&) = default;
    CzarContactInfo& operator=(CzarContactInfo const&) = default;

    /// &&& doc
    bool compare(CzarContactInfo const& other) {
        return (czName == other.czName && czId == other.czId && czPort == other.czPort &&
                czHostName == other.czHostName);
    }

    static Ptr create(std::string const& czName_, CzarIdType czId_, int czPort_,
                      std::string const& czHostName_, uint64_t czStartupTime_) {
        return Ptr(new CzarContactInfo(czName_, czId_, czPort_, czHostName_, czStartupTime_));
    }

    static Ptr createFromJson(nlohmann::json const& czarJson);

    std::string const czName;      ///< czar "name"
    CzarIdType const czId;         ///< czar "id"
    int const czPort;              ///< czar "management-port"
    std::string const czHostName;  ///< czar "management-host-name"
    uint64_t const czStartupTime;  ///< czar startup time

    /// &&& doc
    nlohmann::json serializeJson() const;

    std::string dump() const;

private:
    CzarContactInfo(std::string const& czName_, CzarIdType czId_, int czPort_, std::string const& czHostName_,
                    uint64_t czStartupTime_)
            : czName(czName_),
              czId(czId_),
              czPort(czPort_),
              czHostName(czHostName_),
              czStartupTime(czStartupTime_) {}
};

/// This class just contains the worker id and network communication information.
class WorkerContactInfo {
public:
    using Ptr = std::shared_ptr<WorkerContactInfo>;

    using WCMap = std::unordered_map<std::string, Ptr>;
    using WCMapPtr = std::shared_ptr<WCMap>;

    static Ptr create(std::string const& wId_, std::string const& wHost_, std::string const& wManagementHost_,
                      int wPort_, TIMEPOINT updateTime_) {
        return Ptr(new WorkerContactInfo(wId_, wHost_, wManagementHost_, wPort_, updateTime_));
    }

    /// &&& doc Used to create WorkerQueryStatusData object from a registry json message.
    static Ptr createFromJsonRegistry(std::string const& wId_, nlohmann::json const& regJson);

    /// &&& doc Used to create WorkerQueryStatusData object from a worker json message.
    static Ptr createFromJsonWorker(nlohmann::json const& workerJson, TIMEPOINT updateTime);

    /// &&& doc
    nlohmann::json serializeJson() const;

    std::string cName(const char* fn) { return std::string("WorkerContactInfo::") + fn; }

    std::string const wId;  ///< key, this is the one thing that cannot change.

    std::string getWHost() const {
        std::lock_guard<std::mutex> lg(_rMtx);
        return _wHost;
    }

    std::string getWManagementHost() const {
        std::lock_guard<std::mutex> lg(_rMtx);
        return _wManagementHost;
    }

    int getWPort() const {
        std::lock_guard<std::mutex> lg(_rMtx);
        return _wPort;
    }

    /// &&doc
    void changeBaseInfo(WorkerContactInfo const& other) {
        auto [oWId, oWHost, oWManagementHost, oWPort] = other.getAll();
        std::lock_guard<std::mutex> lg(_rMtx);
        _wHost = oWHost;
        _wManagementHost = oWManagementHost;
        _wPort = oWPort;
    }

    /// @return wId - workerId
    /// @return _wHost - worker host
    /// @return _wManagementHost - management host
    /// @return _wPort - worker port
    std::tuple<std::string, std::string, std::string, int> getAll() const {
        std::lock_guard<std::mutex> lg(_rMtx);
        return {wId, _wHost, _wManagementHost, _wPort};
    }

    /// Return true if communication related items are the same.
    bool isSameContactInfo(WorkerContactInfo const& other) const {
        auto [oWId, oWHost, oWManagementHost, oWPort] = other.getAll();
        std::lock_guard<std::mutex> lg(_rMtx);
        return (wId == oWId && _wHost == oWHost && _wManagementHost == oWManagementHost && _wPort == oWPort);
    }

    void setRegUpdateTime(TIMEPOINT updateTime) {
        std::lock_guard<std::mutex> lg(_rMtx);
        _regUpdateTime = updateTime;
    }

    TIMEPOINT getRegUpdateTime(TIMEPOINT updateTime) {
        std::lock_guard<std::mutex> lg(_rMtx);
        return _regUpdateTime;
    }

    double timeSinceRegUpdateSeconds() const {
        std::lock_guard<std::mutex> lg(_rMtx);
        double secs = std::chrono::duration<double>(CLOCK::now() - _regUpdateTime).count();
        return secs;
    }

    TIMEPOINT getRegUpdateTime() const {
        std::lock_guard<std::mutex> lg(_rMtx);
        return _regUpdateTime;
    }

    /// @return true if startupTime equals _wStartupTime or _wStartupTime was never set,
    ///   if _wStartupTime was never set, it is set to startupTime.
    /// @return false indicates the worker was restarted and all associated jobs need
    ///   re-assignment.
    bool checkWStartupTime(uint64_t startupTime) {
        std::lock_guard<std::mutex> lg(_rMtx);
        if (_wStartupTime == startupTime) {
            return true;
        }
        if (_wStartupTime == 0) {
            _wStartupTime = startupTime;
            return true;
        }
        _wStartupTime = startupTime;
        return false;
    }

    uint64_t getWStartupTime() const {
        std::lock_guard<std::mutex> lg(_rMtx);
        return _wStartupTime;
    }

    std::string dump() const;

private:
    WorkerContactInfo(std::string const& wId_, std::string const& wHost_, std::string const& wManagementHost_,
                      int wPort_, TIMEPOINT updateTime_)
            : wId(wId_), _wHost(wHost_), _wManagementHost(wManagementHost_), _wPort(wPort_) {
        setRegUpdateTime(updateTime_);
    }

    // _rMtx must be locked before calling
    std::string _dump() const;

    // _rMtx must be locked before calling
    nlohmann::json _serializeJson() const;

    std::string _wHost;            ///< "host-addr" entry.
    std::string _wManagementHost;  ///< "management-host-name" entry.
    int _wPort;                    ///< "management-port" entry.

    /// Last time the registry heard from this worker. The ActiveWorker class
    /// will use this to determine the worker's state.
    /// &&& Store in seconds since epoch to make atomic?
    TIMEPOINT _regUpdateTime;

    /// "w-startup-time", it's value is set to zero until the real value is
    /// received from the worker. Once it is non-zero, any change indicates
    /// the worker was restarted and all UberJobs that were assigned there
    /// need to be unassigned. On the worker, this should always be set from
    /// foreman()->getStartupTime();
    uint64_t _wStartupTime = 0;

    mutable std::mutex _rMtx;  ///< protects _regUpdate
};

/// This classes purpose is to be a structure to store and transfer information
/// about which queries have been completed or cancelled on the worker. This
/// class contains the functions that encode and decode the data they contain
/// to and from a json format.
class WorkerQueryStatusData {
public:
    using Ptr = std::shared_ptr<WorkerQueryStatusData>;

    WorkerQueryStatusData() = delete;
    WorkerQueryStatusData(WorkerQueryStatusData const&) = delete;
    WorkerQueryStatusData& operator=(WorkerQueryStatusData const&) = delete;

    std::string cName(const char* fName) { return std::string("WorkerQueryStatusData::") + fName; }

    static Ptr create(WorkerContactInfo::Ptr const& wInfo_, CzarContactInfo::Ptr const& czInfo_,
                      std::string const& replicationInstanceId_, std::string const& replicationAuthKey_) {
        return Ptr(new WorkerQueryStatusData(wInfo_, czInfo_, replicationInstanceId_, replicationAuthKey_));
    }

    /// &&& doc Used to create WorkerQueryStatusData object from a worker json message.
    static Ptr createFromJson(nlohmann::json const& czarJson, std::string const& replicationInstanceId_,
                              std::string const& replicationAuthKey_, TIMEPOINT updateTm);

    ~WorkerQueryStatusData() = default;

    void setWInfo(WorkerContactInfo::Ptr const& wInfo_) {
        std::lock_guard<std::mutex> lgI(_infoMtx);
        if (_wInfo == nullptr) {
            _wInfo = wInfo_;
            return;
        }
        if (wInfo_ != nullptr) {
            // This only change host and port values of _wInfo.
            _wInfo->changeBaseInfo(*wInfo_);
        }
    }

    WorkerContactInfo::Ptr getWInfo() const {
        std::lock_guard<std::mutex> lgI(_infoMtx);
        return _wInfo;
    }
    CzarContactInfo::Ptr getCzInfo() const { return _czInfo; }

    /// doc &&&
    void addDeadUberJob(QueryId qId, UberJobId ujId, TIMEPOINT tm);

    /// &&& doc
    void addDeadUberJobs(QueryId qId, std::vector<UberJobId> ujIds, TIMEPOINT tm);

    /// &&& doc
    void addToDoneDeleteFiles(QueryId qId);

    /// &&& doc
    void addToDoneKeepFiles(QueryId qId);

    /// &&& doc
    void removeDeadUberJobsFor(QueryId qId);

    void setCzarCancelAfterRestart(CzarIdType czId, QueryId lastQId) {
        std::lock_guard<std::mutex> mapLg(mapMtx);
        czarCancelAfterRestart = true;
        czarCancelAfterRestartCzId = czId;
        czarCancelAfterRestartQId = lastQId;
    }

    bool isCzarRestart() const { return czarCancelAfterRestart; }
    CzarIdType getCzarRestartCzarId() const { return czarCancelAfterRestartCzId; }
    QueryId getCzarRestartQueryId() const { return czarCancelAfterRestartQId; }

    /// Create a json object held by a shared pointer to use as a message.
    /// Old objects in this instance will be removed after being added to the
    /// json message.
    std::shared_ptr<nlohmann::json> serializeJson(double maxLifetime);

    /// Add contents of qIdDoneKeepFiles, _qIdDoneDeleteFiles, and _qIdDeadUberJobs to `jsWR`,
    /// and remove map elements that have an age (tmMark - element.touchTime) greater
    /// than maxLifetime.
    void addListsToJson(nlohmann::json& jsWR, TIMEPOINT tmMark, double maxLifetime);

    /// &&& doc
    /// @throws std::invalid_argument
    void parseLists(nlohmann::json const& jsWR, TIMEPOINT updateTm);

    /// &&& doc
    nlohmann::json serializeResponseJson(uint64_t workerStartupTime);

    /// &&& doc
    std::pair<bool, bool> handleResponseJson(nlohmann::json const& jsResp);

    /// &&& doc
    static void parseListsInto(nlohmann::json const& jsWR, TIMEPOINT updateTm,
                               std::map<QueryId, TIMEPOINT>& doneKeepF,
                               std::map<QueryId, TIMEPOINT>& doneDeleteF,
                               std::map<QueryId, std::map<UberJobId, TIMEPOINT>>& deadUberJobs);

    std::string dump() const;

    // Making these private requires member functions to be written
    // that cause issues with linking. All of the workarounds are ugly.
    std::map<QueryId, TIMEPOINT> qIdDoneKeepFiles;                      ///< &&& doc - limit reached
    std::map<QueryId, TIMEPOINT> qIdDoneDeleteFiles;                    ///< &&& doc -cancelled/finished
    std::map<QueryId, std::map<UberJobId, TIMEPOINT>> qIdDeadUberJobs;  ///< &&& doc
    std::atomic<bool> czarCancelAfterRestart = false;
    CzarIdType czarCancelAfterRestartCzId = 0;
    QueryId czarCancelAfterRestartQId = 0;
    /// Protects _qIdDoneKeepFiles, _qIdDoneDeleteFiles, _qIdDeadUberJobs,
    /// and czarCancelAfter variables.
    mutable std::mutex mapMtx;

private:
    WorkerQueryStatusData(WorkerContactInfo::Ptr const& wInfo_, CzarContactInfo::Ptr const& czInfo_,
                          std::string const& replicationInstanceId_, std::string const& replicationAuthKey_)
            : _wInfo(wInfo_),
              _czInfo(czInfo_),
              _replicationInstanceId(replicationInstanceId_),
              _replicationAuthKey(replicationAuthKey_) {}

    WorkerContactInfo::Ptr _wInfo;       ///< &&& doc
    CzarContactInfo::Ptr const _czInfo;  //< &&& doc
    mutable std::mutex _infoMtx;         ///< protects wInfo

    std::string const _replicationInstanceId;  ///< &&& doc
    std::string const _replicationAuthKey;     ///< &&& doc

    /// _infoMtx must be locked before calling.
    std::string _dump() const;
};

/// &&& doc
/// This class is used to send/receive a message from the worker to a specific
/// czar when there has been a communication issue with the worker sending UberJob
/// file ready messages. If there have been timeouts, the worker will send this
/// message to the czar immediately after the worker receives a
/// WorkerQueryStatusData message from the czar (indicating that communication
/// is now possible).
/// If communication with the czar has failed for a long time, the worker
/// will set "_thoughtCzarWasDead" and delete all incomplete work associated
/// with that czar. Result files will remain until garbage cleanup or the czar
/// calls for their removal.
/// TODO:UJ &&& UberJob complete messages that failed to be sent to the czar
/// TODO:UJ &&& will be added to this message.
/// Upon successful completion, the worker will clear all values set by the
/// the czar.
/// This message is expected to only be needed rarely.
class WorkerCzarComIssue {
public:
    using Ptr = std::shared_ptr<WorkerCzarComIssue>;

    WorkerCzarComIssue() = delete;
    ~WorkerCzarComIssue() = default;

    std::string cName(const char* funcN) { return std::string("WorkerCzarComIssue") + funcN; }

    static Ptr create(std::string const& replicationInstanceId_, std::string const& replicationAuthKey_) {
        return Ptr(new WorkerCzarComIssue(replicationInstanceId_, replicationAuthKey_));
    }

    static Ptr createFromJson(nlohmann::json const& workerJson, std::string const& replicationInstanceId_,
                              std::string const& replicationAuthKey_);

    void setThoughtCzarWasDead(bool wasDead) {
        std::lock_guard lg(_wciMtx);
        _thoughtCzarWasDead = wasDead;
    }

    bool getThoughtCzarWasDead() const { return _thoughtCzarWasDead; }

    /// &&& doc
    bool needToSend() const {
        std::lock_guard lg(_wciMtx);
        return _thoughtCzarWasDead;  // &&& or list of failed transmits not empty.
    }

    /// &&& doc
    void setContactInfo(WorkerContactInfo::Ptr const& wInfo_, CzarContactInfo::Ptr const& czInfo_) {
        std::lock_guard lgWci(_wciMtx);
        if (_wInfo == nullptr && wInfo_ != nullptr) _wInfo = wInfo_;
        if (_czInfo == nullptr && czInfo_ != nullptr) _czInfo = czInfo_;
    }

    CzarContactInfo::Ptr getCzarInfo() const {
        std::lock_guard lgWci(_wciMtx);
        return _czInfo;
    }

    WorkerContactInfo::Ptr getWorkerInfo() const {
        std::lock_guard lgWci(_wciMtx);
        return _wInfo;
    }

    /// &&& doc
    std::shared_ptr<nlohmann::json> serializeJson();

    /// &&& doc
    nlohmann::json serializeResponseJson();

    std::string dump() const;

private:
    WorkerCzarComIssue(std::string const& replicationInstanceId_, std::string const& replicationAuthKey_)
            : _replicationInstanceId(replicationInstanceId_), _replicationAuthKey(replicationAuthKey_) {}

    std::string _dump() const;

    WorkerContactInfo::Ptr _wInfo;
    CzarContactInfo::Ptr _czInfo;
    std::string const _replicationInstanceId;  ///< &&& doc
    std::string const _replicationAuthKey;     ///< &&& doc

    /// Set to by the worker true if the czar was considered dead, and reset to false
    /// after the czar has acknowledged successful reception of this message.
    bool _thoughtCzarWasDead = false;

    mutable std::mutex _wciMtx;  ///< protects all members.
};

}  // namespace lsst::qserv::http

#endif  // LSST_QSERV_HTTP_WORKERQUERYSTATUSDATA_H
