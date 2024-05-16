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

/// &&& doc
class CzarContactInfo {
public:
    using Ptr = std::shared_ptr<CzarContactInfo>;
    std::string cName(const char* fnc) const {
        return std::string("CzarContactInfo") + fnc;
    }

    CzarContactInfo() = delete;
    CzarContactInfo(CzarContactInfo const&) = default;
    CzarContactInfo& operator=(CzarContactInfo const&) = default;

    /// &&& doc
    bool compare(CzarContactInfo const& other) {
        return (czName == other.czName && czId == other.czId && czPort == other.czPort && czHostName == other.czHostName);
    }

    static Ptr create(std::string const& czName_, CzarIdType czId_, int czPort_, std::string const& czHostName_) {
        return Ptr(new CzarContactInfo(czName_, czId_, czPort_, czHostName_));
    }

    static Ptr createJson(nlohmann::json const& czarJson);

    std::string const czName;  ///< czar "name"
    CzarIdType const czId;  ///< czar "id"
    int const czPort;     ///< czar "management-port"
    std::string const czHostName; ///< czar "management-host-name"

    /// &&& doc
    nlohmann::json serializeJson() const;

    /// &&& doc
    //&&&bool parse(nlohmann::json const& czarJson);

    std::string dump() const;
    /* &&&
    auto& jsWCzar = jsWorkerR["czar"];
    jsWCzar["name"] = czarConfig->name();
    jsWCzar["id"]= czarConfig->id();
    jsWCzar["management-port"] = czarConfig->replicationHttpPort();
    jsWCzar["management-host-name"] = util::get_current_host_fqdn();
    */
private:
    CzarContactInfo(std::string const& czName_, CzarIdType czId_, int czPort_, std::string const& czHostName_)
        : czName(czName_), czId(czId_), czPort(czPort_), czHostName(czHostName_) {}
};


/// &&& doc This class just contains the worker id and network communication
///     information, but it may be desirable to store connections to the
///     worker here as well.
class WorkerContactInfo {
public:
    using Ptr = std::shared_ptr<WorkerContactInfo>;

    using WCMap = std::unordered_map<std::string, Ptr>;
    using WCMapPtr = std::shared_ptr<WCMap>;

    static Ptr create(std::string const& wId_, std::string const& wHost_,
            std::string const& wManagementHost_, int wPort_, TIMEPOINT updateTime_) {
        return Ptr(new WorkerContactInfo(wId_, wHost_,
             wManagementHost_, wPort_, updateTime_));
    }

    /// &&& doc
    static Ptr createJson(nlohmann::json const& workerJson, TIMEPOINT updateTime);

    /// &&& doc
    nlohmann::json serializeJson() const;

    std::string cName(const char* fn) { return std::string("WorkerContactInfo::") + fn; }

    /// &&& make private
    WorkerContactInfo(std::string const& wId_, std::string const& wHost_,
            std::string const& wManagementHost_, int wPort_, TIMEPOINT updateTime_)
    : wId(wId_),
      wHost(wHost_),
      wManagementHost(wManagementHost_),
      wPort(wPort_) {
        regUpdateTime(updateTime_);
    }
    std::string const wId;              ///< key
    std::string const wHost;            ///< "host-addr" entry.
    std::string const wManagementHost;  ///< "management-host-name" entry.
    int const wPort;                    ///< "management-port" entry.


    /// Return true if all members, aside from updateTime, are equal.
    bool isSameContactInfo(WorkerContactInfo const& other) const {
        return (wId == other.wId && wHost == other.wHost && wManagementHost == other.wManagementHost &&
                wPort == other.wPort);
    }

    void regUpdateTime(TIMEPOINT updateTime) {
        std::lock_guard<std::mutex> lg(_rMtx);
        _regUpdate = updateTime;
    }

    double timeSinceRegUpdateSeconds() const {
        std::lock_guard<std::mutex> lg(_rMtx);
        double secs = std::chrono::duration<double>(CLOCK::now() - _regUpdate).count();
        return secs;
    }

    TIMEPOINT getRegUpdate() const {
        std::lock_guard<std::mutex> lg(_rMtx);
        return _regUpdate;
    }

    std::string dump() const;

private:
    /// Last time the registry heard from this worker. The ActiveWorker class
    /// will use this to determine the worker's state.
    /// &&& Store in seconds since epoch to make atomic?
    TIMEPOINT _regUpdate;

    mutable std::mutex _rMtx; ///< protects _regUpdate
};


/// &&& doc
class WorkerQueryStatusData  {
public:
    using Ptr = std::shared_ptr<WorkerQueryStatusData>;

    /* &&&
    enum State {
        ALIVE = 0,
        QUESTIONABLE,
        DEAD
    };
    */

    WorkerQueryStatusData() = delete;
    WorkerQueryStatusData(WorkerQueryStatusData const&) = delete;
    WorkerQueryStatusData& operator=(WorkerQueryStatusData const&) = delete;

    std::string cName(const char* fName) {
        return std::string("WorkerQueryStatusData::") + fName + " " + ((_wInfo == nullptr) ? "?" : _wInfo->wId);
    }

    //&&&static std::string getStateStr(State st);

    static Ptr create(WorkerContactInfo::Ptr const& wInfo, CzarContactInfo::Ptr const& czInfo,
            std::string const& replicationInstanceId, std::string const& replicationAuthKey) {
        return Ptr(new WorkerQueryStatusData(wInfo, czInfo, replicationInstanceId, replicationAuthKey));
    }

    /// &&& doc
    static Ptr createJson(nlohmann::json const& czarJson,
            std::string const& replicationInstanceId, std::string const& replicationAuthKey, TIMEPOINT updateTm);


    ~WorkerQueryStatusData() = default;

    WorkerContactInfo::Ptr getWInfo() const { return _wInfo; }

    /// &&& doc
    void addDeadUberJobs(QueryId qId, std::vector<UberJobId> ujIds, TIMEPOINT tm);

    std::string dump() const;

//&&&private:
    WorkerQueryStatusData(WorkerContactInfo::Ptr const& wInfo, CzarContactInfo::Ptr const& czInfo,
            std::string const& replicationInstanceId, std::string const& replicationAuthKey)
        : _wInfo(wInfo), _czInfo(czInfo),
          _replicationInstanceId(replicationInstanceId), _replicationAuthKey(replicationAuthKey) {}

    std::map<QueryId, TIMEPOINT> _qIdDoneKeepFiles;  ///< &&& doc - limit reached
    std::map<QueryId, TIMEPOINT> _qIdDoneDeleteFiles;  ///< &&& doc -cancelled/finished
    std::map<QueryId, std::map<UberJobId, TIMEPOINT>> _qIdDeadUberJobs; ///< &&& doc

    /// &&& TODO:UJ Worth the effort to inform worker of killed UberJobs?
    //std::map<QueryId, std::set<UberJobId>> _killedUberJobs;

    WorkerContactInfo::Ptr _wInfo; ///< &&& doc
    CzarContactInfo::Ptr _czInfo; //< &&& doc

    std::string const _replicationInstanceId; ///< &&& doc
    std::string const _replicationAuthKey;  ///< &&& doc

    /// &&& doc
    std::shared_ptr<nlohmann::json> serializeJson(double timeoutAliveSecs, double timeoutDeadSecs, double maxLifetime);

    /// &&& doc
    /// @throws std::invalid_argument
    bool _parseLists(nlohmann::json const& jsWorkerReq, TIMEPOINT updateTm); // &&& delete after basic testing
};

}  // namespace lsst::qserv::http

#endif  // LSST_QSERV_HTTP_WORKERQUERYSTATUSDATA_H
