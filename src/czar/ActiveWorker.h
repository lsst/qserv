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
#ifndef LSST_QSERV_CZAR_ACTIVEWORKER_H
#define LSST_QSERV_CZAR_ACTIVEWORKER_H

// System headers
#include <chrono>
#include <map>
#include <memory>
#include <set>
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// qserv headers
// &&& #include "global/clock_defs.h"
// &&& #include "global/intTypes.h"
#include "http/WorkerQueryStatusData.h"


// This header declarations
namespace lsst::qserv::czar {


/* &&&
/// &&& doc This class just contains the worker id and network communication
///     information, but it may be desirable to store connections to the
///     worker here as well.
class WorkerContactInfo {
public:
    using Ptr = std::shared_ptr<WorkerContactInfo>;

    using WCMap = std::unordered_map<std::string, Ptr>;
    using WCMapPtr = std::shared_ptr<WCMap>;

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
*/

/// &&& doc  - maintain list of done/cancelled queries for an active worker, and send
///            that list to the worker. Once the worker has accepted the list, remove
///            all of those queryId's from the list.
///          - maintain a list of killed UberJobs. If an UberJob is killed, nothing
///            will every look for its files, so they should be deleted, and the
///            worker should avoid working on Tasks for that UberJob.
///            The only UberJob deaths that need to be sent to a worker is when
///            the czar kills an UberJob because the worker died/vanished, and
///            the only time this would be sent is when a worker came back from
///            the dead.
///            The reason this only applies to died/vanished workers is that all
///            other workers know their UberJobs are dead because the worker killed
///            them. If the worker isn't told, it will continue working on
///            the UberJob until it finishes, and then find out the UberJob was killed
///            when it tries to return results to the czar (worker should delete files
///            for said UberJob at that point).
///            So, this should be very rare, only results in extra load, and therefore
///            is a low priority.
///
///            If a worker goes missing from the registry, it is considered DEAD and will be
///              removed after a period of time.
///            If a worker hasn't been heard from in (timeout period), it is considered QUESIONABLE.
///               When switching to QUESTIONABLE, a message will be sent to the worker asking
///               for an update.
///            If a QUESTIONABLE worker hasn't been heard from in (timeout period), its state is changed
///               to LOST_CONTACT and a message is sent to the worker asking for an update.
///            If a LOST_CONTACT worker hasn't been heard from in (timeout period), it becomes DEAD.
///
///            When a worker becomes DEAD: (this should probably all happen in _monitor).
///              - Affected UberJobs are killed.
///              - maps are remade without the dead workers
///              - uberjobs built to handle unassigned jobs.
///
class ActiveWorker : public std::enable_shared_from_this<ActiveWorker> {
public:
    using Ptr = std::shared_ptr<ActiveWorker>;

    enum State {
        ALIVE = 0,
        QUESTIONABLE,
        DEAD
    };

    ActiveWorker() = delete;
    ActiveWorker(ActiveWorker const&) = delete;
    ActiveWorker& operator=(ActiveWorker const&) = delete;

    std::string cName(const char* fName) {
        return std::string("ActiveWorker::") + fName + " " + ((_wqsData == nullptr) ? "?" : _wqsData->dump());
    }

    static std::string getStateStr(State st);

    static Ptr create(http::WorkerContactInfo::Ptr const& wInfo, http::CzarContactInfo::Ptr const& czInfo,
            std::string const& replicationInstanceId, std::string const& replicationAuthKey) {
        return Ptr(new ActiveWorker(wInfo, czInfo, replicationInstanceId, replicationAuthKey));
    }

    http::WorkerContactInfo::Ptr getWInfo() const {
        if (_wqsData == nullptr) return nullptr;
        return _wqsData->_wInfo;
    }

    ~ActiveWorker() = default;

    /// &&& doc
    bool compareContactInfo(http::WorkerContactInfo const& wcInfo) const;

    void setWorkerContactInfo(http::WorkerContactInfo::Ptr const& wcInfo);

    /// &&& doc
    void updateStateAndSendMessages(double timeoutAliveSecs, double timeoutDeadSecs, double maxLifetime);

    std::string dump() const;

private:
    ///&&&ActiveWorker(WorkerContactInfo::Ptr const& wInfo) : _wInfo(wInfo) {}
    ActiveWorker(http::WorkerContactInfo::Ptr const& wInfo, http::CzarContactInfo::Ptr const& czInfo,
            std::string const& replicationInstanceId, std::string const& replicationAuthKey)
    : _wqsData(http::WorkerQueryStatusData::create(wInfo, czInfo, replicationInstanceId, replicationAuthKey)) {}

    /// &&& doc
    /// _aMtx must be held before calling.
    void _changeStateTo(State newState, double secsSinceUpdate, std::string const& note);

    /// &&& doc
    void _sendStatusMsg(std::shared_ptr<nlohmann::json> const& jsWorkerReqPtr);

    /// &&& doc
    /// _aMtx must be held before calling.
    std::string _dump() const;

    /* &&&
    std::map<QueryId, TIMEPOINT> _qIdDoneKeepFiles;  ///< &&& doc - limit reached
    std::map<QueryId, TIMEPOINT> _qIdDoneDeleteFiles;  ///< &&& doc -cancelled/finished
    std::map<QueryId, std::map<UberJobId, TIMEPOINT>> _qIdDeadUberJobs; ///< &&& doc

    /// &&& TODO:UJ Worth the effort to inform worker of killed UberJobs?
    //std::map<QueryId, std::set<UberJobId>> _killedUberJobs;

    WorkerContactInfo::Ptr _wInfo; ///< &&& doc
    */
    http::WorkerQueryStatusData::Ptr _wqsData; ///< &&& doc

    State _state{QUESTIONABLE}; ///< current state of this worker.

    mutable std::mutex _aMtx; ///< protects _wInfo, _state, _qIdDoneKeepFiles, _qIdDoneDeleteFiles

    /// The number of communication threads currently in use by this class instance.
    std::atomic<int> _conThreadCount{0};
    int _maxConThreadCount{2};

    /// &&& doc
    /// @throws std::invalid_argument
    bool _parse(nlohmann::json const& jsWorkerReq); // &&& delete after basic testing
};

/// &&& doc
/// Maintain a list of all workers, indicating which are considered active. Communication
/// problems with workers could cause interesting race conditions, so workers will remain
/// on the list for a very long time after they have disappeared in the off chance they
/// come back from the dead.
class ActiveWorkerMap {
public:
    ActiveWorkerMap() = default;
    ActiveWorkerMap(ActiveWorkerMap const&) = delete;
    ActiveWorkerMap operator=(ActiveWorkerMap const&) = delete;
    ~ActiveWorkerMap() = default;

    std::string cName(const char* fName) {
        return std::string("ActiveWorkerMap::") + fName + " ";
    }

    /// &&& doc
    void updateMap(http::WorkerContactInfo::WCMap const& wcMap, http::CzarContactInfo::Ptr const& czInfo, std::string const& replicationInstanceId, std::string const& replicationAuthKey);

    //&&&void pruneMap(); /// &&& may not be needed ???

    // &&& doc
    void sendActiveWorkersMessages();

private:
    std::map<std::string, ActiveWorker::Ptr> _awMap;
    std::mutex _awMapMtx; ///< protects _awMap;

    //&&&double const _maxDeadTimeSeconds = 60.0 * 15.0; ///< &&& set from config.
    double _timeoutAliveSecs = 60.0 * 5.0; ///< &&& set from config. 5min
    double _timeoutDeadSecs = 60.0 * 10.0; ///< &&& set from config. 10min
    double _maxLifetime = 60.0 * 60.0; ///< &&& set from config. 1hr
};

}  // namespace lsst::qserv::czar

#endif  // LSST_QSERV_CZAR_ACTIVEWORKER_H
