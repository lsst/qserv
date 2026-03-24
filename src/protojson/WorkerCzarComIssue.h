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
#ifndef LSST_QSERV_PROTOJSON_WORKERCZARCOMISSUE_H
#define LSST_QSERV_PROTOJSON_WORKERCZARCOMISSUE_H

// System headers
#include <chrono>
#include <map>
#include <memory>
#include <set>
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// qserv headers
#include "protojson/ResponseMsg.h"
#include "protojson/WorkerQueryStatusData.h"

// This header declarations
namespace lsst::qserv::protojson {

class UberJobStatusMsg;

typedef std::shared_ptr<UberJobStatusMsg> FailedTransmitType;
typedef std::map<std::pair<QueryId, UberJobId>, FailedTransmitType> FailedTransmitsMap;

typedef std::tuple<CzarContactInfo::Ptr, QueryId, UberJobId> UberJobIdentifierType;

/// This class is used to send/receive a message from the worker to a specific
/// czar. It is used when there has been a communication issue with the worker
/// sending UberJob file ready messages. If there have been timeouts, the worker
/// will send this message to the czar immediately after the worker receives a
/// WorkerQueryStatusData message from the czar. Receiving that message indicates
/// that czar is once again capable of communicating.
///
/// If communication with the czar has failed for a long time, the worker
/// will set "_thoughtCzarWasDead" and delete all incomplete work associated
/// with that czar. Result files will remain until garbage cleanup or the czar
/// calls for their removal.
///
/// UberJob file ready messages that failed to be sent to the czar will be
/// added to this message via the `_failtedTransmit` map. The czar response to
/// this will include a list of QueryId + UberJobId values, which will be
/// cleared from `_failtedTransmit`.
///
/// Since QueryId + UberJobId is unique, the czar ignores all calls after the
/// first one to collect the worker's file, but attempts are made to minimize
/// duplicate calls.
///
/// This message is expected to rarely be needed.
class WorkerCzarComIssue {
public:
    using Ptr = std::shared_ptr<WorkerCzarComIssue>;

    WorkerCzarComIssue() = delete;
    ~WorkerCzarComIssue() = default;

    bool operator==(WorkerCzarComIssue const& other) const;

    std::string cName(const char* funcN) const { return std::string("WorkerCzarComIssue") + funcN; }

    static Ptr create(AuthContext const& authContext_) { return Ptr(new WorkerCzarComIssue(authContext_)); }

    static Ptr createFromJson(nlohmann::json const& workerJson, AuthContext const& authContext_);

    void setThoughtCzarWasDeadTime(uint64_t msDeadNowAliveTime) {
        std::lock_guard lg(_wciMtx);
        _thoughtCzarWasDeadTime = msDeadNowAliveTime;
    }

    /// Remove all entries from the failedTransmits map with QueryId `qId`.
    void clearTransmitsForQid(QueryId qId) {  //&&& this needs to be called somewhere. &&&
        std::lock_guard lg(_wciMtx);
        for (auto it = _failedTransmits->begin(); it != _failedTransmits->end();) {
            if (it->first.first == qId) {
                it = _failedTransmits->erase(it);
            } else {
                ++it;
            }
        }
    }

    /// Go through the list of QueryId + UberJobId values in the response and clear those entries from the
    /// failedTransmits map.
    /// @return - the number of entries cleared,
    ///         - a vector of obsolete UberJob identifiers
    ///         - a vector of UberJob identifier that the czar could not parse
    /// Nothing really needs to be done with the vector of obsolete UberJob identifiers, but deleting them
    /// will conserve resources. The vector of UberJob identifiers that the czar could not parse is a problem.
    /// An error message should be sent back to the czar for each of those in an attempt to maintain system
    /// stability, but they really should never have happened in the first place.
    std::tuple<size_t, std::vector<UberJobIdentifierType>, std::vector<UberJobIdentifierType>>
    clearMapEntries(nlohmann::json const& response);

    uint64_t getThoughtCzarWasDeadTime() const { return _thoughtCzarWasDeadTime; }

    /// Return true if there is a reason this WorkerCzarComIssue should be sent to this czar.
    bool needToSend() const {
        std::lock_guard lg(_wciMtx);
        return (_thoughtCzarWasDeadTime > 0 || _failedTransmits->size() > 0);
    }

    /// Set the contact information for the appropriate czar and worker.
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

    /// The `request` may indicate success or failure
    void addFailedTransmit(QueryId qId, UberJobId ujId,
                           std::shared_ptr<protojson::UberJobStatusMsg> const& ujMsg);

    /// Return a json version of the contents of this class.
    nlohmann::json toJson();

    /// Return a json object indicating the status of the message for the
    /// original requester.
    nlohmann::json responseToJson(uint64_t msgThoughtCzarWasDeadTime,
                                  std::vector<protojson::ExecutiveRespMsg::Ptr> const& execRespMsgs) const;

    /// Take the failedTransmitsMap and make an empty one to take its place.
    std::shared_ptr<FailedTransmitsMap> takeFailedTransmitsMap();

    std::string dump() const;

private:
    WorkerCzarComIssue(AuthContext const& authContext_) : _authContext(authContext_) {}

    /// The `request` may indicate success or failure
    void _addFailedTransmit(QueryId qId, UberJobId ujId,
                            std::shared_ptr<protojson::UberJobStatusMsg> const& ujMsg);

    /// Return a json object indicating the status of the message for the
    /// original requester.
    nlohmann::json _responseToJson(uint64_t thoughtCzarWasDeadTime,
                                   std::vector<protojson::ExecutiveRespMsg::Ptr> const& execRespMsgs_) const;

    std::string _dump() const;

    WorkerContactInfo::Ptr _wInfo;
    CzarContactInfo::Ptr _czInfo;
    AuthContext _authContext;

    /// If the worker thought the czar was dead, this is the time in milliseconds that the worker
    /// thought the czar came back to life. This is passed to the czar so the czar knows
    /// the worker has killed all related UberJobs. The czar sends this value back to
    /// the worker in the response to avoid race conditions. If the returned value matches
    /// or is greater than what is stored, it is certain that there have not been any
    /// dead/alive cycles since the czar received the message.
    uint64_t _thoughtCzarWasDeadTime = 0;

    /// Map of failed transmits using QueryId + UberJobId for the key
    std::shared_ptr<FailedTransmitsMap> _failedTransmits{new FailedTransmitsMap};

    mutable MUTEX _wciMtx;  ///< protects all members.
};

}  // namespace lsst::qserv::protojson

#endif  // LSST_QSERV_PROTOJSON_WORKERCZARCOMISSUE_H
