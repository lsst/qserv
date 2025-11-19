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
#include "protojson/WorkerQueryStatusData.h"

// This header declarations
namespace lsst::qserv::protojson {

class UberJobStatusMsg;

typedef std::shared_ptr<UberJobStatusMsg> FailedTransmitType;
typedef std::map<std::pair<QueryId, UberJobId>, FailedTransmitType> FailedTransmitsMap;

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

    static Ptr create(std::string const& replicationInstanceId_, std::string const& replicationAuthKey_) {
        return Ptr(new WorkerCzarComIssue(replicationInstanceId_, replicationAuthKey_));
    }

    static Ptr createFromJson(nlohmann::json const& workerJson, std::string const& replicationInstanceId_,
                              std::string const& replicationAuthKey_);

    void setThoughtCzarWasDead(bool wasDead) {
        std::lock_guard lg(_wciMtx);
        _thoughtCzarWasDead = wasDead;
    }

    size_t clearMapEntries(nlohmann::json const& response);

    bool getThoughtCzarWasDead() const { return _thoughtCzarWasDead; }

    /// Return true if there is a reason this WorkerCzarComIssue should be sent to this czar.
    bool needToSend() const {
        std::lock_guard lg(_wciMtx);
        return (_thoughtCzarWasDead || _failedTransmits->size() > 0);
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
    std::shared_ptr<nlohmann::json> toJson();

    /// Return a json object indicating the status of the message for the
    /// original requester.
    nlohmann::json responseToJson() const;

    /// Take the failedTransmitsMap and make an empty one to take its place.
    std::shared_ptr<FailedTransmitsMap> takeFailedTransmitsMap();

    std::string dump() const;

private:
    WorkerCzarComIssue(std::string const& replicationInstanceId_, std::string const& replicationAuthKey_)
            : _replicationInstanceId(replicationInstanceId_), _replicationAuthKey(replicationAuthKey_) {}

    /// The `request` may indicate success or failure
    void _addFailedTransmit(QueryId qId, UberJobId ujId,
                            std::shared_ptr<protojson::UberJobStatusMsg> const& ujMsg);

    /// Return a json object indicating the status of the message for the
    /// original requester.
    nlohmann::json _responseToJson() const;

    std::string _dump() const;

    WorkerContactInfo::Ptr _wInfo;
    CzarContactInfo::Ptr _czInfo;
    std::string const _replicationInstanceId;  ///< Used for message verification.
    std::string const _replicationAuthKey;     ///< Used for message verification.

    /// Set to by the worker true if the czar was considered dead, and reset to false
    /// after the czar has acknowledged successful reception of this message.
    bool _thoughtCzarWasDead = false;

    /// Map of failed transmits using QueryId + UberJobId for the key
    std::shared_ptr<FailedTransmitsMap> _failedTransmits{new FailedTransmitsMap};

    mutable MUTEX _wciMtx;  ///< protects all members.
};

}  // namespace lsst::qserv::protojson

#endif  // LSST_QSERV_PROTOJSON_WORKERCZARCOMISSUE_H
