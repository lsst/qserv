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
#ifndef LSST_QSERV_PROTOJSON_RESPONSEMSG_H
#define LSST_QSERV_PROTOJSON_RESPONSEMSG_H

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

namespace lsst::qserv::qdisp {
class UberJob;
}

namespace lsst::qserv::wbase {
class UberJobData;
}

// This header declarations
namespace lsst::qserv::protojson {

/// This class handles the message used for most success/fail responses.
class ResponseMsg {
public:
    using Ptr = std::shared_ptr<ResponseMsg>;

    ResponseMsg(bool success_, std::string const& errorType_ = "none",
                std::string const& note_ = std::string());

    ResponseMsg() = delete;
    ResponseMsg(ResponseMsg const&) = delete;
    ResponseMsg& operator=(ResponseMsg const&) = delete;

    bool equal(ResponseMsg const& other) const;

    static Ptr create(bool success_, std::string const& errorType_ = "none",
                      std::string const& note_ = std::string()) {
        return Ptr(new ResponseMsg(success_, errorType_, note_));
    }

    /// This function creates ResponseMessage from respJson, if reasonable.
    static Ptr createFromJson(nlohmann::json const& respJson);

    virtual ~ResponseMsg() = default;

    /// Action for worker to take if its message to the czar returned failed.
    /// In most cases, nothing needs to be done.
    virtual void failedUpdateUberJobData(std::shared_ptr<wbase::UberJobData>) {}

    /// Action for czar to take if its message to the worker returned failed.
    /// In most cases, nothing needs to be done.
    virtual void failedUpdateUberJob(std::shared_ptr<qdisp::UberJob>) {}

    /// Return a json version of this object.
    virtual nlohmann::json toJson() const;

    /// class name for log, fName is expected to be __func__.
    std::string cName(const char* fName) const { return std::string("ResponseMsg::") + fName; }

    /// Returns a string for logging.
    /// Note: Naming dumpOs just dump should work, but it causes gcc to fail when child classes in
    ///       unit tests try to call dump() and the base class version is not found.
    virtual std::ostream& dumpOs(std::ostream& os) const;
    std::string dump() const;
    friend std::ostream& operator<<(std::ostream& os, ResponseMsg const& cmd);

    bool success;
    std::string errorType;
    std::string note;
};

class ExecutiveRespMsg : public ResponseMsg {
public:
    using Ptr = std::shared_ptr<ExecutiveRespMsg>;

    ExecutiveRespMsg(bool success_, bool dataObsolete_, QueryId qId_, UberJobId ujId_, CzarId czId_,
                     std::string const& errorType_ = "none", std::string const& note_ = std::string());

    virtual ~ExecutiveRespMsg() = default;

    static Ptr create(bool success_, bool dataObsolete_, QueryId qId_, UberJobId ujId_, CzarId czId_,
                      std::string const& errorType_ = "none", std::string const& note_ = std::string()) {
        return Ptr(new ExecutiveRespMsg(success_, dataObsolete_, qId_, ujId_, czId_, errorType_, note_));
    }

    /// This function creates ExecutiveRespMsg from respJson, if reasonable.
    static Ptr createFromJson(nlohmann::json const& respJson);

    std::string cName(const char* fName) const { return std::string("ExecutiveRespMsg::") + fName; }

    nlohmann::json toJson() const override;

    std::ostream& dumpOs(std::ostream& os) const override;

    bool dataObsolete;  ///< Indicates that the result data the worker has is obsolete and may be deleted.
    QueryId qId;        ///< The query id for the data, if applicable.
    UberJobId ujId;     ///< The uberjob id for the data, if applicable.
    CzarId czId;        ///< The czar id for the data, if applicable.
};

class WorkerCzarComRespMsg : public ResponseMsg {
public:
    using Ptr = std::shared_ptr<WorkerCzarComRespMsg>;

    WorkerCzarComRespMsg(bool success_, uint64_t thoughtCzarWasDeadTime_,
                         std::string const& errorType_ = "none", std::string const& note_ = std::string())
            : ResponseMsg(success_, errorType_, note_), thoughtCzarWasDeadTime(thoughtCzarWasDeadTime_) {}

    virtual ~WorkerCzarComRespMsg() = default;

    static Ptr create(bool success_, uint64_t thoughtCzarWasDeadTime_, std::string const& errorType_ = "none",
                      std::string const& note_ = std::string()) {
        return Ptr(new WorkerCzarComRespMsg(success_, thoughtCzarWasDeadTime_, errorType_, note_));
    }

    /// This function creates WorkerCzarComRespMsg from inJson, if reasonable.
    static Ptr createFromJson(nlohmann::json const& inJson);

    std::string cName(const char* fName) const { return std::string("WorkerCzarComRespMsg::") + fName; }

    nlohmann::json toJson() const override;

    std::ostream& dumpOs(std::ostream& os) const override;

    /// Indicates the `thoughtCzarWasDeadTime` sent by the worker (normally 0)
    uint64_t thoughtCzarWasDeadTime;

    /// List of ExecutiveRespMsg objects for the UberJobs that were
    /// in the originating WorkerCzarComIssue message.
    std::vector<ExecutiveRespMsg::Ptr> execRespMsgs;
};

}  // namespace lsst::qserv::protojson

#endif  // LSST_QSERV_PROTOJSON_RESPONSEMSG_H
