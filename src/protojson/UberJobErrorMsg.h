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
#ifndef LSST_QSERV_PROTOJSON_UBERJOBERRORMSG_H
#define LSST_QSERV_PROTOJSON_UBERJOBERRORMSG_H

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
#include "protojson/WorkerQueryStatusData.h"

// This header declarations
namespace lsst::qserv::protojson {

/// This class handles the message used to inform the czar that there has
/// been a problem with an UberJob.
class UberJobErrorMsg {
public:
    using Ptr = std::shared_ptr<UberJobErrorMsg>;

    UberJobErrorMsg(std::string const& replicationInstanceId, std::string const& replicationAuthKey,
                    unsigned int version, std::string const& workerId, std::string const& czarName,
                    CzarIdType czarId, QueryId queryId, UberJobId uberJobId, int errorCode,
                    std::string const& errorMsg);

    UberJobErrorMsg() = delete;
    UberJobErrorMsg(UberJobErrorMsg const&) = delete;
    UberJobErrorMsg& operator=(UberJobErrorMsg const&) = delete;

    static Ptr create(std::string const& replicationInstanceId, std::string const& replicationAuthKey,
                      unsigned int version, std::string const& workerIdStr, std::string const& czarName,
                      CzarIdType czarId, QueryId queryId, UberJobId uberJobId, int errorCode,
                      std::string const& errorMsg);

    /// This function creates a UberJobErrorMsg object from the worker json `czarJson`, the
    /// other parameters are used to verify the json message.
    static Ptr createFromJson(nlohmann::json const& czarJson, std::string const& replicationInstanceId,
                              std::string const& replicationAuthKey);

    ~UberJobErrorMsg() = default;

    /// Return a json object with data allowing collection of UberJob result file.
    nlohmann::json toJson() const;

    std::string const& getWorkerId() const { return _workerId; }
    std::string const& getCzarName() const { return _czarName; }
    CzarIdType getCzarId() const { return _czarId; }
    QueryId getQueryId() const { return _queryId; }
    UberJobId getUberJobId() const { return _uberJobId; }
    std::string const& getErrorMsg() const { return _errorMsg; }
    uint getErrorCode() const { return _errorCode; }

private:
    /// class name for log, fName is expected to be __func__.
    std::string _cName(const char* fName) const;

    std::string const _replicationInstanceId;
    std::string const _replicationAuthKey;
    unsigned int const _version;
    std::string const _workerId;
    std::string const _czarName;
    CzarIdType const _czarId;
    QueryId const _queryId;
    UberJobId const _uberJobId;
    int const _errorCode;
    std::string const _errorMsg;
};

}  // namespace lsst::qserv::protojson

#endif  // LSST_QSERV_PROTOJSON_UBERJOBERRORMSG_H
