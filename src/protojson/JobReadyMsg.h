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
#ifndef LSST_QSERV_PROTOJSON_JOBREADYMSG_H
#define LSST_QSERV_PROTOJSON_JOBREADYMSG_H

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

/// This class handles the message used to inform the czar that a result file
/// for an UberJob is ready.
class JobReadyMsg {
public:
    using Ptr = std::shared_ptr<JobReadyMsg>;

    JobReadyMsg() = delete;
    JobReadyMsg(std::string const& replicationInstanceId, std::string const& replicationAuthKey)
            : _replicationInstanceId(replicationInstanceId), _replicationAuthKey(replicationAuthKey) {}
    JobReadyMsg(JobReadyMsg const&) = delete;
    JobReadyMsg& operator=(JobReadyMsg const&) = delete;

    std::string cName(const char* fName) { return std::string("WorkerQueryStatusData::") + fName; }

    static Ptr create(std::string const& replicationInstanceId, std::string const& replicationAuthKey,
                      std::string const& workerIdStr, std::string const& czarName, CzarIdType czarId,
                      QueryId queryId, UberJobId uberJobId, std::string const& fileUrl, uint64_t rowCount,
                      uint64_t fileSize);

    static Ptr create(std::string const& replicationInstanceId, std::string const& replicationAuthKey) {
        return Ptr(new JobReadyMsg(replicationInstanceId, replicationAuthKey));
    }

    /// This function creates a JobReadyMsg object from the worker json `czarJson`, the
    /// other parameters are used to verify the json message.
    static Ptr createFromJson(nlohmann::json const& czarJson, std::string const& replicationInstanceId,
                              std::string const& replicationAuthKey);

    ~JobReadyMsg() = default;

    /// Return a json object with data allowing collection of UberJob result file.
    nlohmann::json serializeJson();

    std::string getWorkerId() const { return _workerId; }
    std::string getCzarName() const { return _czarName; }
    CzarIdType getCzarId() const { return _czarId; }
    QueryId getQueryId() const { return _queryId; }
    UberJobId getUberJobId() const { return _uberJobId; }
    std::string getFileUrl() const { return _fileUrl; }
    uint64_t getRowCount() const { return _rowCount; }
    uint64_t getFileSize() const { return _fileSize; }

private:
    std::string const _replicationInstanceId;
    std::string const _replicationAuthKey;
    std::string _workerId;
    std::string _czarName;
    CzarIdType _czarId = 0;
    QueryId _queryId = 0;
    UberJobId _uberJobId = 0;
    std::string _fileUrl;
    uint64_t _rowCount = 0;
    uint64_t _fileSize = 0;
};

}  // namespace lsst::qserv::protojson

#endif  // LSST_QSERV_PROTOJSON_JOBREADYMSG_H
