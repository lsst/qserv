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
#ifndef LSST_QSERV_PROTOJSON_UBERJOBREADYMSG_H
#define LSST_QSERV_PROTOJSON_UBERJOBREADYMSG_H

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

class UberJobStatusMsg {
public:
    using Ptr = std::shared_ptr<UberJobStatusMsg>;
    virtual std::string cName(const char* fName) const { return std::string("UberJobStatusMsg") + fName; }
    UberJobStatusMsg() = delete;
    UberJobStatusMsg(UberJobStatusMsg const&) = delete;
    UberJobStatusMsg& operator=(UberJobStatusMsg const&) = delete;
    virtual ~UberJobStatusMsg() = default;

    virtual std::shared_ptr<nlohmann::json> toJsonPtr() const = 0;

    virtual bool equals(UberJobStatusMsg const& other) const = 0;

    std::string const& getWorkerId() const { return _workerId; }
    std::string const& getCzarName() const { return _czarName; }
    CzarId getCzarId() const { return _czarId; }
    QueryId getQueryId() const { return _queryId; }
    UberJobId getUberJobId() const { return _uberJobId; }

    /// Returns a string for logging.
    virtual std::ostream& dumpOS(std::ostream& os) const;
    std::string dump() const;
    friend std::ostream& operator<<(std::ostream& os, UberJobStatusMsg const& ujMsg);

protected:
    UberJobStatusMsg(std::string const& replicationInstanceId, std::string const& replicationAuthKey,
                     unsigned int version, std::string const& workerId, std::string const& czarName,
                     CzarId czarId, QueryId queryId, UberJobId uberJobId);
    bool equalsBase(UberJobStatusMsg const& other) const;

    std::string const _replicationInstanceId;
    std::string const _replicationAuthKey;
    unsigned int const _version;
    std::string const _workerId;
    std::string const _czarName;
    CzarId const _czarId;
    QueryId const _queryId;
    UberJobId const _uberJobId;
};

/// This class handles the message used to inform the czar that a result file
/// for an UberJob is ready.
class UberJobReadyMsg : public UberJobStatusMsg {
public:
    using Ptr = std::shared_ptr<UberJobReadyMsg>;

    /// class name for log, fName is expected to be __func__.
    std::string cName(const char* fName) const override;

    UberJobReadyMsg(std::string const& replicationInstanceId, std::string const& replicationAuthKey,
                    unsigned int version, std::string const& workerId, std::string const& czarName,
                    CzarId czarId, QueryId queryId, UberJobId uberJobId, std::string const& fileUrl,
                    uint64_t rowCount, uint64_t fileSize);

    UberJobReadyMsg() = delete;
    UberJobReadyMsg(UberJobReadyMsg const&) = delete;
    UberJobReadyMsg& operator=(UberJobReadyMsg const&) = delete;

    static Ptr create(std::string const& replicationInstanceId, std::string const& replicationAuthKey,
                      unsigned int version, std::string const& workerIdStr, std::string const& czarName,
                      CzarId czarId, QueryId queryId, UberJobId uberJobId, std::string const& fileUrl,
                      uint64_t rowCount, uint64_t fileSize);

    /// This function creates a UberJobReadyMsg object from the worker json `czarJson`, the
    /// other parameters are used to verify the json message.
    static Ptr createFromJson(nlohmann::json const& czarJson);

    ~UberJobReadyMsg() override = default;

    bool equals(UberJobStatusMsg const& other) const override;

    /// Return a json object with data allowing collection of UberJob result file.
    std::shared_ptr<nlohmann::json> toJsonPtr() const override;

    std::ostream& dumpOS(std::ostream& os) const override;

    std::string const& getFileUrl() const { return _fileUrl; }
    uint64_t getRowCount() const { return _rowCount; }
    uint64_t getFileSize() const { return _fileSize; }

private:
    std::string const _fileUrl;
    uint64_t const _rowCount;
    uint64_t const _fileSize;
};

}  // namespace lsst::qserv::protojson

#endif  // LSST_QSERV_PROTOJSON_UBERJOBREADYMSG_H
