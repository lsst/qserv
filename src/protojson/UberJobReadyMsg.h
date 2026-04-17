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

/// Base class for returning UberJob results on the worker back to the czar.
/// If the worker successful collected results for the UberJob, it sends back
/// an UberJobReadyMsg with information needed to collect the result file.
/// If it failed, it send back an UberJobErrorMsg with information about the
/// error.
class UberJobStatusMsg {
public:
    using Ptr = std::shared_ptr<UberJobStatusMsg>;
    virtual std::string cName(const char* fName) const { return std::string("UberJobStatusMsg") + fName; }
    UberJobStatusMsg() = delete;
    UberJobStatusMsg(UberJobStatusMsg const&) = delete;
    UberJobStatusMsg& operator=(UberJobStatusMsg const&) = delete;
    virtual ~UberJobStatusMsg() = default;

    virtual nlohmann::json toJson() const = 0;

    virtual bool equals(UberJobStatusMsg const& other) const = 0;

    /// Returns a string for logging.
    virtual std::ostream& dump(std::ostream& os) const;
    std::string dump() const;
    friend std::ostream& operator<<(std::ostream& os, UberJobStatusMsg const& ujMsg);

    AuthContext const authContext;
    unsigned int const version;
    std::string const workerId;
    std::string const czarName;
    CzarId const czarId;
    QueryId const queryId;
    UberJobId const uberJobId;

protected:
    UberJobStatusMsg(AuthContext const& authContext_, unsigned int version_, std::string const& workerId_,
                     std::string const& czarName_, CzarId czarId_, QueryId queryId_, UberJobId uberJobId_);
    bool equalsBase(UberJobStatusMsg const& other) const;
};

/// This class stores some information about the result file to be collected by the czar.
class FileUrlInfo {
public:
    FileUrlInfo() = default;
    FileUrlInfo(std::string const& fileUrl_, uint64_t rowCount_, uint64_t fileSize_)
            : fileUrl(fileUrl_), rowCount(rowCount_), fileSize(fileSize_) {}
    ~FileUrlInfo() = default;

    bool operator==(FileUrlInfo const& other) const {
        return (fileUrl == other.fileUrl && rowCount == other.rowCount && fileSize == other.fileSize);
    }

    std::string dump() const;

    std::string fileUrl;
    uint64_t rowCount = 0;
    uint64_t fileSize = 0;
};

/// This class handles the message used to inform the czar that a result file
/// for an UberJob is ready.
class UberJobReadyMsg : public UberJobStatusMsg {
public:
    using Ptr = std::shared_ptr<UberJobReadyMsg>;

    /// class name for log, fName is expected to be __func__.
    std::string cName(const char* fName) const override;

    UberJobReadyMsg() = delete;
    UberJobReadyMsg(UberJobReadyMsg const&) = delete;
    UberJobReadyMsg& operator=(UberJobReadyMsg const&) = delete;

    static Ptr create(AuthContext const& authContext_, unsigned int version_, std::string const& workerIdStr_,
                      std::string const& czarName_, CzarId czarId_, QueryId queryId_, UberJobId uberJobId_,
                      FileUrlInfo const& fileUrlInfo_);

    /// This function creates a UberJobReadyMsg object from the worker json `czarJson`, the
    /// other parameters are used to verify the json message.
    static Ptr createFromJson(nlohmann::json const& czarJson);

    ~UberJobReadyMsg() override = default;

    bool equals(UberJobStatusMsg const& other) const override;

    /// Return a json object with data allowing collection of UberJob result file.
    nlohmann::json toJson() const override;

    std::ostream& dump(std::ostream& os) const override;

    FileUrlInfo const fileUrlInfo;

private:
    UberJobReadyMsg(AuthContext const& authContext_, unsigned int version_, std::string const& workerId_,
                    std::string const& czarName_, CzarId czarId_, QueryId queryId_, UberJobId uberJobId_,
                    FileUrlInfo const& fileUrlInfo_);
};

}  // namespace lsst::qserv::protojson

#endif  // LSST_QSERV_PROTOJSON_UBERJOBREADYMSG_H
