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
#include "protojson/UberJobReadyMsg.h"

// This header declarations
namespace lsst::qserv::protojson {

/// This class handles the message used to inform the czar that there has
/// been a problem with an UberJob.
class UberJobErrorMsg : public UberJobStatusMsg {
public:
    using Ptr = std::shared_ptr<UberJobErrorMsg>;
    /// class name for log, fName is expected to be __func__.
    std::string cName(const char* fName) const override;

    UberJobErrorMsg() = delete;
    UberJobErrorMsg(UberJobErrorMsg const&) = delete;
    UberJobErrorMsg& operator=(UberJobErrorMsg const&) = delete;

    static Ptr create(AuthContext const& authContext_, unsigned int version_, std::string const& workerIdStr_,
                      std::string const& czarName_, CzarId czarId_, QueryId queryId_, UberJobId uberJobId_,
                      int errorCode_, std::string const& errorMsg_);

    /// This function creates a UberJobErrorMsg object from the worker json `czarJson`.
    static Ptr createFromJson(nlohmann::json const& czarJson);

    ~UberJobErrorMsg() = default;

    bool equals(UberJobStatusMsg const& other) const override;

    /// Return a json object with data for collection of the UberJob result file.
    nlohmann::json toJson() const override;
    std::ostream& dump(std::ostream& os) const override;

    int const errorCode;
    std::string const errorMsg;

private:
    UberJobErrorMsg(AuthContext const& authContext_, unsigned int version, std::string const& workerId,
                    std::string const& czarName, CzarId czarId, QueryId queryId, UberJobId uberJobId,
                    int errorCode, std::string const& errorMsg);
};

}  // namespace lsst::qserv::protojson

#endif  // LSST_QSERV_PROTOJSON_UBERJOBERRORMSG_H
