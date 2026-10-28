// -*- LSST-C++ -*-
/*
 * This file is part of qserv.
 *
 * Developed for the LSST Data Management System.
 * This product includes software developed by the LSST Project
 * (https://www.lsst.org).
 * See the COPYRIGHT file at the top-level directory of this distribution
 * for details of code ownership.
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
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

#ifndef LSST_QSERV_PROTOJSON_QUERYEXPLAINMSG_H
#define LSST_QSERV_PROTOJSON_QUERYEXPLAINMSG_H

// System headers
#include <chrono>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

// Third party headers
#include "nlohmann/json.hpp"

// qserv headers
#include "global/clock_defs.h"
#include "global/DbTable.h"
#include "global/intTypes.h"
#include "protojson/WorkerQueryStatusData.h"
#include "util/MultiError.h"

// This header declarations
namespace lsst::qserv::protojson {

/** This class provides a transport mechanism to pass the information
 * needed to run an explain query on a worker.
 */
class QueryExplainMsg {
public:
    using Ptr = std::shared_ptr<QueryExplainMsg>;
    /// class name for logging, fName is expected to be __func__.
    std::string cName(const char* fName) const;

    QueryExplainMsg() = delete;
    QueryExplainMsg(QueryExplainMsg const&) = delete;
    QueryExplainMsg& operator=(QueryExplainMsg const&) = delete;

    static Ptr create(AuthContext const& authContext_, unsigned int version_, std::string const& workerIdStr_,
                      CzarId czarId_, QueryId queryId_, std::string const& dbName_, int chunkId_,
                      int subchunkId_, std::vector<std::string> const& queries_,
                      DbTableSet const& subchunkTables_);

    /// This function creates a UberJobErrorMsg object from the worker json `czarJson`.
    static Ptr createFromJson(nlohmann::json const& czarJson);
    static util::MultiError multiErrorFromJson(nlohmann::json const& czarJson);

    ~QueryExplainMsg() = default;

    /// Return a json object with data for collection of the UberJob result file.
    nlohmann::json toJson() const;
    std::string dump() const;
    std::ostream& dump(std::ostream& os) const;

    std::vector<std::string> queries;
    std::string workerId;
    CzarId czarId;
    QueryId queryId;
    std::string dbName;
    int chunkId = -1;
    int subchunkId = -1;
    DbTableSet subchunkTables;

private:
    QueryExplainMsg(AuthContext const& authContext_, unsigned int version_, std::string const& workerId_,
                    CzarId czarId_, QueryId queryId_, std::string const& dbName, int chunkId_,
                    int subchunkId_, std::vector<std::string> const& queries_,
                    DbTableSet const& subchunkTables_);

    AuthContext _authContext;
    unsigned int _version;
};

class QueryExplainResultElementMsg {
public:
    using Ptr = std::shared_ptr<QueryExplainResultElementMsg>;

    QueryExplainResultElementMsg() = default;
    QueryExplainResultElementMsg(QueryExplainResultElementMsg const&) = delete;
    QueryExplainResultElementMsg& operator=(QueryExplainResultElementMsg const&) = delete;
    ~QueryExplainResultElementMsg() = default;

    static Ptr create() { return Ptr(new QueryExplainResultElementMsg()); }
    static Ptr createFromJson(nlohmann::json const& jsIn);

    nlohmann::json toJson() const;

    std::string dump() const;
    std::ostream& dump(std::ostream& os) const;

    nlohmann::json multiErrorToJson() const;
    void multiErrorFromJson(nlohmann::json const& jsMErr);

    /// The result of the explain query in json format. Columns may vary.
    nlohmann::json explainResult;
    util::MultiError multiError;
};

class QueryExplainResultMsg {
public:
    using Ptr = std::shared_ptr<QueryExplainResultMsg>;

    QueryExplainResultMsg() = default;
    QueryExplainResultMsg(QueryExplainResultMsg const&) = delete;
    QueryExplainResultMsg& operator=(QueryExplainResultMsg const&) = delete;
    ~QueryExplainResultMsg() = default;

    static Ptr create() { return Ptr(new QueryExplainResultMsg()); }

    static Ptr createFromJson(nlohmann::json const& jsIn);

    nlohmann::json toJson() const;

    /// Return a string with the result of the explain query in a format suitable for display to a user.
    std::string toUserString() const;

    /// Return raw output of the data in the class.
    std::ostream& dumpOs(std::ostream& os) const;
    std::string dump() const;

    bool success = false;
    std::string errorType;
    std::string note;
    QueryId queryId = -1;
    std::string dbName;
    int chunkId = -1;
    int subchunkId = -1;
    std::vector<QueryExplainResultElementMsg::Ptr> resultElements;
};

}  // namespace lsst::qserv::protojson

#endif  // LSST_QSERV_PROTOJSON_QUERYEXPLAINMSG_H
