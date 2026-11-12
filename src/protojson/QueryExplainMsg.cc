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

// Class header
#include "protojson/QueryExplainMsg.h"

#include <sstream>
#include <stdexcept>

// Qserv headers
#include "http/MetaModule.h"
#include "http/RequestBodyJSON.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using namespace nlohmann;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.protojson.QueryExplainMsg");
}  // namespace

namespace lsst::qserv::protojson {

string QueryExplainMsg::cName(const char* fName) const {
    return string("QueryExplainMsg::") + fName + " queryId=" + to_string(queryId);
}

QueryExplainMsg::Ptr QueryExplainMsg::create(AuthContext const& authContext_, unsigned int version_,
                                             string const& workerIdStr_, CzarId czarId_, QueryId queryId_,
                                             string const& dbName_, int chunkId_, int subchunkId_,
                                             vector<string> const& queries_,
                                             DbTableSet const& subchunkTables_) {
    return Ptr(new QueryExplainMsg(authContext_, version_, workerIdStr_, czarId_, queryId_, dbName_, chunkId_,
                                   subchunkId_, queries_, subchunkTables_));
}

QueryExplainMsg::Ptr QueryExplainMsg::createFromJson(json const& czarJson) {
    string const fName("QueryExplainMsg::createFromJson");
    LOGS(_log, LOG_LVL_DEBUG, fName);
    try {
        AuthContext const authContext(http::RequestBodyJSON::required<string>(czarJson, "instance_id"),
                                      http::RequestBodyJSON::required<string>(czarJson, "auth_key"));
        auto const version = http::RequestBodyJSON::required<unsigned int>(czarJson, "version");
        auto const workerId = http::RequestBodyJSON::required<string>(czarJson, "worker");
        auto const czarId = http::RequestBodyJSON::required<CzarId>(czarJson, "czarid");
        auto const queryId = http::RequestBodyJSON::required<QueryId>(czarJson, "queryid");
        auto const dbName = http::RequestBodyJSON::required<string>(czarJson, "dbname");
        auto const chunkId = http::RequestBodyJSON::required<int>(czarJson, "chunkid");
        auto const subchunkId = http::RequestBodyJSON::required<int>(czarJson, "subchunkid");
        auto const queries = http::RequestBodyJSON::required<vector<string>>(czarJson, "queries");
        auto const jsSubchunkTables = http::RequestBodyJSON::required<json>(czarJson, "subchunktables");
        DbTableSet subchunkTables;
        for (auto const& jsTable : jsSubchunkTables) {
            subchunkTables.emplace(http::RequestBodyJSON::required<string>(jsTable, "db"),
                                   http::RequestBodyJSON::required<string>(jsTable, "table"));
        }
        if (version != http::MetaModule::version) {
            throw invalid_argument(fName + " bad version " + to_string(version));
        }
        return create(authContext, version, workerId, czarId, queryId, dbName, chunkId, subchunkId, queries,
                      subchunkTables);
    } catch (exception const& exc) {
        LOGS(_log, LOG_LVL_ERROR, fName << " invalid message: " << exc.what());
    }
    return nullptr;
}

QueryExplainMsg::QueryExplainMsg(AuthContext const& authContext_, unsigned int version_,
                                 string const& workerId_, CzarId czarId_, QueryId queryId_,
                                 string const& dbName_, int chunkId_, int subchunkId_,
                                 vector<string> const& queries_, DbTableSet const& subchunkTables_)
        : queries(queries_),
          workerId(workerId_),
          czarId(czarId_),
          queryId(queryId_),
          dbName(dbName_),
          chunkId(chunkId_),
          subchunkId(subchunkId_),
          subchunkTables(subchunkTables_),
          _authContext(authContext_),
          _version(version_) {
    if (_version != http::MetaModule::version) {
        throw invalid_argument(cName(__func__) + " bad version " + to_string(_version));
    }
}

json QueryExplainMsg::toJson() const {
    json jsonMessage;
    jsonMessage["instance_id"] = _authContext.replicationInstanceId;
    jsonMessage["auth_key"] = _authContext.replicationAuthKey;
    jsonMessage["version"] = _version;
    jsonMessage["worker"] = workerId;
    jsonMessage["czarid"] = czarId;
    jsonMessage["queryid"] = queryId;
    jsonMessage["dbname"] = dbName;
    jsonMessage["chunkid"] = chunkId;
    jsonMessage["subchunkid"] = subchunkId;
    jsonMessage["queries"] = queries;
    jsonMessage["subchunktables"] = json::array();
    for (auto const& table : subchunkTables) {
        jsonMessage["subchunktables"].push_back({{"db", table.db}, {"table", table.table}});
    }

    return jsonMessage;
}

string QueryExplainMsg::dump() const {
    ostringstream os;
    dump(os);
    return os.str();
}

ostream& QueryExplainMsg::dump(ostream& os) const {
    os << "{QueryExplainMsg: queryId=" << queryId << " workerId=" << workerId << " czarId=" << czarId
       << " queryId=" << queryId << " dbName=" << dbName << " chunkId=" << chunkId
       << " subchunkId=" << subchunkId << " queries=" << queries.size() << "{";
    for (auto const& query : queries) {
        os << query << ",";
    }
    os << "} subchunkTables={";
    for (auto const& table : subchunkTables) {
        os << "{" << table.db << "," << table.table << "},";
    };
    os << "}";

    return os;
}

nlohmann::json QueryExplainResultElementMsg::toJson() const {
    nlohmann::json jsOut;
    jsOut["explainresult"] = explainResult;
    jsOut["multiError"] = multiErrorToJson();
    return jsOut;
}

QueryExplainResultElementMsg::Ptr QueryExplainResultElementMsg::createFromJson(nlohmann::json const& jsIn) {
    string const fName("QueryExplainResultElementMsg::createFromJson");
    try {
        auto msg = create();
        msg->explainResult = http::RequestBodyJSON::required<json>(jsIn, "explainresult");
        msg->multiErrorFromJson(http::RequestBodyJSON::required<json>(jsIn, "multiError"));
        return msg;
    } catch (exception const& exc) {
        LOGS(_log, LOG_LVL_ERROR, fName << " invalid message: " << exc.what());
    }
    return nullptr;
}

string QueryExplainResultElementMsg::dump() const {
    ostringstream os;
    dump(os);
    return os.str();
}

ostream& QueryExplainResultElementMsg::dump(ostream& os) const {
    os << "{QueryExplainResultElementMsg: explainResult=" << explainResult.dump()
       << " multiError=" << multiError << "}";
    return os;
}

nlohmann::json QueryExplainResultElementMsg::multiErrorToJson() const {
    nlohmann::json jsMultiError = nlohmann::json::array();
    for (auto const& err : multiError.getVector()) {
        jsMultiError.push_back({{"eCode", err.getCode()},
                                {"subCode", err.getSubCode()},
                                {"count", err.getCount()},
                                {"eMsg", err.getMsg()}});
    }
    return jsMultiError;
}

void QueryExplainResultElementMsg::multiErrorFromJson(nlohmann::json const& jsMErr) {
    if (!jsMErr.is_array()) {
        throw std::invalid_argument("UberJobErrorMsg::multiErrorFromJson MultiError is not a json::array");
    }
    // Fill in multiErr_ with the values in jsMErr
    for (auto const& jsElem : jsMErr) {
        try {
            auto const errCode = http::RequestBodyJSON::required<int>(jsElem, "eCode");
            auto const subCode = http::RequestBodyJSON::required<int64_t>(jsElem, "subCode");
            auto const count = http::RequestBodyJSON::required<int>(jsElem, "count");
            auto const eMsg = http::RequestBodyJSON::required<string>(jsElem, "eMsg");
            util::Error err(errCode, subCode, eMsg);
            err.incrCount(count - 1);
            multiError.insert(err);
        } catch (invalid_argument const& ex) {
            // skip to next element
            LOGS(_log, LOG_LVL_WARN, "UberJobErrorMsg::multiErrorFromJson failed to read Error:" << jsElem);
        }
    }
}

nlohmann::json QueryExplainResultMsg::toJson() const {
    nlohmann::json jsOut;
    jsOut["explainSuccess"] = explainSuccess ? 1 : 0;
    jsOut["errortype"] = errorType;
    jsOut["note"] = note;
    jsOut["queryid"] = queryId;
    jsOut["dbname"] = dbName;
    jsOut["chunkid"] = chunkId;
    jsOut["subchunkid"] = subchunkId;
    jsOut["resultElements"] = nlohmann::json::array();
    for (auto const& resultElement : resultElements) {
        jsOut["resultElements"].push_back(resultElement->toJson());
    }
    return jsOut;
}

QueryExplainResultMsg::Ptr QueryExplainResultMsg::createFromJson(nlohmann::json const& jsIn) {
    string const fName("QueryExplainResultMsg::createFromJson");
    try {
        auto msg = create();
        msg->explainSuccess = http::RequestBodyJSON::required<int>(jsIn, "explainSuccess") != 0;
        msg->errorType = http::RequestBodyJSON::required<string>(jsIn, "errortype");
        msg->note = http::RequestBodyJSON::required<string>(jsIn, "note");
        msg->queryId = http::RequestBodyJSON::required<QueryId>(jsIn, "queryid");
        if (jsIn.contains("dbname")) {
            msg->dbName = http::RequestBodyJSON::required<string>(jsIn, "dbname");
        }
        msg->chunkId = http::RequestBodyJSON::required<int>(jsIn, "chunkid");
        msg->subchunkId = http::RequestBodyJSON::required<int>(jsIn, "subchunkid");
        auto const jsResultElements = http::RequestBodyJSON::required<json>(jsIn, "resultElements");
        if (!jsResultElements.is_array()) {
            throw invalid_argument(fName + " resultElements is not a json::array");
        }
        for (auto const& jsResultElement : jsResultElements) {
            auto resultElement = QueryExplainResultElementMsg::createFromJson(jsResultElement);
            if (resultElement == nullptr) {
                throw invalid_argument(fName + " invalid result element");
            }
            msg->resultElements.emplace_back(resultElement);
        }
        return msg;
    } catch (exception const& exc) {
        LOGS(_log, LOG_LVL_ERROR, fName << " invalid message: " << exc.what());
    }
    return nullptr;
}

ostream& QueryExplainResultMsg::dumpOs(ostream& os) const {
    os << "{QueryExplainResultMsg: explainSuccess=" << explainSuccess << " errorType=" << errorType
       << " note=" << note << " queryId=" << queryId << " dbName=" << dbName << " chunkId=" << chunkId
       << " subchunkId=" << subchunkId << " resultElements={";
    for (auto const& resultElement : resultElements) {
        os << resultElement->dump() << ",";
    }
    os << "}}";
    return os;
}

string unescapeQuotesAndNewlines(string const& input) {
    string result;
    result.reserve(input.size());

    for (size_t i = 0; i < input.size(); ++i) {
        if (input[i] == '\\' && i + 1 < input.size()) {
            char next = input[i + 1];
            if (next == '"') {
                result += '"';
                ++i;  // skip the next char
                continue;
            } else if (next == 'n') {
                result += '\n';
                ++i;  // skip the next char
                continue;
            }
        }
        result += input[i];
    }
    return result;
}

string QueryExplainResultMsg::toUserString() const {
    ostringstream os;
    os << "{WorkerResult: explainSuccess=" << explainSuccess << " errorType=" << errorType << " note=" << note
       << " queryId=" << queryId << " dbName=" << dbName << " chunkId=" << chunkId
       << " subchunkId=" << subchunkId << " resultElements={";
    for (auto const& resultElement : resultElements) {
        os << resultElement->dump() << ",";
    }
    os << "}}";
    return unescapeQuotesAndNewlines(os.str());
}

string QueryExplainResultMsg::dump() const {
    ostringstream os;
    dumpOs(os);
    return os.str();
}

}  // namespace lsst::qserv::protojson
