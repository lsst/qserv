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

// Class header
#include "czar/HttpCzarQueryModule.h"

// System headers
#include <map>
#include <stdexcept>
#include <vector>

// Qserv headers
#include "cconfig/CzarConfig.h"
#include "czar/Czar.h"
#include "czar/SubmitResult.h"
#include "global/intTypes.h"
#include "http/Exceptions.h"
#include "http/RequestQuery.h"
#include "qdisp/CzarStats.h"
#include "sql/SqlConnection.h"
#include "sql/SqlConnectionFactory.h"
#include "sql/SqlResults.h"
#include "sql/Schema.h"
#include "util/String.h"

using namespace std;
using json = nlohmann::json;

namespace lsst::qserv::czar {

void HttpCzarQueryModule::process(string const& context, shared_ptr<qhttp::Request> const& req,
                                  shared_ptr<qhttp::Response> const& resp, string const& subModuleName,
                                  http::AuthType const authType) {
    HttpCzarQueryModule module(context, req, resp);
    module.execute(subModuleName, authType);
}

HttpCzarQueryModule::HttpCzarQueryModule(string const& context, shared_ptr<qhttp::Request> const& req,
                                         shared_ptr<qhttp::Response> const& resp)
        : HttpModule(context, req, resp) {}

json HttpCzarQueryModule::executeImpl(string const& subModuleName) {
    string const func = string(__func__) + "[sub-module='" + subModuleName + "']";
    debug(func);
    if (subModuleName == "SUBMIT")
        return _submit();
    else if (subModuleName == "SUBMIT-ASYNC")
        return _submitAsync();
    else if (subModuleName == "CANCEL")
        return _cancel();
    else if (subModuleName == "STATUS")
        return _status();
    else if (subModuleName == "RESULT")
        return _result();
    throw invalid_argument(context() + func + " unsupported sub-module");
}

json HttpCzarQueryModule::_submit() {
    debug(__func__);
    checkApiVersion(__func__, 32);
    SubmitResult const submitResult = _getRequestParamsAndSubmit(__func__, false);
    return _waitAndExtractResult(submitResult);
}

json HttpCzarQueryModule::_submitAsync() {
    debug(__func__);
    checkApiVersion(__func__, 32);
    SubmitResult const submitResult = _getRequestParamsAndSubmit(__func__, true);
    return json::object({{"queryId", submitResult.queryId}});
}

SubmitResult HttpCzarQueryModule::_getRequestParamsAndSubmit(string const& func, bool async) {
    string const userQuery = body().required<string>("query");
    string const defaultDatabase = body().optional<string>("database", string());
    debug(__func__, "query=" + userQuery);
    debug(__func__, "database=" + defaultDatabase);
    string const query = async ? "SUBMIT " + userQuery : userQuery;
    map<string, string> const hints{{"db", defaultDatabase}};
    SubmitResult const submitResult = Czar::getCzar()->submitQuery(query, hints);
    if (!submitResult.errorMessage.empty()) {
        _dropTable(submitResult.messageTable);
        throw http::Error(context() + __func__, submitResult.errorMessage);
    }
    return submitResult;
}

json HttpCzarQueryModule::_cancel() {
    debug(__func__);
    checkApiVersion(__func__, 30);
    QueryId const queryId = _getQueryId();
    string const clientId;
    Czar::getCzar()->killQuery("CANCEL " + to_string(queryId), clientId);
    return json::object();
}

json HttpCzarQueryModule::_status() {
    debug(__func__);
    checkApiVersion(__func__, 30);
    SubmitResult const submitResult = _getQueryInfo();
    json statusJson = json::object();
    statusJson["queryId"] = submitResult.queryId;
    statusJson["status"] = submitResult.status;
    statusJson["totalChunks"] = submitResult.totalChunks;
    statusJson["completedChunks"] = submitResult.completedChunks;
    statusJson["queryBeginEpoch"] = submitResult.queryBeginEpoch;
    statusJson["lastUpdateEpoch"] = submitResult.lastUpdateEpoch;
    return json::object({{"status", statusJson}});
}

json HttpCzarQueryModule::_result() {
    debug(__func__);
    checkApiVersion(__func__, 30);
    return _waitAndExtractResult(_getQueryInfo());
}

QueryId HttpCzarQueryModule::_getQueryId() const {
    // The input is going to sanitized by turning the string into a number of
    // the corresponding type to ensure it's formally valid.
    string const queryIdStr = params().at("qid");
    debug(__func__, "qid=" + queryIdStr);
    return stoull(queryIdStr);
}

SubmitResult HttpCzarQueryModule::_getQueryInfo() const {
    QueryId const queryId = _getQueryId();
    SubmitResult submitResult;
    try {
        submitResult = Czar::getCzar()->getQueryInfo(queryId);
    } catch (exception const& ex) {
        string const msg =
                "failed to obtain info for queryId=" + to_string(queryId) + ", ex: " + string(ex.what());
        error(__func__, msg);
        throw http::Error(context() + __func__, msg);
    }
    if (!submitResult.errorMessage.empty()) {
        throw http::Error(context() + __func__, submitResult.errorMessage);
    }
    return submitResult;
}

json HttpCzarQueryModule::_waitAndExtractResult(SubmitResult const& submitResult) const {
    // Block the current thread before the query will finish or fail.
    string const messageSelectQuery =
            "SELECT chunkId, code, message, severity+0, timeStamp FROM " + submitResult.messageTable;
    auto const conn =
            sql::SqlConnectionFactory::make(cconfig::CzarConfig::instance()->getMySqlResultConfig());
    sql::SqlResults messageQueryResults;
    sql::SqlErrorObject messageQueryErr;
    if (!conn->runQuery(messageSelectQuery, messageQueryResults, messageQueryErr)) {
        _dropTable(submitResult.messageTable);
        _dropTable(submitResult.resultTable);
        string const msg = "failed query=" + messageSelectQuery + " err=" + messageQueryErr.printErrMsg();
        error(__func__, msg);
        throw http::Error(context() + __func__, msg);
    }

    // Read thе message table to see if the user query suceeded or failed
    vector<string> chunkId;
    vector<string> code;
    vector<string> message;
    vector<string> severity;
    sql::SqlErrorObject messageProcessErr;
    if (!messageQueryResults.extractFirst4Columns(chunkId, code, message, severity, messageProcessErr)) {
        messageQueryResults.freeResults();
        _dropTable(submitResult.messageTable);
        _dropTable(submitResult.resultTable);
        string const msg = "failed to extract results of query=" + messageSelectQuery +
                           " err=" + messageProcessErr.printErrMsg();
        error(__func__, msg);
        throw http::Error(context() + __func__, msg);
    }
    string errorMsg;
    for (size_t i = 0; i < chunkId.size(); ++i) {
        if (stoi(code[i]) > 0) {
            errorMsg += "[chunkId=" + chunkId[i] + " code=" + code[i] + " message=" + message[i] +
                        " severity=" + severity[i] + "], ";
        }
    }
    if (!errorMsg.empty()) {
        messageQueryResults.freeResults();
        _dropTable(submitResult.messageTable);
        _dropTable(submitResult.resultTable);
        error(__func__, errorMsg);
        throw http::Error(context() + __func__, errorMsg);
    }
    messageQueryResults.freeResults();
    _dropTable(submitResult.messageTable);

    // Read a result set from the result table, package it into the JSON object
    // and sent it back to a user.
    sql::SqlResults resultQueryResults;
    sql::SqlErrorObject resultQueryErr;
    if (!conn->runQuery(submitResult.resultQuery, resultQueryResults, resultQueryErr)) {
        _dropTable(submitResult.resultTable);
        string const msg =
                "failed query=" + submitResult.resultQuery + " err=" + resultQueryErr.printErrMsg();
        error(__func__, msg);
        throw http::Error(context() + __func__, msg);
    }

    sql::SqlErrorObject makeSchemaErr;
    json const schemaJson = _schemaToJson(resultQueryResults.makeSchema(makeSchemaErr));
    if (makeSchemaErr.isSet()) {
        resultQueryResults.freeResults();
        _dropTable(submitResult.resultTable);
        string const msg = "failed to extract schema for query=" + submitResult.resultQuery +
                           " err=" + makeSchemaErr.printErrMsg();
        error(__func__, msg);
        throw http::Error(context() + __func__, msg);
    }
    json rowsJson = _rowsToJson(resultQueryResults);
    resultQueryResults.freeResults();
    _dropTable(submitResult.resultTable);
    return json::object({{"schema", schemaJson}, {"rows", rowsJson}});
}

void HttpCzarQueryModule::_dropTable(string const& tableName) const {
    if (tableName.empty()) return;
    string const query = "DROP TABLE " + tableName;
    debug(__func__, query);
    auto const conn =
            sql::SqlConnectionFactory::make(cconfig::CzarConfig::instance()->getMySqlResultConfig());
    sql::SqlErrorObject err;
    if (!conn->runQuery(query, err)) {
        error(__func__, "failed query=" + query + " err=" + err.printErrMsg());
    }
}

json HttpCzarQueryModule::_schemaToJson(sql::Schema const& schema) const {
    json schemaJson = json::array();
    for (auto const& colDef : schema.columns) {
        json columnJson = json::object();
        columnJson["table"] = colDef.table;
        columnJson["column"] = colDef.name;
        columnJson["type"] = colDef.colType.sqlType;
        schemaJson.push_back(columnJson);
    }
    return schemaJson;
}

json HttpCzarQueryModule::_rowsToJson(sql::SqlResults& results) const {
    json rowsJson = json::array();
    for (sql::SqlResults::iterator itr = results.begin(); itr != results.end(); ++itr) {
        sql::SqlResults::value_type const& row = *itr;
        json rowJson = json::array();
        for (size_t i = 0; i < row.size(); ++i) {
            rowJson.push_back(string(row[i].first ? row[i].first : "NULL"));
        }
        rowsJson.push_back(rowJson);
    }
    return rowsJson;
}

}  // namespace lsst::qserv::czar
