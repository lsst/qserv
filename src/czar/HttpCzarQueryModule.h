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
#ifndef LSST_QSERV_CZAR_HTTPCZARQUERYMODULE_H
#define LSST_QSERV_CZAR_HTTPCZARQUERYMODULE_H

// System headers
#include <memory>
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "czar/ChttpModule.h"
#include "global/intTypes.h"
#include "http/BinaryEncoding.h"

// Forward declarations

namespace lsst::qserv::czar {
struct SubmitResult;
}  // namespace lsst::qserv::czar

namespace httplib {
class Request;
class Response;
}  // namespace httplib

namespace lsst::qserv::sql {
class SqlResults;
struct Schema;
}  // namespace lsst::qserv::sql

// This header declarations
namespace lsst::qserv::czar {

/**
 * Class HttpCzarQueryModule implements a handler for processing user
 * queries submitted to Czar via the HTTP-based frontend.
 */
class HttpCzarQueryModule : public czar::ChttpModule {
public:
    /**
     * @note supported values for parameter 'subModuleName' are:
     *   'SUBMIT'          - submit a sync query
     *   'SUBMIT-ASYNC'    - submit an async query
     *   'CANCEL'          - cancel the previously submited async query
     *   'STATUS'          - return a status of the previously submited async query
     *   'RESULT'          - return data of the previously submited async query
     *   'RESULT-DELETE'   - delete a result set of an async query
     *
     * @throws std::invalid_argument for unknown values of parameter 'subModuleName'
     */
    static void process(std::string const& context, httplib::Request const& req, httplib::Response& resp,
                        std::string const& subModuleName,
                        http::AuthType const authType = http::AuthType::NONE);

    HttpCzarQueryModule() = delete;
    HttpCzarQueryModule(HttpCzarQueryModule const&) = delete;
    HttpCzarQueryModule& operator=(HttpCzarQueryModule const&) = delete;

    ~HttpCzarQueryModule() final = default;

protected:
    virtual nlohmann::json executeImpl(std::string const& subModuleName) final;

private:
    HttpCzarQueryModule(std::string const& context, httplib::Request const& req, httplib::Response& resp);

    nlohmann::json _submit();
    nlohmann::json _submitAsync();
    nlohmann::json _cancel();
    nlohmann::json _status();
    nlohmann::json _result();
    nlohmann::json _resultDelete();

    SubmitResult _getRequestParamsAndSubmit(std::string const& func, bool async);
    SubmitResult _getQueryInfo() const;
    QueryId _getQueryId() const;
    nlohmann::json _waitAndExtractResult(SubmitResult const& submitResult,
                                         http::BinaryEncodingMode binaryEncoding) const;
    void _dropTable(std::string const& tableName) const;
    nlohmann::json _schemaToJson(sql::Schema const& schema) const;
    nlohmann::json _rowsToJson(sql::SqlResults& results, nlohmann::json const& schemaJson,
                               http::BinaryEncodingMode binaryEncoding) const;
};

}  // namespace lsst::qserv::czar

#endif  // LSST_QSERV_CZAR_HTTPCZARQUERYMODULE_H
