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
#include "czar/HttpModule.h"
#include "global/intTypes.h"

// Forward declarations

namespace lsst::qserv::czar {
struct SubmitResult;
}  // namespace lsst::qserv::czar

namespace lsst::qserv::qhttp {
class Request;
class Response;
}  // namespace lsst::qserv::qhttp

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
class HttpCzarQueryModule : public czar::HttpModule {
public:
    /**
     * @note supported values for parameter 'subModuleName' are:
     *   'SUBMIT'          - submit a sync query
     *   'SUBMIT-ASYNC'    - submit an async query
     *   'CANCEL'          - cancel the previously submited async query
     *   'STATUS'          - return a status of the previously submited async query
     *   'RESULT'          - return data of the previously submited async query
     *
     * @throws std::invalid_argument for unknown values of parameter 'subModuleName'
     */
    static void process(std::string const& context, std::shared_ptr<qhttp::Request> const& req,
                        std::shared_ptr<qhttp::Response> const& resp, std::string const& subModuleName,
                        http::AuthType const authType = http::AuthType::NONE);

    HttpCzarQueryModule() = delete;
    HttpCzarQueryModule(HttpCzarQueryModule const&) = delete;
    HttpCzarQueryModule& operator=(HttpCzarQueryModule const&) = delete;

    ~HttpCzarQueryModule() final = default;

protected:
    virtual nlohmann::json executeImpl(std::string const& subModuleName) final;

private:
    HttpCzarQueryModule(std::string const& context, std::shared_ptr<qhttp::Request> const& req,
                        std::shared_ptr<qhttp::Response> const& resp);

    /// Options for encoding data of the binary columns in the JSON result.
    enum BinaryEncodingMode {
        BINARY_ENCODE_HEX,   ///< The hexadecimal representation stored as a string
        BINARY_ENCODE_ARRAY  ///< JSON array of 8-bit unsigned integers in a range of 0 .. 255.
    };

    /**
     * @param str The string to parse.,
     * @return The parsed and validated representation of the encoding.
     * @throw std::invalid_argument If the input can't be translated into a valid mode.
     */
    BinaryEncodingMode _parseBinaryEncoding(std::string const& str);

    nlohmann::json _submit();
    nlohmann::json _submitAsync();
    nlohmann::json _cancel();
    nlohmann::json _status();
    nlohmann::json _result();

    SubmitResult _getRequestParamsAndSubmit(std::string const& func, bool async);
    SubmitResult _getQueryInfo() const;
    QueryId _getQueryId() const;
    nlohmann::json _waitAndExtractResult(SubmitResult const& submitResult,
                                         BinaryEncodingMode binaryEncoding) const;
    void _dropTable(std::string const& tableName) const;
    nlohmann::json _schemaToJson(sql::Schema const& schema) const;
    nlohmann::json _rowsToJson(sql::SqlResults& results, nlohmann::json const& schemaJson,
                               BinaryEncodingMode binaryEncoding) const;
};

}  // namespace lsst::qserv::czar

#endif  // LSST_QSERV_CZAR_HTTPCZARQUERYMODULE_H
