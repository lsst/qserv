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
#ifndef LSST_QSERV_HTTP_EXCEPTIONS_H
#define LSST_QSERV_HTTP_EXCEPTIONS_H

// System headers
#include <stdexcept>
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// This header declarations
namespace lsst::qserv::http {

/**
 * Class Error represents exceptions thrown by HTTP modules in case of
 * any errors which require additional info to be sent back to clients in
 * response to the requests.
 */
class Error : public std::runtime_error {
public:
    /**
     * @param func A scope in which the error originated.
     * @param errorMsg A reason for the exception.
     * @param errorExt (optional) The additional information on the error.
     */
    Error(std::string const& func, std::string const& errorMsg,
          nlohmann::json const& errorExt = nlohmann::json::object())
            : std::runtime_error(errorMsg), _func(func), _errorExt(errorExt) {}

    Error() = default;
    Error(Error const&) = default;
    Error& operator=(Error const&) = default;

    std::string const& func() const { return _func; }
    nlohmann::json const& errorExt() const { return _errorExt; }

private:
    std::string const _func;
    nlohmann::json const _errorExt = nlohmann::json::object();
};

/**
 * Report an error as an exception.
 * @note This method always throws and it never returns control back to a caller.
 *   The method is meant to be used by Ingest system workers to report error conditions
 *   that won't require aborting a transaction.
 * @param scope A scope of the error.
 * @param error A human readable error message.
 * @param (optional) HTTP code of an error if applies.
 * @throws Error
 */
void raiseRetryAllowedError(std::string const& scope, std::string const& error, long httpErrCode = 0);

}  // namespace lsst::qserv::http

#endif  // LSST_QSERV_HTTP_EXCEPTIONS_H
