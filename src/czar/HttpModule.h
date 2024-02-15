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
#ifndef LSST_QSERV_CZAR_HTTPMODULE_H
#define LSST_QSERV_CZAR_HTTPMODULE_H

// System headers
#include <memory>
#include <string>

// Qserv headers
#include "http/ModuleBase.h"

// Forward declarations
namespace lsst::qserv::qhttp {
class Request;
class Response;
}  // namespace lsst::qserv::qhttp

// This header declarations
namespace lsst::qserv::czar {

/**
 * Class HttpModule is an intermediate base class of the Qserv Czar modules.
 */
class HttpModule : public http::ModuleBase {
public:
    HttpModule() = delete;
    HttpModule(HttpModule const&) = delete;
    HttpModule& operator=(HttpModule const&) = delete;

    virtual ~HttpModule() = default;

protected:
    HttpModule(std::string const& context, std::shared_ptr<qhttp::Request> const& req,
               std::shared_ptr<qhttp::Response> const& resp);

    virtual std::string context() const final;

    /**
     * Check if Czar identifier is present in a request and if so then the identifier
     * is the same as the one of the current Czar. Throw an exception in case of mismatch.
     * @param func The name of the calling context (it's used for error reporting).
     * @throws std::invalid_argument If the dentifiers didn't match.
     */
    void enforceCzarName(std::string const& func) const;

private:
    std::string const _context;
};

}  // namespace lsst::qserv::czar

#endif  // LSST_QSERV_CZAR_HTTPMODULE_H
