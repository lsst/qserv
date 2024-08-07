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
#ifndef LSST_QSERV_CZAR_CHTTPMODULE_H
#define LSST_QSERV_CZAR_CHTTPMODULE_H

// System headers
#include <string>

// Qserv headers
#include "http/ChttpModule.h"

// Forward declarations
namespace httplib {
class Request;
class Response;
}  // namespace httplib

// This header declarations
namespace lsst::qserv::czar {

/**
 * Class ChttpModule is an intermediate base class of the Qserv Czar modules.
 */
class ChttpModule : public http::ChttpModule {
public:
    ChttpModule() = delete;
    ChttpModule(ChttpModule const&) = delete;
    ChttpModule& operator=(ChttpModule const&) = delete;

    virtual ~ChttpModule() = default;

protected:
    ChttpModule(std::string const& context, httplib::Request const& req, httplib::Response& resp);

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

#endif  // LSST_QSERV_CZAR_CHTTPMODULE_H
