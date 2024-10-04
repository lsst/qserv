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
#ifndef LSST_QSERV_REPLICA_INGESTUTILS_H
#define LSST_QSERV_REPLICA_INGESTUTILS_H

// System headers
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "http/RequestBodyJSON.h"

// Forward declarations

namespace lsst::qserv::http {
class RequestBodyJSON;
}  // namespace lsst::qserv::http

namespace lsst::qserv::replica::csv {
class DialectInput;
}  // namespace lsst::qserv::replica::csv

// This header declarations
namespace lsst::qserv::replica {

/**
 * Parse the dialect input from the request body.
 * @param body The request body.
 * @return The parsed dialect input.
 */
csv::DialectInput parseDialectInput(http::RequestBodyJSON const& body);

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_INGESTUTILS_H
