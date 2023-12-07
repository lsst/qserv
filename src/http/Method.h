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
#ifndef LSST_QSERV_HTTP_METHOD_H
#define LSST_QSERV_HTTP_METHOD_H

// System headers
#include <ostream>
#include <string>
#include <vector>

// This header declarations
namespace lsst::qserv::http {

/// The names of the allowed methods.
static std::vector<std::string> const allowedMethods = {"GET", "POST", "PUT", "DELETE"};

/// The type-safe representation of the HTTP methods.
enum class Method : int { GET, POST, PUT, DELETE };

/// @return The string representation.
/// @throws std::invalid_argument If the method is not valid.
std::string method2string(Method method);

/// @return The method.
/// @throws std::invalid_argument If the input value doesn't correspond to any method.
Method string2method(std::string const& str);

inline std::ostream& operator<<(std::ostream& os, Method method) {
    os << method2string(method);
    return os;
}
}  // namespace lsst::qserv::http

#endif  // LSST_QSERV_HTTP_METHOD_H