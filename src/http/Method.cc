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
#include "http/Method.h"

// Standard headers
#include <stdexcept>

using namespace std;

namespace lsst::qserv::http {

string method2string(Method method) {
    switch (method) {
        case Method::GET:
            return "GET";
        case Method::POST:
            return "POST";
        case Method::PUT:
            return "PUT";
        case Method::DELETE:
            return "DELETE";
    }
    throw invalid_argument("http::" + string(__func__) + " invalid method " +
                           to_string(static_cast<int>(method)));
}

Method string2method(string const& str) {
    if ("GET" == str)
        return Method::GET;
    else if ("POST" == str)
        return Method::POST;
    else if ("PUT" == str)
        return Method::PUT;
    else if ("DELETE" == str)
        return Method::DELETE;
    throw invalid_argument("http::" + string(__func__) + " invalid method " + str);
}

}  // namespace lsst::qserv::http
