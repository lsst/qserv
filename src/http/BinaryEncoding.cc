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
#include "http/BinaryEncoding.h"

// Standard headers
#include <stdexcept>

using namespace std;

namespace lsst::qserv::http {

BinaryEncodingMode parseBinaryEncoding(string const& str) {
    if (str == "hex")
        return BinaryEncodingMode::HEX;
    else if (str == "array")
        return BinaryEncodingMode::ARRAY;
    throw invalid_argument("http::" + string(__func__) + " unsupported mode '" + str + "'");
}

string binaryEncoding2string(BinaryEncodingMode mode) {
    switch (mode) {
        case BinaryEncodingMode::HEX:
            return "hex";
        case BinaryEncodingMode::ARRAY:
            return "array";
    }
    throw invalid_argument("http::" + string(__func__) + " invalid mode " +
                           to_string(static_cast<int>(mode)));
}

}  // namespace lsst::qserv::http
