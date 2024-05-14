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
#ifndef LSST_QSERV_HTTP_BINARYENCODING_H
#define LSST_QSERV_HTTP_BINARYENCODING_H

// System headers
#include <string>
#include <vector>

// This header declarations
namespace lsst::qserv::http {

/// The names of the allowed modes.
static std::vector<std::string> const allowedBinaryEncodingModes = {"hex", "b64", "array"};

/// Options for encoding data of the binary columns in the JSON result.
enum class BinaryEncodingMode : int {
    HEX,   ///< The hexadecimal representation stored as a string
    B64,   ///< Data encoded using Base64 algorithm (with padding as needed)
    ARRAY  ///< JSON array of 8-bit unsigned integers in a range of 0 .. 255.
};

/**
 * @param str The string to parse.
 * @return The parsed and validated representation of the encoding.
 * @throw std::invalid_argument If the input can't be translated into a valid mode.
 */
BinaryEncodingMode parseBinaryEncoding(std::string const& str);

/**
 * Translate the mode into a string representation.
 * @param mode The mode to be translated.
 * @throw std::invalid_argument If the input can't be translated into a valid string.
 */
std::string binaryEncoding2string(BinaryEncodingMode mode);

}  // namespace lsst::qserv::http

#endif  // LSST_QSERV_HTTP_BINARYENCODING_H
