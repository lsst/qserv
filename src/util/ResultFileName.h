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

#ifndef LSST_QSERV_UTIL_RESULTFILENAME_H
#define LSST_QSERV_UTIL_RESULTFILENAME_H

// System headers
#include <limits>
#include <ostream>
#include <stdexcept>
#include <string>
#include <vector>

// Third-party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "global/intTypes.h"

// Forward declarations

namespace boost::filesystem {
class path;
}  // namespace boost::filesystem

// This header declarations

namespace lsst::qserv::util {

/**
 * Class ResultFileName is an abstraction representing result files at workers.
 * The class has two purposes:
 *
 * - Extracting attributes of a file from the file path/name. Values are parsed, validated
 *   to ensure they the valid range, and stored in the corresponding data members.
 * - Building the the file name from its attributes. The file name is built
 *   according to the same rules as those used for parsing.
 *
 * All operations are done in the class's constructors. A few forms of the construction are
 * provided for convenience of the client applications.
 *
 * QueryId + UberJobId results is a unique identifier.
 * CzarId can be useful for some operations.
 *
 * The file path has the following general format:
 * @code
 *   [<folder>/]<czar-id>-<query-id>[.<ext>]
 * @code
 */
class ResultFileName {
public:
    /// The file extention including the '.' prefix.
    static std::string const fileExt;

    static std::string splitToken() { return std::string("-"); }

    ResultFileName() = default;
    ResultFileName(ResultFileName const&) = default;
    ResultFileName& operator=(ResultFileName const&) = default;

    /// This form of construction stores attributes of a file and generates
    /// the name of a file in a format specified in the class description section.
    ResultFileName(CzarId czarId, QueryId queryId, UberJobId ujId);

    /// @param filePath The file to be evaluated.
    /// @throw std::invalid_argument If the file path did not match expectations.
    explicit ResultFileName(boost::filesystem::path const& filePath);

    /// @param filePath The file to be evaluated.
    /// @throw std::invalid_argument If the file path did not match expectations.
    explicit ResultFileName(std::string const& filePath);

    /// @return The name of a file including its extension and excluding the optional base folder.
    std::string const& fileName() const { return _fileName; }

    CzarId czarId() const { return _czarId; }
    QueryId queryId() const { return _queryId; }
    UberJobId ujId() const { return _ujId; }

    /// @return The JSON object (dictionary) encapsulating values of the attributes.
    nlohmann::json toJson() const;

    bool operator==(ResultFileName const& rhs) const;
    bool operator!=(ResultFileName const& rhs) const { return operator==(rhs); }

    friend std::ostream& operator<<(std::ostream& os, ResultFileName const& parser);

private:
    static std::string _context(std::string const& func);
    void _parse();

    // This only works with unsigned, which wouldn't work with UberJobId
    // except that negative UberJobId's never make it off of the czar.
    template <typename T>
    void _validateAndStoreAttr(std::size_t attrIndex, std::string const& attrName, T& attr) {
        size_t const& attrValue = _taskAttributes[attrIndex];
        size_t const maxVal = std::numeric_limits<T>::max();
        /// min value for size_t is 0, so only max matters
        if (attrValue <= maxVal) {
            attr = static_cast<T>(attrValue);
            return;
        }
        throw std::invalid_argument(_context(__func__) + " failed for attribute=" + attrName +
                                    ", value=" + std::to_string(attrValue) + ", allowed range=[0," +
                                    std::to_string(maxVal) + "], file=" + _fileName);
    }

    std::string _fileName;
    CzarId _czarId = 0;
    QueryId _queryId = 0;
    UberJobId _ujId = 0;

    std::vector<std::uint64_t> _taskAttributes;
};

}  // namespace lsst::qserv::util

#endif  // LSST_QSERV_UTIL_RESULTFILENAME_H
