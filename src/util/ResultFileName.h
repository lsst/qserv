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
#include "qmeta/types.h"

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
 * The file path has the following general format:
 * @code
 *   [<folder>/]<czar-id>-<query-id>-<job-id>-<chunk-id>-<attemptcount>[.<ext>]
 * @code
 */
class ResultFileName {
public:
    /// The file extention including the '.' prefix.
    static std::string const fileExt;

    ResultFileName() = default;
    ResultFileName(ResultFileName const&) = default;
    ResultFileName& operator=(ResultFileName const&) = default;

    /// This form of constructionstores attributes of a file and generates
    /// the name of a file in a format specified in the class description section.
    ResultFileName(qmeta::CzarId czarId, QueryId queryId, std::uint32_t jobId, std::uint32_t chunkId,
                   std::uint32_t attemptCount);

    /// @param filePath The file to be evaluated.
    /// @throw std::invalid_argument If the file path did not match expectations.
    explicit ResultFileName(boost::filesystem::path const& filePath);

    /// @param filePath The file to be evaluated.
    /// @throw std::invalid_argument If the file path did not match expectations.
    explicit ResultFileName(std::string const& filePath);

    /// @return The name of a file including its extension and excluding the optional base folder.
    std::string const& fileName() const { return _fileName; }

    qmeta::CzarId czarId() const { return _czarId; }
    QueryId queryId() const { return _queryId; }
    std::uint32_t jobId() const { return _jobId; }
    std::uint32_t chunkId() const { return _chunkId; }
    std::uint32_t attemptCount() const { return _attemptCount; }

    /// @return The JSON object (dictionary) encapsulating values of the attributes.
    nlohmann::json toJson() const;

    bool operator==(ResultFileName const& rhs) const;
    bool operator!=(ResultFileName const& rhs) const { return operator==(rhs); }

    friend std::ostream& operator<<(std::ostream& os, ResultFileName const& parser);

private:
    static std::string _context(std::string const& func);
    void _parse();

    template <typename T>
    void _validateAndStoreAttr(std::size_t attrIndex, std::string const& attrName, T& attr) {
        std::uint64_t const& attrValue = _taskAttributes[attrIndex];
        T const minVal = std::numeric_limits<T>::min();
        T const maxVal = std::numeric_limits<T>::max();
        if ((attrValue >= minVal) && (attrValue <= maxVal)) {
            attr = static_cast<T>(attrValue);
            return;
        }
        throw std::invalid_argument(_context(__func__) + " failed for attribute=" + attrName + ", value=" +
                                    std::to_string(attrValue) + ", allowed range=[" + std::to_string(minVal) +
                                    "," + std::to_string(maxVal) + "], file=" + _fileName);
    }

    std::string _fileName;
    qmeta::CzarId _czarId = 0;
    QueryId _queryId = 0;
    std::uint32_t _jobId = 0;
    std::uint32_t _chunkId = 0;
    std::uint32_t _attemptCount = 0;

    std::vector<std::uint64_t> _taskAttributes;
};

}  // namespace lsst::qserv::util

#endif  // LSST_QSERV_UTIL_RESULTFILENAME_H
