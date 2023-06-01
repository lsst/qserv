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
#ifndef LSST_QSERV_HTTPREQUESTQUERY_H
#define LSST_QSERV_HTTPREQUESTQUERY_H

// System headers
#include <cstdint>
#include <string>
#include <vector>
#include <unordered_map>

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class HttpRequestQuery implements a parser for parameters passed into
 * the Web services via the optional query part of a URL.
 *
 * There are two kinds of the parameter extraction methods in this class:
 *
 * "required<type>"
 *    methods expect parameters to exist and have a value which
 *    could be translated from a string into a value of the required type.
 *    Otherwise these methods will throw exceptions std::invalid_argument (for
 *    the missing parameters) or std::out_of_range (for invalid input values).
 *
 * "optional<type>"
 *    methods have an additional argument "defaultValue" which carries
 *    a value to be returned if the parameter wasn't found. Note that these
 *    methods may still throw std::out_of_range (for invalid invalid values).
 */
class HttpRequestQuery {
public:
    explicit HttpRequestQuery(std::unordered_map<std::string, std::string> const& query);

    HttpRequestQuery() = default;
    HttpRequestQuery(HttpRequestQuery const&) = default;
    HttpRequestQuery& operator=(HttpRequestQuery const&) = default;

    ~HttpRequestQuery() = default;

    std::string requiredString(std::string const& param) const;
    std::string optionalString(std::string const& param,
                               std::string const& defaultValue = std::string()) const;

    bool requiredBool(std::string const& param) const;
    bool optionalBool(std::string const& param, bool defaultValue = false) const;

    uint16_t requiredUInt16(std::string const& param) const;
    uint16_t optionalUInt16(std::string const& param, uint16_t defaultValue = 0) const;

    unsigned int requiredUInt(std::string const& param) const;
    unsigned int optionalUInt(std::string const& param, unsigned int defaultValue = 0) const;

    int requiredInt(std::string const& param) const;
    int optionalInt(std::string const& param, int defaultValue = 0) const;

    std::uint64_t requiredUInt64(std::string const& param) const;
    std::uint64_t optionalUInt64(std::string const& param, std::uint64_t defaultValue = 0) const;

    double requiredDouble(std::string const& param) const;

    /**
     * For the optional parameter, parse its input string value into
     * a collection of numbers. Where the input string is expected to have
     * the following syntax:
     * @code
     * [<n1>[,<n2>[,<n2> ... ]]]
     * @endcode
     */
    std::vector<std::uint64_t> optionalVectorUInt64(
            std::string const& param,
            std::vector<std::uint64_t> const& defaultValue = std::vector<std::uint64_t>()) const;

    /**
     * For the optional parameter, parse its input string value into
     * a collection of strings. Where the input string is expected to have
     * the following syntax:
     * @code
     * [<s1>[,<s2>[,<s2> ... ]]]
     * @endcode
     */
    std::vector<std::string> optionalVectorStr(
            std::string const& param,
            std::vector<std::string> const& defaultValue = std::vector<std::string>()) const;

    bool has(std::string const& param) const;

private:
    /// The input map of parameters
    std::unordered_map<std::string, std::string> _query;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_HTTPREQUESTQUERY_H
