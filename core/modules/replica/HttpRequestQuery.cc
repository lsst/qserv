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
#include "replica/HttpRequestQuery.h"

// System headers
#include <limits>
#include <stdexcept>

using namespace std;

namespace lsst {
namespace qserv {
namespace replica {

HttpRequestQuery::HttpRequestQuery(std::unordered_map<std::string,std::string> const& query)
    :   _query(query) {
}


string HttpRequestQuery::requiredString(string const& param) const {
    auto const val = optionalString(param);
    if (val.empty()) {
        throw invalid_argument(
                string(__func__) + " parameter '" + param + "' is missing or has an invalid value");
    }
    return val;
}


string HttpRequestQuery::optionalString(string const& param,
                                        string const& defaultValue) const {
    auto&& itr = _query.find(param);
    if (itr == _query.end()) return defaultValue;
    return itr->second;
}


uint16_t HttpRequestQuery::requiredUInt16(string const& param) const {
    auto const val = optionalUInt16(param, 0);
    if (val == 0) {
        throw invalid_argument(
                string(__func__) + " parameter '" + param + "' is missing or has an invalid value");
    }
    return val;
}


uint16_t HttpRequestQuery::optionalUInt16(string const& param,
                                          uint16_t defaultValue) const {
    auto&& itr = _query.find(param);
    if (itr == _query.end()) return defaultValue;
    unsigned long val = stoul(itr->second);
    if (val >= numeric_limits<uint16_t>::max()) {
        throw out_of_range(
                "HttpProcessor::" + string(__func__) + " value of parameter: " + param +
                " exceeds allowed limit for type 'uint16_t'");
    }
    return static_cast<uint16_t>(val);
}


uint64_t HttpRequestQuery::requiredUInt64(string const& param) const {
    auto const val = optionalUInt64(param, 0);
    if (val == 0) {
        throw invalid_argument(
                string(__func__) + " parameter '" + param + "' is missing or has an invalid value");
    }
    return val;
}


uint64_t HttpRequestQuery::optionalUInt64(string const& param,
                                          uint64_t defaultValue) const {
    auto&& itr = _query.find(param);
    if (itr == _query.end()) return defaultValue;
    return stoull(itr->second);
}


int HttpRequestQuery::optionalInt(string const& param,
                                  int defaultValue) const {
    auto&& itr = _query.find(param);
    if (itr == _query.end()) return defaultValue;
    return stoi(itr->second);
}


unsigned int HttpRequestQuery::requiredUInt(string const& param) const {
    auto&& itr = _query.find(param);
    if (itr == _query.end()) {
        throw invalid_argument("mandatory parameter '" + param + "' is missing");
    }
    unsigned long val = stoul(itr->second);
    if (val > numeric_limits<unsigned int>::max()) {
        throw out_of_range(
                "HttpProcessor::" + string(__func__) + " value of parameter: " + param +
                " exceeds allowed limit for type 'unsigned int'");
    }
    return static_cast<unsigned int>(val);
}


unsigned int HttpRequestQuery::optionalUInt(string const& param,
                                            unsigned int defaultValue) const {
    auto&& itr = _query.find(param);
    if (itr == _query.end()) return defaultValue;
    return stoul(itr->second);
}


bool HttpRequestQuery::requiredBool(string const& param) const {
    auto const val = optionalInt(param);
    if (val < 0) {
        throw invalid_argument(
                string(__func__) + " parameter '" + param + "' is missing or has an invalid value");
    }
    return val != 0;
}


bool HttpRequestQuery::optionalBool(string const& param,
                                    bool defaultValue) const {
    auto&& itr = _query.find(param);
    if (itr == _query.end()) return defaultValue;
    return not (itr->second.empty() or (itr->second == "0"));
}

}}}  // namespace lsst::qserv::replica
