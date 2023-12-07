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
#include "http/RequestQuery.h"

// System headers
#include <algorithm>
#include <cctype>
#include <limits>
#include <iostream>
#include <sstream>
#include <stdexcept>

using namespace std;

namespace {

void throwIf(bool cond, string const& func, string const& param) {
    if (!cond) return;
    throw invalid_argument("RequestQuery::" + func + " mandatory parameter '" + param + "' is missing");
}

template <typename T>
T parseRestrictedIntegerType(string const& func, string const& param, string const& str) {
    int64_t val = stoll(str);
    if ((val < numeric_limits<T>::min()) || (val > numeric_limits<T>::max())) {
        throw out_of_range("HttpProcessor::" + func + " value of parameter: " + param +
                           " exceeds allowed range for the target type");
    }
    return static_cast<T>(val);
}

bool parseBool(string const& str) {
    string strLowerCase = str;
    std::transform(strLowerCase.begin(), strLowerCase.end(), strLowerCase.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    if (strLowerCase == "true") {
        return true;
    } else if (strLowerCase == "false") {
        return false;
    }
    return stoull(str) != 0;
}

vector<string> parseVectorStr(string const& in) {
    vector<string> result;
    stringstream ss(in);
    while (ss.good()) {
        string str;
        getline(ss, str, ',');
        if (!str.empty()) result.push_back(str);
    }
    return result;
}

}  // namespace

namespace lsst::qserv::http {

RequestQuery::RequestQuery(std::unordered_map<std::string, std::string> const& query) : _query(query) {}

string RequestQuery::requiredString(string const& param) const {
    auto const val = optionalString(param);
    ::throwIf(val.empty(), __func__, param);
    return val;
}

string RequestQuery::optionalString(string const& param, string const& defaultValue) const {
    auto itr = _query.find(param);
    if (itr == _query.end()) return defaultValue;
    return itr->second;
}

bool RequestQuery::requiredBool(string const& param) const {
    auto itr = _query.find(param);
    ::throwIf(itr == _query.end(), __func__, param);
    return ::parseBool(itr->second);
}

bool RequestQuery::optionalBool(string const& param, bool defaultValue) const {
    auto itr = _query.find(param);
    if (itr == _query.end()) return defaultValue;
    return ::parseBool(itr->second);
}

uint16_t RequestQuery::requiredUInt16(string const& param) const {
    auto itr = _query.find(param);
    ::throwIf(itr == _query.end(), __func__, param);
    return ::parseRestrictedIntegerType<uint16_t>(__func__, param, itr->second);
}

uint16_t RequestQuery::optionalUInt16(string const& param, uint16_t defaultValue) const {
    auto itr = _query.find(param);
    if (itr == _query.end()) return defaultValue;
    return ::parseRestrictedIntegerType<uint16_t>(__func__, param, itr->second);
}

unsigned int RequestQuery::requiredUInt(string const& param) const {
    auto itr = _query.find(param);
    ::throwIf(itr == _query.end(), __func__, param);
    return ::parseRestrictedIntegerType<unsigned int>(__func__, param, itr->second);
}

unsigned int RequestQuery::optionalUInt(string const& param, unsigned int defaultValue) const {
    auto itr = _query.find(param);
    if (itr == _query.end()) return defaultValue;
    return ::parseRestrictedIntegerType<unsigned int>(__func__, param, itr->second);
}

int RequestQuery::requiredInt(string const& param) const {
    auto itr = _query.find(param);
    ::throwIf(itr == _query.end(), __func__, param);
    return ::parseRestrictedIntegerType<int>(__func__, param, itr->second);
}

int RequestQuery::optionalInt(string const& param, int defaultValue) const {
    auto itr = _query.find(param);
    if (itr == _query.end()) return defaultValue;
    return ::parseRestrictedIntegerType<int>(__func__, param, itr->second);
}

uint64_t RequestQuery::requiredUInt64(string const& param) const {
    auto itr = _query.find(param);
    ::throwIf(itr == _query.end(), __func__, param);
    return stoull(itr->second);
}

uint64_t RequestQuery::optionalUInt64(string const& param, uint64_t defaultValue) const {
    auto itr = _query.find(param);
    if (itr == _query.end()) return defaultValue;
    return stoull(itr->second);
}

double RequestQuery::requiredDouble(string const& param) const {
    auto itr = _query.find(param);
    ::throwIf(itr == _query.end(), __func__, param);
    return stod(itr->second);
}

vector<uint64_t> RequestQuery::optionalVectorUInt64(string const& param,
                                                    vector<uint64_t> const& defaultValue) const {
    auto itr = _query.find(param);
    if (itr == _query.end()) return defaultValue;
    vector<uint64_t> result;
    stringstream ss(itr->second);
    for (uint64_t num; ss >> num;) {
        result.push_back(num);
        if (ss.peek() == ',') ss.ignore();
    }
    return result;
}

vector<string> RequestQuery::optionalVectorStr(string const& param,
                                               vector<string> const& defaultValue) const {
    auto itr = _query.find(param);
    if (itr == _query.end()) return defaultValue;
    return ::parseVectorStr(itr->second);
}

vector<string> RequestQuery::requiredVectorStr(string const& param) const {
    auto itr = _query.find(param);
    ::throwIf(itr == _query.end(), __func__, param);
    return ::parseVectorStr(itr->second);
}

bool RequestQuery::has(string const& param) const { return _query.find(param) != _query.end(); }

}  // namespace lsst::qserv::http
