// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2020 LSST.
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
#include "util/String.h"

// System headers
#include <functional>
#include <stdexcept>

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

#define CONTEXT_(func) ("String::" + string(func) + " ")

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.util.String");

template <typename T>
vector<T> getNumericVectFromStr(string const& func, vector<string> const& strings,
                                function<T(string const&, size_t&)> const& parseNumber, bool throwOnError,
                                T defaultVal) {
    vector<T> result;
    for (string const& str : strings) {
        try {
            size_t sz = 0;
            T const val = parseNumber(str, sz);
            if (sz != str.length()) {
                LOGS(_log, LOG_LVL_WARN,
                     CONTEXT_(func) << "unused characters when converting '" << str << "' to " << val);
            }
            result.push_back(val);
        } catch (invalid_argument const& ex) {
            string const msg = CONTEXT_(func) + "unable to parse '" + str + "', ex: " + string(ex.what());
            LOGS(_log, LOG_LVL_ERROR, msg);
            if (throwOnError) throw invalid_argument(msg);
            result.push_back(defaultVal);
        }
    }
    return result;
}

char const hexChars[16] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

}  // namespace

namespace lsst::qserv::util {

vector<string> String::split(string const& original, string const& delimiter, bool greedy) {
    // Apply trivial optimizations. Note that the specified "greedy" behavior
    // must be preserved during the optimisations.
    vector<string> result;
    if (original.empty()) {
        if (!greedy) result.push_back(original);
        return result;
    }
    if (delimiter.empty()) {
        if (!original.empty() || !greedy) result.push_back(original);
        return result;
    }
    string str(original);
    size_t pos;
    bool loop = true;
    while (loop) {
        pos = str.find(delimiter);
        if (pos == string::npos) {
            loop = false;
        }
        auto const candidate = str.substr(0, pos);
        if (!candidate.empty() || !greedy) result.push_back(candidate);
        str = str.substr(pos + delimiter.length());
    }
    return result;
}

vector<int> String::parseToVectInt(string const& str, string const& delimiter, bool throwOnError,
                                   int defaultVal, bool greedy) {
    auto const parseNumber = [](string const& str, size_t& sz) -> int { return stoi(str, &sz); };
    return ::getNumericVectFromStr<int>(__func__, split(str, delimiter, greedy), parseNumber, throwOnError,
                                        defaultVal);
}

vector<uint64_t> String::parseToVectUInt64(string const& str, string const& delimiter, bool throwOnError,
                                           uint64_t defaultVal, bool greedy) {
    auto const parseNumber = [](string const& str, size_t& sz) -> uint64_t { return stoull(str, &sz); };
    return ::getNumericVectFromStr<uint64_t>(__func__, split(str, delimiter, greedy), parseNumber,
                                             throwOnError, defaultVal);
}

string String::toHex(char const* ptr, size_t length) {
    if (ptr == nullptr) {
        throw invalid_argument(CONTEXT_(__func__) + "sequnce pointer is nullptr");
    }
    if (length == 0) return string();
    string out;
    out.resize(2 * length);
    char* outPtr = &out[0];
    for (char const* inPtr = ptr; inPtr < ptr + length; ++inPtr) {
        char const& byte = *inPtr;
        *(outPtr++) = ::hexChars[(byte & 0xF0) >> 4];
        *(outPtr++) = ::hexChars[(byte & 0x0F) >> 0];
    }
    return out;
}

}  // namespace lsst::qserv::util
