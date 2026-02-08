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
#include <algorithm>
#include <cctype>
#include <chrono>
#include <functional>
#include <random>
#include <stdexcept>

// Third party headers
#include <boost/algorithm/string.hpp>
#include <boost/archive/iterators/transform_width.hpp>
#include <boost/archive/iterators/base64_from_binary.hpp>
#include <boost/archive/iterators/binary_from_base64.hpp>

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using namespace boost::algorithm;
using namespace boost::archive::iterators;

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

char const hexCharsUC[16] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};
char const hexCharsLC[16] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

}  // namespace

namespace lsst::qserv::util {

vector<string> String::split(string const& original, string const& delimiter, bool skipEmpty) {
    // Apply trivial optimizations. Note that the specified "skipEmpty" behavior
    // must be preserved during the optimisations.
    vector<string> result;
    if (original.empty()) {
        if (!skipEmpty) result.push_back(original);
        return result;
    }
    if (delimiter.empty()) {
        if (!original.empty() || !skipEmpty) result.push_back(original);
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
        if (!candidate.empty() || !skipEmpty) result.push_back(candidate);
        str = str.substr(pos + delimiter.length());
    }
    return result;
}

vector<int> String::parseToVectInt(string const& str, string const& delimiter, bool throwOnError,
                                   int defaultVal, bool skipEmpty) {
    auto const parseNumber = [](string const& str, size_t& sz) -> int { return stoi(str, &sz); };
    return ::getNumericVectFromStr<int>(__func__, split(str, delimiter, skipEmpty), parseNumber, throwOnError,
                                        defaultVal);
}

vector<uint64_t> String::parseToVectUInt64(string const& str, string const& delimiter, bool throwOnError,
                                           uint64_t defaultVal, bool skipEmpty) {
    auto const parseNumber = [](string const& str, size_t& sz) -> uint64_t { return stoull(str, &sz); };
    return ::getNumericVectFromStr<uint64_t>(__func__, split(str, delimiter, skipEmpty), parseNumber,
                                             throwOnError, defaultVal);
}

string String::toHex(char const* ptr, size_t length, string const& prefix, bool lowerCase) {
    if (ptr == nullptr) {
        throw invalid_argument(CONTEXT_(__func__) + "sequnce pointer is nullptr");
    }
    if (length == 0) return string();
    char const* hexChars = lowerCase ? hexCharsLC : hexCharsUC;
    string out(prefix);
    out.resize(2 * length + prefix.size());
    char* outPtr = &out[prefix.size()];
    for (char const* inPtr = ptr; inPtr < ptr + length; ++inPtr) {
        char const& byte = *inPtr;
        *(outPtr++) = hexChars[(byte & 0xF0) >> 4];
        *(outPtr++) = hexChars[(byte & 0x0F) >> 0];
    }
    return out;
}

string String::fromHex(string const& hex, string const& prefix) {
    size_t const hexSize = hex.size();
    size_t const prefixSize = prefix.size();
    if (hexSize == 0 || prefixSize >= hexSize) return string();
    if ((hexSize - prefixSize) % 2 != 0) {
        throw invalid_argument(CONTEXT_(__func__) + "odd number of significant characters in the input");
    }
    if (prefixSize > 0) {
        if (hex.substr(0, prefixSize) != prefix) {
            throw invalid_argument(CONTEXT_(__func__) + "the input doesn't start with the prefix");
        }
    }
    string out;
    out.resize((hexSize - prefixSize) / 2);
    char* outPtr = &out[0];
    for (char const* ptr = hex.data() + prefixSize; ptr != hex.data() + hexSize;) {
        int v = 0;
        char c = *(ptr++);
        if ((c >= '0') && (c <= '9')) {
            v = c - '0';
        } else if ((c >= 'a') && (c <= 'f')) {
            v = 10 + (c - 'a');
        } else if ((c >= 'A') && (c <= 'F')) {
            v = 10 + (c - 'A');
        } else {
            throw range_error(CONTEXT_(__func__) + "not a valid hexadecimal character");
        }
        v <<= 4;
        c = *(ptr++);
        if ((c >= '0') && (c <= '9')) {
            v += (c - '0');
        } else if ((c >= 'a') && (c <= 'f')) {
            v += 10 + (c - 'a');
        } else if ((c >= 'A') && (c <= 'F')) {
            v += 10 + (c - 'A');
        } else {
            throw range_error(CONTEXT_(__func__) + "not a valid hexadecimal character");
        }
        *(outPtr++) = static_cast<char>(v);
    }
    return out;
}

string String::toLower(string const& str) {
    string result = str;
    transform(result.begin(), result.end(), result.begin(), ::tolower);
    return result;
}

string String::toUpper(string const& str) {
    string result = str;
    transform(result.begin(), result.end(), result.begin(), ::toupper);
    return result;
}

string String::toBase64(char const* ptr, size_t length) {
    if (ptr == nullptr) {
        throw invalid_argument(CONTEXT_(__func__) + "sequnce pointer is nullptr");
    }
    if (length == 0) return string();

    size_t const padding = (3 - length % 3) % 3;              // calculate padding size
    size_t const encodedLength = (length + padding) * 4 / 3;  // calculate encoded length

    string encoded;
    encoded.reserve(encodedLength);

    // Append base64 characters to result string.
    typedef base64_from_binary<transform_width<const char*, 6, 8>> base64_iterator;
    for (base64_iterator itr(ptr), end(ptr + length); itr != end; ++itr) {
        encoded.push_back(*itr);
    }

    // Add padding characters if necessary.
    for (size_t i = 0; i < padding; ++i) {
        encoded.push_back('=');
    }
    return encoded;
}

string String::fromBase64(string const& str) {
    if (str.empty()) return string();
    string decoded;
    try {
        typedef transform_width<binary_from_base64<string::const_iterator>, 8, 6> base64_decoder;
        decoded = trim_right_copy_if(string(base64_decoder(str.begin()), base64_decoder(str.end())),
                                     [](char c) { return c == '\0'; });
    } catch (exception const& ex) {
        throw range_error(CONTEXT_(__func__) + "failed to decode base64 string: " + ex.what());
    }
    return decoded;
}

string String::translateModel(string const& model) {
    unsigned seed = chrono::system_clock::now().time_since_epoch().count();
    mt19937 engine(seed);
    uniform_int_distribution<int> dist(0, 15);
    string result;
    for (char c : model) {
        if (c == '%') {
            int const num = dist(engine);
            result += ::hexCharsLC[num];
        } else {
            result += c;
        }
    }
    return result;
}

}  // namespace lsst::qserv::util
