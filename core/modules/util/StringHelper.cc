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
#include "StringHelper.h"

// System headers
#include <sstream>

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.util.StringToVector");

}

namespace lsst {
namespace qserv {
namespace util {


vector<string> StringHelper::splitString(string const& original, string const& separator) {
    vector<string> result;
    string str(original);
    size_t pos;
    bool loop = true;
    while (loop) {
        pos = str.find(separator);
        if (pos == string::npos) {
            loop = false;
        }
        result.push_back(str.substr(0, pos));
        str = str.substr(pos + separator.length());
    }
    return result;
}


vector<int> StringHelper::getIntVectFromStr(string const& str, string const& separator, bool throwOnError, int defaultVal) {
    auto vectString = splitString(str, separator);
    vector<int> result;
    for (auto iStr:vectString) {
        try {
            size_t sz = 0;
            int val = stoi(iStr, &sz);
            if (sz != iStr.length()) {
                LOGS(_log, LOG_LVL_WARN, "unused characters when converting " << iStr << " to " << val);
            }
            result.push_back(val);
        } catch (invalid_argument const& e) {
            string msg("getIntsFromString invalid argument in str=" + str + " iStr=" + iStr);
            LOGS(_log, LOG_LVL_ERROR, msg);
            if (throwOnError) {
                throw invalid_argument(msg);
            } else {
                result.push_back(defaultVal);
            }
        }
    }
    return result;
}


}}} // namespace lsst::qserv::util

