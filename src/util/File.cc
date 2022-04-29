/*
 * LSST Data Management System
 * Copyright 2019 LSST Corporation.
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
#include "util/File.h"

// System headers
#include <fstream>
#include <iostream>
#include <stdexcept>

// Qserv headers
#include "lsst/log/Log.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.util.File");

}  // namespace

namespace lsst { namespace qserv { namespace util {

vector<string> File::getLines(string const& fileName, bool assertNotEmpty) {
    string const context = "File::" + string(__func__) + "  ";

    LOGS(_log, LOG_LVL_DEBUG, context + "fileName='" + fileName + "'");

    if (fileName.empty()) {
        string const err = context + "the file name can't be empty";
        LOGS(_log, LOG_LVL_ERROR, err);
        throw invalid_argument(err);
    }

    vector<string> result;
    string line;
    if (fileName == "-") {
        while (getline(cin, line)) {
            result.push_back(line);
        }
    } else {
        ifstream file(fileName);
        if (not file.good()) {
            string const err = context + "failed to open the file: '" + fileName + "'";
            LOGS(_log, LOG_LVL_ERROR, err);
            throw runtime_error(err);
        }
        while (getline(file, line)) {
            result.push_back(line);
        }
    }
    if (assertNotEmpty and result.empty()) {
        string const err = context + "no lines found in the file: '" + fileName + "'";
        LOGS(_log, LOG_LVL_ERROR, err);
        throw range_error(err);
    }
    return result;
}

}}}  // namespace lsst::qserv::util
