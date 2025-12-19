// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2015 AURA/LSST.
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
#include "util/MultiError.h"

// System headers
#include <algorithm>
#include <iterator>
#include <map>
#include <sstream>

// LSST headers
#include "lsst/log/Log.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.util.MultiError");
}  // namespace

using namespace std;

namespace lsst::qserv::util {

string MultiError::toString() const {
    ostringstream oss;
    oss << *this;
    return oss.str();
}

string MultiError::toOneLineString() const {
    ostringstream oss;
    bool first = true;
    for (auto const& [key, elem] : _errorMap) {
        if (first) {
            oss << elem;
            first = false;
        } else {
            oss << ", " << elem;
        }
    }
    return oss.str();
}

util::Error MultiError::firstError() const {
    auto const iter = _errorMap.begin();
    return (iter == _errorMap.end()) ? Error() : iter->second;
}

bool MultiError::empty() const { return _errorMap.empty(); }

std::vector<Error>::size_type MultiError::size() const { return _errorMap.size(); }

void MultiError::insert(Error const& err) {
    // Error with code == NONE being added to the map indicates a coding
    // error. It will be added to the map, but the
    // problem should be fixed as soon as it is discovered. Throwing an
    // exception is likely overkill. Not adding it could hide useful information
    // as the message could still valuable.
    if (err.getCode() == Error::NONE) {
        LOGS(_log, LOG_LVL_WARN, "MultiError::insert adding error with code=NONE " << err);
    }
    auto const key = make_pair(err.getCode(), err.getSubCode());
    auto iter = _errorMap.find(key);
    if (iter == _errorMap.end()) {
        _errorMap[key] = err;
    } else {
        iter->second.incrCount();
    }
}

void MultiError::merge(MultiError const& other) {
    for (auto const& [key, err] : other._errorMap) {
        auto iter = _errorMap.find(key);
        if (iter != _errorMap.end()) {
            // Entry already exists, increase the count
            iter->second.incrCount(err.getCount());
        } else {
            _errorMap[key] = err;
        }
    }
}

std::vector<Error> MultiError::getVector() const {
    std::vector<Error> errVect;
    for (auto const& [key, elem] : _errorMap) {
        errVect.push_back(elem);
    }
    return errVect;
}

std::ostream& operator<<(std::ostream& out, MultiError const& multiError) {
    // This string is meant to be provided to end users on a failure, so
    // there is an attempt made to reduce extraneous information.

    // To get numerous '[0]' entries in the output under control...
    // Put all errors in a map, and count how many times each occurs.
    bool firstLoop = true;
    for (auto const& [key, err] : multiError._errorMap) {
        if (firstLoop) {
            firstLoop = false;
        } else {
            out << "\n";
        }
        out << err;

        // Limit this to about 10,000 characters, as that's more than will
        // likely be useful to end users.
        if (out.tellp() > 10'000) break;
    }

    return out;
}

}  // namespace lsst::qserv::util
