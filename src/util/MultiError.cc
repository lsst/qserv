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

using namespace std;

namespace lsst::qserv::util {

string MultiError::toString() const {
    ostringstream oss;
    oss << *this;
    return oss.str();
}

string MultiError::toOneLineString() const {
    ostringstream oss;
    if (!empty()) {
        if (size() > 1) {
            std::ostream_iterator<Error> string_it(oss, ", ");
            std::copy(_errorVector.begin(), _errorVector.end() - 1, string_it);
        }
        oss << _errorVector.back();
    }
    return oss.str();
}

int MultiError::firstErrorCode() const { return empty() ? ErrorCode::NONE : _errorVector.front().getCode(); }

string MultiError::firstErrorStr() const {
    ostringstream os;
    if (!empty()) {
        os << _errorVector.front();
    }
    return os.str();
}

util::Error MultiError::firstError() const {
    Error err;
    if (!empty()) {
        err = _errorVector.front();
    }
    return err;
}

bool MultiError::empty() const { return _errorVector.empty(); }

std::vector<Error>::size_type MultiError::size() const { return _errorVector.size(); }

void MultiError::push_back(const std::vector<Error>::value_type& val) { _errorVector.push_back(val); }

std::ostream& operator<<(std::ostream& out, MultiError const& multiError) {
    // This string is meant to be provided to end users on a failure, so
    // there is an attempt made to reduce extraneous information.

    // To get numerous '[0]' entries in the output under control...
    // Put all errors in a map, and count how many times each occurs.
    std::map<string, int> errMap;
    for (auto const& err : multiError._errorVector) {
        stringstream sstrm;
        sstrm << err;
        string errStr = sstrm.str();
        auto iter = errMap.find(errStr);
        if (iter == errMap.end()) {
            errMap[errStr] = 1;
        } else {
            iter->second += 1;
        }
    }

    // Write the map to `out`
    bool firstLoop = true;
    for (auto const& elem : errMap) {
        int count = elem.second;
        if (firstLoop) {
            firstLoop = false;
        } else {
            out << "\n";
        }
        out << elem.first;
        if (count > 1) {
            out << "   (Occurences = " << count << ")";
        }

        // Limit this to about 10,000 characters, as that's more than will
        // likely be useful to end users.
        if (out.tellp() > 10'000) break;
    }
    return out;
}

}  // namespace lsst::qserv::util
