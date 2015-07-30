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
#include "global/debugUtil.h"

// System headers
#include <sstream>
#include <unistd.h>

namespace lsst {
namespace qserv {

std::string
makeByteStreamAnnotated(char const* tag, char const*buf, int bufLen) {
    std::ostringstream os;
    os << tag << "(" << bufLen << ")[";
    for(int i=0; i < bufLen; ++i) {
        os << (int)buf[i] << ",";
    }
    os << "]";
    return os.str();
}

std::string initHostName() {
        char buf[_SC_HOST_NAME_MAX+1];
        buf[_SC_HOST_NAME_MAX] = '\0';
        gethostname(buf, sizeof buf - 1);
        return std::string(buf);
}

std::string const& getHostname() {
    static std::string const cachedHostname = initHostName();
    return cachedHostname;
}

}} // namespace lsst::qserv
