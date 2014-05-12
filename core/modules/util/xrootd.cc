// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2009-2014 LSST Corporation.
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
// xrootd.h -- Helper funcitons for xrootd-based dispatch

// System headers
#include <sstream>

// Third-party headers
#include "boost/format.hpp"

// Local headers
#include "util/xrootd.h"

namespace lsst {
namespace qserv {
namespace util {

std::string
makeUrl(char const* hostport, char const* typeStr, int chunk) {
    std::stringstream s;
    s << chunk;
    // boost::format version is 5x slower.
    //std::string s = (boost::format("%d") % chunk).str();
    return makeUrl(hostport, typeStr, s.str());
}

std::string
makeUrl(char const* hostport, std::string const& path) {
    return makeUrl(hostport, NULL, path);
}

std::string
makeUrl(char const* hostport, char const* typeStr,
        std::string const& s, char mode) {
    // typeStr is either "query" or "result"
    if(hostport == NULL) {
        hostport = ::getenv("QSERV_XRD");
        if(hostport == NULL) {
            hostport = "lsst-dev01:1094";
        }
    }
#if 0
    char* user = "qsmaster";
    boost::format f("xroot://%s@%s//%s/%s");
    return (f % user % hostport % typeStr % s).str();
#else
    // This is ~8.5x faster than the boost::format version.
    std::string pfx = "xroot://";
    std::string user("qsmaster");
    std::string tstr;
    std::string ret;
    if(typeStr != NULL) tstr = typeStr;

    if(mode != '\0') {
        user += ".";
        user += mode;
    }
    ret.reserve(pfx.size() + user.size() + 1 + 2 + 1
                + tstr.size() + s.size());
    ret += pfx;
    ret += user;
    ret += "@";
    ret += hostport;
    ret += "/";
    if(typeStr != NULL) {
        ret += "/";
        ret += typeStr;
        ret += "/";
    } // else: assume s contains leading "/"
    ret += s;
    return ret;
#endif
}

}}} // namespace lsst::qserv::util
