// -*- LSST-C++ -*-

/* 
 * LSST Data Management System
 * Copyright 2008, 2009, 2010 LSST Corporation.
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
 
#include <openssl/md5.h>
#include "boost/format.hpp"
#include "lsst/qserv/master/xrootd.h"

namespace qMaster = lsst::qserv::master;

std::string qMaster::makeUrl(char const* typeStr, int chunk) {
    std::string s = (boost::format("%d") % chunk).str();
    return makeUrl(NULL, typeStr, s);
}

std::string qMaster::makeUrl(char const* hostport, 
			     char const* typeStr, std::string const& s) {

    // typeStr is either "query" or "result"
    if(hostport == NULL) {
	hostport = ::getenv("QSERV_XRD");
	if(hostport == NULL) {
	    hostport = "lsst-dev01:1094";
	}
    }
    char* user = "qsmaster";
    boost::format f("xroot://%s@%s//%s/%s");
    return (f % user % hostport % typeStr % s).str();
}

// hashQuery
// a query hasher.  
// This must match the version in lsst/qserv/worker/src/MySqlFsFile.cc
std::string qMaster::hashQuery(char const* buffer, int bufferSize) {
    unsigned char hashVal[MD5_DIGEST_LENGTH];
    MD5(reinterpret_cast<unsigned char const*>(buffer), bufferSize, hashVal);
    std::string result;
    for (int i = 0; i < MD5_DIGEST_LENGTH; ++i) {
        result += (boost::format("%02x") % static_cast<int>(hashVal[i])).str();
    }
    return result;
}
