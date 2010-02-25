// -*- LSST-C++ -*-
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
