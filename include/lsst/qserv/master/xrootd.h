// -*- LSST-C++ -*-
#ifndef LSST_QSERV_MASTER_XROOTD_H
#define LSST_QSERV_MASTER_XROOTD_H


// xrootd.h : consolidates xrootd/lower-level helper functions.

namespace lsst {
namespace qserv {
namespace master {

std::string makeUrl(char const* typeStr, int chunk);
std::string makeUrl(char const* hostport, char const* typeStr, 
		    std::string const& s);
std::string hashQuery(char const* buffer, int bufferSize);

}}}

#endif // LSST_QSERV_MASTER_XROOTD_H
 
