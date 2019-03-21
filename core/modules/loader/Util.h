// -*- LSST-C++ -*-
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
 *
 */
#ifndef LSST_QSERV_LOADER_UTIL_H
#define LSST_QSERV_LOADER_UTIL_H

// system headers
#include <functional>
#include <string>
#include <vector>

// Qserv headers



/// Header file for misc things that should probably be added to qserv/util when
/// this code is ready to be merged to master.


namespace lsst {
namespace qserv {
namespace loader {

/// @return - Returns the the hostname for this system, possibly including the entire domain.
/// ** Non-reentrant - This function uses inet_ntoa, which is non-reentrant.
/// @param domains - This indicates how much of the hostname and domain to return in the string.
///          ex:  "iworker-sts-0.iworker-svc.default.svc.cluster.local"
///          domains=0 returns the entire hostname and domain.
///                            "iworker-sts-0.iworker-svc.default.svc.cluster.local"
///          domains=1 returns "iworker-sts-0"
///          domains=2 returns "iworker-sts-0.iworker-svc" (kubernetes needs at least this much)
///          domains=3 returns "iworker-sts-0.iworker-svc.default"
///          ...
///          domains=11 returns "iworker-sts-0.iworker-svc.default.svc.cluster.local"
std::string getOurHostName(unsigned int domains=0);

/// Split a string into a vector of strings based on function func.
/// @return a vector of strings, which will never contain less than 1 string.
/// @param func is expected to be a lambda similar to [](char c) {return c == '.';}
///        which would split the string on '.'/
/// ex: auto out = split("www.github.com", [](char c) {return c == '.';});
///        out contains "www", "github", "com"
std::vector<std::string> split(std::string const& in, std::function<bool(char)> func);

}}} // namespace lsst::qserv::loader

#endif // LSST_QSERV_LOADER_UTIL_H
