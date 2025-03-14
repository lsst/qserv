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

#ifndef LSST_QSERV_UTIL_XROOTD_H
#define LSST_QSERV_UTIL_XROOTD_H

// xrootd.h : consolidates xrootd/lower-level helper functions (i.e.,
// dealing with xrootd URLs)

// Third-party headers
#include <string>

/// &&& file seems unused, delete if possible

namespace lsst::qserv::util {

std::string makeUrl(char const* hostport, char const* typeStr, int chunk);
std::string makeUrl(char const* hostport, char const* typeStr, std::string const& s, char mode = 0);
std::string makeUrl(char const* hostport, std::string const& path);

}  // namespace lsst::qserv::util

#endif  // LSST_QSERV_UTIL_XROOTD_H
