// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013 LSST Corporation.
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
#ifndef LSST_QSERV_WORKER_XRDNAME_H
#define LSST_QSERV_WORKER_XRDNAME_H
#include <cctype>
#include <cstdlib>
#include <string>

namespace lsst {
namespace qserv {
namespace worker {

/// XrdName is a small class that helps extract the name of a running xrootd (or
/// cmsd) instance. It does this by checking an environment variable that is
/// specified to be set during initialization of any xrootd/cmsd process.
class XrdName {
public:
    XrdName() {
        char const * name = std::getenv("XRDNAME");
        _setName(name ? name : "unknown");
    }

    std::string const & getName() const { return _name; }

private:
    void _setName(char const * name) {
        _name.clear();
        // Discard non alpha-numeric characters other than '_'
        for (char const * s = name; *s != '\0'; ++s) {
            if (std::isalnum(*s) || *s == '_') { _name.push_back(*s); }
        }
    }

    std::string _name;
};

}}} // namespace lsst::qserv::worker

#endif // LSST_QSERV_WORKER_XRDNAME_H
