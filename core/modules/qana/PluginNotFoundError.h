// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2014 LSST Corporation.
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

#ifndef LSST_QSERV_QANA_PLUGINNOTFOUNDERROR_H
#define LSST_QSERV_QANA_PLUGINNOTFOUNDERROR_H
/**
  * @file
  *
  * @brief
  *
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <exception>
#include <sstream>
#include <string>

namespace lsst {
namespace qserv {
namespace qana {

/// PluginNotFoundError is an exception class thrown when a plugin is requested
/// by a name that has not been registered.
class PluginNotFoundError: public std::exception {
public:
    explicit PluginNotFoundError(std::string const& name) {
        std::stringstream ss;
        ss << "PluginNotFoundError '" << name << "' requested but not found.";
        _descr = ss.str();
    }
    virtual ~PluginNotFoundError() throw() {}

    virtual const char* what() const throw() {
        return _descr.c_str();
    }
private:
    std::string _descr;
};

}}} // namespace lsst::qserv::qana

#endif // LSST_QSERV_QANA_PLUGINNOTFOUNDERROR_H

