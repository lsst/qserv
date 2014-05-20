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
// Logger.cc houses the implementation of class Logger. Logger is a
// simple logging facility that abstracts log messages away from the
// xrootd system log so that qserv worker code can be debugged outside
// of a running xrootd instance. It also provides levels of logging
// priority as a mechanism for reducing logging clutter.

#include "wlog/WLogger.h"

// System headers
#include <iostream>
#include <sstream>

namespace lsst {
namespace qserv {
namespace wlog {

void WLogger::message(WLogger::LogLevel logLevel, char const* s) {
    if(logLevel <= _logLevel) { // Lower is higher priority
        std::string o1(_prefix + s);
        if(_backend) {
            _backend->message(logLevel, o1.c_str());
        } else if(_printer) {
            (*_printer)(s);
        } else {
            std::cerr << o1 << std::endl;
        }
    }
}

void WLogger::_init() {
}

}}} // namespace lsst::qserv::wlog
