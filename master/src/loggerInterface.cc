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
// loggerInterface.cc houses the implementation of
// loggerInterface.h (SWIG-exported functions for writing to log.)

#include <iostream>
#include "lsst/qserv/Logger.h"
#include "lsst/qserv/master/loggerInterface.h"

namespace qMaster=lsst::qserv::master;

void qMaster::logger_threshold(int severity) {
    lsst::qserv::Logger::Instance()
        .setSeverityThreshold(static_cast<lsst::qserv::Logger::Severity>(severity));
}

void qMaster::logger(int severity, std::string const& s) {
    lsst::qserv::Logger::Instance(static_cast<lsst::qserv::Logger::Severity>(severity)) << "<py> " << s << std::endl;
}
