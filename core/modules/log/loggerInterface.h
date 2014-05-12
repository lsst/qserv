// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012 LSST Corporation.
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
/// loggerInterface.h declares an interface for the logging mechanism for exporting
/// via SWIG to the python layer of Qserv.

#ifndef LSST_QSERV_LOG_LOGGERINTERFACE_H
#define LSST_QSERV_LOG_LOGGERINTERFACE_H

// System headers
#include <string>

namespace lsst {
namespace qserv {
namespace log {

void logger_threshold(int severity);
void logger(int severity, std::string const& s);

}}} // namespace lsst::qserv::log

#endif // LSST_QSERV_LOG_LOGGERINTERFACE_H
