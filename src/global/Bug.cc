// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2016 AURA/LSST.
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


// Class header
#include "global/Bug.h"

// LSST headers
#include "lsst/log/Log.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv");

}


namespace lsst {
namespace qserv {

Bug::Bug(char const* msg) : std::logic_error(msg) {
    // Log the error immediately so it appears in the thread that is throwing.
    LOGS(_log, LOG_LVL_ERROR, "Bug::Bug:" << msg);
}


Bug::Bug(std::string const& msg) : std::logic_error(msg) {
    // Log the error immediately so it appears in the thread that is throwing.
    LOGS(_log, LOG_LVL_ERROR, "Bug::Bug:" << msg);
}

}} // namespace lsst::qserv
