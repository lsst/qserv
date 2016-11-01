// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2016 AURA/LSST.
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
/// Implement logging hook to route xrootd/cmsd messages to our logger.
/// This source file has no header file.

// Class header

// System headers
#include <string>

// Third party headers
#include "XrdSsi/XrdSsiLogger.hh"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers

/******************************************************************************/
/*                L o g g i n g   I n t e r c e p t   H o o k                 */
/******************************************************************************/
  
namespace {

const char* origin;

void QservLogger(struct timeval const& mtime,
                 unsigned long         tID,
                 const char*           msg,
                 int                   mlen) {

    static log4cxx::spi::LocationInfo xrdLoc(origin, "<xrdssi>", 0);
    static LOG_LOGGER myLog = LOG_GET("lsst.qserv.xrdssi.msgs");

    if (myLog.isInfoEnabled()) {
        std::string theMsg(msg, mlen);
        lsst::log::Log::MDC("LWP", std::to_string(tID));
        myLog.logMsg(log4cxx::Level::getInfo(), xrdLoc, theMsg);
    }
}

XrdSsiLogger::MCB_t& ConfigLog() {
    // Set the originator of the messages
    origin = (getenv("XRDPROG") ? getenv("XRDPROG") : "<SSI>");

    // Configure the logging system
    LOG_CONFIG();

    // Return the address the logger to be used
    return QservLogger;
}

bool dummy = XrdSsiLogger::SetMCB(ConfigLog(), XrdSsiLogger::mcbServer);
}
