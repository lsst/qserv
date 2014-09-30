// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014 AURA/LSST.
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

#include "wlog/XrootdAppender.h"

// Third-party headers
#include "XrdSsi/XrdSsiLogger.hh"



namespace XrdSsi {
    extern XrdSsiLogger SsiLogger;
}

// macro below dows not work without this using directive
// (and it breaks if placed inside namespaces)
using lsst::qserv::wlog::XrootdAppender;
IMPLEMENT_LOG4CXX_OBJECT(XrootdAppender)

namespace lsst {
namespace qserv {
namespace wlog {

XrootdAppender::XrootdAppender()
{
}

void XrootdAppender::append(const spi::LoggingEventPtr& event, log4cxx::helpers::Pool& p)
{
    if (layout) {
        LogString msg;
        layout->format(msg, event, p);
        // get rid of trailing new-line, xrootd logger adds it
        if (not msg.empty() and msg[msg.size()-1] == '\n') {
            msg[msg.size()-1] = '\0';
        }
        XrdSsi::SsiLogger.Msg(0, msg.c_str());
    }
}

void XrootdAppender::close()
{
}

bool XrootdAppender::requiresLayout() const
{
    return true;
}

}}} // namespace lsst::qserv::wlog
