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

#ifndef LSST_QSERV_WLOG_XROOTDAPPENDER_H
#define LSST_QSERV_WLOG_XROOTDAPPENDER_H

// Base class header
#include "log4cxx/appenderskeleton.h"

// Third-party headers
#include "log4cxx/helpers/object.h"

namespace lsst {
namespace qserv {
namespace wlog {

// This needs to be here for all LOG4CXX macros to work
using namespace log4cxx;

/**
 *  This class defines special log4cxx appender which "appends" log messages
 *  to xrootd logging system using \c XrdSsiLogger facility. To use this logger
 *  one has to explicitly add it to configuration using \c XrootdAppender as
 *  appender class name, for example:
 *  \code
 *  log4j.rootLogger = INFO, XrdLog
 *  log4j.appender.XrdLog = org.apache.log4j.XrootdAppender
 *  log4j.appender.XrdLog.layout = org.apache.log4j.PatternLayout
 *  log4j.appender.XrdLog.layout.ConversionPattern = %d{MMdd HH:mm:ss.SSS} [%t] %-5p %c{2} (%F:%L) - %m%n
 *  \endcode
 *
 *  XrootdAppender simply forwards all messages to \c XrdSsiLogger which is
 *  only guaranteed to work after xrootd has been configured. This implies that
 *  this appender cannot be used outside xrootd plugins, it will likely crash
 *  if used in a regular application.
 */
class XrootdAppender : public AppenderSkeleton {
public:

    DECLARE_LOG4CXX_OBJECT(XrootdAppender)
    BEGIN_LOG4CXX_CAST_MAP()
            LOG4CXX_CAST_ENTRY(XrootdAppender)
            LOG4CXX_CAST_ENTRY_CHAIN(AppenderSkeleton)
    END_LOG4CXX_CAST_MAP()

    // Make an instance
    XrootdAppender();

    /**
     *  Formats the message (if layout has been defined for this appender)
     *  and sends resulting string to  \c XrdSsiLogger.
     *  The format of the output will depend on this appender's layout.
     */
    virtual void append(const spi::LoggingEventPtr& event, log4cxx::helpers::Pool& p);

    /**
     * Close this appender instance, this is no-op.
     */
    virtual void close();

    /**
     *  Returns true if appender requires layout to be defined for it.
     *  Always true for this appender.
     */
    virtual bool requiresLayout() const;

private:

    // we do not support copying
    XrootdAppender(const XrootdAppender&);
    XrootdAppender& operator=(const XrootdAppender&);
};

}}} // namespace lsst::qserv::wlog

#endif // LSST_QSERV_WLOG_XROOTDAPPENDER_H
