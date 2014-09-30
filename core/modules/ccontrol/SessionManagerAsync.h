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
// SessionManagerAsync is a type-specialized version of SessionManager
// This interface provides static instance retrieval that was formerly
// buried in dispatcher.cc

#ifndef LSST_QSERV_CCONTROL_SESSIONMANAGERASYNC_H
#define LSST_QSERV_CCONTROL_SESSIONMANAGERASYNC_H

// Third-party headers
#include "boost/shared_ptr.hpp"

// Local headers
#include "ccontrol/SessionManager.h"

namespace lsst {
namespace qserv {
namespace ccontrol {

class AsyncQueryManager;

typedef SessionManager<boost::shared_ptr<AsyncQueryManager> > SessionManagerAsync;
typedef boost::shared_ptr<SessionManagerAsync> SessionManagerAsyncPtr;

SessionManagerAsync& getSessionManagerAsync();
AsyncQueryManager& getAsyncManager(int session);

}}} // namespace lsst::qserv::ccontrol

#endif // LSST_QSERV_CCONTROL_SESSIONMANAGERASYNC_H
