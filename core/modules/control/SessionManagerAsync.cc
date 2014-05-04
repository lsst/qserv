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
// SessionManagerAsync.cc houses the static instance of the
// AsyncQueryManager-type of SessionManager.

#include "control/SessionManagerAsync.h"

// Third-party headers
#include <boost/make_shared.hpp>

namespace lsst {
namespace qserv {
namespace control {
            
SessionManagerAsync&
getSessionManagerAsync() {

    // Singleton for now.
    static SessionManagerAsyncPtr sm;
    if(sm.get() == NULL) {
        sm = boost::make_shared<SessionManagerAsync>();
    }
    assert(sm.get() != NULL);
    return *sm;
}

AsyncQueryManager&
getAsyncManager(int session) {
    return *(getSessionManagerAsync().getSession(session));
}

}}} // namespace lsst::qserv::control
