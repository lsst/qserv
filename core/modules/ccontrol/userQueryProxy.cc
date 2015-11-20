// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2015 AURA/LSST.
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
/**
  * @file
  *
  * Basic usage:
  *
  * Construct a UserQueryFactory, then create a new UserQuery
  * object. You will get a session ID that will identify the UserQuery
  * for use with this proxy. The query is parsed and prepared for
  * execution as much as possible, without knowing partition coverage.
  *
  *
  * UserQuery_getQueryProcessingError(int session) // See if there are errors
  *
  * UserQuery_getConstraints(int session)  // Retrieve the detected
  * constraints so that we can apply them to see which chunks we
  * need. (done in Python)
  *
  * UserQuery_submit(int session) // Trigger the dispatch of all chunk
  * queries for the UserQuery
  *
  * @author Daniel L. Wang, SLAC
  */

// Class header
#include "ccontrol/userQueryProxy.h"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "ccontrol/MissingUserQuery.h"
#include "ccontrol/SessionManager.h"
#include "ccontrol/UserQuery.h"
#include "util/StringHash.h"

namespace {
} // anonymous namespace


namespace lsst {
namespace qserv {
namespace ccontrol {

class UserQueryManager : public SessionManager<UserQuery::Ptr> {
public:
    UserQueryManager() {}
    UserQuery::Ptr get(int id) {
        UserQuery::Ptr p = getSession(id);
        if(!p) {
            throw MissingUserQuery(id);
        }
        return p;
    }
};
static UserQueryManager uqManager;

std::string UserQuery_getQueryProcessingError(int session) {
    std::string s;
    try {
        s = uqManager.get(session)->getError();
    } catch (std::exception& e) {
        s = e.what();
        LOGF_WARN(s);
    }
    return s;
}

/// Abort a running query
void UserQuery_kill(int session) {
    LOGF_INFO("EXECUTING UserQuery_kill(%1%)" % session);
    try {
        uqManager.get(session)->kill();
    } catch (const std::exception& e) {
        LOGF_WARN(e.what());
    }
}

/// Dispatch all chunk queries for this query
void UserQuery_submit(int session) {
    LOGF_DEBUG("EXECUTING UserQuery_submit(%1%)" % session);
    try {
        uqManager.get(session)->submit();
    } catch (const std::exception& e) {
        LOGF_ERROR(e.what());
    }
}

/// Block until execution succeeds or fails completely
QueryState UserQuery_join(int session) {
    LOGF_DEBUG("EXECUTING UserQuery_join(%1%)" % session);
    try {
        return uqManager.get(session)->join();
    } catch (const std::exception& e) {
        LOGF_ERROR(e.what());
        return QueryState::ERROR;
    }
}

/// Discard the UserQuery by destroying it and forgetting about its id.
void UserQuery_discard(int session) {
    try {
        UserQuery::Ptr p = uqManager.get(session);
        p->discard();
        p.reset();
        uqManager.discardSession(session);
    } catch (const std::exception& e) {
        LOGF_ERROR(e.what());
    }
}

/// Take ownership of a UserQuery object and return a sessionId
int UserQuery_takeOwnership(UserQuery* uq) {
    UserQuery::Ptr uqp(uq);
    return uqManager.newSession(uqp);
}

UserQuery& UserQuery_get(int session) {
    return *uqManager.get(session);
}

}}} // namespace lsst::qserv::ccontrol
