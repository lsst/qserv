// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014 LSST Corporation.
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

#ifndef LSST_QSERV_CCONTROL_USERQUERYPROXY_H
#define LSST_QSERV_CCONTROL_USERQUERYPROXY_H
/**
  * @file
  *
  * @brief Object-less interface to UserQuery objects, for SWIG-exporting
  *
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <string>

// Local headers
#include "ccontrol/QueryState.h"
#include "css/StripingParams.h"
#include "qproc/ChunkSpec.h"
#include "query/Constraint.h"

namespace lsst {
namespace qserv {
namespace ccontrol {
class UserQuery;

// Set UserQueryEnable=1 to enable code that prefers UserQuery framework
// UserQueryEnable=0 to prefer AsyncQueryManager
enum _userQueryProxyConst {UserQueryEnable=1};

/// @return error description
std::string const& UserQuery_getError(int session);

/// @return discovered constraints in the query
lsst::qserv::query::ConstraintVec UserQuery_getConstraints(int session);

/// @return the dominant db for the query
std::string const& UserQuery_getDominantDb(int session);

/// @return number of stripes and substripes
lsst::qserv::css::StripingParams UserQuery_getDbStriping(int session);

/// @return a string describing the progress on the query at a chunk-by-chunk
/// level. Userful for diagnosis when queries are squashed or return errors.
std::string UserQuery_getExecDesc(int session);

/// Add a chunk spec for execution
void UserQuery_addChunk(int session, qproc::ChunkSpec const& cs);

/// Dispatch all chunk queries for this query
void UserQuery_submit(int session);

QueryState UserQuery_join(int session);

/// Kill this user query immediately (system is shutting down now)
void UserQuery_kill(int session);

/// Release resources held for this user query
void UserQuery_discard(int session);

/// @return sessionId
int UserQuery_takeOwnership(UserQuery* uq);

/// @return true if czar/css believes db exists
bool UserQuery_containsDb(int session, std::string const& dbName);

/// For peer python-interface code. Not to be called directly from
/// the Python layer.
UserQuery& UserQuery_get(int session);

}}} // namespace lsst::qserv:ccontrol

#endif // LSST_QSERV_CCONTROL_USERQUERYPROXY_H
