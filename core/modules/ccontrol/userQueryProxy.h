// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2015 LSST Corporation.
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


/// @return error description
std::string UserQuery_getQueryProcessingError(int session);

/// Dispatch all chunk queries for this query
void UserQuery_submit(int session);

QueryState UserQuery_join(int session);

/// Kill this user query immediately (system is shutting down now)
void UserQuery_kill(int session);

/// Release resources held for this user query
void UserQuery_discard(int session);

/// @return sessionId
int UserQuery_takeOwnership(UserQuery* uq);

/// For peer python-interface code. Not to be called directly from
/// the Python layer.
UserQuery& UserQuery_get(int session);

}}} // namespace lsst::qserv:ccontrol

#endif // LSST_QSERV_CCONTROL_USERQUERYPROXY_H
