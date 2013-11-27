// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2009-2013 LSST Corporation.
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
#ifndef LSST_QSERV_CONTROL_DISPATCHER_H
#define LSST_QSERV_CONTROL_DISPATCHER_H
/**
  * @file dispatcher.h
  *
  * @brief Main interface to be exported via SWIG for the
  * frontend's Python layer to initiate subqueries and join them.
   *
  * @author Daniel L. Wang, SLAC
  */

#include "control/transaction.h"
#include "css/StripingParams.h"
#include "xrdc/xrdfile.h"
#include "merger/TableMerger.h"
#include "query/Constraint.h"
#include "util/common.h"
#include "xrdc/xrdfile.h"


namespace lsst {
namespace qserv {

namespace qproc {
    // Forward
    class ChunkSpec;
} // namespace qproc
    
namespace control {

enum QueryState {UNKNOWN, WAITING, DISPATCHED, SUCCESS, ERROR};

int submitQuery(int session, control::TransactionSpec const& s,
                std::string const& resultName=std::string());

// Parser model 3:
/// Setup a query for execution.
void setupQuery(int session,
                std::string const& query,
                std::string const& resultTable);
/// @return error description
std::string const& getSessionError(int session);
/// @return discovered constraints in the query
lsst::qserv::query::ConstraintVec getConstraints(int session);
/// @return the dominant db for the query
std::string const& getDominantDb(int session);
/// @return true if CSS contains given db
bool containsDb(int session, std::string const& dbName);
/// @return number of stripes and substripes
lsst::qserv::css::StripingParams getDbStriping(int session);
/// Add a chunk spec for execution
void addChunk(int session, qproc::ChunkSpec const& cs );
/// Dispatch all chunk queries for this query
void submitQuery3(int session);
// TODO: need pokes into running state for debugging.

QueryState joinSession(int session);
std::string const& getQueryStateString(QueryState const& qs);
std::string getErrorDesc(int session);
int newSession(std::map<std::string,std::string> const& cfg);
void configureSessionMerger(int session,
                            lsst::qserv::merger::TableMergerConfig const& c);
void configureSessionMerger3(int session);
std::string getSessionResultName(int session);
void discardSession(int session);

}}} // namespace lsst::qserv:control

#endif // LSST_QSERV_CONTROL_DISPATCHER_H
