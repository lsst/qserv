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
#ifndef LSST_QSERV_MASTER_DISPATCHER_H
#define LSST_QSERV_MASTER_DISPATCHER_H
/**
  * @file dispatcher.h
  *
  * @brief Main interface to be exported via SWIG for the
  * frontend's Python layer to initiate subqueries and join them.
   *
  * @author Daniel L. Wang, SLAC
  */
#include "lsst/qserv/master/common.h"
#include "lsst/qserv/master/transaction.h" 
#include "lsst/qserv/master/xrdfile.h"
#include "lsst/qserv/master/TableMerger.h"

namespace lsst { namespace qserv { namespace master {
class ChunkSpec; // Forward    

enum QueryState {UNKNOWN, WAITING, DISPATCHED, SUCCESS, ERROR};

void initDispatcher();
// TODO: Eliminate obsolete code.
int submitQuery(int session, int chunk, char* str, int len, char* savePath,
                std::string const& resultName=std::string());
int submitQueryMsg(int session, char* dbName, int chunk,
                   char* str, int len, char* savePath,
                   std::string const& resultName=std::string());
int submitQuery(int session, lsst::qserv::master::TransactionSpec const& s, 
                std::string const& resultName=std::string());
void pauseReadTrans(int session);
void resumeReadTrans(int session);

// Parser model 3:
void setupQuery(int session, 
                std::string const& query,
                std::string const& resultTable); // new model.
std::string const& getSessionError(int session);
lsst::qserv::master::ConstraintVec getConstraints(int session);
std::string const& getDominantDb(int session);

void addChunk(int session, lsst::qserv::master::ChunkSpec const& cs );
void submitQuery3(int session);
// TODO: need pokes into running state for debugging.

QueryState joinQuery(int session, int id);
QueryState tryJoinQuery(int session, int id);
QueryState joinSession(int session);
std::string const& getQueryStateString(QueryState const& qs);
std::string getErrorDesc(int session);
int newSession(std::map<std::string,std::string> const& cfg);
void configureSessionMerger(int session, 
                            lsst::qserv::master::TableMergerConfig const& c);
void configureSessionMerger3(int session);
std::string getSessionResultName(int session);
void discardSession(int session);
lsst::qserv::master::XrdTransResult getQueryResult(int session, int chunk);

}}} // namespace lsst::qserv:master
#endif // LSST_QSERV_MASTER_DISPATCHER_H
