/* 
 * LSST Data Management System
 * Copyright 2008, 2009, 2010 LSST Corporation.
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

/// dispatcher.h - main interface to be exported via SWIG for the
/// frontend's Python layer to initiate subqueries and join them.
 
#ifndef LSST_QSERV_MASTER_DISPATCHER_H
#define LSST_QSERV_MASTER_DISPATCHER_H

#include "lsst/qserv/master/transaction.h" 
#include "lsst/qserv/master/xrdfile.h"
#include "lsst/qserv/master/TableMerger.h"

namespace lsst {
namespace qserv {
namespace master {
    
enum QueryState {UNKNOWN, WAITING, DISPATCHED, SUCCESS, ERROR};

void initDispatcher();
int submitQuery(int session, int chunk, char* str, int len, char* savePath,
		std::string const& resultName=std::string());
int submitQuery(int session, lsst::qserv::master::TransactionSpec const& s, 
		std::string const& resultName=std::string());
QueryState joinQuery(int session, int id);
QueryState tryJoinQuery(int session, int id);
QueryState joinSession(int session);
std::string const& getQueryStateString(QueryState const& qs);
int newSession();
void configureSessionMerger(int session, 
			    lsst::qserv::master::TableMergerConfig const& c);
std::string getSessionResultName(int session);
void discardSession(int session);
lsst::qserv::master::XrdTransResult getQueryResult(int session, int chunk);


}}} // namespace lsst::qserv:master
#endif // LSST_QSERV_MASTER_DISPATCHER_H
