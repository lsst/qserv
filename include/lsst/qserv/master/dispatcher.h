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
