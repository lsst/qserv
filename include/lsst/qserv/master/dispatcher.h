#ifndef LSST_QSERV_MASTER_DISPATCHER_H
#define LSST_QSERV_MASTER_DISPATCHER_H

#include "lsst/qserv/master/transaction.h" 
#include "lsst/qserv/master/xrdfile.h"

namespace lsst {
namespace qserv {
namespace master {
    
enum QueryState {UNKNOWN, WAITING, DISPATCHED, SUCCESS, ERROR};

void initDispatcher();
int submitQuery(int session, int chunk, char* str, int len, char* savePath);
int submitQuery(int session, lsst::qserv::master::TransactionSpec const& s);
QueryState joinQuery(int session, int id);
QueryState tryJoinQuery(int session, int id);
QueryState joinSession(int session);
std::string const& getQueryStateString(QueryState const& qs);
int newSession();
void discardSession(int session);
lsst::qserv::master::XrdTransResult getQueryResult(int session, int chunk);


}}} // namespace lsst::qserv:master
#endif // LSST_QSERV_MASTER_DISPATCHER_H
