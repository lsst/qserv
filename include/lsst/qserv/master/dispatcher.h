#ifndef LSST_QSERV_MASTER_DISPATCHER_H
#define LSST_QSERV_MASTER_DISPATCHER_H

namespace lsst {
namespace qserv {
namespace master {
    
enum QueryState {UNKNOWN, WAITING, DISPATCHED, SUCCESS, ERROR};

void initDispatcher();
int submitQuery(int session, int chunk, char* str, int len, char* savePath);
QueryState joinChunk(int session, int chunk);
QueryState joinSession(int session);
void discardSession(int session);
XrdTransResult getQueryResult(int session, int chunk);


}}} // namespace lsst::qserv:master
#endif // LSST_QSERV_MASTER_DISPATCHER_H
