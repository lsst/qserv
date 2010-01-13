#include "lsst/qserv/worker/Thread.h"

namespace qWorker = lsst::qserv::worker;

XrdSysMutex qWorker::ThreadManager::_detailMutex;
qWorker::ThreadManager::DetailSet qWorker::ThreadManager::_details;
