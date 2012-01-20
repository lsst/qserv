#include "lsst/qserv/worker/Foreman.h"

// Std C++
#include <deque>

// Boost
#include <boost/thread.hpp>
#include <boost/make_shared.hpp>

// Pkg: lsst::qserv::worker
#include "lsst/qserv/worker/TodoList.h"
#include "lsst/qserv/worker/FifoScheduler.h"

namespace qWorker = lsst::qserv::worker;

// ForemanImpl
class ForemanImpl : public lsst::qserv::worker::Foreman {
public:
    ForemanImpl(Scheduler::Ptr s);
    virtual ~ForemanImpl() {}
    
    void add(boost::shared_ptr<qWorker::Task> c) {
        // FIXME
    }

    //boost::shared_ptr<Callable> getNextCallable();
    // For use by runners.
    class Runner;
    // void registerRunner(Runner* r);
    // void signalDeath(Runner* r);
    // bool isPoison(Callable const* const c) { 
    //     return (Callable const* const)0 == c; 
    // }
    // void _addRunner();

    //    typedef std::deque<boost::shared_ptr<Callable> > WorkDeque;
    typedef std::deque<Runner*> RunnerDeque;
    boost::mutex _mutex;
    boost::mutex _runnersMutex;
    boost::condition_variable _queueNonEmpty;
    boost::condition_variable _runnersEmpty;
    boost::condition_variable _runnerRegistered;
    //    WorkDeque _queue;
    RunnerDeque _runners;
};

// Foreman factory function
qWorker::Foreman::Ptr newForeman() {
    qWorker::FifoScheduler::Ptr fsch(new qWorker::FifoScheduler());
    ForemanImpl::Ptr fmi(new ForemanImpl(fsch));
    return fmi;;
}

////////////////////////////////////////////////////////////////////////
// ForemanImpl
////////////////////////////////////////////////////////////////////////
ForemanImpl::ForemanImpl(Scheduler::Ptr s) {
    
}
