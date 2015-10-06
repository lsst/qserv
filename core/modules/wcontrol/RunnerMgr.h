// -*- LSST-C++ -*-

#ifndef LSST_QSERV_WCONTROL_RUNNERMGR_H
#define LSST_QSERV_WCONTROL_RUNNERMGR_H

// System headers
#include <atomic>
#include <memory>

// LSST headers
#include "lsst/log/Log.h"

// Local headers
#include "wcontrol/Foreman.h"

namespace lsst {
namespace qserv {
namespace wdb {
	class QueryRunner;
}}}

namespace lsst {
namespace qserv {
namespace wcontrol {
/** Run an event driven thread that is fed by the Foreman's Scheduler.
 */
class Runner : public std::enable_shared_from_this<Runner> {
public:
	using Ptr = std::shared_ptr<Runner>;
    Runner(RunnerMgr& rm, wbase::Task::Ptr firstTask);
    Runner(Runner&) = delete;
    Runner operator=(Runner&) = delete;
    virtual ~Runner();
    void operator()();
    std::string const& getHash() const { return _task->hash; }

private:
    RunnerMgr& _rm;
    wbase::Task::Ptr _task;
    std::atomic<bool> _poisoned{false}; // TODO:remove in DM-3945
};

/** Keep track of the existing Runner objects for the Foreman.
 */
class RunnerMgr {
public:
    RunnerMgr(Foreman& f);
    RunnerMgr(RunnerMgr&) = delete;
    RunnerMgr operator=(RunnerMgr&) = delete;
    void registerRunner(Runner::Ptr const& r, wbase::Task::Ptr t);
    std::shared_ptr<wdb::QueryRunner> newQueryAction(wbase::Task::Ptr t);
    void reportComplete(wbase::Task::Ptr t);
    void reportStart(wbase::Task::Ptr t);
    void signalDeath(Runner::Ptr const& r);
    wbase::Task::Ptr getNextTask(Runner::Ptr const& r, wbase::Task::Ptr previous);
    LOG_LOGGER getLog();
    wbase::TaskQueuePtr queueTask(wbase::Task::Ptr const& task, Scheduler::Ptr const& scheduler);

private:
    void _reportStartHelper(wbase::Task::Ptr t);
    class StartTaskF;
    Foreman& _f;
    TaskWatcher& _taskWatcher;

    std::mutex _runnersMutex; ///< Protects _runners and _running
    std::deque<Runner::Ptr> _runners;
    wbase::TaskQueuePtr _running {std::make_shared<wbase::TaskQueue>()};
};

}}} // namespace

#endif
