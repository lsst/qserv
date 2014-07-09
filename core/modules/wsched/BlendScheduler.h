// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013 LSST Corporation.
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
#ifndef LSST_QSERV_WSCHED_BLENDSCHEDULER_H
#define LSST_QSERV_WSCHED_BLENDSCHEDULER_H

// Local headers
#include "wcontrol/Foreman.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace wlog {
    class WLogger;
}
namespace wsched {
    class GroupScheduler;
    class ScanScheduler;
}}} // End of forward declarations


namespace lsst {
namespace qserv {
namespace wsched {

/// BlendScheduler -- A scheduler that switches between two underlying
/// schedulers based on the incoming task properties. If the incoming
/// task has a scanTables spec in its message, it is scheduled with a
/// ScanScheduler; otherwise it uses the GroupScheduler.
/// The GroupScheduler has concessions for chunk grouping as well, but
/// it should be set for reduced concurrency limited I/O sharing.
class BlendScheduler : public wcontrol::Foreman::Scheduler {
public:
    typedef boost::shared_ptr<BlendScheduler> Ptr;

    BlendScheduler(boost::shared_ptr<wlog::WLogger> logger,
                   boost::shared_ptr<GroupScheduler> group,
                   boost::shared_ptr<ScanScheduler> scan);
    virtual ~BlendScheduler() {}

    virtual void queueTaskAct(wbase::Task::Ptr incoming);
    virtual wbase::TaskQueuePtr nopAct(wbase::TaskQueuePtr running);
    virtual wbase::TaskQueuePtr newTaskAct(wbase::Task::Ptr incoming,
                                              wbase::TaskQueuePtr running);
    virtual wbase::TaskQueuePtr taskFinishAct(wbase::Task::Ptr finished,
                                                 wbase::TaskQueuePtr running);

    // TaskWatcher interface
    virtual void markStarted(wbase::Task::Ptr t);
    virtual void markFinished(wbase::Task::Ptr t);

    static std::string getName()  { return std::string("BlendSched"); }
    bool checkIntegrity();

    wcontrol ::Foreman::Scheduler* lookup(wbase::Task::Ptr p);
private:
    wbase::TaskQueuePtr _getNextIfAvail(wbase::TaskQueuePtr running);
    bool _integrityHelper() const;
    wcontrol::Foreman::Scheduler* _lookup(wbase::Task::Ptr p);

    boost::shared_ptr<GroupScheduler> _group;
    boost::shared_ptr<ScanScheduler> _scan;
    boost::shared_ptr<wlog::WLogger> _logger;
    typedef std::map<wbase::Task*, wcontrol::Foreman::Scheduler*> Map;
    Map _map;
    boost::mutex _mapMutex;
};

}}} // namespace lsst::qserv::wsched

extern lsst::qserv::wsched::BlendScheduler* dbgBlendScheduler; //< A symbol for gdb

#endif // LSST_QSERV_WSCHED_BLENDSCHEDULER_H
