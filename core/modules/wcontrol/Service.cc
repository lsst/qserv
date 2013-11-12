/*
 * LSST Data Management System
 * Copyright 2010-2013 LSST Corporation.
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
#include "wcontrol/Service.h"
#include "wcontrol/Foreman.h"
#include "wsched/FifoScheduler.h"
#include "wsched/ScanScheduler.h"
#include "wsched/GroupScheduler.h"
#include "wsched/BlendScheduler.h"
#include "wlog/WLogger.h"

namespace lsst {
namespace qserv {
namespace worker {

Service::Service(WLogger::Ptr log) {
    if(!log.get()) {
        log.reset(new WLogger());
    }
    WLogger::Ptr schedLog(new WLogger(log));
#if 0 // Shared scan only
    schedLog->setPrefix(ScanScheduler::getName() + ":");
    ScanScheduler::Ptr sch(new ScanScheduler(schedLog));
#elif 0 // Group scan only (interactive)
    schedLog->setPrefix(GroupScheduler::getName() + ":");
    GroupScheduler::Ptr sch(new GroupScheduler(schedLog));
#else // Blend scheduler
    WLogger::Ptr gLog(new WLogger(log));
    gLog->setPrefix(GroupScheduler::getName() + ":");
    GroupScheduler::Ptr gro(new GroupScheduler(gLog));

    WLogger::Ptr sLog(new WLogger(log));
    sLog->setPrefix(ScanScheduler::getName() + ":");
    ScanScheduler::Ptr sca(new ScanScheduler(sLog));

    schedLog->setPrefix(BlendScheduler::getName() + ":");
    BlendScheduler::Ptr sch(new BlendScheduler(schedLog, gro, sca));
#endif
    _foreman = newForeman(sch, log);
}

TaskAcceptor::Ptr Service::getAcceptor() {
    return _foreman;
}

void Service::squashByHash(std::string const& hash) {
    _foreman->squashByHash(hash);
}

}}} // lsst::qserv::worker
