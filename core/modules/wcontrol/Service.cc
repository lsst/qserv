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
namespace wcontrol {

Service::Service(wlog::WLogger::Ptr log) {
    if(!log.get()) {
        log.reset(new wlog::WLogger());
    }
    wlog::WLogger::Ptr schedLog(new wlog::WLogger(log));
#if 0 // Shared scan only
    schedLog->setPrefix(ScanScheduler::getName() + ":");
    ScanScheduler::Ptr sch(new ScanScheduler(schedLog));
#elif 0 // Group scan only (interactive)
    schedLog->setPrefix(wsched::GroupScheduler::getName() + ":");
    wsched::GroupScheduler::Ptr sch(new wsched::GroupScheduler(schedLog));
#else // Blend scheduler
    wlog::WLogger::Ptr gLog(new wlog::WLogger(log));
    gLog->setPrefix(wsched::GroupScheduler::getName() + ":");
    wsched::GroupScheduler::Ptr gro(new wsched::GroupScheduler(gLog));

    wlog::WLogger::Ptr sLog(new wlog::WLogger(log));
    sLog->setPrefix(wsched::ScanScheduler::getName() + ":");
    wsched::ScanScheduler::Ptr sca(new wsched::ScanScheduler(sLog));

    schedLog->setPrefix(wsched::BlendScheduler::getName() + ":");
    wsched::BlendScheduler::Ptr sch(new wsched::BlendScheduler(schedLog, gro, sca));
#endif
    _foreman = newForeman(sch, log);
}

wbase::TaskAcceptor::Ptr
Service::getAcceptor() {
    return _foreman;
}

void Service::squashByHash(std::string const& hash) {
    _foreman->squashByHash(hash);
}

}}} // namespace lsst::qserv::wcontrol
