// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2010-2014 LSST Corporation.
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

// Third-party headers
#include "boost/make_shared.hpp"

// Local headers
#include "wcontrol/Foreman.h"
#include "wsched/BlendScheduler.h"
#include "wsched/FifoScheduler.h"
#include "wsched/GroupScheduler.h"
#include "wsched/ScanScheduler.h"

namespace lsst {
namespace qserv {
namespace wcontrol {

Service::Service() {
#if 0 // Shared scan only
    ScanScheduler::Ptr schrc = boost::make_shared<ScanScheduler>();
#elif 0 // Group scan only (interactive)
    wsched::GroupScheduler::Ptr sch =
            boost::make_shared<wsched::GroupScheduler>();
#else // Blend scheduler
    wsched::GroupScheduler::Ptr gro =
            boost::make_shared<wsched::GroupScheduler>();

    wsched::ScanScheduler::Ptr sca =
            boost::make_shared<wsched::ScanScheduler>();

    wsched::BlendScheduler::Ptr sch =
            boost::make_shared<wsched::BlendScheduler>(gro, sca);
#endif
    _foreman = newForeman(sch);
}

wbase::TaskAcceptor::Ptr
Service::getAcceptor() {
    return _foreman;
}

boost::shared_ptr<wbase::MsgProcessor>
Service::getProcessor() {
    // Make a task processor that returns results in a channel rather than
    // a separate file.
    return _foreman->getProcessor();
}

void Service::squashByHash(std::string const& hash) {
    _foreman->squashByHash(hash);
}

}}} // namespace lsst::qserv::wcontrol
