// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2016 AURA/LSST.
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
// Class header
#include "wsched/SchedulerBase.h"

// System headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "wsched/BlendScheduler.h"


namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.wsched.ScanScheduler");
}

namespace lsst {
namespace qserv {
namespace wsched {

/// Set priority to use when starting next chunk.
void SchedulerBase::setPriority(int priority) {
    _priorityNext = priority;
    // apply immediately if this is effectively a better priority.
    if (_priorityNext < _priority) applyPriority();
}


/// Apply _priorityNext to this scheduler.
void SchedulerBase::applyPriority() {
    if (_priority != _priorityNext) {
        LOGS(_log, LOG_LVL_DEBUG,
            getName() << " applying priority old=" << _priority << " new=" << _priorityNext);
        _priority = _priorityNext;
        if (_blendScheduler != nullptr) _blendScheduler->setFlagReorderScans();
    }
}


/// Return to default priority for next chunk.
void SchedulerBase::setPriorityDefault() {
    _priorityNext = _priorityDefault;
}


}}} // namespace lsst::qserv::wsched
