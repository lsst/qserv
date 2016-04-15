// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2008-2015 AURA/LSST.
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
#include "wconfig/WorkerConfig.h"

// System headers
#include <sstream>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "mysql/MySqlConfig.h"
#include "wsched/BlendScheduler.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.wconfig.WorkerConfig");

}


namespace lsst {
namespace qserv {
namespace wconfig {

WorkerConfig::WorkerConfig(const util::ConfigStore& configStore)
    : _mySqlConfig(configStore.getString("mysql.username"),
            configStore.getStringOrDefault("mysql.password"),
            configStore.getString("mysql.socket")),
      _memManClass(configStore.getStringOrDefault("memman.class", "MemManReal")),
      _memManSizeMb(configStore.getIntOrDefault("memman.memory", 1000)),
      _memManLocation(configStore.getString("memman.location")),
      _threadPoolSize(configStore.getIntOrDefault("scheduler.thread_pool_size", wsched::BlendScheduler::getMinPoolSize())),
      _maxGroupSize(configStore.getIntOrDefault("scheduler.group_size", 1)),
      _prioritySlow(configStore.getIntOrDefault("scheduler.priority_slow", 1)),
      _priorityMed(configStore.getIntOrDefault("scheduler.priority_med", 2)),
      _priorityFast(configStore.getIntOrDefault("scheduler.priority_fast", 3)),
      _maxReserveSlow(configStore.getIntOrDefault("scheduler.reserve_slow", 2)),
      _maxReserveMed(configStore.getIntOrDefault("scheduler.reserve_med", 2)),
      _maxReserveFast(configStore.getIntOrDefault("scheduler.reserve_fast", 2)) {
}

std::ostream& operator<<(std::ostream &out, WorkerConfig const& workerConfig) {
    out << "MemManClass=" << workerConfig._memManClass;
    if (workerConfig._memManClass == "MemManReal") {
        out << "MemManSizeMb=" << workerConfig._memManSizeMb;
    }
    out << "poolSize=" << workerConfig._threadPoolSize << ", maxGroupSize=" << workerConfig._maxGroupSize;

    out << " priority fast=" << workerConfig._priorityFast
        << " med=" << workerConfig._priorityMed
        << " slow=" << workerConfig._prioritySlow;

    out << "Reserved threads fast=" << workerConfig._maxReserveFast
         << " med=" << workerConfig._maxReserveMed << " slow=" << workerConfig._maxReserveSlow;

    return out;
}

}}} // namespace lsst::qserv::wconfig


