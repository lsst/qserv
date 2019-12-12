// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2019 LSST.
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
#include "loader/DoListItem.h"

// System headers
#include <iostream>

// Qserv headers
#include "loader/Central.h"
#include "loader/LoaderMsg.h"
#include "proto/loader.pb.h"

// LSST headers
#include "lsst/log/Log.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.loader.DoListItem");

}

namespace lsst {
namespace qserv {
namespace loader {


util::CommandTracked::Ptr DoListItem::runIfNeeded(TimeOut::TimePoint now) {
     std::lock_guard<std::mutex> lock(_mtx);
     if (_command == nullptr) {
         if (_isOneShotDone()) return nullptr;
         if ((_needInfo || _timeOut.due(now)) && _timeRateLimit.due(now)) {
             _timeRateLimit.triggered();
             // Randomly vary the next rate limit timeout
             int rand = (std::rand()/(RAND_MAX/1000)); // 0 to 1000
             rand += std::min(_commandsCreated * 10000, 120000);
             auto rateLimitRandom = now + std::chrono::milliseconds(rand);
             _timeRateLimit.triggered(rateLimitRandom);
             _command = createCommand();
             if (_oneShot) ++_commandsCreated;
             LOGS(_log, LOG_LVL_DEBUG, "cCreated=" << _commandsCreated << " rand=" << rand);
             return _command;
         }
     } else if (_command->isFinished()) {
         _command.reset(); // Allow the command to be sent again later.
     }
     return nullptr;
 }


}}} // namespace lsst:qserv::loader

