// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2016 LSST Corporation.
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
#include "wsched/QueryStatistics.h"

// System headers


// LSST headers
#include "lsst/log/Log.h"

// Qserv headers


namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.wsched.QueryStatistics");
}

namespace lsst {
namespace qserv {
namespace wsched {

void Queries::addQueryTask(wbase::Task::Ptr const& task) {
    auto qid = task->getQueryId();
    auto ent = std::pair<QueryId, QueryStatistics>(qid, QueryStatistics(qid));
    auto res = _queryStats.insert(ent);
    auto iter = res.first;
    iter->second._tasksTotal += 1;
}


}}} // namespace lsst:qserv:wsched
