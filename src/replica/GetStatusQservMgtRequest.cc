/*
 * LSST Data Management System
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
#include "replica/GetStatusQservMgtRequest.h"

// Qserv headers
#include "util/String.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.GetStatusQservMgtRequest");

}  // namespace

namespace lsst::qserv::replica {

string taskSelectorToHttpQuery(wbase::TaskSelector const& taskSelector) {
    string query;
    query += "?include_tasks=" + string(taskSelector.includeTasks ? "1" : "0");
    query += "&max_tasks=" + to_string(taskSelector.maxTasks);
    if (!taskSelector.queryIds.empty()) {
        query += "&query_ids=" + util::String::toString(taskSelector.queryIds);
    }
    if (!taskSelector.taskStates.empty()) {
        query += "&task_states=" + util::String::toString(taskSelector.taskStates);
    }
    return query;
}

shared_ptr<GetStatusQservMgtRequest> GetStatusQservMgtRequest::create(
        shared_ptr<ServiceProvider> const& serviceProvider, string const& workerName,
        wbase::TaskSelector const& taskSelector, GetStatusQservMgtRequest::CallbackType const& onFinish) {
    return shared_ptr<GetStatusQservMgtRequest>(
            new GetStatusQservMgtRequest(serviceProvider, workerName, taskSelector, onFinish));
}

GetStatusQservMgtRequest::GetStatusQservMgtRequest(shared_ptr<ServiceProvider> const& serviceProvider,
                                                   string const& workerName,
                                                   wbase::TaskSelector const& taskSelector,
                                                   GetStatusQservMgtRequest::CallbackType const& onFinish)
        : QservWorkerMgtRequest(serviceProvider, "QSERV_GET_STATUS", workerName),
          _taskSelector(taskSelector),
          _onFinish(onFinish) {}

void GetStatusQservMgtRequest::createHttpReqImpl(replica::Lock const& lock) {
    string const service = "/status";
    string const query = replica::taskSelectorToHttpQuery(_taskSelector);
    createHttpReq(lock, service, query);
}

void GetStatusQservMgtRequest::notify(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_TRACE, context() << __func__);
    notifyDefaultImpl<GetStatusQservMgtRequest>(lock, _onFinish);
}

}  // namespace lsst::qserv::replica
