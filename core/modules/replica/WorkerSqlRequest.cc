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
#include "replica/WorkerSqlRequest.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/Performance.h"
#include "util/BlockPost.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.WorkerSqlRequest");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

WorkerSqlRequest::Ptr WorkerSqlRequest::create(ServiceProvider::Ptr const& serviceProvider,
                                               string const& worker,
                                               string const& id,
                                               int priority,
                                               std::string const& query,
                                               std::string const& user,
                                               std::string const& password) {
    return WorkerSqlRequest::Ptr(
        new WorkerSqlRequest(serviceProvider,
                             worker,
                             id,
                             priority,
                             query,
                             user,
                             password));
}


WorkerSqlRequest::WorkerSqlRequest(ServiceProvider::Ptr const& serviceProvider,
                                     string const& worker,
                                     string const& id,
                                     int priority,
                                     std::string const& query,
                                     std::string const& user,
                                     std::string const& password)
    :   WorkerRequest(serviceProvider,
                      worker,
                      "SQL",
                      id,
                      priority),
        _query(query),
        _user(user),
        _password(password) {
}


void WorkerSqlRequest::setInfo(ProtocolResponseSql& response) const {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    util::Lock lock(_mtx, context() + __func__);

    // Return the performance of the target request

    response.set_allocated_target_performance(performance().info());

    // TODO: implement extracting and setting a result set
    //response.set_data(data());
}


bool WorkerSqlRequest::execute() {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    util::Lock lock(_mtx, context() + __func__);

    switch (status()) {

        case STATUS_IN_PROGRESS:
            break;

        case STATUS_IS_CANCELLING:

            // Abort the operation right away

            setStatus(lock, STATUS_CANCELLED);
            throw WorkerRequestCancelled();

        default:
            throw logic_error(
                    context() + string(__func__) + "  not allowed while in state: " +
                    WorkerRequest::status2string(status()));
    }

    // TODO: implement database connection and query execution

    setStatus(lock, STATUS_SUCCEEDED);
    return true;
}

}}} // namespace lsst::qserv::replica
