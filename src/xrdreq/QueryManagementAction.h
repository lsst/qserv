// -*- LSST-C++ -*-
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
#ifndef LSST_QSERV_XRDREQ_QUERY_MANAGEMENT_ACTION_H
#define LSST_QSERV_XRDREQ_QUERY_MANAGEMENT_ACTION_H

// System headers
#include <atomic>
#include <functional>
#include <map>
#include <memory>
#include <string>

// Qserv headers
#include "global/intTypes.h"
#include "proto/worker.pb.h"

namespace lsst::qserv::xrdreq {

/**
 * Class QueryManagementAction is an interface for managing query completion/cancellation
 * at all Qserv workers that are connected as "publishers" to the XROOTD redirector.
 */
// &&&QM need to get the same functionality using json messages, and not in xrdreq.
class QueryManagementAction : public std::enable_shared_from_this<QueryManagementAction> {
public:
    /// The reponse type represents errors reported by the workers, where worker
    /// names are the keys. And the values are the error messages. Empty strings
    /// indicate the succesful completion of the requests.
    using Response = std::map<std::string, std::string>;

    /// The callback function type to be used for notifications on the operation completion.
    using CallbackType = std::function<void(Response const&)>;

    /**
     * The front-end method for initiating the operation at all workers.
     *
     * @note The only way to track the completion of the requests sent via
     * this interface is by providing the callback function. The request delivery
     * is not guaranteeded in case if the XROOTD/SSI network will be clogged by
     * the heavy traffic. It's safe to call the same operation many times if needed.
     *
     * @param xrootdFrontendUrl A location of the XROOTD redirector.
     * @param op An operation be initiated at the workers.
     * @param onFinish The optional callback to be fired upon the completion of
     *   the requested operation.
     *
     * @throws std::runtime_error For failures encountered when connecting to
     *   the manager or initiating the requesed operation.
     */
    static void notifyAllWorkers(std::string const& xrootdFrontendUrl, proto::QueryManagement::Operation op,
                                 uint32_t czarId, QueryId queryId, CallbackType onFinish = nullptr);

    QueryManagementAction(QueryManagementAction const&) = delete;
    QueryManagementAction& operator=(QueryManagementAction const&) = delete;
    virtual ~QueryManagementAction();

private:
    QueryManagementAction();

    /**
     * The actual implementation of the request processor.
     * @see QueryManagementAction::notifyAllWorkers()
     */
    void _notifyAllWorkers(std::string const& xrootdFrontendUrl, proto::QueryManagement::Operation op,
                           uint32_t czarId, QueryId queryId, CallbackType onFinish);

    /// The collection of worker responses.
    Response _response;

    /// The counter will get incremented as worker responses will be received.
    /// User-provided callback function (if any) will be called when all requests
    /// will finish (succeed or fail).
    std::atomic<std::size_t> _numWorkerRequestsFinished{0};
};

}  // namespace lsst::qserv::xrdreq

#endif  // LSST_QSERV_XRDREQ_QUERY_MANAGEMENT_ACTION_H
