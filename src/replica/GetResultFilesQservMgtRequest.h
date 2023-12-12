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
#ifndef LSST_QSERV_REPLICA_GETRESULTFILESQSERVMGTREQUEST_H
#define LSST_QSERV_REPLICA_GETRESULTFILESQSERVMGTREQUEST_H

// System headers
#include <memory>
#include <string>
#include <vector>

// Qserv headers
#include "global/intTypes.h"
#include "replica/QservMgtRequest.h"

namespace lsst::qserv::replica {
class ServiceProvider;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class GetResultFilesQservMgtRequest is a request for obtaining info
 * on the partial result files from the Qserv worker.
 */
class GetResultFilesQservMgtRequest : public QservMgtRequest {
public:
    typedef std::shared_ptr<GetResultFilesQservMgtRequest> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    GetResultFilesQservMgtRequest() = delete;
    GetResultFilesQservMgtRequest(GetResultFilesQservMgtRequest const&) = delete;
    GetResultFilesQservMgtRequest& operator=(GetResultFilesQservMgtRequest const&) = delete;

    virtual ~GetResultFilesQservMgtRequest() final = default;

    /**
     * Static factory method is needed to prevent issues with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     * @param serviceProvider A reference to a provider of services for accessing
     *   Configuration, saving the request's persistent state to the database.
     * @param worker The name of a worker to send the request to.
     * @param queryIds The optional selector for queries. If empty then all queries will
     *   be considered.
     * @param maxFiles The optional limit for maximum number of files to be reported.
     *   If 0 then no limit is set.
     * @param onFinish (optional) callback function to be called upon request completion.
     * @return A pointer to the created object.
     */
    static std::shared_ptr<GetResultFilesQservMgtRequest> create(
            std::shared_ptr<ServiceProvider> const& serviceProvider, std::string const& worker,
            std::vector<QueryId> const& queryIds = std::vector<QueryId>(), unsigned int maxFiles = 0,
            CallbackType const& onFinish = nullptr);

protected:
    /// @see QservMgtRequest::createHttpReqImpl()
    virtual void createHttpReqImpl(replica::Lock const& lock) final;

    /// @see QservMgtRequest::notify()
    virtual void notify(replica::Lock const& lock) final;

private:
    /// @see GetResultFilesQservMgtRequest::create()
    GetResultFilesQservMgtRequest(std::shared_ptr<ServiceProvider> const& serviceProvider,
                                  std::string const& worker, std::vector<QueryId> const& queryIds,
                                  unsigned int maxFiles, CallbackType const& onFinish);

    // Input parameters

    std::vector<QueryId> const _queryIds;
    unsigned int const _maxFiles;
    CallbackType _onFinish;  ///< This callback is reset after finishing the request.
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_GETRESULTFILESQSERVMGTREQUEST_H
