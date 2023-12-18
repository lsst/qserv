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
#ifndef LSST_QSERV_REPLICA_QSERVWORKERMGTREQUEST_H
#define LSST_QSERV_REPLICA_QSERVWORKERMGTREQUEST_H

// System headers
#include <memory>
#include <string>

// Qserv headers
#include "http/AsyncReq.h"
#include "replica/QservMgtRequest.h"

// Forward declarations
namespace lsst::qserv::replica {
class Performance;
class ServiceProvider;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * @brief QservWorkerMgtRequest is a base class for a family of the Qserv worker
 *   management requests within the master server.
 */
class QservWorkerMgtRequest : public QservMgtRequest {
public:
    QservWorkerMgtRequest() = delete;
    QservWorkerMgtRequest(QservWorkerMgtRequest const&) = delete;
    QservWorkerMgtRequest& operator=(QservWorkerMgtRequest const&) = delete;

    virtual ~QservWorkerMgtRequest() override = default;

    /// @return name of a worker
    std::string const& workerName() const { return _workerName; }

protected:
    /**
     * @brief Construct the request with the pointer to the services provider.
     * @param serviceProvider Is required to access configuration services.
     * @param type The type name of he request (used for debugging and error reporting).
     * @param workerName The name of the Qserv worker.
     */
    QservWorkerMgtRequest(std::shared_ptr<ServiceProvider> const& serviceProvider, std::string const& type,
                          std::string const& workerName);

    /// @return The callback function for tracking connection parameters of the worker.
    virtual http::AsyncReq::GetHostPort getHostPortTracker() const override;

    /// @see QservMgtRequest::updatePersistentState
    virtual void updatePersistentState(Performance const& performance,
                                       std::string const& serverError) const override;

private:
    std::string const _workerName;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_QSERVWORKERMGTREQUEST_H
