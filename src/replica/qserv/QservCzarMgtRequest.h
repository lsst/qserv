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
#ifndef LSST_QSERV_REPLICA_QSERVCZARMGTREQUEST_H
#define LSST_QSERV_REPLICA_QSERVCZARMGTREQUEST_H

// System headers
#include <memory>
#include <string>

// Qserv headers
#include "http/AsyncReq.h"
#include "replica/qserv/QservMgtRequest.h"

// Forward declarations
namespace lsst::qserv::replica {
class ServiceProvider;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * @brief QservCzarMgtRequest is a base class for a family of the Qserv Czar
 *   management requests within the master server.
 */
class QservCzarMgtRequest : public QservMgtRequest {
public:
    QservCzarMgtRequest() = delete;
    QservCzarMgtRequest(QservCzarMgtRequest const&) = delete;
    QservCzarMgtRequest& operator=(QservCzarMgtRequest const&) = delete;

    virtual ~QservCzarMgtRequest() override = default;

    /// @return name of a Czar
    std::string const& czarName() const { return _czarName; }

protected:
    /**
     * @brief Construct the request with the pointer to the services provider.
     * @param serviceProvider Is required to access configuration services.
     * @param type The type name of he request (used for debugging and error reporting).
     * @param czarName The name of the Qserv Czar.
     */
    QservCzarMgtRequest(std::shared_ptr<ServiceProvider> const& serviceProvider, std::string const& type,
                        std::string const& czarName);

    /// @return The callback function for tracking connection parameters of the Czar.
    virtual http::AsyncReq::GetHostPort getHostPortTracker() const override;

private:
    std::string const _czarName;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_QSERVCZARMGTREQUEST_H
