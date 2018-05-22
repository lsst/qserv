/*
 * LSST Data Management System
 * Copyright 2017 LSST Corporation.
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
#ifndef LSST_QSERV_REPLICA_REQUEST_MESSENGER_H
#define LSST_QSERV_REPLICA_REQUEST_MESSENGER_H

/// RequestMessenger.h declares:
///
/// class RequestMessenger
/// (see individual class documentation for more information)

// System headers
#include <memory>
#include <string>

// Third party headers
#include <boost/asio.hpp>

// Qserv headers
#include "replica/Request.h"
#include "replica/ServiceProvider.h"

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

// Forward declarations
class Messenger;

/**
  * Class RequestMessenger is a base class for a family of requests within
  * the replication Controller server.
  */
class RequestMessenger
    :   public Request  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<RequestMessenger> Ptr;

    // Default construction and copy semantics are prohibited

    RequestMessenger() = delete;
    RequestMessenger(RequestMessenger const&) = delete;
    RequestMessenger& operator=(RequestMessenger const&) = delete;

    /// Destructor
    ~RequestMessenger() override = default;

protected:

    /**
     * Construct the request with the pointer to the services provider.
     *
     * NOTE: options 'keepTracking' and 'allowDuplicate' have effect for
     *       specific request only.
     *
     * @param serviceProvider - a provider of various services
     * @param type            - its type name (used informally for debugging)
     * @param worker          - the name of a worker
     * @io_service            - BOOST ASIO service
     * @priority              - may affect an execution order of the request by
     *                          the worker service. Higher number means higher
     *                          priority.
     * @param keepTracking    - keep tracking the request before it finishes or fails
     * @param allowDuplicate  - follow a previously made request if the current one duplicates it
     * @param messenger       - an interface for communicating with workers
     */
    RequestMessenger(ServiceProvider::Ptr const& serviceProvider,
                     boost::asio::io_service& io_service,
                     std::string const& type,
                     std::string const& worker,
                     int  priority,
                     bool keepTracking,
                     bool allowDuplicate,
                     std::shared_ptr<Messenger> const& messenger);

    /// @return pointer to the messenging service
    std::shared_ptr<Messenger> const& messenger() const { return _messenger; }

    /**
     * Implement a method defined in the base class.
     */
    void finishImpl(util::Lock const& lock) override;
    
protected:

    /// Worker messenging service
    std::shared_ptr<Messenger> _messenger;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_REQUEST_MESSENGER_H
