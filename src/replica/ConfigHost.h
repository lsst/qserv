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
#ifndef LSST_QSERV_REPLICA_CONFIGHOST_H
#define LSST_QSERV_REPLICA_CONFIGHOST_H

// System headers
#include <iosfwd>
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// This header declarations
namespace lsst::qserv::replica {

/**
 * @brief The structure ConfigHost encapsulates the DNS name and the IP address
 *   of a machine where the corresponding services run.
 *
 * In the current inplementation of the Replication/Ingest system,
 * the IP address of the machine is captured by the Workers Registry
 * service on the connections made by the services to the Registry.
 * So far, this is the most reliable way of determine a location of
 * the services. One known disadvantage of relying on the IP addresses
 * is that they may change in the cloud environmemt should services
 * be moved to different hosts or the correposponding pods be restarted.
 *
 * An alternative mechanism is based on using the DNS name of the machine
 * as it's known to the service itself locally. Note that information
 * recorded in the host name attribute may be ambigous at the presence of
 * many network interfaces on the target machine. In this case, it's up to
 * a user to correctly interpret and use the name for establishing
 * connections to the services.
 */
struct ConfigHost {
    std::string addr;  ///< The IP address
    std::string name;  ///< The DNS name(short or long)

    /// @return JSON representation of the object
    nlohmann::json toJson() const;

    /// @return 'true' if host objects have the same values of attributes
    bool operator==(ConfigHost const& other) const;

    /// @return 'true' if host objects don't have the same values of attributes
    bool operator!=(ConfigHost const& other) const { return !(operator==(other)); }
};

std::ostream& operator<<(std::ostream& os, ConfigHost const& info);

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_CONFIGHOST_H
