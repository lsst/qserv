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
#ifndef LSST_QSERV_REPLICA_CONFIGCZAR_H
#define LSST_QSERV_REPLICA_CONFIGCZAR_H

// System headers
#include <cstdint>
#include <iosfwd>
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "global/intTypes.h"
#include "replica/config/ConfigHost.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class ConfigCzar encapsulates various parameters describing a Czar.
 */
class ConfigCzar {
public:
    std::string name;   ///< The logical name of a Czar.
    CzarId id;          ///< The unique name of a Czar.
    ConfigHost host;    ///< The host name (and IP address) of the Czar management service.
    uint16_t port = 0;  ///< The port number of the Czar management service.

    /**
     * Construct from a JSON object.
     * @param obj The object to be used as a source of the Czar's state.
     * @throw std::invalid_argument If the input object can't be parsed, or if it has
     *   incorrect schema.
     */
    explicit ConfigCzar(nlohmann::json const& obj);

    ConfigCzar() = default;
    ConfigCzar(ConfigCzar const&) = default;
    ConfigCzar& operator=(ConfigCzar const&) = default;

    /// @return JSON representation of the object
    nlohmann::json toJson() const;

    /// @return 'true' if Czar objects have the same values of attributes
    bool operator==(ConfigCzar const& other) const;

    /// @return 'true' if Czar objects don't have the same values of attributes
    bool operator!=(ConfigCzar const& other) const { return !(operator==(other)); }
};

std::ostream& operator<<(std::ostream& os, ConfigCzar const& info);

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_CONFIGCZAR_H
