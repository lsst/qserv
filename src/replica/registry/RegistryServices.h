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
#ifndef LSST_QSERV_REGISTRYSERVICES_H
#define LSST_QSERV_REGISTRYSERVICES_H

// System headers
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "replica/util/Mutex.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class RegistryServices represents a synchronized collection of the registered services.
 */
class RegistryServices {
public:
    /**
     * Merge (complete or partial) worker definition into the worker entry.
     * @param name A unique identifier of the worker.
     * @param workerInfo A payload to be merged.
     * @throws std::invalid_argument If the worker name is empty or if the worker
     *   definition is not a valid JSON object.
     */
    void updateWorker(std::string const& name, nlohmann::json const& workerInfo);

    /**
     * Remove (if exists) a worker entry.
     * @param name A unique identifier of the worker.
     * @throws std::invalid_argument If the worker name is empty.
     */
    void removeWorker(std::string const& name);

    /**
     * Add or update (if exists) the complete definition into the Czar entry.
     * @param name A unique identifier of the czar.
     * @param czarInfo A payload to be added/updated.
     * @throws std::invalid_argument If the definition of the czar is not a valid JSON object.
     */
    void updateCzar(std::string const& name, nlohmann::json const& czarInfo);

    /**
     * Remove (if exists) a Czar entry.
     * @param name A unique identifier of the czar.
     */
    void removeCzar(std::string const& name);

    /**
     * Add or update (if exists) the complete definition into the Controller entry.
     * @param name A unique identifier of the controller.
     * @param controllerInfo A payload to be added/updated.
     * @throws std::invalid_argument If the definition of the controller is not a valid JSON object.
     */
    void updateController(std::string const& name, nlohmann::json const& controllerInfo);

    /**
     * Remove (if exists) a Controller entry.
     * @param name A unique identifier of the controller.
     */
    void removeController(std::string const& name);

    /// @return nlohmann::json The whole collection of services.
    nlohmann::json toJson() const;

private:
    /// This mutex is needed for implementing synchronized operations over
    /// the collection.
    mutable replica::Mutex _mtx;

    /// The collection of the registered services.
    nlohmann::json _services = nlohmann::json::object({{"workers", nlohmann::json::object()},
                                                       {"czars", nlohmann::json::object()},
                                                       {"controllers", nlohmann::json::object()}});
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REGISTRYSERVICES_H
