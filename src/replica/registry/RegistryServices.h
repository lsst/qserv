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
     * @param id A unique identifier of the worker.
     * @param desc A payload to be merged.
     * @throws std::invalid_argument If the worker id is empty or if the worker
     *   descriptor is not a valid JSON object.
     */
    void updateWorker(std::string const& id, nlohmann::json const& desc);

    /**
     * Remove an existing worker entry.
     * @param id A unique identifier of the worker.
     * @throws std::invalid_argument If the worker id is empty.
     * @throws http::ErrorNotFound404 If the worker id is not found in the collection.
     */
    void removeWorker(std::string const& id);

    /**
     * Add or update (if exists) the complete definition into the Czar entry.
     * @param id A unique identifier of the czar.
     * @param desc A payload to be added/updated.
     * @throws std::invalid_argument If the czar id is empty or if the czar
     *   definition is not a valid JSON object.
     */
    void updateCzar(std::string const& id, nlohmann::json const& desc);

    /**
     * Remove an existing Czar entry.
     * @param id A unique identifier of the czar.
     * @throws std::invalid_argument If the czar id is empty.
     * @throws http::ErrorNotFound404 If the czar id is not found in the collection.
     */
    void removeCzar(std::string const& id);

    /**
     * Add or update (if exists) the complete definition into the controller entry.
     * @param id A unique identifier of the controller.
     * @param desc A payload to be added/updated.
     * @throws std::invalid_argument If the controller id is empty or if the controller
     *   definition is not a valid JSON object.
     */
    void updateController(std::string const& id, nlohmann::json const& desc);

    /**
     * Remove an existing controller entry.
     * @param id A unique identifier of the controller.
     * @throws std::invalid_argument If the controller id is empty.
     * @throws http::ErrorNotFound404 If the controller id is not found in the collection.
     */
    void removeController(std::string const& id);

    /// @return nlohmann::json The whole collection of services.
    nlohmann::json toJson() const;

private:
    /**
     * Insert or update (if exists) a service entry in the collection. If the specified entry already exists
     * in the specified service type category, the function will insert new attributes or replace existing
     * ones of the service entry.
     * @param func A name of the calling function (for logging purposes).
     * @param serviceType A type of the service (worker, czar, controller).
     * @param serviceId A unique identifier of the service.
     * @param desc A payload to be merged into the service entry.
     * @throws std::invalid_argument If the service id is empty or if the service descriptor is not a valid
     * JSON object.
     */
    void _update(std::string const& func, std::string const& serviceType, std::string const& serviceId,
                 nlohmann::json const& desc);

    /**
     * Remove an existing service entry from the collection.
     * @param func A name of the calling function (for logging purposes).
     * @param serviceType A type of the service (worker, czar, controller).
     * @param serviceId A unique identifier of the service.
     * @throws std::invalid_argument If the service id is empty.
     * @throws http::ErrorNotFound404 If the service id is not found in the collection.
     */
    void _remove(std::string const& func, std::string const& serviceType, std::string const& serviceId);

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
