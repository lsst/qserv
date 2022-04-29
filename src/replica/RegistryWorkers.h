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
#ifndef LSST_QSERV_REGISTRYWORKERS_H
#define LSST_QSERV_REGISTRYWORKERS_H

// System headers
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "util/Mutex.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class RegistryWorkers represents a synchronized collection of workers.
 */
class RegistryWorkers {
public:
    /**
     * @brief Insert (or replace if exists) worker entry
     * @param worker A definition of the worker
     * @throws std::invalid_argument If the worker definition is not correct.
     */
    void insert(nlohmann::json const& worker);

    /**
     * @brief Remove (if exists) a worker entry
     * @param id A unique identifier of the worker
     * @throws std::invalid_argument If the identifier is empty.
     */
    void remove(std::string const& id);

    /**
     * @return nlohmann::json All workers
     */
    nlohmann::json workers() const;

private:
    /// This mutex is needed for implementing synchronized operations over
    /// the collection.
    mutable util::Mutex _mtx;

    /// The collection of workers, where the key is the unique identifier
    // of a worker.
    nlohmann::json _workers = nlohmann::json::object();
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REGISTRYWORKERS_H
