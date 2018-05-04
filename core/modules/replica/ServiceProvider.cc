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

// Class header
#include "replica/ServiceProvider.h"

// System headers
#include <algorithm>
#include <stdexcept>

// Qserv headers
#include "replica/ChunkLocker.h"
#include "replica/Configuration.h"
#include "replica/DatabaseServices.h"
#include "replica/QservMgtServices.h"

namespace lsst {
namespace qserv {
namespace replica {

ServiceProvider::Ptr ServiceProvider::create(std::string const& configUrl) {
    auto ptr = ServiceProvider::Ptr(new ServiceProvider(configUrl));
    // This initialization is made a posteriori because the shared pointer
    // onto the object can't be accessed via the usual call to shared_from_this()
    // inside the contsructor.
    ptr->_qservMgtServices = QservMgtServices::create(ptr);
    return ptr;
}

ServiceProvider::ServiceProvider(std::string const& configUrl) {
    _configuration    = Configuration::load(configUrl);
    _databaseServices = DatabaseServices::create(_configuration);
}

void ServiceProvider::assertWorkerIsValid(std::string const& name) {
    if (not _configuration->isKnownWorker(name)) {
        throw std::invalid_argument(
            "Request::assertWorkerIsValid: worker name is not valid: " + name);
    }
}

void ServiceProvider::assertWorkersAreDifferent(std::string const& firstName,
                                                std::string const& secondName) {
    assertWorkerIsValid(firstName);
    assertWorkerIsValid(secondName);

    if (firstName == secondName) {
        throw std::invalid_argument(
            "Request::assertWorkersAreDifferent: worker names are the same: " + firstName);
    }
}

void ServiceProvider::assertDatabaseIsValid(std::string const& name) {
    if (not _configuration->isKnownDatabase(name)) {
        throw std::invalid_argument(
            "Request::assertDatabaseIsValid: database name is not valid: " + name);
    }
}

}}} // namespace lsst::qserv::replica
