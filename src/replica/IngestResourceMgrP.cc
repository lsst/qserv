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

// Class header
#include "replica/IngestResourceMgrP.h"

// Qserv headers
#include "global/stringUtil.h"
#include "replica/DatabaseServices.h"
#include "replica/HttpClient.h"
#include "replica/ServiceProvider.h"

using namespace std;

namespace lsst::qserv::replica {

shared_ptr<IngestResourceMgrP> IngestResourceMgrP::create(
        shared_ptr<ServiceProvider> const& serviceProvider) {
    return shared_ptr<IngestResourceMgrP>(new IngestResourceMgrP(serviceProvider));
}

IngestResourceMgrP::IngestResourceMgrP(shared_ptr<ServiceProvider> const& serviceProvider)
        : _serviceProvider(serviceProvider) {}

unsigned int IngestResourceMgrP::asyncProcLimit(string const& databaseName) const {
    throwIfEmpty(__func__, databaseName);
    auto const databaseServices = _serviceProvider->databaseServices();
    try {
        string const str = databaseServices
                                   ->ingestParam(databaseName, HttpClientConfig::category,
                                                 HttpClientConfig::asyncProcLimitKey)
                                   .value;
        return lsst::qserv::stoui(str);
    } catch (DatabaseServicesNotFound const&) {
        // Assume the default value if no parameter was recorded in
        // the configuration for the database.
        ;
    }
    return 0;
}

}  // namespace lsst::qserv::replica
