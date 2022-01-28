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
#include "replica/RedirectorHttpApp.h"

// Qserv headers
#include "replica/RedirectorHttpSvc.h"

using namespace std;

namespace {
string const description =
    "This application runs the worker registration (redirection) service "
    "that's used by the workers to report themselves and by the controllers to locate "
    "connection and configuration parameters of the workers. The service can be used "
    "to obtain the run-time status of the workers for the system monitoring purposes";

bool const injectDatabaseOptions = true;
bool const boostProtobufVersionCheck = false;
bool const enableServiceProvider = true;

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

RedirectorHttpApp::Ptr RedirectorHttpApp::create(int argc, char* argv[]) {
    return Ptr(new RedirectorHttpApp(argc, argv));
}


RedirectorHttpApp::RedirectorHttpApp(int argc, char* argv[])
    :   Application(
            argc, argv,
            description,
            injectDatabaseOptions,
            boostProtobufVersionCheck,
            enableServiceProvider
        ) {
}


int RedirectorHttpApp::runImpl() {
    auto const svc = RedirectorHttpSvc::create(serviceProvider());
    svc->run();
    return 0;
}

}}} // namespace lsst::qserv::replica
