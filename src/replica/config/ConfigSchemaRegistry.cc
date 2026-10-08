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
#include "replica/config/ConfigSchemaRegistry.h"

// System headers
#include <algorithm>
#include <thread>

// Qserv headers
#include "replica/util/Common.h"
#include "replica/util/ProtocolBuffer.h"

// Third party headers
#include "boost/asio.hpp"
#include "nlohmann/json.hpp"

using namespace std;
using json = nlohmann::json;

namespace {
unsigned int const max_listen_connections = boost::asio::socket_base::max_listen_connections;
unsigned int const num_threads = max(1U, thread::hardware_concurrency());
}  // namespace

namespace lsst::qserv::replica {

json ConfigSchemaRegistry::_build() {
    json schema = json::object();
    for (auto const& category : {"common", "registry", "security"}) {
        schema[category] = ConfigSchema::sharedSchema(category);
    }

    // Add Registry-specific parameters to the category.
    json& registry = schema["registry"];
    registry["max-listen-conn"] = {
            {"description",
             "The maximum length of the queue of pending connections sent to the registry's HTTP server."
             " Must be greater than 0."},
            {"default", max_listen_connections}};
    registry["threads"] = {
            {"description",
             "The number of threads managed by BOOST ASIO for the HTTP server. Must be greater than 0."},
            {"default", min(8U, num_threads)}};

    return schema;
}

ConfigSchemaRegistry::ConfigSchemaRegistry() : ConfigSchema(ConfigSchemaRegistry::_build()) {}

}  // namespace lsst::qserv::replica
