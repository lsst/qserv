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
#include "replica/RedirectorWorkers.h"

// System headers
#include <stdexcept>

using namespace std;
using json = nlohmann::json;

namespace lsst {
namespace qserv {
namespace replica {

void RedirectorWorkers::insert(json const& worker) {
    string const context = "RedirectorWorkers::" + string(__func__) + " ";
    if (!worker.is_object()) {
        throw invalid_argument(context + "worker definition is not a valid JSON object.");
    }
    auto const itr = worker.find("name");
    if (itr == worker.end()) {
        throw invalid_argument(context + "attribute 'name' is missing in the worker definition JSON object.");
    }
    string const& id = *itr;
    util::Lock const lock(_mtx, context);
    _workers[id] = worker;
}


void RedirectorWorkers::remove(std::string const& id) {
    string const context = "RedirectorWorkers::" + string(__func__) + " ";
    if (id.empty()) {
        throw invalid_argument(context + "worker identifier is empty.");
    }
    util::Lock const lock(_mtx, context);
    _workers.erase(id);
}


json RedirectorWorkers::workers() const {
    string const context = "RedirectorWorkers::" + string(__func__) + " ";
    util::Lock const lock(_mtx, context);
    return _workers;
}

}}} // namespace lsst::qserv::replica
