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
#include "partition/ConfigStore.h"

// System headers
#include <fstream>
#include <sstream>
#include <stdexcept>

// Third party headers
#include <boost/core/demangle.hpp>

// LSST headers
#include "partition/ObjectIndex.h"

using namespace std;
using json = nlohmann::json;

namespace lsst::partition {

ConfigStore::ConfigStore(json const& config)
        : _config(json::object()),
          _objectIndex1(make_shared<ObjectIndex>()),
          _objectIndex2(make_shared<ObjectIndex>()) {
    string const context = "ConfigStore::" + string(__func__) + ": ";
    if (config.is_null() || config.empty()) return;
    if (!config.is_object()) throw invalid_argument(context + "config is not a valid JSON object");
    _config = config;
}

void ConfigStore::parse(string const& filename) {
    string const context = "ConfigStore::" + string(__func__) + ": ";
    if (filename.empty()) throw invalid_argument(context + "filename can't be empty");
    ifstream file(filename, ios_base::in);
    if (!file.good()) throw invalid_argument(context + "failed to open file: '" + filename + "'");
    json config;
    try {
        file >> config;
    } catch (...) {
        throw runtime_error(context + "file: '" + filename + "' doesn't have a valid JSON payload");
    }
    add(config);
}

void ConfigStore::add(json const& config) {
    string const context = "ConfigStore::" + string(__func__) + "(json): ";
    if (config.is_null() || config.empty()) return;
    if (!config.is_object()) throw invalid_argument(context + "config is not a valid JSON object");
    _config.merge_patch(config);
}

void ConfigStore::add(boost::program_options::variables_map const& vm) {
    string const context = "ConfigStore::" + string(__func__) + "(boost::program_options): ";
    for (auto&& e : vm) {
        string const& path = e.first;
        // Interpret empty parameters as boolean flags
        if (e.second.empty()) {
            set<bool>(path, true);
            return;
        }
        // Also skip parameters for which only their default values are available (since they were not
        // present in the command line), unless the store doesn't have the parameter.
        if (e.second.defaulted() && has(path)) continue;
        if (e.second.value().type() == typeid(std::string)) {
            set<std::string>(path, e.second.as<std::string>());
        } else if (e.second.value().type() == typeid(std::vector<std::string>)) {
            set<std::vector<std::string>>(path, e.second.as<std::vector<std::string>>());
        } else if (e.second.value().type() == typeid(bool)) {
            set<bool>(path, e.second.as<bool>());
        } else if (e.second.value().type() == typeid(char)) {
            set<char>(path, e.second.as<char>());
        } else if (e.second.value().type() == typeid(int)) {
            set<int>(path, e.second.as<int>());
        } else if (e.second.value().type() == typeid(uint32_t)) {
            set<uint32_t>(path, e.second.as<uint32_t>());
        } else if (e.second.value().type() == typeid(size_t)) {
            set<size_t>(path, e.second.as<size_t>());
        } else if (e.second.value().type() == typeid(float)) {
            set<float>(path, e.second.as<float>());
        } else if (e.second.value().type() == typeid(double)) {
            set<double>(path, e.second.as<double>());
        } else {
            throw ConfigTypeError(context + "command-line parameter '" + path + "' has unsupported type: '" +
                                  boost::core::demangle(e.second.value().type().name()) + "'");
        }
    }
}

bool ConfigStore::has(string const& path) const { return _config.contains(_path2pointer(path)); }

bool ConfigStore::flag(std::string const& path) const {
    if (!has(path)) return false;
    return get<bool>(path);
}

json::json_pointer ConfigStore::_path2pointer(string const& path) {
    string const context = "ConfigStore::" + string(__func__) + ": ";
    json::json_pointer pointer;
    stringstream ss(path);
    string elem;
    while (getline(ss, elem, '.')) {
        pointer.push_back(elem);
    }
    if (pointer.empty()) {
        throw invalid_argument(context + "path '" + path +
                               "' can\'t be translated into a valid JSON pointer");
    }
    return pointer;
}

json const& ConfigStore::_get(string const& path) const {
    string const context = "ConfigStore::" + string(__func__) + ": ";
    auto const pointer = _path2pointer(path);
    if (!_config.contains(pointer)) {
        throw invalid_argument(context + "no parameter exists for path: '" + path + "'");
    }
    return _config[pointer];
}

}  // namespace lsst::partition
