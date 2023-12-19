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
#include "replica/config/ConfigParserJSON.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "replica/config/ConfigurationExceptions.h"

using namespace std;
using json = nlohmann::json;

namespace lsst::qserv::replica {

ConfigParserJSON::ConfigParserJSON(json& data, map<string, ConfigWorker>& workers,
                                   map<string, DatabaseFamilyInfo>& databaseFamilies,
                                   map<string, DatabaseInfo>& databases, map<string, ConfigCzar>& czars)
        : _data(data),
          _workers(workers),
          _databaseFamilies(databaseFamilies),
          _databases(databases),
          _czars(czars) {}

void ConfigParserJSON::parse(json const& obj) {
    if (!obj.is_object()) throw invalid_argument(_context + "a JSON object is required.");

    // Validate and update the configuration parameters.
    //
    // IMPORTANT: Note an order in which the parameter categories are being evaluated.
    //   This order guarantees data consistency based on the dependency between
    //   the parameters of the categories. For instance, the database definitions in the category
    //   'databases' will be processed after processing database families in the category
    //   'database_families' so that the database's family name would be validated
    //   against names of the known families.
    //
    // OTHER NOTES:
    //   - The ordering approach also allows to process incomplete input configurations,
    //     or inject configuration options in more than one object.
    //   - Unknown categories of parameters as well as unknown parameters will be ignored.
    //   - The last insert always wins.

    if (obj.count("general") != 0) {
        for (auto&& itr : obj.at("general").items()) {
            string const& category = itr.key();
            json const& inCategoryObj = itr.value();
            json& outCategoryObj = _data[category];
            for (auto&& itr : inCategoryObj.items()) {
                string const& param = itr.key();
                json const& inParamObj = itr.value();

                // Skip missing parameters
                if (outCategoryObj.count(param) == 0) continue;

                json& outParamObj = outCategoryObj[param];
                if (inParamObj.type() != outParamObj.type()) {
                    throw std::invalid_argument(_context +
                                                " no transient schema match for the parameter, category: '" +
                                                category + "' param: '" + param + "'.");
                }
                if (inParamObj.is_string()) {
                    _storeGeneralParameter<string>(outParamObj, inParamObj, category, param);
                } else if (inParamObj.is_number_unsigned()) {
                    _storeGeneralParameter<uint64_t>(outParamObj, inParamObj, category, param);
                } else if (inParamObj.is_number_integer()) {
                    _storeGeneralParameter<int64_t>(outParamObj, inParamObj, category, param);
                } else if (inParamObj.is_number_float()) {
                    _storeGeneralParameter<double>(outParamObj, inParamObj, category, param);
                } else {
                    throw invalid_argument(
                            _context + " unsupported transient schema type for the parameter, category: '" +
                            category + "' param: '" + param + "'.");
                }
            }
        }
    }

    // Parse entries representing objects. Note that the families are parsed
    // before databases in order to enforce the database-to-family referential
    // integrity.
    if (obj.count("workers") != 0) {
        for (auto&& inWorker : obj.at("workers")) {
            // Use this constructor to validate the schema and to fill in the missing (optional)
            // parameters. If it won't throw then the input description is correct and can be placed
            // into the output object. Using defaults is needed to ensure the worker entry is
            // complete before storying in the transient state. Note that users of the API may rely
            // on the default values of some parameters of workers.
            ConfigWorker const worker(inWorker);
            _workers[worker.name] = worker;
        }
    }
    if (obj.count("database_families") != 0) {
        for (auto&& inFamily : obj.at("database_families")) {
            // Use this constructor to validate the schema. If it won't throw then
            // the input description is correct and can be placed into the output object.
            DatabaseFamilyInfo const family(inFamily);
            _databaseFamilies[family.name] = family;
        }
    }
    if (obj.count("databases") != 0) {
        for (auto&& inDatabase : obj.at("databases")) {
            // Use this constructor to validate the schema. If it won't throw then
            // the input description is correct and can be placed into the output object.
            // Note that the parser expects a collection of the database families to ensure
            // an existing family name was provided in the input spec.
            DatabaseInfo const database = DatabaseInfo::parse(inDatabase, _databaseFamilies, _databases);
            _databases[database.name] = database;
        }
    }
    if (obj.count("czars") != 0) {
        for (auto&& inCzar : obj.at("czars")) {
            // Use this constructor to validate the schema and to fill in the missing (optional)
            // parameters. If it won't throw then the input description is correct and can be placed
            // into the output object. Using defaults is needed to ensure the worker entry is
            // complete before storying in the transient state. Note that users of the API may rely
            // on the default values of some parameters of workers.
            ConfigCzar const czar(inCzar);
            _czars[czar.name] = czar;
        }
    }
}

}  // namespace lsst::qserv::replica
