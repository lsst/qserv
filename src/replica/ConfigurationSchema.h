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
#ifndef LSST_QSERV_REPLICA_CONFIGURATIONSCHEMA_H
#define LSST_QSERV_REPLICA_CONFIGURATIONSCHEMA_H

// System headers
#include <map>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

// Third party headers
#include "nlohmann/json.hpp"

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {
namespace detail {

template <typename T>
struct EmptyValueValidator {
    static void validate(T const& val) {
        if (val == 0) {
            throw std::invalid_argument(
                    "ConfigurationSchema::EmptyValueValidator: 0 is not permited.");
        }
    }
};

template <>
struct EmptyValueValidator<std::string> {
    static void validate(std::string const& val) {
        if (val.empty()) {
            throw std::invalid_argument(
                    "ConfigurationSchema::EmptyValueValidator: empty string is not permited.");
        }
    }
};
}   // namespace detail

/**
 * This utility class ConfigurationSchema provides methods returning known JSON schemas of
 * the Configuration service.
 */
class ConfigurationSchema {
public:
    /// @return A documentation string for the specified parameter or the empty string
    ///   if none is available in the schema.
    static std::string description(std::string const& category, std::string const& param);

    /// @return A 'true' if the parameter can't be modified via the 'set' methods
    ///   of the Configuration class. This information is used by class Configuration
    ///   to validate the parameters.
    static bool readOnly(std::string const& category, std::string const& param);

    /// @return A 'true' if the parameter represents the security context (passwords,
    ///   authorization keys, etc.). Parameters possessing this attribute are supposed
    ///   to be used with care by the dependent automation tools to avoid exposing
    ///   sensitive information in log files, reports, etc.
    static bool securityContext(std::string const& category, std::string const& param);

    /// @return The default configuration data as per the current JSON schema to be loaded
    ///   into the transient state of the class Configuration upon its initialization.
    static nlohmann::json defaultConfigData();

    /**
     * The directory method for locating categories and parameters within
     * the given category known to the current implementation.
     * @note The method only returns the so called "general" categories
     *   of primitive parameters that exclude workers, database families,
     *   databases, etc.
     * @return A collection of categories and parameters within the given category.
     *   The name of a category would be the dictionary key, and a value of
     *   the dictionary will contains a set of the parameter names within
     *   the corresponding category.
     */
    static std::map<std::string, std::set<std::string>> parameters();

    /**
     * Serialize a primitive JSON object into a non-quoted string.
     * @param context A context from which the operation was initiated. It's used for
     *   error reporting purposes.
     * @param obj A JSON object to be serialized.
     * @throws std::invalid_argument If the input object can't be serialized into a string.
     */
    static std::string json2string(std::string const& context, nlohmann::json const& obj);

    template <typename T>
    static void validate(std::string const& category, std::string const& param, T const& val) {

        // The test for parameters that have "zero" numeric value or the "empty"
        // string restrictions.
        if (!_emptyAllowed(category, param)) detail::EmptyValueValidator<T>::validate(val);

        // The test is for parameters whose values are restricted by a fixed set.
        nlohmann::json const restrictor = _restrictor(category, param);
        if (restrictor.is_null()) return;
        std::string const type = restrictor.at("type").get<std::string>();
        if (type != "set") {
            throw std::runtime_error(
                    "ConfigurationSchema::" + std::string(__func__) + " unsupported restrictor type: '"
                    + type + "', category: '" + category + "', param: '" + param + "'.");
        }
        for (auto&& obj: restrictor.at("values")) {
            if (obj.get<T>() == val) return;
        }
        throw std::invalid_argument(
                "ConfigurationSchema::" + std::string(__func__)
                + " a value of the parameter isn't allowed due to schema restrictions, category: '"
                + category + "', param: '" + param + "'.");
    }

private:
    /// @return A 'true' if, depending on the actual type of the parameter, the empty
    ///   string (for strings) or zero value (for numeric parameters) is allowed.
    ///   This information is used by class Configuration to validate input values
    ///   of the parameters.
    static bool _emptyAllowed(std::string const& category, std::string const& param);

    /// @return The optional restrictor object or JSON's null object for teh parameter.
    static nlohmann::json _restrictor(std::string const& category, std::string const& param);

    /// The schema of the transient configuration.
    static nlohmann::json const _schemaJson;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_CONFIGURATIONSCHEMA_H
