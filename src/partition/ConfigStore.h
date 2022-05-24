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

/// \file
/// \brief Configuration service

#ifndef LSST_PARTITION_CONFIG_H
#define LSST_PARTITION_CONFIG_H

// System headers
#include <string>
#include <vector>

// Third party headers
#include "boost/program_options.hpp"
#include "nlohmann/json.hpp"

namespace lsst { namespace partition {
/**
 * Objects of class ConfigTypeError are thrown when a client is attempting
 * incorrect types in the parameter type conversion.
 */
class ConfigTypeError : public std::logic_error {
public:
    using std::logic_error::logic_error;
};
}}  // namespace lsst::partition

namespace lsst { namespace partition { namespace detail {
/**
 * Class Value is a utility class for type-safe conversion of parameters into
 * values of desired types.
 */
template <typename T>
class Value {
public:
    static T convert(nlohmann::json const& param, std::string const& path) {
        std::string const context = "ConfigStore::Value<T>::convert: ";
        try {
            return param.get<T>();
        } catch (std::exception const& ex) {
            throw lsst::partition::ConfigTypeError(context + "incorrect type conversion for parameter: '" +
                                                   path + "', details: " + std::string(ex.what()));
        }
    }
};

/**
 * Template specialization of the utility class Value for converting parameters of
 * the JSON array type into vectors.
 *
 * @note the implementation of method convert() allows for nested convertion of
 *   the vector types. For example, it would support std::vector<std::vector<int>>, etc.
 */
template <typename T>
class Value<std::vector<T>> {
public:
    static std::vector<T> convert(nlohmann::json const& param, std::string const& path) {
        std::string const context = "ConfigStore::Value<std::vector<T>>::convert: ";
        if (!param.is_array()) {
            throw lsst::partition::ConfigTypeError(context + "parameter: '" + path + "' is not an array.");
        }
        std::vector<T> v;
        for (auto const& e : param) {
            v.push_back(Value<T>::convert(e, path));
        }
        return v;
    }
};

/**
 * Template specialization of the utility class Value to allow interpreting single
 * character strings as values of the 'char' type.
 */
template <>
class Value<char> {
public:
    static char convert(nlohmann::json const& param, std::string const& path) {
        std::string const context = "ConfigStore::Value<char>::convert: ";
        if (param.is_string()) {
            std::string const s = Value<std::string>::convert(param, path);
            if (s.size() != 1) {
                throw lsst::partition::ConfigTypeError(
                        context + "parameter: '" + path +
                        "' is a string, but not the single-character one"
                        " to allow interpreting it as a value of the 'char' type.");
            }
            return s[0];
        }
        try {
            return param.get<char>();
        } catch (std::exception const& ex) {
            throw lsst::partition::ConfigTypeError(context + "incorrect type conversion for parameter: '" +
                                                   path + "', details: " + std::string(ex.what()));
        }
    }
};
}}}  // namespace lsst::partition::detail

namespace lsst { namespace partition {
/**
 * Class ConfigStore is a unified transient storage of the configuration parameters read
 * from the configuration files or command-line parameters.
 *
 * Parameters are fetched from the store using their "path" specification. The dot
 * character '.' is used for separating components of the path:
 * @code
 * foo.bar.p1
 * dir.p2
 * p3
 * @code
 * The intermediate elements of the path would directly map to the corresponding keys
 * of the nested JSON objects in the store. For example, the above shown paths would
 * be represented by the following JSON structure passed into the class's constructor,
 * into method add(), or read from an input file when calling method parse().
 * from the :
 * @code
 * {
 *   "foo": {
 *       "bar": {
 *           "p1": <value>
 *       }
 *    },
 *    "dir": {
 *       "p2": <value>
 *    },
 *    "p3": <value>
 * }
 * @code
 * Values of the command-line parameters are expected to be injected into the configuration
 * by calling method set(). The long names (excluding preceeding "--") of the parameters
 * would be treated as above explained path names.
 *
 * @note The class's implementation is not thread-safe.
 * @note Modifications are last-one wins.
 */
class ConfigStore {
public:
    /**
     * Initialize the store with parameters found in the input JSON object.
     *
     * Empty JSON objects are allowed on the input.
     *
     * @param config  A JSON object to be used to initialize the store.
     * @throw std::invalid_argument  If the input value is not a JSON Object.
     */
    explicit ConfigStore(nlohmann::json const& config = nlohmann::json::object());

    /**
     * Parse the content of the JSON file and merge parameters found in the file into
     * the object.
     *
     * It's allowed to call this method many times for the same or different files.
     * Parameters read from a file will be merged into the store.
     *
     * @param filename  The name of a JSON file to be parsed.
     * @throw std::invalid_argument  The file name is not valid.
     * @throw std::runtime_error  Failed to open or parse the file.
     */
    void parse(std::string const& filename);

    /**
     * Merge the content of the input JSON object into the store.
     *
     * It's allowed to call this method many times for the same or different objects.
     * Parameters read from the input object will be merged into the store.
     * Empty objects are allowed on the input.
     *
     * @param config  A JSON object to be merged into the store.
     * @throw std::invalid_argument  If the input value is not a JSON Object.
     */
    void add(nlohmann::json const& config);

    /**
     * Merge values of the command-line parameters into the store.
     *
     * @note Interpret empty parameters as boolean flags set as 'true'. Also ignore parameters for which
     *   only their default values are available (since they were not present in the command line),
     *   unless the parameters aren't present in the store. In this approach, the last explicitly
     *   set value would always have the higher priority.
     *
     * @param vm  A collection of parameters to be merged.
     * @throw ConfigStore  For unsupported types of the input parameters.
     */
    void add(boost::program_options::variables_map const& vm);

    /**
     * Set/update a value of a parameter at the specified path.
     *
     * @param path  A path to the parameter.
     * @param value A value of the parameter.
     * @throw std::invalid_argument The path is not valid.
     */
    template <typename T>
    void set(std::string const& path, T const& value) {
        _config[_path2pointer(path)] = value;
    }

    /**
     * Extract a value of an existing parameter given its expected type.
     *
     * @param path  A path to a parameter.
     * @return A value of requested given type.
     * @throw std::invalid_argument The path is not valid.
     * @throw ConfigStore  If using incorrect types in type conversion.
     */
    template <typename T>
    T get(std::string const& path) const {
        return detail::Value<T>::convert(_get(path), path);
    }

    /**
     * Check if the specified parameter exists in the configuration.
     *
     * @param path  A path to a parameter.
     * @return 'true' if the specified parameter exists at the specified path.
     * @throw std::invalid_argument The path is not valid.
     */
    bool has(std::string const& path) const;

    /**
     * This method should be used instead of 'has(path)' or 'get<bool>(path)' for checking
     * a status of parameters meant to be used as flags. A reason why this method is needed is
     * because JSON allows attributes of the 'bool' type which could be explicitly set to 'true'
     * or 'false'. In this case just relying on 'has(path)' may produce incorrect results.
     * Method 'has(path)' would only work for parameters found in the command line.
     * On the other hand, relying on 'get<bool>(path)' would only work for the parameters
     * which were explicitly set from JSON. Using the getter method would result in an exception
     * if the parameter wasn't explicitly specified in the JSON file.
     *
     * The current implementation of method 'flag(path)' addresses this problem in the following way:
     * 1) if the parameter is found in the store then the method would return its stored value.
     * 2) otherwise the method would return 'false'.
     *
     * @param path  A path to a parameter.
     * @return A state of the flag.
     * @throw std::invalid_argument The path is not valid.
     */
    bool flag(std::string const& path) const;

private:
    /**
     * Translate a dot-separated path into a JSON pointer.
     *
     * @param path  A path to be translated.
     * @return A non-empty pointer.
     * @throw std::invalid_argument The path is not valid.
     */
    static nlohmann::json::json_pointer _path2pointer(std::string const& path);

    /**
     * @param path  A path to a parameter.
     * @return an intermediate representation of a parameter.
     * @throw std::invalid_argument The path is not valid.
     */
    nlohmann::json const& _get(std::string const& path) const;

    /// Parameter storage
    nlohmann::json _config = nlohmann::json::object();
};
}}  // namespace lsst::partition

#endif  // LSST_PARTITION_CONFIG_H
