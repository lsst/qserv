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
#ifndef LSST_QSERV_HTTPREQUESTBODY_H
#define LSST_QSERV_HTTPREQUESTBODY_H

// System headers
#include <stdexcept>
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "qhttp/Server.h"

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * Helper class HttpRequestBody parses a body of an HTTP request
 * which has the following header:
 * 
 *   Content-Type: application/json
 * 
 * Exceptions may be thrown by the constructor of the class if
 * the request has an unexpected content type, or if its payload
 * is not a proper JSON object.
 */
class HttpRequestBody {
public:

    /// parsed body of the request
    nlohmann::json objJson = nlohmann::json::object();

    HttpRequestBody() = default;
    HttpRequestBody(HttpRequestBody const&) = default;
    HttpRequestBody& operator=(HttpRequestBody const&) = default;

    ~HttpRequestBody() = default;

    /**
     * The constructor will parse and evaluate a body of an HTTP request
     * and populate the 'kv' dictionary. Exceptions may be thrown in
     * the following scenarios:
     *
     * - the required HTTP header is not found in the request
     * - the body doesn't have a valid JSON string (unless the body is empty)
     * 
     * @param req
     *   the request to be parsed
     */
    explicit HttpRequestBody(qhttp::Request::Ptr const& req);

    /**
     * The helper method for finding and returning a value of a required
     * parameter.
     *
     * @param obj  the JSON object to be inspected
     * @param name  the name of a parameter
     * @return a value of the parameter
     * @throws invalid_argument  if the parameter wasn't found
     */
    template <typename T>
    static T required(nlohmann::json const& obj,
                      std::string const& name) {
        if (not obj.is_object()) {
            throw std::invalid_argument(
                "HttpRequestBody::" + std::string(__func__) + "<T>[static] parameter 'obj' is not a valid JSON object");
        }
        if (obj.find(name) != obj.end()) return obj[name];
        throw std::invalid_argument(
                "HttpRequestBody::" + std::string(__func__) + "<T>[static] required parameter " + name +
                " is missing in the request body");
    }

    /**
     * Find and return a value of a required parameter
     * @param name  the name of a parameter
     * @return a value of the parameter
     * @throws invalid_argument  if the parameter wasn't found
     */
    template <typename T>
    T required(std::string const& name) const {
        return required<T>(objJson, name);
    }

    /**
     * Find and return a value for the specified optional parameter
     * @param name  the name of a parameter
     * @param defaultValue  a value to be returned if the parameter wasn't found
     * @return a value of the parameter or the default value
     */
    template <typename T>
    T optional(std::string const& name, T const& defaultValue) const {
        if (objJson.find(name) != objJson.end()) return objJson[name];
        return defaultValue;
    }

    /**
     * Find and return a vector of values for the specified required parameter
     * @param name  the name of a parameter
     * @return a collection of the values for the parameter
     * @throws invalid_argument  if the parameter wasn't found
     */
    template <typename T>
    std::vector<T> requiredColl(std::string const& name) const {
        auto const itr = objJson.find(name);
        if (objJson.find(name) == objJson.end()) {
            throw std::invalid_argument(
                    "HttpRequestBody::" + std::string(__func__) + "<T> required parameter " + name +
                    " is missing in the request body");
        }
        if (not itr->is_array()) {
            throw std::invalid_argument(
                    "HttpRequestBody::" + std::string(__func__) + "<T> a value of the required parameter " + name +
                    " is not an array");
        }
        std::vector<T> coll;
        for (size_t i = 0, size = itr->size(); i < size; ++i) {
            T val = (*itr)[i];
            coll.push_back(val);
        }
        return coll;
    }};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_HTTPREQUESTBODY_H
