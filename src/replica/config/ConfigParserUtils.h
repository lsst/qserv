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
#ifndef LSST_QSERV_REPLICA_CONFIGPARSEUTILS_H
#define LSST_QSERV_REPLICA_CONFIGPARSEUTILS_H

/**
 * @brief The header provides utilities for parsing various configuration entities from
 *   the JSON representation into the transient ones.
 */

// System headers
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// This header declarations
namespace lsst::qserv::replica {

// Template functions for filling worker attributes from JSON.

template <typename T>
inline void parseRequired(T& dest, nlohmann::json const& obj, std::string const& attr) {
    dest = obj.at(attr).get<T>();
}

template <>
inline void parseRequired<bool>(bool& dest, nlohmann::json const& obj, std::string const& attr) {
    dest = obj.at(attr).get<int>() != 0;
}

template <typename T>
inline void parseOptional(T& dest, nlohmann::json const& obj, std::string const& attr) {
    if (auto const itr = obj.find(attr); itr != obj.end()) dest = itr->get<T>();
}

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_CONFIGPARSEUTILS_H
