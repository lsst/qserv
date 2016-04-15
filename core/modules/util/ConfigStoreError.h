// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2015 LSST Corporation.
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

/**
  * @file
  *
  * @brief RunTimeErrors for ConfigStore.
  *
  * @Author Fabrice Jammes, IN2P3
  */

#ifndef LSST_QSERV_CONFIGSTOREERROR_H
#define LSST_QSERV_CONFIGSTOREERROR_H

// System headers
#include <stdexcept>
#include <string>

// Qserv headers

namespace lsst {
namespace qserv {
namespace util {

/**
 * Base class for ConfigStore run-time errors, represents a generic ConfigStore run-time error.
 */
class ConfigStoreError : public std::runtime_error {
public:
    explicit ConfigStoreError(std::string const& msg)
        : std::runtime_error(msg) {}

};

/**
 * Specialized run-time error: configuration key is missing.
 */
class KeyNotFoundError : public ConfigStoreError {
public:
    explicit KeyNotFoundError(std::string const& key)
        : ConfigStoreError("Missing configuration key: " + key) {}
};

/**
 * Specialized run-time error: invalid integer value
 */
class InvalidIntegerValue : public ConfigStoreError {
public:
    explicit InvalidIntegerValue(std::string const& key, std::string const& value)
        : ConfigStoreError("Configuration key [" + key + "] has invalid integer value: '" + value +"'") {}
};

}}} // namespace lsst::qserv::util

#endif // LSST_QSERV_CONFIGSTOREERROR_H
