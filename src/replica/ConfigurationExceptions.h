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
#ifndef LSST_QSERV_REPLICA_CONFIGURATIONEXCEPTIONS_H
#define LSST_QSERV_REPLICA_CONFIGURATIONEXCEPTIONS_H

/**
 * This header defines classes thrown as exceptions on Configuration-specific
 * failures.
 */

// System headers
#include <stdexcept>
#include <string>

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {
/**
 * The class ConfigError is the base class representing exceptions thrown by
 * the Configuration service.
 */
class ConfigError: public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

/**
 * The class ConfigVersionMismatch represents exceptions thrown on the expected versus
 * actual version mismatch of the configuration found in the peristent store.
 */
class ConfigVersionMismatch: public ConfigError {
public:
    int const version;
    int const requiredVersion;
    explicit ConfigVersionMismatch(std::string const& msg, int version_=0, int requiredVersion_=0)
        :   ConfigError(msg),
            version(version_),
            requiredVersion(requiredVersion_) {
    }
};

/**
 * The class ConfigTypeMismatch represents exceptions thrown during type conversions
 * of the parameter values if the expected type of a parameter doesn't match the actual
 * one stored in the configuration.
 */
class ConfigTypeMismatch: public ConfigError {
public:
    using ConfigError::ConfigError;
};
}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_CONFIGURATIONEXCEPTIONS_H
