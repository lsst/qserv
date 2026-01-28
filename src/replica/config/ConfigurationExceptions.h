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
namespace lsst::qserv::replica {
/**
 * The class ConfigError is the base class representing exceptions thrown by
 * the Configuration service.
 */
class ConfigError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

/**
 * The class ConfigVersionMismatch represents exceptions thrown on the expected versus
 * actual version mismatch of the configuration found in the peristent store.
 */
class ConfigVersionMismatch : public ConfigError {
public:
    int const version;
    int const requiredVersion;
    explicit ConfigVersionMismatch(std::string const& msg, int version_ = 0, int requiredVersion_ = 0)
            : ConfigError(msg), version(version_), requiredVersion(requiredVersion_) {}
};

/**
 * The class ConfigTypeMismatch represents exceptions thrown during type conversions
 * of the parameter values if the expected type of a parameter doesn't match the actual
 * one stored in the configuration.
 */
class ConfigTypeMismatch : public ConfigError {
public:
    using ConfigError::ConfigError;
};

/**
 * The class ConfigNotEmpty represents exceptions thrown when an operation
 * is attempted on a configuration object that is not empty, but it was expected
 * to be empty.
 */
class ConfigNotEmpty : public ConfigError {
public:
    using ConfigError::ConfigError;
};

/**
 * The class ConfigUnknownDatabaseFamily represents exceptions thrown when
 * a database family is not known to the configuration.
 */
class ConfigUnknownDatabaseFamily : public ConfigError {
public:
    std::string const familyName;
    explicit ConfigUnknownDatabaseFamily(std::string const& msg, std::string const& familyName_)
            : ConfigError(msg), familyName(familyName_) {}
};

/**
 * The class ConfigUnknownDatabase represents exceptions thrown when
 * a database is not known to the configuration.
 */
class ConfigUnknownDatabase : public ConfigError {
public:
    std::string const databaseName;
    explicit ConfigUnknownDatabase(std::string const& msg, std::string const& databaseName_)
            : ConfigError(msg), databaseName(databaseName_) {}
};

/**
 * The class ConfigUnknownTable represents exceptions thrown when
 * a table is not known to the configuration.
 */
class ConfigUnknownTable : public ConfigError {
public:
    std::string const databaseName;
    std::string const tableName;
    explicit ConfigUnknownTable(std::string const& msg, std::string const& databaseName_,
                                std::string const& tableName_)
            : ConfigError(msg), databaseName(databaseName_), tableName(tableName_) {}
};

/**
 * The class ConfigUnknownWorker represents exceptions thrown when
 * a worker is not known to the configuration.
 */
class ConfigUnknownWorker : public ConfigError {
public:
    std::string const workerName;
    explicit ConfigUnknownWorker(std::string const& msg, std::string const& workerName_)
            : ConfigError(msg), workerName(workerName_) {}
};

/**
 * The class ConfigUnknownCzar represents exceptions thrown when
 * a czar is not known to the configuration.
 */
class ConfigUnknownCzar : public ConfigError {
public:
    std::string const czarName;
    explicit ConfigUnknownCzar(std::string const& msg, std::string const& czarName_)
            : ConfigError(msg), czarName(czarName_) {}
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_CONFIGURATIONEXCEPTIONS_H
