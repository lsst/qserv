// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2008-2015 LSST Corporation.
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

#ifndef LSST_QSERV_CZAR_CZARCONFIG_H
#define LSST_QSERV_CZAR_CZARCONFIG_H

// System headers

// Qserv headers
#include "mysql/MySqlConfig.h"
#include "util/ConfigStore.h"

namespace lsst {
namespace qserv {
namespace czar {

/**
 *  Provide all configuration parameters for a Qserv Czar instance
 *
 *  Parse an INI configuration file, identify required parameters and ignore
 *  others, analyze and store them inside private member variables, use default
 *  values for missing parameters, provide accessor for each of these variable.
 *  This class hide configuration complexity from other part of the code.
 *  All private member variables are related to Czar parameters and are immutables.
 *
 */
class CzarConfig {
public:
    CzarConfig(std::string configFileName)
        : CzarConfig(util::ConfigStore(configFileName)) {
    }

    CzarConfig(CzarConfig const&) = delete;
    CzarConfig& operator=(CzarConfig const&) = delete;

    /** Overload output operator for current class
     *
     * @param out
     * @param workerConfig
     * @return an output stream
     */
    friend std::ostream& operator<<(std::ostream &out, CzarConfig const& czarConfig);

    /* Get MySQL configuration for czar MySQL result database
     *
     * @return a structure containing MySQL parameters
     */
    mysql::MySqlConfig const& getMySqlResultConfig() const {
        return _mySqlResultConfig;
    }

    std::string const& getLogConfig() const {
        return _logConfig;
    }

    /* Get MySQL configuration for czar MySQL qmeta database
     *
     * @return a structure containing MySQL parameters
     */
    mysql::MySqlConfig const& getMySqlQmetaConfig() const {
        return _mySqlQmetaConfig;
    }

    /* Get MySQL configuration for czar MySQL QStatusData
     *
     * @return a structure containing MySQL parameters
     */
    mysql::MySqlConfig const& getMySqlQStatusDataConfig() const {
        return _mySqlQstatusDataConfig;
    }


    /* Get CSS parameters as a collection of key-value
     *
     * Do not check CSS parameters consistency
     *
     * @return a structure containing CSS parameters
     */
    std::map<std::string, std::string> const& getCssConfigMap() const {
        return _cssConfigMap;
    }

    /* Get path to directory where the empty chunk files resides
     *
     * Each empty chunk file is related to one cosmic dataset
     *
     * @return path to directory where the empty chunk files resides
     */
    std::string const& getEmptyChunkPath() const {
        return _emptyChunkPath;
    }

    /* Get the maximum number of chunks that can be in an interactive query.
     * Queries that are not limited in area to a small number of chunks must
     * be part of a full table scan.
     */
    int getInteractiveChunkLimit() const {
        return _interactiveChunkLimit;
    }

    /* Get hostname and port for xrootd manager
     *
     * "localhost:1094" is the most reasonable default, even though it is
     * the wrong choice for all but small developer installations
     *
     * @return a string containing "<hostname>:<port>"
     */
    std::string const& getXrootdFrontendUrl() const {
        return _xrootdFrontendUrl;
    }

    /* Get the maximum number of threads for xrootd to use.
     *
     * @return the maximum number of threads for xrootd to use.
     */
    int getXrootdCBThreadsMax() const {
        return _xrootdCBThreadsMax;
    }

    /* Get the initial number of threads for xrootd to create and maintain.
     *
     * @return the initial number of threads for xrootd to use.
     */
    int getXrootdCBThreadsInit() const {
        return _xrootdCBThreadsInit;
    }

    bool getQueryDistributionTestVer() const {
        return _queryDistributionTestVer;
    }

    /*
     * @return A value of the "spread" parameter. This may improve a performance
     * of xrootd for catalogs with the large number of chunks. The default value
     * of this parameter in xrootd is 4.
     */
    int getXrootdSpread() const {
        return _xrootdSpread;
    }

    /* Get minimum number of seconds between QMeta chunk completion updates.
     *
     * @return seconds between QMeta chunk completion updates.
     */
    int getQMetaSecondsBetweenChunkUpdates() const {
        return _qMetaSecsBetweenChunkCompletionUpdates;
    }

    /// Getters for result aggregation options.
    int getMaxTableSizeMB() const { return _maxTableSizeMB; }
    int getMaxSqlConnectionAttempts() const { return _maxSqlConnectionAttempts; }
    std::string getResultEngine() const { return _resultEngine; }
    int getResultMaxConnections() const { return _resultMaxConnections; }

    /// Getters for QdispPool configuration
    /// @return the number of threads to create for the pool.
    int getQdispPoolSize() const { return _qdispPoolSize; }
    /// @return the maximum priority for a queue. The number of queues
    ///    equals this value +1.
    int getQdispMaxPriority() const { return _qdispMaxPriority; }
    /// @return a string with substrings separated by ':' like "2:45:32:9"
    ///      The values indicate the maximum number of commands for each
    ///      priority that can be running concurrently.
    std::string getQdispVectRunSizes() const { return _qdispVectRunSizes; }
    /// @return a string with substrings separated by ':' like "2:45:32:9"
    ///      The values indicate the minimum number of commands for each
    ///      priority that should be running concurrently
    std::string getQdispVectMinRunningSizes() const { return _qdispVectMinRunningSizes; }

    int getOldestResultKeptDays() const { return _oldestResultKeptDays; }

private:

    CzarConfig(util::ConfigStore const& ConfigStore);

    // Parameters below used in czar::Czar
    mysql::MySqlConfig const _mySqlResultConfig;
    std::string const _logConfig;

    // Parameters used to affect result aggregation in rproc.
    int const         _maxTableSizeMB;
    int const         _maxSqlConnectionAttempts;
    std::string const _resultEngine;
    int const         _resultMaxConnections;
    /// Any table in the result table not updated in this many days will be deleted.
    int const         _oldestResultKeptDays;

    // Parameters below used in ccontrol::UserQueryFactory
    std::map<std::string, std::string> const _cssConfigMap;
    mysql::MySqlConfig const _mySqlQmetaConfig;
    mysql::MySqlConfig const _mySqlQstatusDataConfig;
    std::string const _xrootdFrontendUrl;
    std::string const _emptyChunkPath;
    int const _interactiveChunkLimit;
    int const _xrootdCBThreadsMax;
    int const _xrootdCBThreadsInit;
    int const _xrootdSpread;
    int const _qMetaSecsBetweenChunkCompletionUpdates;
    int const _queryDistributionTestVer;

    // Parameters for QdispPool configuration
    int const _qdispPoolSize;
    int const _qdispMaxPriority;
    std::string const _qdispVectRunSizes; // No spaces, values separated by ':'
    std::string const _qdispVectMinRunningSizes; // No spaces, values separated by ':'

};

}}} // namespace lsst::qserv::czar

#endif // LSST_QSERV_CZAR_CZARCONFIG_H
