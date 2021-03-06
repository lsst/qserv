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
#ifndef LSST_QSERV_REPLICA_CONFIGWORKER_H
#define LSST_QSERV_REPLICA_CONFIGWORKER_H

// System headers
#include <cstdint>
#include <iosfwd>
#include <string>

// Third party headers
#include "nlohmann/json.hpp"


// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class WorkerInfo encapsulates various parameters describing a worker.
 */
class WorkerInfo {
public:
    std::string name;           // The logical name of a worker

    bool isEnabled  = true;     // The worker is allowed to participate in the replication operations
    bool isReadOnly = false;    // The worker can only serve as a source of replicas.
                                // New replicas can't be placed on it.

    std::string svcHost;        // The host name (or IP address) of the worker service
    uint16_t    svcPort = 0;    // The port number of the worker service

    std::string fsHost;         // The host name (or IP address) of the file service for the worker
    uint16_t    fsPort = 0;     // The port number for the file service for the worker

    std::string dataDir;        // An absolute path to the data directory under which the MySQL
                                // database folders are residing.

    std::string loaderHost;     // The host name (or IP address) of the ingest (loader) service
    uint16_t    loaderPort = 0; // The port number of the ingest service

    std::string loaderTmpDir;   // An absolute path to the temporary directory which would be used
                                // by the service. The folder must be write-enabled for a user
                                // under which the service will be run.

    std::string exporterHost;       // The host name (or IP address) of the data exporting service
    uint16_t    exporterPort = 0;   // The port number of the data exporting service

    std::string exporterTmpDir;     // An absolute path to the temporary directory which would be used
                                    // by the service. The folder must be write-enabled for a user
                                    // under which the service will be run.

    std::string httpLoaderHost;     // The host name (or IP address) of the HTTP-based ingest (loader) service
    uint16_t    httpLoaderPort = 0; // The port number of the HTTP-based ingest service

    std::string httpLoaderTmpDir;   // An absolute path to the temporary directory which would be used
                                    // by the HTTP-based service. The folder must be write-enabled for a user
                                    // under which the service will be run.

    /**
     * This function treats its numeric input as a tri-state variable, where
     * any negative value means no user input was provided, 0 value represents
     * the boolean 'false', and any other positive number represents 'true'.
     * 
     * @param in  The tri-state input number to be evaluated.
     * @param out A reference to the output variable to be updated if required
     *   conditions are met. 
     */
    static void update(int const in, bool& out) {
        if (in == 0) out = false;
        else if (in > 0) out = true;
    }

    /**
     * This function assumes that any non-empty value given on the input
     * means an explicit user input that needs to be checked against the present
     * state of the parameter to decide if a change is needed.
     *
     * @param in  The input string to be evaluated.
     * @param out A reference to the output variable to be updated if required
     *   conditions are met. 
     */
    static void update(std::string const& in, std::string& out) {
        if (!in.empty()) out = in;
    }

    /**
     * This function is specialized for 16-bit port numbers. It treats
     * any non-zero input as an explicit user input that needs to be checked
     * against the present state of the parameter to decide if a change is needed.
     *
     * @param in  The input number to be evaluated.
     * @param out A reference to the output variable to be updated if required
     *   conditions are met. 
     */
    static void update(uint16_t const in, uint16_t& out) {
        if (in != 0) out = in;
    }

    /**
     * Construct from another worker descriptor using a required collection of
     * the default parameters to compensate for incomplete info in the input descriptor.
     * @param defaults The required JSON object to be used as a source of the default
     *   values for parameters missing in the input descriptor.
     * @throw std::invalid_argument If the JSON object is not valid, is incomplete,
     *   or has incorrect types of the default attributes.
     */
    WorkerInfo(WorkerInfo const& info, nlohmann::json const& defaults);

    /**
     * Construct from a JSON object.
     * @param obj The optional object to be used as a source of the worker's state.
     * @param defaults The optional object to be used as a source of the default values
     *   for parameters missing in 'obj'.
     * @throw std::invalid_argument If the input object can't be parsed, or if it has
     *   incorrect schema.
     */
    explicit WorkerInfo(nlohmann::json const& obj=nlohmann::json::object(),
                        nlohmann::json const& defaults=nlohmann::json::object());

    /**
     * Check if required folders exist and they're write-enabled for an effective user
     * of the current process. Create missing folders if needed.
     * @param createMissingFolders The optional flag telling the method to create missing folders.
     * @throw std::runtime_error If any folder can't be created, or if any folder is not
     *   write-enabled for the current user.
     */
    void verifyFolders(bool createMissingFolders=false) const;

    /// @return JSON representation of the object
    nlohmann::json toJson() const;
};

std::ostream& operator <<(std::ostream& os, WorkerInfo const& info);

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_CONFIGWORKER_H
