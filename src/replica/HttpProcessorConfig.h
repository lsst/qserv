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
#ifndef LSST_QSERV_HTTPPROCESSORCONFIG_H
#define LSST_QSERV_HTTPPROCESSORCONFIG_H

// System headers
#include <string>

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class HttpProcessorConfig captures a configuration of the HTTP service
 * within the Master Replication Controller to facilitate distributing
 * the configuration parameters between the service's modules.
 */
class HttpProcessorConfig {
public:
    /// The normal and the default constructor
    explicit HttpProcessorConfig(unsigned int czarResponseTimeoutSec_ = 0,
                                 unsigned int workerResponseTimeoutSec_ = 0,
                                 unsigned int qservSyncTimeoutSec_ = 0,
                                 unsigned int workerReconfigTimeoutSec_ = 0,
                                 std::string const& httpRoot_ = std::string())
            : czarResponseTimeoutSec(czarResponseTimeoutSec_),
              workerResponseTimeoutSec(workerResponseTimeoutSec_),
              qservSyncTimeoutSec(qservSyncTimeoutSec_),
              workerReconfigTimeoutSec(workerReconfigTimeoutSec_),
              httpRoot(httpRoot_) {}
    HttpProcessorConfig(HttpProcessorConfig const&) = default;
    HttpProcessorConfig& operator=(HttpProcessorConfig const&) = default;

    ~HttpProcessorConfig() = default;

    /// The maximum number of seconds to wait before giving up on requests
    /// sent to workers.
    unsigned int czarResponseTimeoutSec = 0;

    /// The maximum number of seconds to wait before giving up on requests
    /// sent to Czar(s).
    unsigned int workerResponseTimeoutSec = 0;

    /// The maximum number of seconds to wait before Qserv workers respond
    /// to the synchronization requests before bailing out and proceeding
    /// and declaring the worker as "non-responsive".
    /// @note a value of the parameter may differ from the default option
    /// supplied by the Configuration.
    unsigned int qservSyncTimeoutSec = 0;

    /// The maximum number of seconds to wait for the completion of the worker
    /// reconfiguration requests.
    unsigned int const workerReconfigTimeoutSec = 0;

    /// The root folder for the static content to be served by the built-in
    /// HTTP service.
    std::string httpRoot;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_HTTPPROCESSORCONFIG_H
