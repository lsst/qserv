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
#ifndef LSST_QSERV_REPLICA_WORKERAPP_H
#define LSST_QSERV_REPLICA_WORKERAPP_H

// System headers
#include <limits>
#include <string>

// Qserv headers
#include "replica/apps/Application.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class WorkerApp implements represents a worker service.
 */
class WorkerApp : public Application {
public:
    typedef std::shared_ptr<WorkerApp> Ptr;

    /**
     * The factory method is the only way of creating objects of this class
     * because of the very base class's inheritance from 'enable_shared_from_this'.
     *
     * @param argc  The number of command-line arguments.
     * @param argv  A vector of command-line arguments.
     */
    static Ptr create(int argc, char* argv[]);

    WorkerApp() = delete;
    WorkerApp(WorkerApp const&) = delete;
    WorkerApp& operator=(WorkerApp const&) = delete;

    virtual ~WorkerApp() final = default;

protected:
    virtual int runImpl() final;

private:
    WorkerApp(int argc, char* argv[]);

    /**
     * @brief Check if required folders exist and they're write-enabled for an effective user
     *   of the current process. Create missing folders if needed and if requested.
     * @note Worker services depend on a number of folders that are used for
     *   storing intermediate files of various sizes. Locations (absolute path names)
     *   of the folders are set in the corresponding configuration parameters.
     *   Desired characteristics (including size, I/O latency, I/O bandwidth, etc.) of
     *   the folders may vary depending on the service type and a scale of a particular
     *   Qserv deployment. Note that the overall performance and scalability greately
     *   depends on the quality of of the underlying filesystems. Usually, in
     *   the large-scale deployments, the folders should be pre-created and be placed
     *   at the large-capacity high-performance filesystems at the Qserv deployment time.
     * @throw std::runtime_error If any folder can't be created, or if any folder is not
     *   write-enabled for the current user.
     */
    void _verifyCreateFolders() const;

    /// A connection url for the MySQL service of the Qserv worker database.
    std::string _qservWorkerDbUrl;

    /// The worker will create missing folders unless told not to do so by
    /// passing the corresponding command-line flag.
    bool _doNotCreateMissingFolders = false;
};

}  // namespace lsst::qserv::replica

#endif /* LSST_QSERV_REPLICA_WORKERAPP_H */
