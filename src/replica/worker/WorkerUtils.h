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
#ifndef LSST_QSERV_REPLICA_WORKERUTILS_H
#define LSST_QSERV_REPLICA_WORKERUTILS_H

// System headers
#include <string>

// This header declarations
namespace lsst::qserv::replica {

/**
 * The utility class WorkerUtils provides useful static methods for worker-side
 * request implementations. The class is meant to prevent unnecessary code duplication
 * across different request types.
 */
class WorkerUtils {
public:
    WorkerUtils() = delete;
    WorkerUtils(WorkerUtils const&) = delete;
    WorkerUtils& operator=(WorkerUtils const&) = delete;

    ~WorkerUtils() = default;

    /**
     * Make the best effort to create a missing database if it doesn't exist.
     * @note This method doesn't throw exceptions but logs errors instead.
     *  An assumption is that the database server of adjacent workers may not be always
     *  up and running, and the operation will eventually succeed when the server
     *  becomes available.
     * @param context a human-readable context of the operation for logging purposes
     * @param databaseName the name of the database to be created
     */
    static void createMissingDatabase(std::string const& context, std::string const& databaseName);
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_WORKERUTILS_H
