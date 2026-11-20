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
#ifndef LSST_QSERV_REPLICA_DIRECTORINDEXREQUESTINFO_H
#define LSST_QSERV_REPLICA_DIRECTORINDEXREQUESTINFO_H

// System headers
#include <cstddef>
#include <iosfwd>
#include <string>

// This header declarations
namespace lsst::qserv::replica {

/**
 *  Structure DirectorIndexRequestInfo represents a result of the requests.
 */
struct DirectorIndexRequestInfo {
    std::string workerName;          ///< The name of the worker server.
    std::string error;               ///< An error (if any) reported by the worker server.
    std::string fileName;            ///< The file that contains the index data (on success).
    std::size_t fileSizeBytes = 0;   ///< The number of bytes that were written into the file.
    std::string remoteFileResource;  ///< The remote file resource identifier.
};

/// Overloaded streaming operator for type DirectorIndexRequestInfo
std::ostream& operator<<(std::ostream& os, const DirectorIndexRequestInfo& info);

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_DIRECTORINDEXREQUESTINFO_H
