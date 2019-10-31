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
#ifndef LSST_QSERV_REPLICA_FILEINGESTAPP_H
#define LSST_QSERV_REPLICA_FILEINGESTAPP_H

// System headers
#include <memory>
#include <string>

// Qserv headers
#include "replica/Application.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class FileIngestApp implements a tool which acts as a catalog data loading
 * client of the Replication system's catalog data ingest server.
 */
class FileIngestApp : public Application {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<FileIngestApp> Ptr;

    /**
     * The factory method is the only way of creating objects of this class
     * because of the very base class's inheritance from 'enable_shared_from_this'.
     *
     * @param argc
     *   the number of command-line arguments
     *
     * @param argv
     *   the vector of command-line arguments
     */
    static Ptr create(int argc, char* argv[]);

    // Default construction and copy semantics are prohibited

    FileIngestApp() = delete;
    FileIngestApp(FileIngestApp const&) = delete;
    FileIngestApp& operator=(FileIngestApp const&) = delete;

    ~FileIngestApp() override = default;

protected:

    /// @see Application::runImpl()
    int runImpl() final;

private:

    /// @see FileIngestApp::create()
    FileIngestApp(int argc, char* argv[]);

    std::string  _workerHost;           /// The host name or an IP address of a worker
    uint16_t     _workerPort = 0;       /// The port number of the Ingest Service
    unsigned int _transactionId = 0;    /// An identifier of the super-transaction
    std::string  _tableName;            /// The base name of a table to be ingested
    std::string  _inFileName;           /// The name of a local file to be ingested
    bool         _verbose = false;      /// Print various stats upon a completion of the ingest
};

}}} // namespace lsst::qserv::replica

#endif /* LSST_QSERV_REPLICA_FILEINGESTAPP_H */
