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
#ifndef LSST_QSERV_REPLICA_FILEREADAPP_H
#define LSST_QSERV_REPLICA_FILEREADAPP_H

// System headers
#include <memory>
#include <string>
#include <vector>

// Qserv headers
#include "replica/Application.h"

// LSST headers
#include "lsst/log/Log.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class FileReadApp implements a tool which acts as a read-only client of
 * the Replication system's file server.
 */
class FileReadApp : public Application {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<FileReadApp> Ptr;

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

    FileReadApp()=delete;
    FileReadApp(FileReadApp const&)=delete;
    FileReadApp& operator=(FileReadApp const&)=delete;

    ~FileReadApp() override=default;

protected:

    /**
     * @see FileReadApp::create()
     */
    FileReadApp(int argc, char* argv[]);

    /**
     * @see Application::runImpl()
     */
    int runImpl() final;

private:

    /// The name of a worker
    std::string _workerName;

    /// The name of a database
    std::string _databaseName;

    /// The name of an input file to be copied from the worker
    std::string _inFileName;

    /// The name of a local file to be created and populated with received data
    std::string _outFileName;

    /// The flag triggering (if 'true') a report on a progress of the operation
    bool _verbose = false;

    /// The maximum number of bytes to be read from a server at each request
    size_t _recordSizeBytes = 1024*1024;

    /// The data buffer for receiving data records from a file server
    std::vector<uint8_t> _buf;
};

}}} // namespace lsst::qserv::replica

#endif /* LSST_QSERV_REPLICA_FILEREADAPP_H */
