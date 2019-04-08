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
#ifndef LSST_QSERV_REPLICA_QSERVWORKERAPP_H
#define LSST_QSERV_REPLICA_QSERVWORKERAPP_H

// System headers
#include <memory>
#include <string>

// Qserv headers
#include "replica/Application.h"
#include "replica/ReplicaInfo.h"

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class QservWorkerApp represents a command-line tool for operations
 * with Qserv workers.
 */
class QservWorkerApp : public Application {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<QservWorkerApp> Ptr;

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

    QservWorkerApp()=delete;
    QservWorkerApp(QservWorkerApp const&)=delete;
    QservWorkerApp& operator=(QservWorkerApp const&)=delete;

    ~QservWorkerApp() override=default;

protected:

    /// @see Application::runImpl()
    int runImpl() final;

private:

    /// @see QservWorkerApp::create()
    QservWorkerApp(int argc, char* argv[]);

    /**
     * Read and parse a space/newline separated stream of pairs from the input
     * file and fill replica entries into the collection. Each pair has
     * the following format:
     *
     *   <database>:<chunk>
     *
     * For example:
     *
     *   LSST:123 LSST:124 LSST:23456
     *   LSST:0
     *
     * @param replicas
     *   collection to be initialized
     */
    void _readInFile(QservReplicaCollection& replicas) const;

    /**
      * Print a collection of replicas
      *
      * @param collection
      */
    void _dump(QservReplicaCollection const& collection) const;


    /// The name of a command (the first mandatory parameter)
    std::string _command;

    /// The name of a worker
    std::string _workerName;

    /// The name of a database
    std::string _databaseName;

    /// The name of a database family
    std::string _familyName;

    /// The number of a chunk
    unsigned int _chunkNumber = 0;

    /// The name of a file with space-separated pairs of <database>:<chunk>
    std::string _inFileName;

    /// The flag forcing the remote services to proceed with requested
    /// replica removal regardless of the replica usage status
    bool _forceRemove = false;

    /// Limit a scope of operations to a subset of chunks which are in use 
    bool _inUseOnly = false;

    /// The flag (if set) for "printing the vertical separator when displaying tabular data in reports
    bool _verticalSeparator = false;

    /// The number of rows in the table of replicas (0 means no pages)
    size_t _pageSize = 0;
};

}}} // namespace lsst::qserv::replica

#endif /* LSST_QSERV_REPLICA_QSERVWORKERAPP_H */
