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
#ifndef LSST_QSERV_REPLICA_CHUNKSAPP_H
#define LSST_QSERV_REPLICA_CHUNKSAPP_H

// System headers
#include <string>

// Qserv headers
#include "replica/Application.h"

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class ChunksApp implements a tool which launches a single job Controller in order
 * to acquire, analyze and reports chunk disposition within a database family.
 */
class ChunksApp : public Application {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<ChunksApp> Ptr;

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

    ChunksApp()=delete;
    ChunksApp(ChunksApp const&)=delete;
    ChunksApp& operator=(ChunksApp const&)=delete;

    ~ChunksApp() override=default;

protected:

    /// @see Application::runImpl()
    int runImpl() final;

private:

    /// @see ChunksApp::create()
    ChunksApp(int argc, char* argv[]);


    /// The name of a database family
    std::string _databaseFamily;

    /// The flag which if set allows selecting all workers for the operation
    bool _allWorkers = false;

    /// Do not save the replica info in the database if set to 'true'
    bool _doNotSaveReplicaInfo = false;

    /// The flag (if set) for pulling chunk disposition from Qserv workers for the combined analysis
    bool _pullQservReplicas = false;

    /// The maximum timeout for the completion of requests sent to
    /// the Replication System's and Qserv workers. The default value (0)
    /// implies using the timeout found in the Configuration.
    unsigned int _timeoutSec = 0;

    /// Dump the detailed report on the replicas if 'true'
    bool _detailedReport = false;

    /// The flag (if set) for "printing the vertical separator when displaying tabular data in reports
    bool _verticalSeparator = false;

    /// The number of rows in the table of replicas (0 means no pages)
    size_t _pageSize = 20;
};

}}} // namespace lsst::qserv::replica

#endif /* LSST_QSERV_REPLICA_CHUNKSAPP_H */
