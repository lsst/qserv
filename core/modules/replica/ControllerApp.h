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
#ifndef LSST_QSERV_REPLICA_CONTROLLERAPP_H
#define LSST_QSERV_REPLICA_CONTROLLERAPP_H

// System headers
#include <string>

// Qserv headers
#include "replica/Application.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class ControllerApp implements a tool for testing all known types of
 * the Controller requests.
 */
class ControllerApp : public Application {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<ControllerApp> Ptr;

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

    ControllerApp()=delete;
    ControllerApp(ControllerApp const&)=delete;
    ControllerApp& operator=(ControllerApp const&)=delete;

    ~ControllerApp() override=default;

protected:

    /// @see Application::runImpl()
    int runImpl() final;

private:

    /// @see ControllerApp::create()
    ControllerApp(int argc, char* argv[]);


    /// The type of a request
    std::string _request;

    /// The type of a request affected by the STATUS and STOP requests
    std::string _affectedRequest;

    /// The name of a worker which will execute a request
    std::string _workerName;

    /// The name of a source worker for the replication operation
    std::string _sourceWorkerName;

    /// The name of database
    std::string _databaseName;

    /// An identifier of a request for operations over known requests
    std::string _affectedRequestId;

    /// The number of a chunk
    unsigned int _chunkNumber;

    /// The data string to be sent to a worker in the ECHO request
    std::string _echoData;

    /// The optional delay (milliseconds) to be made by a worker before replying
    /// to the ECHO requests
    uint64_t _echoDelayMilliseconds;

    /// An SQL query to be executed by a worker
    std::string _sqlQuery;

    /// A database user for establishing a connection with the worker's database
    std::string _sqlUser;

    /// A database password for establishing a connection with the worker's database
    std::string _sqlPassword;

    /// The optional limit for the total number f rows to be pulled from a result
    /// set when executing queries against the worker's database. The default value
    /// of 0 won't enforce any such limit.
    uint64_t _sqlMaxRows;

    /// The number of rows in the table of a query result set (0 means no pages)
    size_t _sqlPageSize = 20;

    /// The optional (milliseconds) to wait before cancelling (if the number of not 0)
    /// the earlier made request.
    uint64_t _cancelDelayMilliseconds;

    /// The priority level of a request
    int  _priority;

    /// Do not track requests waiting before they finish
    bool _doNotTrackRequest = false;

    /// Allow requests which duplicate the previously made one. This applies
    /// to requests which change the replica disposition at a worker, and only
    /// for those requests which are still in the worker's queues.
    bool _allowDuplicates = false;
    
    /// Do not save the replica info in the database if set to 'true'
    bool _doNotSaveReplicaInfo = false;

    /// Automatically compute and store in the database check/control sums of
    /// the replica's files.
    bool _computeCheckSum  = false;

    /// Print 'heartbeats' while waiting before an on-going request finishes
    bool _enableHeartbeat;
};

}}} // namespace lsst::qserv::replica

#endif /* LSST_QSERV_REPLICA_CONTROLLERAPP_H */