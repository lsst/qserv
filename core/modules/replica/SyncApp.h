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
#ifndef LSST_QSERV_REPLICA_SYNCAPP_H
#define LSST_QSERV_REPLICA_SYNCAPP_H

// Qserv headers
#include "replica/Application.h"

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class SyncApp implements a tool which synchronizes
 * collections of chunks at the Qserv workers with what the Replication
 * system sees as "good" chunks in the data directories.
 */
class SyncApp : public Application {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<SyncApp> Ptr;

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

    SyncApp()=delete;
    SyncApp(SyncApp const&)=delete;
    SyncApp& operator=(SyncApp const&)=delete;

    ~SyncApp() final=default;

protected:

    /// @see Application::runImpl()
    int runImpl() final;

private:

    /// @see SyncApp::create()
    SyncApp(int argc, char* argv[]);

    /// The name of a database family affected by the operation
    std::string _databaseFamily;

    /// The maximum timeout for the operations with workers
    unsigned int _timeoutSec = 0;

    /// The flag forcing the remote services to proceed with requested
    /// chunk updates regardless of the chunk usage status
    bool _force = false;
};

}}} // namespace lsst::qserv::replica

#endif /* LSST_QSERV_REPLICA_SYNCAPP_H */
