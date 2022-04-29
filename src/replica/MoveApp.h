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
#ifndef LSST_QSERV_REPLICA_MOVEAPP_H
#define LSST_QSERV_REPLICA_MOVEAPP_H

// Qserv headers
#include "replica/Application.h"

// This header declarations
namespace lsst { namespace qserv { namespace replica {

/**
 * Class MoveApp implements a tool which runs the rebalancing algorithm
 * in a scope of a database family.
 */
class MoveApp : public Application {
public:
    typedef std::shared_ptr<MoveApp> Ptr;

    /**
     * The factory method is the only way of creating objects of this class
     * because of the very base class's inheritance from 'enable_shared_from_this'.
     *
     * @param argc the number of command-line arguments
     * @param argv the vector of command-line arguments
     */
    static Ptr create(int argc, char* argv[]);

    MoveApp() = delete;
    MoveApp(MoveApp const&) = delete;
    MoveApp& operator=(MoveApp const&) = delete;

    ~MoveApp() final = default;

protected:
    /// @see Application::runImpl()
    int runImpl() final;

private:
    /// @see MoveApp::create()
    MoveApp(int argc, char* argv[]);

    /// The name of a database family
    std::string _databaseFamily;

    /// The chunk to be affected by the operation
    unsigned int _chunk;

    /// The name of a worker which has the replica to be moved
    std::string _sourceWorker;

    /// The name of a worker where the replica will be moved (must not
    /// be the same worker as the source one)
    std::string _destinationWorker;

    /// Purge the input replica at the source worker upon a successful
    /// completion of the operation.
    bool _purge = false;

    /// The number of rows in the table of replicas (0 means no pages)
    size_t _pageSize = 20;
};

}}}  // namespace lsst::qserv::replica

#endif /* LSST_QSERV_REPLICA_MOVEAPP_H */
