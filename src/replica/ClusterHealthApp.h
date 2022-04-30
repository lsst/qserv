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
#ifndef LSST_QSERV_REPLICA_CLUSTERHEALTHAPP_H
#define LSST_QSERV_REPLICA_CLUSTERHEALTHAPP_H

// Qserv headers
#include "replica/Application.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class ClusterHealthApp probes and reports a status of the Replication system's
 * and Qserv workers to see if they respond within the specified (or implied)
 * timeout.
 */
class ClusterHealthApp : public Application {
public:
    typedef std::shared_ptr<ClusterHealthApp> Ptr;

    /**
     * The factory method is the only way of creating objects of this class
     * because of the very base class's inheritance from 'enable_shared_from_this'.
     *
     * @param argc the number of command-line arguments
     * @param argv the vector of command-line arguments
     */
    static Ptr create(int argc, char* argv[]);

    ClusterHealthApp() = delete;
    ClusterHealthApp(ClusterHealthApp const&) = delete;
    ClusterHealthApp& operator=(ClusterHealthApp const&) = delete;

    ~ClusterHealthApp() override = default;

protected:
    /// @see Application::runImpl()
    int runImpl() final;

private:
    /// @see ClusterHealthApp::create()
    ClusterHealthApp(int argc, char* argv[]);

    /// Extend a scope of the operation to probes all known workers instead of
    /// just the ENABLED ones.
    bool _allWorkers = false;
};

}  // namespace lsst::qserv::replica

#endif /* LSST_QSERV_REPLICA_CLUSTERHEALTHAPP_H */
