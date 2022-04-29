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
#ifndef LSST_QSERV_REPLICA_VERIFYAPP_H
#define LSST_QSERV_REPLICA_VERIFYAPP_H

// Qserv headers
#include "replica/Application.h"

// This header declarations
namespace lsst { namespace qserv { namespace replica {

/**
 * Class VerifyApp implements a tool which runs the replica verification algorithm
 * for all known replicas across all ENABLED workers.
 */
class VerifyApp : public Application {
public:
    typedef std::shared_ptr<VerifyApp> Ptr;

    /**
     * The factory method is the only way of creating objects of this class
     * because of the very base class's inheritance from 'enable_shared_from_this'.
     *
     * @param argc the number of command-line arguments
     * @param argv the vector of command-line arguments
     */
    static Ptr create(int argc, char* argv[]);

    VerifyApp() = delete;
    VerifyApp(VerifyApp const&) = delete;
    VerifyApp& operator=(VerifyApp const&) = delete;

    ~VerifyApp() final = default;

protected:
    /// @see Application::runImpl()
    int runImpl() final;

private:
    /// @see VerifyApp::create()
    VerifyApp(int argc, char* argv[]);

    /// The maximum number of replicas to be processed simultaneously
    size_t _maxReplicas = 1;

    /// Automatically compute and store in the database check/control sums of
    /// the replica's files.
    bool _computeCheckSum = false;
};

}}}  // namespace lsst::qserv::replica

#endif /* LSST_QSERV_REPLICA_VERIFYAPP_H */
