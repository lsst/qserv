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
#ifndef LSST_QSERV_REPLICA_REBALANCEAPP_H
#define LSST_QSERV_REPLICA_REBALANCEAPP_H

// Qserv headers
#include "replica/Application.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class RebalanceApp implements a tool which runs the rebalancing algorithm
 * in a scope of a database family.
 */
class RebalanceApp : public Application {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<RebalanceApp> Ptr;

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

    RebalanceApp()=delete;
    RebalanceApp(RebalanceApp const&)=delete;
    RebalanceApp& operator=(RebalanceApp const&)=delete;

    ~RebalanceApp() override=default;

protected:

    /**
     * @see Application::runImpl()
     */
    int runImpl() final;

private:

    /**
     * @see RebalanceApp::create()
     */
    RebalanceApp(int argc, char* argv[]);

private:

    /// The name of a database family
    std::string _databaseFamily;

    /// Do not make any changes to chunk disposition. Just produce and print
    /// an estimated re-balancing plan.
    bool _estimateOnly = false;

    /// The number of rows in the table of replicas (0 means no pages)
    size_t _pageSize = 20;
};

}}} // namespace lsst::qserv::replica

#endif /* LSST_QSERV_REPLICA_REBALANCEAPP_H */
