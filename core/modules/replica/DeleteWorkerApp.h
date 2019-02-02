/*
 * LSST Data Management System
 * Copyright 2018 LSST Corporation.
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
#ifndef LSST_QSERV_REPLICA_DELETEWORKERAPP_H
#define LSST_QSERV_REPLICA_DELETEWORKERAPP_H

// Qserv headers
#include "replica/Application.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class DeleteWorkerApp implements a tool which disables a worker
 * from any active use in a replication setup. All chunks hosted by
 * the worker node will be distributed across the cluster.
 */
class DeleteWorkerApp: public Application {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<DeleteWorkerApp> Ptr;

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

    DeleteWorkerApp()=delete;
    DeleteWorkerApp(DeleteWorkerApp const&)=delete;
    DeleteWorkerApp& operator=(DeleteWorkerApp const&)=delete;

    ~DeleteWorkerApp() override=default;

protected:

    /**
     * @see DeleteWorkerApp::create()
     */
    DeleteWorkerApp(int argc, char* argv[]);

    /**
     * @see Application::runImpl()
     */
    int runImpl() final;

private:

    /// The name of a worker to be deleted
    std::string _workerName;

    /// Permanently delete a worker from the Configuration
    bool _permanentDelete = false;

    /// The number of rows in the table of replicas (0 means no pages)
    size_t _pageSize = 20;
};

}}} // namespace lsst::qserv::replica

#endif /* LSST_QSERV_REPLICA_DELETEWORKERAPP_H */
