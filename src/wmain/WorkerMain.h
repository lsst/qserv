// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2015 LSST Corporation.
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

#ifndef LSST_QSERV_WMAIN_WORKERMAIN_H
#define LSST_QSERV_WMAIN_WORKERMAIN_H

// System headers
#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>

// Third-party headers

namespace lsst::qserv::util {
class FileMonitor;
}  // namespace lsst::qserv::util

namespace lsst::qserv::wcontrol {
class Foreman;
}  // namespace lsst::qserv::wcontrol

namespace lsst::qserv::wcomms {
class HttpSvc;
}  // namespace lsst::qserv::wcomms

namespace lsst::qserv::wmain {

class WorkerMain {
public:
    using Ptr = std::shared_ptr<WorkerMain>;

    static std::weak_ptr<WorkerMain> get() { return _globalWorkerMain; }
    static Ptr setup();

    ~WorkerMain();

    void terminate();
    void waitForTerminate();

private:
    WorkerMain();

    /// Weak pointer to allow global access without complicating lifetime issues.
    static std::weak_ptr<WorkerMain> _globalWorkerMain;

    /// There should only be one WorkerMain, this prevents more than
    /// one from being created.
    static std::atomic<bool> _setup;

    /// &&& originally from xrdsvc::XrdName x; getName() from std::getenv("XRDNAME");
    std::string _workerName{"worker"};  // &&& set on command line, config file ???

    // The Foreman contains essential structures for adding and running tasks.
    std::shared_ptr<wcontrol::Foreman> _foreman;

    /// Reloads the log configuration file on log config file change.
    std::shared_ptr<util::FileMonitor> _logFileMonitor;

    /// The HTTP server processing worker management requests.
    std::shared_ptr<wcomms::HttpSvc> _controlHttpSvc;

    /// Set to true when the program should terminate.
    bool _terminate = false;
    std::mutex _terminateMtx;
    std::condition_variable _terminateCv;
};

}  // namespace lsst::qserv::wmain
#endif  // LSST_QSERV_WMAIN_WORKERMAIN_H
