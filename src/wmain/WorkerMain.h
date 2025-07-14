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

    /// Returns a pointer to the global instance.
    /// @throw std::runtime_error if global pointer is null.
    static std::shared_ptr<WorkerMain> get();
    static Ptr setup();

    ~WorkerMain();

    std::string getName() const { return _name; }

    /// End WorkerMain, calling this multiple times is harmless.
    void terminate();
    void waitForTerminate();

private:
    WorkerMain();

    void _registryUpdateLoop();
    std::thread _registryUpdateThread;

    /// Weak pointer to allow global access without complicating lifetime issues.
    static std::weak_ptr<WorkerMain> _globalWorkerMain;

    /// There should only be one WorkerMain, this prevents more than
    /// one from being created.
    static std::atomic<bool> _setup;

    /// Worker name, used in some database lookups.
    std::string _name{"worker"};

    // The Foreman contains essential structures for adding and running tasks.
    std::shared_ptr<wcontrol::Foreman> _foreman;

    /// Reloads the log configuration file on log config file change.
    std::shared_ptr<util::FileMonitor> _logFileMonitor;

    /// The HTTP server processing worker management requests.
    std::shared_ptr<wcomms::HttpSvc> _controlHttpSvc;

    /// Set to true when the program should terminate.
    std::atomic<bool> _terminate{false};
    std::mutex _terminateMtx;
    std::condition_variable _terminateCv;
};

}  // namespace lsst::qserv::wmain
#endif  // LSST_QSERV_WMAIN_WORKERMAIN_H
