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
#ifndef LSST_QSERV_REPLICA_QSERVXROOTDSSIAPP_H
#define LSST_QSERV_REPLICA_QSERVXROOTDSSIAPP_H

// System headers
#include <memory>
#include <string>

// Qserv headers
#include "replica/apps/Application.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class QservXrootdSsiApp is performance and scalability test for the XROOTD/SSI.
 */
class QservXrootdSsiApp : public Application {
public:
    /// The pointer type for instances of the class
    typedef std::shared_ptr<QservXrootdSsiApp> Ptr;

    /**
     * The factory method is the only way of creating objects of this class
     * because of the very base class's inheritance from 'enable_shared_from_this'.
     *
     * @param argc The number of command-line arguments.
     * @param argv The vector of command-line arguments.
     */
    static Ptr create(int argc, char* argv[]);

    QservXrootdSsiApp() = delete;
    QservXrootdSsiApp(QservXrootdSsiApp const&) = delete;
    QservXrootdSsiApp& operator=(QservXrootdSsiApp const&) = delete;

    virtual ~QservXrootdSsiApp() final = default;

protected:
    /// @see Application::runImpl()
    virtual int runImpl() final;

private:
    /// @see QservXrootdSsiApp::create()
    QservXrootdSsiApp(int argc, char* argv[]);

    std::string _url;              ///< The connection URL for the XROOTD/SSI services.
    size_t _numThreads = 1;        ///< The number of threads for running the test.
    int _reportIntervalMs = 1000;  ///< An interval for reporting the performance counters.
    bool _progress = false;        ///< For periodic progress reports on the requests.
    bool _verbose = false;         ///< For detailed report on the requests.
};

}  // namespace lsst::qserv::replica

#endif /* LSST_QSERV_REPLICA_QSERVXROOTDSSIAPP_H */
