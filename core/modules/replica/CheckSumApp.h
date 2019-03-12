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
#ifndef LSST_QSERV_REPLICA_CHECKSUMAPP_H
#define LSST_QSERV_REPLICA_CHECKSUMAPP_H

// System headers
#include <string>
#include <memory>

// Qserv headers
#include "replica/Application.h"


// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class CheckSumApp implements calculates and prints a checksum of a file.
 */
class CheckSumApp : public Application {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<CheckSumApp> Ptr;

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

    CheckSumApp() = delete;
    CheckSumApp(CheckSumApp const&) = delete;
    CheckSumApp& operator=(CheckSumApp const&) = delete;

    ~CheckSumApp() override = default;

protected:

    /**
     * @see CheckSumApp::create()
     */
    CheckSumApp(int argc, char* argv[]);

    /**
     * @see Application::runImpl()
     */
    int runImpl() final;

private:

    /// The name of a worker
    std::string _file;

    // Use the incremental file reader if 'true'
    bool _incremental;
};

}}} // namespace lsst::qserv::replica

#endif /* LSST_QSERV_REPLICA_CHECKSUMAPP_H */
