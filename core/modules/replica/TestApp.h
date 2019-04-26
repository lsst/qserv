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
#ifndef LSST_QSERV_REPLICA_TESTAPP_H
#define LSST_QSERV_REPLICA_TESTAPP_H

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
 * Class TestApp is meant for very basic testing of the functionality of
 * the command line applications framework.
 */
class TestApp: public Application {
public:

    /// The pointer type for instances of the class
    using Ptr=std::shared_ptr<TestApp>;

    /**
     * The factory method is the only way of creating objects of this class
     * because of the very base inheritance from 'enable_shared_from_this'.
     *
     * @param argc
     *   the number of command-line arguments
     *
     * @param argv
     *   the vector of command-line arguments
     */
    static Ptr create(int argc, char* argv[]);

    // Default construction and copy semantics are prohibited

    TestApp() = delete;
    TestApp(TestApp const&) = delete;
    TestApp& operator=(TestApp const&) = delete;

    ~TestApp() override = default;

protected:

    /// @see TestApp::create()
    TestApp(int argc, char* argv[]);

    /**
     * Implement a user-defined sequence of actions here.
     *
     * @return completion status
     */
    int runImpl() final;

private:

    // Variables for storing captured values of the command line parameters
    // after the Parser successfully finishes parsing them.

    std::string _cmd;
    int _p1;
    std::string _p11;
    std::string _p2;
    unsigned int _o1;
    unsigned int _o2;
    unsigned int _o11;
    std::string _o12;
    bool _verbose;
    bool _f21;
};

}}} // namespace lsst::qserv::replica

#endif /* LSST_QSERV_REPLICA_TESTAPP_H */
