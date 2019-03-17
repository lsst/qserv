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

/**
 * qserv-replica-application-test.cc is for testing class Application which
 * is meant to take care of typical tasks in building command-line
 * tools.
 */

// System headers
#include <iostream>
#include <string>

// Qserv headers
#include "replica/Application.h"

using namespace std;
using namespace lsst::qserv::replica;

namespace {
    
class TestApplication: public Application {

public:

    /// The pointer type for instances of the class
    typedef shared_ptr<TestApplication> Ptr;

    /**
     * The factory method is the only way of creating objects of this class
     * because of the very bqse inheritance from 'enable_shared_from_this'.
     *
     * @param argc
     *   the number of command-line arguments
     *
     * @param argv
     *   the vector of command-line arguments
     */
    static Ptr create(int argc, char* argv[]) {
        return Ptr(new TestApplication(argc, argv));
    }

    // Default construction and copy semantics are prohibited

    TestApplication() = delete;
    TestApplication(TestApplication const&) = delete;
    TestApplication& operator=(TestApplication const&) = delete;

    ~TestApplication() override = default;

protected:

    /**
     * @see TestApplication::create()
     */
    TestApplication(int argc, char* argv[])
        :   Application(
                argc, argv,
                "This is a simple demo illustrating how to use class Application"
                " for constructing user applications with very little efforts spent"
                " on typical tasks.",
                true /* injectDatabaseOptions */,
                true /* boostProtobufVersionCheck */,
                true /* enableServiceProvider */
            ),
            _p2("ONE"),
            _o1(123),
            _o2(false) {

        /* Configure the parser for the following syntax:
         *
         * COMMAND1 <p1> <p11> [<o1>] [<o11>] [--o12=<v>] [--verbose]
         * COMMAND2 <p1>       [<o1>]                     [--verbose] [--f21]
         * COMMAND2 <p1>       [<o1>]                     [--verbose]
         * 
         * Note that the parser is guaranteed to run before invoking method "runImpl()".
         */
        parser().commands(
            "command",
            {"COMMAND1", "COMMAND2", "COMMAND3"},
            _cmd);

        parser().required(
            "p1",
            "description of the required parameter p1 for all commands",
            _p1);

        parser().optional(
            "o1",
            "description of the optional parameter o1 for all commands",
            _o1);

        parser().flag(
            "verbose",
            "verbose mode",
            _verbose);

        auto&& command1 = parser().command("COMMAND1");
        command1.description(
            "This is the first command"
        );
        command1.required(
            "p11",
            "description of the additional required parameter specific for the command",
            _p11
        );
        command1.optional(
            "o11",
            "description of the additional optional parameter specific for the command",
            _o11
        );
        command1.option(
            "o12",
            "description of the additional option specific to the command",
            _o12
        );

        parser().command("COMMAND2").description(
            "This is the second command"
        );
        parser().command("COMMAND2").flag(
           "f21",
           "description of the additional flag specific to the command",
           _f21
        );
    }

    /**
     * Implement a user-defined sequence of actions here.
     *
     * @return completion status
     */
    int runImpl() final {
        cout
            << "Hello from TestApplication:\n"
            << "      cmd: " << _cmd << "\n"
            << "      p11: " << _p11 << "\n"
            << "       p1: " << _p1  << "\n"
            << "       p2: " << _p2  << "\n"
            << "       o1: " << _o1  << "\n"
            << "       o2: " << (_o2      ? "true" : "false") << "\n"
            << "      o11: " << _o11 << "\n"
            << "      o12: " << _o12 << "\n"
            << "      f21: " << (_f21     ? "true" : "false") << "\n"
            << "  verbose: " << (_verbose ? "true" : "false") << endl;
        return 0;
    }

private:

    // Values of the command line parameters

    string  _cmd;
    int     _p1;
    string  _p11;
    string  _p2;
    unsigned int _o1;
    unsigned int _o2;
    unsigned int _o11;
    string  _o12;
    bool    _verbose;
    bool    _f21;
};
} /// namespace

int main(int argc, char* argv[]) {
    try {
        auto const app = ::TestApplication::create(argc, argv);
        return app->run();
    } catch (exception const& ex) {
        cerr << "main()  the application failed, exception: " << ex.what() << endl;
        return 1;
    }
}
