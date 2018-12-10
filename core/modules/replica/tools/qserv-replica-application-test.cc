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

using namespace lsst::qserv::replica;

namespace {
    
class TestApplication
    :   public Application {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<TestApplication> Ptr;

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
    static Ptr create(int argc,
                      const char* const argv[]) {
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
    TestApplication(int argc,
                    const char* const argv[])
        :   Application(
                argc,
                argv,
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

        // Configure the parser of the command-line arguments. Note that
        // the parser is guaranteed to run before invoking method "runImpl()".

        parser().required(
            "p1",
            "The first positional parameter description",
            _p1
        ).optional(
            "p2",
            "The second positional parameter description. Note, this"
            " parameter is optional, and it allows a limited set of"
            " values: 'ONE', 'TWO' or 'THREE'",
            _p2,
            {"ONE","TWO","THREE"}
        ).option(
            "o1",
            "The first option description",
            _o1
        ).option(
            "o2",
            "The 'bool' option",
            _o2
        ).flag(
            "verbose",
            "verbose mode",
            _verbose
        );

#if 0
        /* The proposed extension to the command line parser to allow
         * the following syntax (as per code example)
         *
         * COMMAND1 <p1> <p11> [<o1>] [<o11>] [--o11=<v>] [--f1]
         * COMMAND2 <p1>       [<o1>]                     [--f1] [--f21]
         * COMMAND2 <p3>       [<o1>]                     [--f1]
         */
        std::string cmd;
        parser({"COMMAND1","COMMAND2","COMMAND3"}, cmd)
            .required("p1", "description of the required parameter p1 for all commands", p1)
            .optional("o1", "description of the optional parameter o1 for all commands", o1)
            .flag("f1", "description of f1", f1);

        command("COMMAND1")
            .required("p11", "description of the additional required parameter specific for the command", p11)
            .optional("o11", "description of the additional optional parameter specific for the command", o11)
            .option("o11", "description of the additional option specific to the command", o11);

        command("COMMAND2")
            .flag("f21", "description of the additional flag specific to the command", f21);
#endif
    }

    /**
     * Implement a user-defined sequence of actions here.
     *
     * @return completion status
     */
    int runImpl() final {
        std::cout
            << "Hello from TestApplication:\n"
            << "       p1: " << _p1 << "\n"
            << "       p2: " << _p2 << "\n"
            << "       o1: " << _o1 << "\n"
            << "       o2: " << (_o2      ? "true" : "false") << "\n"
            << "  verbose: " << (_verbose ? "true" : "false") << std::endl;
        return 0;
    }

private:

    // Values of the command line parameters

    int _p1;
    std::string _p2;
    unsigned int _o1;
    bool _o2;
    bool _verbose;
};
} /// namespace

int main(int argc, const char* const argv[]) {
    try {
        auto app = ::TestApplication::create(argc, argv);
        return app->run();
    } catch (std::exception const& ex) {
        std::cerr << "main()  the application failed, exception: " << ex.what() << std::endl;
    }
    return 1;
}
