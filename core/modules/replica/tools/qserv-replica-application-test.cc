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
 * is meant to take care of typical mandane tasks in building command-line
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

    TestApplication(int argc,
                    const char* const argv[])
        :   Application(
                argc,
                argv,
                "This is a simple demo illustrating how to use class Application"
                " for constructing user applications with very little efforts spent"
                " on mandane tasks.",
                true /* injectHelpOption */,
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
    }

protected:

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

    int          _p1;
    std::string  _p2;
    unsigned int _o1;
    bool         _o2;
    bool         _verbose;
};
} /// namespace

int main(int argc,
         const char* const argv[]) {

    try {
        ::TestApplication app(argc, argv);
        return app.run();
    } catch (std::exception const& ex) {
        std::cerr << "the application failed, exception: " << ex.what() << std::endl;
    }
    return 1;
}
