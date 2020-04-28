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
  * @brief test ChunkLocker
  */

// System headers
#include <string>

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "replica/ApplicationTypes.h"

// Boost unit test header
#define BOOST_TEST_MODULE ApplicationParser
#include "boost/test/included/unit_test.hpp"

using namespace std;
namespace test = boost::test_tools;
using namespace lsst::qserv::replica::detail;

namespace {
string const descr  = "Unit test application for Parser";
}
BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(ApplicationParser1) {

    LOGS_INFO("ApplicationParser1 test begins");

    // Simple test of the non-throwing constructor

    int const         argc   = 1;
    char const* const argv[] = {"testApplicationParser"};

    BOOST_REQUIRE_NO_THROW({
        Parser parser(argc, argv, descr);
    });

    LOGS_INFO("ApplicationParser1 test ends");
}


BOOST_AUTO_TEST_CASE(ApplicationParser2) {

    LOGS_INFO("ApplicationParser2 test begins");

    // Simple use case - no 'commands' configured. Still, no exceptions
    //
    // Syntax tested:
    //
    //  <r1>  <r2>  <r3>  <r4>  <r5>  <r6> [<o1>] [<o2>] [--o3=<val>]  [--f1]  [--f2]  [--no-f3]
    //
    // Values of the input arguments tested:
    //
    //  "1"   "2"   "3"   4.4   5.5   1    "o1"          --o3="o3"      --f1            --no-f3

    string       r1;
    int          r2 = -1;
    unsigned int r3 =  1;
    float        r4 = -1.;
    double       r5 = -1.;
    bool         r6 = false;
    string       o1;
    string       o2;
    string       o3;
    bool         f1 = false;
    bool         f2 = false;
    bool         f3 = true;

    int const         argc   = 11;
    char const* const argv[] = {
        "testApplicationParser",
        "1", "2", "3", "4.4", "5.5", "1",   // required
        "o1",                               // optional
        "--o3=o3",                          // options
        "--f1",                             // flags
        "--no-f3"                           // flags
    };

    BOOST_REQUIRE_NO_THROW({
        Parser parser(argc, argv, descr);

        parser.required("r1", "required parameter r1", r1)
              .required("r2", "required parameter r2", r2)
              .required("r3", "required parameter r3", r3)
              .required("r4", "required parameter r4", r4)
              .required("r5", "required parameter r5", r5)
              .required("r6", "required parameter r6", r6)
              .optional("o1", "optional parameter o1", o1)
              .optional("o2", "optional parameter o2", o2)
              .option(  "o3", "option o3",             o3)
              .flag(    "f1", "flag f1",               f1)
              .flag(    "f2", "flag f2",               f2)
              .reversedFlag("no-f3", "reversed flag f3", f3);
        parser.parse();

        LOGS_INFO("ApplicationParser: input strings  " + parser.serializeArguments());
        LOGS_INFO("ApplicationParser: parsed values  r1=" + r1 +
                  " r2=" + to_string(r2) +
                  " r3=" + to_string(r3) +
                  " r4=" + to_string(r4) +
                  " r5=" + to_string(r5) +
                  " r6=" + string(r6 ? "true" : "false") +
                  " o1=" + o1 +
                  " o2=" + o2 +
                  " o3=" + o3 +
                  " f1=" + string(f1 ? "true" : "false") +
                  " f2=" + string(f2 ? "true" : "false") +
                  " f3=" + string(f3 ? "true" : "false"));
        
        BOOST_CHECK_EQUAL(r1, "1");
        BOOST_CHECK_EQUAL(r2, 2);
        BOOST_CHECK_EQUAL(r3, 3u);
        BOOST_CHECK_EQUAL(r4, 4.4f);
        BOOST_CHECK_EQUAL(r5, 5.5);
        BOOST_CHECK(r6);
        BOOST_CHECK_EQUAL(o1, "o1");
        BOOST_CHECK(o2.empty());
        BOOST_CHECK_EQUAL(o3, "o3");
        BOOST_CHECK(f1);
        BOOST_CHECK(not f2);
        BOOST_CHECK(not f3);
    });
    
    LOGS_INFO("ApplicationParser2 test ends");
}


BOOST_AUTO_TEST_CASE(ApplicationParser3) {

    LOGS_INFO("ApplicationParser3 test begins");

    // Test for expected exceptions

    bool isHelp = false;
    bool isEmpty = false;

    int const         argc   = 1;
    char const* const argv[] = {"testApplicationParser"};

    Parser parser(argc, argv, descr);

    BOOST_CHECK_THROW(parser.flag("help", "reserved argument name", isHelp),  invalid_argument);
    BOOST_CHECK_THROW(parser.flag("",     "empty    argument name", isEmpty), invalid_argument);

    LOGS_INFO("ApplicationParser3 test ends");
}


BOOST_AUTO_TEST_CASE(ApplicationParser4) {

    LOGS_INFO("ApplicationParser4 test begins");

    string command;

    // Required by all commands

    string       r1;
    int          r2 = -1;
    unsigned int r3 =  1;
    float        r4 = -1.;
    double       r5 = -1.;
    bool         r6 = false;
    string       o1;

    // Running the Parser in the 'commands' mode for command "C1"
    //
    // Syntax tested:
    //
    //  C1   <r1>  <r2>  <r3>  <r4>  <r5>  <r6> <c1r1>  [<o1>]  [<c1o1>]
    //  C2   <r1>  <r2>  <r3>  <r4>  <r5>  <r6>         [<o1>]
    //  C3   <r1>  <r2>  <r3>  <r4>  <r5>  <r6>         [<o1>]
    //
    // Values of the input arguments tested:
    //
    //  "C1" "1"   "2"   "3"   4.4   5.5   1    "c1r1"  "o1"    11

    string c1r1;
    int    c1o1 = -1;

    int const         argcC1   = 11;
    char const* const argvC1[] = {
        "testApplicationParser",
        "C1",                               // command
        "1", "2", "3", "4.4", "5.5", "1",   // required by all commands
        "c1r1",                             // required by the command
        "o1",                               // optional for all commands
        "11"                                // optional for the command
    };

    BOOST_REQUIRE_NO_THROW({
        Parser parser(argcC1, argvC1, descr);

        parser.commands("command", {"C1", "C2", "C3"}, command);
        parser.required("r1", "required parameter r1", r1)
              .required("r2", "required parameter r2", r2)
              .required("r3", "required parameter r3", r3)
              .required("r4", "required parameter r4", r4)
              .required("r5", "required parameter r5", r5)
              .required("r6", "required parameter r6", r6)
              .optional("o1", "optional parameter o1", o1);
        parser.command("C1")
              .description("This is the first command")
              .required("c1r1", "required parameter c1r1 of command C1", c1r1)
              .optional("c1o1", "optional parameter c1o1 of command C1", c1o1);
        parser.parse();

        LOGS_INFO("ApplicationParser: input strings  " + parser.serializeArguments());
        LOGS_INFO("ApplicationParser: parsed values  command=" + command +
                  " r1="   + r1 +
                  " r2="   + to_string(r2) +
                  " r3="   + to_string(r3) +
                  " r4="   + to_string(r4) +
                  " r5="   + to_string(r5) +
                  " r6="   + string(r6 ? "true" : "false") +
                  " c1r1=" + c1r1 +
                  " o1="   + o1 +
                  " c1o1=" + to_string(c1o1));
        
        BOOST_CHECK_EQUAL(command, "C1");
        BOOST_CHECK_EQUAL(r1, "1");
        BOOST_CHECK_EQUAL(r2, 2);
        BOOST_CHECK_EQUAL(r3, 3u);
        BOOST_CHECK_EQUAL(r4, 4.4f);
        BOOST_CHECK_EQUAL(r5, 5.5);
        BOOST_CHECK(r6);
        BOOST_CHECK_EQUAL(c1r1, "c1r1");
        BOOST_CHECK_EQUAL(o1,   "o1");
        BOOST_CHECK_EQUAL(c1o1, 11);
    });

    // Running the Parser in the 'commands' mode for command "C2"
    //
    // Syntax tested:
    //
    //  C1   <r1>  <r2>  <r3>  <r4>  <r5>  <r6> <c1r1>  [<o1>]
    //  C2   <r1>  <r2>  <r3>  <r4>  <r5>  <r6> <c2r1>  [<o1>]  [--c2f1]
    //  C3   <r1>  <r2>  <r3>  <r4>  <r5>  <r6>         [<o1>]
    //
    // Values of the input arguments tested:
    //
    //  "C2" "1"   "2"   "3"   4.4   5.5   1    "c2r1"  "o1"    --c2f1

    string c2r1;
    bool   c2f1 = false;

    int const         argcC2   = 11;
    char const* const argvC2[] = {
        "testApplicationParser",
        "C2",                               // command
        "1", "2", "3", "4.4", "5.5", "1",   // required by all commands
        "c2r1",                             // required by the command
        "o1",                               // optional for all commands
        "--c2f1"                            // flag for the command
    };

    BOOST_REQUIRE_NO_THROW({
        Parser parser(argcC2, argvC2, descr);

        parser.commands("command", {"C1", "C2", "C3"}, command);
        parser.required("r1", "required parameter r1", r1)
              .required("r2", "required parameter r2", r2)
              .required("r3", "required parameter r3", r3)
              .required("r4", "required parameter r4", r4)
              .required("r5", "required parameter r5", r5)
              .required("r6", "required parameter r6", r6)
              .optional("o1", "optional parameter o1", o1);
        parser.command("C2")
              .description("This is the second command")
              .required("c2r1", "required parameter c2r1 of command C2", c2r1)
              .flag(    "c2f1", "flag c2f1 of command C2",               c2f1);
        parser.parse();

        LOGS_INFO("ApplicationParser: input strings  " + parser.serializeArguments());
        LOGS_INFO("ApplicationParser: parsed values  command=" + command +
                  " r1="   + r1 +
                  " r2="   + to_string(r2) +
                  " r3="   + to_string(r3) +
                  " r4="   + to_string(r4) +
                  " r5="   + to_string(r5) +
                  " r6="   + string(r6 ? "true" : "false") +
                  " c2r1=" + c2r1 +
                  " o1="   + o1 +
                  " c2f1=" + string(c2f1 ? "true" : "false"));

        BOOST_CHECK_EQUAL(command, "C2");
        BOOST_CHECK_EQUAL(r1, "1");
        BOOST_CHECK_EQUAL(r2, 2);
        BOOST_CHECK_EQUAL(r3, 3u);
        BOOST_CHECK_EQUAL(r4, 4.4f);
        BOOST_CHECK_EQUAL(r5, 5.5);
        BOOST_CHECK(r6);
        BOOST_CHECK_EQUAL(c2r1, "c2r1");
        BOOST_CHECK_EQUAL(o1,   "o1");
        BOOST_CHECK(c2f1);
    });

    // Running the Parser in the 'commands' mode to test '--help'
    //
    // Syntax tested:
    //
    //  C1   <r1>  <r2>  <r3>  <r4>  <r5>  <r6> <c1r1>  [<o1>]  [<c1o1>]
    //  C2   <r1>  <r2>  <r3>  <r4>  <r5>  <r6> <c2r1>  [<o1>]            [--c2f1]
    //  C3   <r1>  <r2>  <r3>  <r4>  <r5>  <r6>         [<o1>]  [<c3o1>]
    //
    // Values of the input arguments tested:
    //
    //  "C3"  1    2     3     4.4   5.5   1             "o1"   31 

    int c3o1 = -1;

    int const         argcC3   = 10;
    char const* const argvC3[] = {
        "testApplicationParser",
        "C3",                               // command
        "1", "2", "3", "4.4", "5.5", "1",   // required by all commands
        "o1",                               // optional for all commands
        "31"                                // optional for all command
    };

    BOOST_REQUIRE_NO_THROW({
        Parser parser(argcC3, argvC3, descr);

        parser.commands("command", {"C1", "C2", "C3"}, command);
        parser.required("r1", "required parameter r1", r1)
              .required("r2", "required parameter r2", r2)
              .required("r3", "required parameter r3", r3)
              .required("r4", "required parameter r4", r4)
              .required("r5", "required parameter r5", r5)
              .required("r6", "required parameter r6", r6)
              .optional("o1", "optional parameter o1", o1);
        parser.command("C1")
              .description("This is the first command")
              .required("c1r1", "required parameter c1r1 of command C1", c1r1)
              .optional("c1o1", "optional parameter c1o1 of command C1", c1o1);
        parser.command("C2")
              .description("This is the second command")
              .required("c2r1", "required parameter c2r1 of command C2", c2r1)
              .optional("c2f1", "flag c2f1 of command C1", c2f1);
        parser.command("C3")
              .description("This is the third command")
              .optional("c3o1", "optional parameter c3o1 of command C3", c3o1);
        parser.parse();

        LOGS_INFO("ApplicationParser: input strings  " + parser.serializeArguments());
        LOGS_INFO("ApplicationParser: parsed values  command=" + command +
                  " r1="   + r1 +
                  " r2="   + to_string(r2) +
                  " r3="   + to_string(r3) +
                  " r4="   + to_string(r4) +
                  " r5="   + to_string(r5) +
                  " r6="   + string(r6 ? "true" : "false") +
                  " o1="   + o1 +
                  " c3o1=" + to_string(c3o1));

        BOOST_CHECK_EQUAL(command, "C3");
        BOOST_CHECK_EQUAL(r1, "1");
        BOOST_CHECK_EQUAL(r2, 2);
        BOOST_CHECK_EQUAL(r3, 3u);
        BOOST_CHECK_EQUAL(r4, 4.4f);
        BOOST_CHECK_EQUAL(r5, 5.5);
        BOOST_CHECK(r6);
        BOOST_CHECK_EQUAL(o1,   "o1");
        BOOST_CHECK_EQUAL(c3o1, 31);
    });

    // Running the Parser in the 'commands' mode to test '--help'
    //
    // Syntax tested:
    //
    //  C1   <r1>  <r2>  <r3>  <r4>  <r5>  <r6> <c1r1>  [<o1>]  [<c1o1>]
    //  C2   <r1>  <r2>  <r3>  <r4>  <r5>  <r6> <c2r1>  [<o1>]            [--c2f1]
    //  C3   <r1>  <r2>  <r3>  <r4>  <r5>  <r6>         [<o1>]  [<c3o1>]
    //
    // Values of the input arguments tested:
    //
    //  "--help"

    int const         argcHelp   = 2;
    char const* const argvHelp[] = {
        "testApplicationParser",
        "--help"
    };
    BOOST_REQUIRE_NO_THROW({
        Parser parser(argcHelp, argvHelp, descr);

        parser.commands("command", {"C1", "C2", "C3"}, command);
        parser.required("r1", "required parameter r1", r1)
              .required("r2", "required parameter r2", r2)
              .required("r3", "required parameter r3", r3)
              .required("r4", "required parameter r4", r4)
              .required("r5", "required parameter r5", r5)
              .required("r6", "required parameter r6", r6)
              .optional("o1", "optional parameter o1", o1);
        parser.command("C1")
              .description("This is the first command")
              .required("c1r1", "required parameter c1r1 of command C1", c1r1)
              .optional("c1o1", "optional parameter c1o1 of command C1", c1o1);
        parser.command("C2")
              .description("This is the second command")
              .required("c2r1", "required parameter c2r1 of command C2", c2r1)
              .optional("c2f1", "flag c2f1 of command C1", c2f1);
        parser.command("C3")
              .description("This is the third command")
              .optional("c3o1", "optional parameter c3o1 of command C3", c3o1);

        int status = parser.parse();

        BOOST_CHECK_EQUAL(status, Parser::Status::HELP_REQUESTED);
    });

    LOGS_INFO("ApplicationParser4 test ends");
}

BOOST_AUTO_TEST_SUITE_END()
