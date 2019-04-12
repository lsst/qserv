// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2011-2018 LSST Corporation.
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
#ifndef LSST_QSERV_WPUBLISH_TEST_ECHO_COMMAND_H
#define LSST_QSERV_WPUBLISH_TEST_ECHO_COMMAND_H

// System headers
#include <string>

// Qserv headers
#include "wbase/WorkerCommand.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace wbase {
    class SendChannel;
}}}

// This class headers
namespace lsst {
namespace qserv {
namespace wpublish {

/**
  * Class TestEchoCommand reloads a list of chunks from the database
  */
class TestEchoCommand : public wbase::WorkerCommand {

public:

    // The default construction and copy semantics are prohibited
    TestEchoCommand& operator=(TestEchoCommand const&) = delete;
    TestEchoCommand(TestEchoCommand const&) = delete;
    TestEchoCommand() = delete;

    /**
     * @param sendChannel - communication channel for reporting results
     * @param value       - value to be send back to a client
     */
    explicit TestEchoCommand(std::shared_ptr<wbase::SendChannel> const& sendChannel,
                             std::string const& value);

    ~TestEchoCommand() override = default;

    void run() override;

private:

    std::string _value;
};

}}} // namespace lsst::qserv::wpublish

#endif // LSST_QSERV_WPUBLISH_TEST_ECHO_COMMAND_H