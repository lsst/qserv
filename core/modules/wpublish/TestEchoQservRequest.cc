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

// Class header
#include "wpublish/TestEchoQservRequest.h"

// System headers
#include <string>

// Qserv headers
#include "lsst/log/Log.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.wpublish.TestEchoQservRequest");

}  // namespace

namespace lsst {
namespace qserv {
namespace wpublish {

TestEchoQservRequest::TestEchoQservRequest (std::string const& value,
                                            calback_type       onFinish)
    :   _value    (value),
        _onFinish (onFinish) {

    LOGS(_log, LOG_LVL_DEBUG, "TestEchoQservRequest  ** CONSTRUCTED **");
}

TestEchoQservRequest::~TestEchoQservRequest () {
    LOGS(_log, LOG_LVL_DEBUG, "TestEchoQservRequest  ** DELETED **");
}

void
TestEchoQservRequest::onRequest (proto::FrameBuffer& buf) {

    proto::WorkerCommandH header;
    header.set_command(proto::WorkerCommandH::TEST_ECHO);
    buf.serialize(header);

    proto::WorkerCommandTestEchoM echo;
    echo.set_value(_value);
    buf.serialize(echo);
}

void
TestEchoQservRequest::onResponse (proto::FrameBufferView& view) {

    static std::string const context = "TestEchoQservRequest  ";

    proto::WorkerCommandTestEchoR reply;
    view.parse(reply);

    LOGS(_log, LOG_LVL_DEBUG, context << "** SERVICE REPLY **  status: "
         << proto::WorkerCommandTestEchoR_Status_Name(reply.status()));

    if (_onFinish)
        _onFinish (
            reply.status() == proto::WorkerCommandTestEchoR::SUCCESS,
            _value,
            reply.value());
}

}}} // namespace lsst::qserv::wpublish