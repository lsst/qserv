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
#include <stdexcept>
#include <string>

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.wpublish.TestEchoQservRequest");

using namespace lsst::qserv;

wpublish::TestEchoQservRequest::Status translate(proto::WorkerCommandTestEchoR::Status status) {
    switch (status) {
        case proto::WorkerCommandTestEchoR::SUCCESS: return wpublish::TestEchoQservRequest::SUCCESS;
        case proto::WorkerCommandTestEchoR::ERROR:   return wpublish::TestEchoQservRequest::ERROR;
    }
    throw domain_error(
            "TestEchoQservRequest::" + string(__func__) + "  no match for Protobuf status: " +
            proto::WorkerCommandTestEchoR_Status_Name(status));
}
}  // namespace

namespace lsst {
namespace qserv {
namespace wpublish {

string TestEchoQservRequest::status2str(Status status) {
    switch (status) {
        case SUCCESS: return "SUCCESS";
        case ERROR:   return "ERROR";
    }
    throw domain_error(
            "TestEchoQservRequest::" + string(__func__) + "  no match for status: " +
            to_string(status));
}


TestEchoQservRequest::Ptr TestEchoQservRequest::create(
                                    string const& value,
                                    TestEchoQservRequest::CallbackType onFinish) {
    return TestEchoQservRequest::Ptr(
        new TestEchoQservRequest(value,
                                 onFinish));
}


TestEchoQservRequest::TestEchoQservRequest(
                                    string const& value,
                                    TestEchoQservRequest::CallbackType onFinish)
    :   _value(value),
        _onFinish(onFinish) {

    LOGS(_log, LOG_LVL_DEBUG, "TestEchoQservRequest  ** CONSTRUCTED **");
}


TestEchoQservRequest::~TestEchoQservRequest() {
    LOGS(_log, LOG_LVL_DEBUG, "TestEchoQservRequest  ** DELETED **");
}


void TestEchoQservRequest::onRequest(proto::FrameBuffer& buf) {

    proto::WorkerCommandH header;
    header.set_command(proto::WorkerCommandH::TEST_ECHO);
    buf.serialize(header);

    proto::WorkerCommandTestEchoM echo;
    echo.set_value(_value);
    buf.serialize(echo);
}


void TestEchoQservRequest::onResponse(proto::FrameBufferView& view) {

    proto::WorkerCommandTestEchoR reply;
    view.parse(reply);

    LOGS(_log, LOG_LVL_DEBUG, "TestEchoQservRequest  ** SERVICE REPLY **  status: "
         << proto::WorkerCommandTestEchoR_Status_Name(reply.status()));

    if (nullptr != _onFinish) {

        // Clearing the stored callback after finishing the up-stream notification
        // has two purposes:
        //
        // 1. it guaranties (exactly) one time notification
        // 2. it breaks the up-stream dependency on a caller object if a shared
        //    pointer to the object was mentioned as the lambda-function's closure

        auto onFinish = move(_onFinish);
        _onFinish = nullptr;
        onFinish(::translate(reply.status()),
                 reply.error(),
                 _value,
                 reply.value());
    }
}


void TestEchoQservRequest::onError(string const& error) {

    if (nullptr != _onFinish) {

        // Clearing the stored callback after finishing the up-stream notification
        // has two purposes:
        //
        // 1. it guaranties (exactly) one time notification
        // 2. it breaks the up-stream dependency on a caller object if a shared
        //    pointer to the object was mentioned as the lambda-function's closure

        auto onFinish = move(_onFinish);
        _onFinish = nullptr;
        onFinish(Status::ERROR,
                 error,
                 _value,
                 string());
    }
}

}}} // namespace lsst::qserv::wpublish
