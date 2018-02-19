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
/// TestEchoQservRequest.h
#ifndef LSST_QSERV_WPUBLISH_TEST_ECHO_QSERV_REQUEST_H
#define LSST_QSERV_WPUBLISH_TEST_ECHO_QSERV_REQUEST_H

// System headers
#include <functional>
#include <string>

// Third party headers

// Qserv headers
#include "wpublish/QservRequest.h"

// Forward declarations

namespace lsst {
namespace qserv {
namespace wpublish {

/**
  * Class TestEchoQservRequest represents a simple test request sending a string
  * to the worker management service and expecting the same value back.
  */
class TestEchoQservRequest
    :    public QservRequest {

public:

    /// Completion status of the operation
    enum Status {
        SUCCESS,    // successful completion of a request
        ERROR       // an error occured during command execution
    };

    /// @return string representation of a status
    static std::string status2str (Status status);

    /// The callback function type to be used for notifications on
    /// the operation completion.
    using calback_type =
        std::function<void(Status,                  // completion status
                           std::string const&,      // error message
                           std::string const&,      // value sent
                           std::string const&)>;    // value received (if success)

    /**
     * Normal constructor
     *
     * @param value    - a value to be sent to the worker service
     * @param onFinish - function to be called upon the completion of a request
     */
    explicit TestEchoQservRequest (std::string const& value,
                                   calback_type       onFinish = nullptr);

    // Default construction and copy semantics is prohibited
    TestEchoQservRequest () = delete;
    TestEchoQservRequest (TestEchoQservRequest const&) = delete;
    TestEchoQservRequest& operator= (TestEchoQservRequest const&) = delete;

    /// Destructor
    ~TestEchoQservRequest () override;

protected:

    /// Implement the corresponding method of the base class
    void onRequest (proto::FrameBuffer& buf) override;

    /// Implement the corresponding method of the base class
    void onResponse (proto::FrameBufferView& view) override;

private:

    /// Value to be sent to a worker
    std::string _value;

    /// Optional callback function to be called upon the completion
    /// (successfull or not) of the request.
    calback_type _onFinish;
};

}}} // namespace lsst::qserv::wpublish

#endif // LSST_QSERV_WPUBLISH_TEST_ECHO_QSERV_REQUEST_H