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
#ifndef LSST_QSERV_WPUBLISH_TEST_ECHO_QSERV_REQUEST_H
#define LSST_QSERV_WPUBLISH_TEST_ECHO_QSERV_REQUEST_H

// System headers
#include <functional>
#include <memory>
#include <string>

// Qserv headers
#include "wpublish/QservRequest.h"

namespace lsst {
namespace qserv {
namespace wpublish {

/**
  * Class TestEchoQservRequest represents a simple test request sending a string
  * to the worker management service and expecting the same value back.
  */
class TestEchoQservRequest : public QservRequest {

public:

    /// Completion status of the operation
    enum Status {
        SUCCESS,    // successful completion of a request
        ERROR       // an error occurred during command execution
    };

    /// @return string representation of a status
    static std::string status2str (Status status);

    /// The pointer type for instances of the class
    typedef std::shared_ptr<TestEchoQservRequest> Ptr;

    /// The callback function type to be used for notifications on
    /// the operation completion.
    using CallbackType =
        std::function<void(Status,                  // completion status
                           std::string const&,      // error message
                           std::string const&,      // value sent
                           std::string const&)>;    // value received (if success)

    /**
     * Static factory method is needed to prevent issues with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param value     a value to be sent to the worker service
     * @param onFinish  optional callback function to be called upon the completion
     *                  (successful or not) of the request.
     * @return smart pointer to the object of the class
     */
    static Ptr create(std::string const& value,
                      CallbackType onFinish=nullptr);

    // Default construction and copy semantics is prohibited
    TestEchoQservRequest() = delete;
    TestEchoQservRequest(TestEchoQservRequest const&) = delete;
    TestEchoQservRequest& operator=(TestEchoQservRequest const&) = delete;

    ~TestEchoQservRequest() override;

protected:

    /**
     * Normal constructor
     *
     * @param value     a value to be sent to the worker service
     * @param onFinish  function to be called upon the completion of a request
     */
    TestEchoQservRequest(std::string const& value,
                         CallbackType onFinish);

    void onRequest(proto::FrameBuffer& buf) override;

    void onResponse(proto::FrameBufferView& view) override;

    void onError(std::string const& error) override;

private:

    // Parameters of the object

    std::string _value;
    CallbackType _onFinish;
};

}}} // namespace lsst::qserv::wpublish

#endif // LSST_QSERV_WPUBLISH_TEST_ECHO_QSERV_REQUEST_H
