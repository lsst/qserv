// -*- LSST-C++ -*-
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
#ifndef LSST_QSERV_WPUBLISH_QUERY_MANAGEMENT_REQUEST_H
#define LSST_QSERV_WPUBLISH_QUERY_MANAGEMENT_REQUEST_H

// System headers
#include <functional>
#include <memory>
#include <string>

// Qserv headers
#include "global/intTypes.h"
#include "proto/worker.pb.h"
#include "wpublish/QservRequest.h"

namespace lsst::qserv::wpublish {

/**
 * Class QueryManagementRequest represents requests for managing query
 * completion/cancellation at Qserv workers.
 * @note No actuall responses are expected from these requests beyond
 * the error messages in case of any problems in delivering or processing
 * notifications.
 */
class QueryManagementRequest : public QservRequest {
public:
    /// Completion status of the operation
    enum Status {
        SUCCESS,  // successful completion of a request
        ERROR     // an error occurred during command execution
    };

    /// @return string representation of a status
    static std::string status2str(Status status);

    /// The pointer type for instances of the class
    typedef std::shared_ptr<QueryManagementRequest> Ptr;

    /// The callback function type to be used for notifications on
    /// the operation completion.
    using CallbackType = std::function<void(Status,                // completion status
                                            std::string const&)>;  // error message

    /**
     * Static factory method is needed to prevent issues with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     * @param op An operation to be initiated.
     * @param queryId An uinque identifier of a query affected by the request.
     *   Note that a cole of the identifier depends on which operation
     *   was requested.
     * @param onFinish (optional) callback function to be called upon the completion
     *   (successful or not) of the request.
     * @return the smart pointer to the object of the class
     */
    static Ptr create(proto::QueryManagement::Operation op, QueryId queryId, CallbackType onFinish = nullptr);

    QueryManagementRequest() = delete;
    QueryManagementRequest(QueryManagementRequest const&) = delete;
    QueryManagementRequest& operator=(QueryManagementRequest const&) = delete;

    virtual ~QueryManagementRequest() override;

protected:
    /// @see QueryManagementRequest::create()
    QueryManagementRequest(proto::QueryManagement::Operation op, QueryId queryId, CallbackType onFinish);

    virtual void onRequest(proto::FrameBuffer& buf) override;
    virtual void onResponse(proto::FrameBufferView& view) override;
    virtual void onError(std::string const& error) override;

private:
    // Parameters of the object

    proto::QueryManagement::Operation _op = proto::QueryManagement::CANCEL_AFTER_RESTART;
    QueryId _queryId = 0;
    CallbackType _onFinish;
};

}  // namespace lsst::qserv::wpublish

#endif  // LSST_QSERV_WPUBLISH_QUERY_MANAGEMENT_REQUEST_H
