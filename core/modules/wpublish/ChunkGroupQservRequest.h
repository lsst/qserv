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
/// ChunkGroupQservRequest.h
#ifndef LSST_QSERV_WPUBLISH_CHUNK_GROUP_QSERV_REQUEST_H
#define LSST_QSERV_WPUBLISH_CHUNK_GROUP_QSERV_REQUEST_H

// System headers
#include <functional>
#include <memory>
#include <vector>

// Third party headers

// Qserv headers
#include "wpublish/QservRequest.h"

// Forward declarations

namespace lsst {
namespace qserv {
namespace wpublish {

/**
  * Class ChunkGroupQservRequest implements a client-side request to
  * the Qserv worker management services.
  */
class ChunkGroupQservRequest
    :    public QservRequest {

public:

    /// Completion status of the operation
    enum Status {
        SUCCESS,    // successful completion of a request
        INVALID,    // invalid parameters of the request
        IN_USE,     // request is rejected because one of the chunks is in use
        ERROR       // an error occured during command execution
    };

    /// @return string representation of a status
    static std::string status2str(Status status);

    /// The callback function type to be used for notifications on
    /// the operation completion.
    using calback_type =
            std::function<void(Status,                  // completion status
                               std::string const&)>;    // error message (depends on a status)

    // Default construction and copy semantics is prohibited
    ChunkGroupQservRequest() = delete;
    ChunkGroupQservRequest(ChunkGroupQservRequest const&) = delete;
    ChunkGroupQservRequest& operator=(ChunkGroupQservRequest const&) = delete;

    /// Destructor
    ~ChunkGroupQservRequest() override;

protected:

    /**
     * Normal constructor
     *
     * @param add       - add a group if 'true', remove otherwise
     * @param chunk     - chunk number
     * @param databases - names of databases in the group
     * @param force     - force the proposed change even if the chunk is in use
     * @param onFinish  - optional callback function to be called upon the completion
     *                   (successful or not) of the request.
     */
    ChunkGroupQservRequest(bool add,
                           unsigned int chunk,
                           std::vector<std::string> const& databases,
                           bool force,
                           calback_type onFinish);

    /// Implement the corresponding method of the base class
    void onRequest(proto::FrameBuffer& buf) override;

    /// Implement the corresponding method of the base class
    void onResponse(proto::FrameBufferView& view) override;

    /// Implement the corresponding method of the base class
    void onError(std::string const& error) override;

private:

    // Parameters of a request

    bool _add;
    unsigned int _chunk;
    std::vector<std::string> _databases;
    bool _force;
    calback_type _onFinish;
};

/**
  * Class AddChunkGroupQservRequest implements a client-side request to
  * the Qserv worker management services.
  */
class AddChunkGroupQservRequest
    :    public ChunkGroupQservRequest {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<AddChunkGroupQservRequest> pointer;

    // Default construction and copy semantics is prohibited
    AddChunkGroupQservRequest() = delete;
    AddChunkGroupQservRequest(AddChunkGroupQservRequest const&) = delete;
    AddChunkGroupQservRequest& operator=(AddChunkGroupQservRequest const&) = delete;

    /// Destructor
    ~AddChunkGroupQservRequest() override  = default;

    /**
     * Static factory method is needed to prevent issues with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param chunk     - the chunk number
     * @param databases - names of databases in the group
     * @param onFinish  - callback function to be called upon request completion
     */
    static pointer create(unsigned int chunk,
                          std::vector<std::string> const& databases,
                          calback_type onFinish = nullptr);

private:

    /**
     * Normal constructor
     *
     * @param chunk     - chunk number
     * @param databases - names of databases in the group
     * @param onFinish  - optional callback function to be called upon the completion
     *                    (successful or not) of the request.
     */
    AddChunkGroupQservRequest(unsigned int chunk,
                              std::vector<std::string> const& databases,
                              calback_type onFinish);
};

/**
  * Class RemoveChunkGroupQservRequest implements a client-side request to
  * the Qserv worker management services.
  */
class RemoveChunkGroupQservRequest
    :    public ChunkGroupQservRequest {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<RemoveChunkGroupQservRequest> pointer;

    // Default construction and copy semantics is prohibited
    RemoveChunkGroupQservRequest() = delete;
    RemoveChunkGroupQservRequest(RemoveChunkGroupQservRequest const&) = delete;
    RemoveChunkGroupQservRequest& operator=(RemoveChunkGroupQservRequest const&) = delete;

    /// Destructor
    ~RemoveChunkGroupQservRequest() override = default;

    /**
     * Static factory method is needed to prevent issues with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param chunk     - the chunk number
     * @param databases - names of databases in the group
     * @param force     - force the proposed change even if the chunk is in use
     * @param onFinish  - callback function to be called upon request completion
     */
    static pointer create(unsigned int chunk,
                          std::vector<std::string> const& databases,
                          bool force,
                          calback_type onFinish = nullptr);

private:

    /**
     * Normal constructor
     *
     * @param chunk     - chunk number
     * @param databases - names of databases in the group
     * @param force     - force the proposed change even if the chunk is in use
     * @param onFinish  - optional callback function to be called upon the completion
     *                   (successful or not) of the request.
     */
    RemoveChunkGroupQservRequest(unsigned int chunk,
                                 std::vector<std::string> const& databases,
                                 bool force,
                                 calback_type onFinish);
};

}}} // namespace lsst::qserv::wpublish

#endif // LSST_QSERV_WPUBLISH_CHUNK_GROUP_QSERV_REQUEST_H
