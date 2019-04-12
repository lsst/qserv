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
/// ReloadChunkListQservRequest.h
#ifndef LSST_QSERV_WPUBLISH_CHUNK_LIST_QSERV_REQUEST_H
#define LSST_QSERV_WPUBLISH_CHUNK_LIST_QSERV_REQUEST_H

// System headers
#include <functional>
#include <list>
#include <memory>

// Qserv headers
#include "wpublish/QservRequest.h"


namespace lsst {
namespace qserv {
namespace wpublish {

/**
  * Class ChunkListQservRequest the base class for client-side requests
  * the Qserv worker services affecting chunk lists.
  */
class ChunkListQservRequest : public QservRequest {

public:

    /// Completion status of the operation
    enum Status {
        SUCCESS,    // successful completion of a request
        ERROR       // an error occured during command execution
    };

    /// @return string representation of a status
    static std::string status2str (Status status);

    /// Struct Chunk a value type encapsulating a chunk number and the name
    /// of a database
    struct Chunk {
        unsigned int chunk;
        std::string  database;
    };

    /// The ChunkCollection type represents a collection of chunks
    using ChunkCollection = std::list<Chunk>;

    /// The callback function type to be used for notifications on
    /// the operation completion.
    using CallbackType =
        std::function<void(Status,                      // completion status
                           std::string const&,          // error message
                           ChunkCollection const&,      // chunks added   (if success)
                           ChunkCollection const&)>;    // chunks removed (if success)

    // Default construction and copy semantics are prohibited
    ChunkListQservRequest() = delete;
    ChunkListQservRequest(ChunkListQservRequest const&) = delete;
    ChunkListQservRequest& operator=(ChunkListQservRequest const&) = delete;

    ~ChunkListQservRequest() override;

protected:

    /**
     * @param rebuild   rebuild the list from actual database tables
     * @param reload    reload the list in worker's memory
     * @param onFinish  optional callback function to be called upon the completion
     *                  (successful or not) of the request.
     */
     ChunkListQservRequest(bool rebuild,
                           bool reload,
                           CallbackType onFinish = nullptr);

    void onRequest(proto::FrameBuffer& buf) override;

    void onResponse(proto::FrameBufferView& view) override;

    void onError(std::string const& error) override;

private:

    // Parameters of the object

    bool _rebuild;
    bool _reload;
    CallbackType _onFinish;
};


/**
  * Class ReloadChunkListQservRequest implements a client-side request to
  * the Qserv worker management services.
  */
class ReloadChunkListQservRequest : public ChunkListQservRequest {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<ReloadChunkListQservRequest> Ptr;

    /**
     * Static factory method is needed to prevent issues with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param onFinish  optional callback function to be called upon the completion
     *                  (successful or not) of the request.
     */
    static Ptr create(CallbackType onFinish=nullptr);

    // Default construction and copy semantics are prohibited
    ReloadChunkListQservRequest() = delete;
    ReloadChunkListQservRequest(ReloadChunkListQservRequest const&) = delete;
    ReloadChunkListQservRequest& operator=(ReloadChunkListQservRequest const&) = delete;

    ~ReloadChunkListQservRequest() override = default;

protected:

    /**     *
     * @param onFinish optional callback function to be called upon the completion
     *                 (successful or not) of the request.
     */
     ReloadChunkListQservRequest(CallbackType onFinish);
};


/**
  * Class RebuildChunkListQservRequest implements a client-side request to
  * the Qserv worker management services.
  */
class RebuildChunkListQservRequest : public ChunkListQservRequest {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<RebuildChunkListQservRequest> Ptr;

    /*
     * Static factory method is needed to prevent issues with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param reload    reload the list in worker's memory
     * @param onFinish  optional callback function to be called upon the completion
     *                  (successful or not) of the request.
     */
    static Ptr create(bool reload,
                      CallbackType onFinish=nullptr);

    // Default construction and copy semantics are prohibited
    RebuildChunkListQservRequest() = delete;
    RebuildChunkListQservRequest(RebuildChunkListQservRequest const&) = delete;
    RebuildChunkListQservRequest& operator=(RebuildChunkListQservRequest const&) = delete;

    ~RebuildChunkListQservRequest() override = default;

protected:

    /**
     * @param reload    reload the list in worker's memory
     * @param onFinish  optional callback function to be called upon the completion
     *                  (successful or not) of the request.
     */
    RebuildChunkListQservRequest(bool reload,
                                 CallbackType onFinish);
};

}}} // namespace lsst::qserv::wpublish

#endif // LSST_QSERV_WPUBLISH_CHUNK_LIST_QSERV_REQUEST_H
