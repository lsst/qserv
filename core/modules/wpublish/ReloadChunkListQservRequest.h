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
#ifndef LSST_QSERV_WPUBLISH_RELOAD_CHUNK_LIST_QSERV_REQUEST_H
#define LSST_QSERV_WPUBLISH_RELOAD_CHUNK_LIST_QSERV_REQUEST_H

// System headers
#include <functional>
#include <list>

// Third party headers

// Qserv headers
#include "wpublish/QservRequest.h"

// Forward declarations

namespace lsst {
namespace qserv {
namespace wpublish {

/**
  * Class ReloadChunkListQservRequest is a base class for client-side requests to
  * the Qserv worker management services.
  */
class ReloadChunkListQservRequest
    :    public QservRequest {

public:

    /// Struct Chunk a value type encapsulating a chunk number and the name
    /// of a database
    struct Chunk {
        unsigned int chunk;
        std::string  database;
    };

    /// The ChunkCollection type refresens a collection of chunks
    using ChunkCollection = std::list<Chunk>;

    /// The callback function type to be used for notifications on
    /// the operation completion.
    using calback_type =
        std::function<void(bool,                        // 'true' if success
                           ChunkCollection const&,      // chunks added   (if success)
                           ChunkCollection const&)>;    // chunks removed (if success)

    /**
     * Normal constructor
     *
     * @param onFinish - optional callback function to be called upon the completion
     *                   (successful or not) of the request.
     */
    explicit ReloadChunkListQservRequest (calback_type onFinish = nullptr);

    // Default construction and copy semantics is prohibited
    ReloadChunkListQservRequest (ReloadChunkListQservRequest const&) = delete;
    ReloadChunkListQservRequest& operator= (ReloadChunkListQservRequest const&) = delete;

    /// Destructor
    ~ReloadChunkListQservRequest () override;

protected:

    /// Implement the corresponding method of the base class
    void onRequest (proto::FrameBuffer& buf) override;

    /// Implement the corresponding method of the base class
    void onResponse (proto::FrameBufferView& view) override;

private:

    /// Optional callback function to be called upon the completion
    /// (successfull or not) of the request.
    calback_type _onFinish;
};

}}} // namespace lsst::qserv::wpublish

#endif // LSST_QSERV_WPUBLISH_RELOAD_CHUNK_LIST_QSERV_REQUEST_H