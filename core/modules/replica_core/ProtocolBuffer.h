// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2017 LSST Corporation.
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
#ifndef LSST_QSERV_REPLICA_CORE_PROTOCOLBUFFER_H
#define LSST_QSERV_REPLICA_CORE_PROTOCOLBUFFER_H

/// ProtocolBuffer.h declares:
///
/// class ProtocolBuffer
/// (see individual class documentation for more information)

// System headers

#include <arpa/inet.h>  // ntohl
#include <cstdint>      // uint32_t
#include <stdexcept>

// Qserv headers

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica_core {

/**
 * The helper class encapsulating serialization, deserialization operations
 * with Google protobuf objects.
 */
class ProtocolBuffer {

public:

    /// Google protobuffers are more efficient below this size (bytes)
    static const size_t DESIRED_LIMIT;

    /// The hard limit (bytes) for a single Google protobuffer
    static const size_t HARD_LIMIT;

    /**
     * Construct the buffer of the specified initial capacity (bytes).
     */
    explicit ProtocolBuffer (size_t capacity);

    // Default construction and copy semantics are proxibited

    ProtocolBuffer () = delete;
    ProtocolBuffer (ProtocolBuffer const&) = delete;
    ProtocolBuffer & operator= (ProtocolBuffer const&) = delete;

    /// Destructor
    virtual ~ProtocolBuffer ();

    /**
     * Pointer to the data blob
     */
    char* data () { return _data; }

    /**
     * Maximum capacity (bytes) of the buffer.
     */
    size_t capacity () const { return _capacity; }

    /**
     * Current meaninful size (bytes) of the buffer.
     *
     * NOTE: a value return by the method will never exceed the buffer's
     * capacity.
     */
    size_t size () const { return _size; }

    /**
     * Set the size of the meaningful content of the buffer. If the buffer
     * capacity is insufficient to accomodate the requested size the buffer
     * will be extended. In the later case its previous content (if any) will
     * be preserved.
     * 
     * The method will throw one of these exceptions:
     *
     *   std::overflow_error
     *      if the buffer doesn't have enough space to accomodate the request
     */
    void resize (size_t newSizeBytes=0);

    /**
     * Add a message into the buffer. The message will be preceeed
     * by a frame header carrying the length of the message.
     *
     * The method will throw one of these exceptions:
     *
     *   std::overflow_error
     *      if the buffer doesn't have enough space to accomodate the data
     *
     *   std::runtime_error
     *      if the serialization failed
     */
    template <class T>
    void serialize (const T &message) {

        const uint32_t bytes = message.ByteSize();

        // Make sure we have enough space to accomodate the frame length
        // and the message body.

        extend(_size + sizeof(uint32_t) + bytes);

        // Serialize the message header carrying the length of the message
    
        *(reinterpret_cast<uint32_t*>(_data + _size)) = htonl(bytes);
        _size += sizeof(uint32_t);

        // Serialize the message itself

        if (!message.SerializeToArray(_data + _size, _capacity - _size))
            throw std::runtime_error("message serialization failed");

        _size += bytes;
    } 

    /**
     * Parse and deserialize the length of a message from the frame header
     * assuming the header is stored at the very begining of the data buffer.
     *
     * The method will throw one of these exceptions:
     *
     *   std::underflow_error
     *      if the buffer doesn't have enough data to be interpreted as the
     *      frame header
     */
    uint32_t parseLength () const;

    /*
     * Parse and deserialize the message given the specified size of
     * the message as informed by a prior frame header. The message is
     * assumed to be stored at the very begining of the data buffer.
     *
     * The method will throw one of these exceptions:
     *
     *   std::underflow_error
     *      if the buffer doesn't have enough data to be interpreted as the
     *      messag eo fthe required size
     *
     *   std::runtime_error
     *      if the deserialization failed
     */
    template <class T>
    void parse (T       &message,
                uint32_t bytes) {

        if (_size != bytes)
            throw std::underflow_error("not enough data to be interpreted as the message");

        if (!message.ParseFromArray(_data, bytes))
            throw std::runtime_error("message deserialization failed");
    }

private:

    /**
     * Ensure the buffer capacity is no less than the specified number of bytes.
     * Extend it otherwise. The previous contents (as per its 'size') of the buffer
     * as well as its size will be preserved.
     */
    void extend (size_t newCapacityBytes);

private:

    char *_data;

    size_t _capacity;
    size_t _size;
};

}}} // namespace lsst::qserv::replica_core

#endif // LSST_QSERV_REPLICA_CORE_PROTOCOLBUFFER_H
