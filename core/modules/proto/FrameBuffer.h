// -*- LSST-C++ -*-
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
#ifndef LSST_QSERV_PROTO_FRAME_BUFFER_H
#define LSST_QSERV_PROTO_FRAME_BUFFER_H

/// FrameBuffer.h declares:
///
/// struct FrameBufferError
/// class FrameBufferView
/// class FrameBuffer
///
/// (see individual class documentation for more information)

// System headers
#include <arpa/inet.h>  // ntohl
#include <cstdint>      // uint32_t
#include <stdexcept>
#include <string>

// Qserv headers

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace proto {

/**
 * Class FrameBufferError is used for throwing exceptions on various
 * ubnormal conditions seen in the implementations of the buffer
 * classes.
 */
struct FrameBufferError
    :   std::runtime_error {

    /// Normal constructor of the exception class
    FrameBufferError(std::string const& msg)
        :   std::runtime_error(msg) {
    }
};


/**
 * The helper class encapsulating deserialization operations with Google
 * protobuf objects on a staticly definied input byte stream. The stream
 * is expected to contain an arbitrary sequence of the following pairs of
 * records:
 *
 *   4-bytes: frame header containing 'N' - the length of a message
 *   N-bytes: the message serialized as a Protobuf object
 *   ... 
 */
class FrameBufferView {

public:

    /**
     * Construct the buffer
     *
     * @param data - pointer to the data blob to be parsed
     * @param size - the length (bytes) in the data blob
     */
    explicit FrameBufferView(char const* data,
                             size_t      size);

    // Default construction and copy semantics are proxibited

    FrameBufferView() = delete;
    FrameBufferView(FrameBufferView const&) = delete;
    FrameBufferView& operator=(FrameBufferView const&) = delete;

    /// Destructor
    ~FrameBufferView() = default;

    /*
     * Parse and deserialize the message given the specified size of
     * the message as informed by a prior frame header.
     * If successful the method will also advance the current pointer within
     * the data blob past the parsed message thus allowing it to parse the next
     * message.
     *
     * The method will throw exception FrameBufferError if:
     * - the buffer doesn't have enough data to be interpreted as
     *   the message of the required size
     * - message deserialization failed
     *
     * @param message - protobuf object to be initialized upon a successful
     *                  completion of the operation
     */
    template <class T>
    void parse(T& message) {

        uint32_t const messageLength = parseLength();

        if (_size - (_next - _data) < messageLength)
            throw FrameBufferError(
                    "FrameBufferView::parse() ** not enough data (" + std::to_string(_size - (_next - _data)) +
                    " bytes instead of " + std::to_string(messageLength) + " to be interpreted as the message");

        if (not message.ParseFromArray(_next, messageLength) ||
            not message.IsInitialized()) {
            throw FrameBufferError(
                    "FrameBufferView::parse() ** message deserialization failed **");
         }

        // Move the pointer to the next message (if any)
        _next += messageLength;
    }

private:

   /**
     * Parse and deserialize the length of a message from the frame header
     * at a curren position of the data pointer.
     * If succeeded the method will also advance the current pointer within
     * the data blob past the parsed message thus allowing to parse the next
     * message.
     *
     * The method will throw one of these exceptions:
     *
     *   std::underflow_error
     *      if the buffer doesn't have enough data to be interpreted as the
     *      frame header
     *
     * @return the length (bytes) of of the next message
     */
    uint32_t parseLength();

private:

    char const* _data;  // start of the data blob
    char const* _next;  // start of the next message within the blob

    size_t _size;
};


/**
 * The helper class encapsulating serialization operations
 * with Google protobuf objects.
 */
class FrameBuffer {

public:

    /// The default capacity of teh buffer
    static const size_t DEFAULT_SIZE;

    /// Google protobuffers are more efficient below this size (bytes)
    static const size_t DESIRED_LIMIT;

    /// The hard limit (bytes) for a single Google protobuffer
    static const size_t HARD_LIMIT;

    /**
     * Construct the buffer of the specified initial capacity (bytes).
     */
    explicit FrameBuffer(size_t capacity=DEFAULT_SIZE);

    // Copy semantics are proxibited

    FrameBuffer(FrameBuffer const&) = delete;
    FrameBuffer& operator=(FrameBuffer const&) = delete;

    /// Destructor
    ~FrameBuffer();

    /**
     * @return pointer to the data blob
     */
    char* data() { return _data; }

    /**
     * @return maximum capacity (bytes) of the buffer
     */
    size_t capacity() const { return _capacity; }

    /**
     * @return meaninful size (bytes) of the buffer
     */
    size_t size() const { return _size; }

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
     *
     * @param newSizeBytes - new size (bytes) of the buffer
     */
    void resize(size_t newSizeBytes=0);

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
     *
     * @param message - protobuf message to be serialized into the buffer
     */
    template <class T>
    void serialize(T const& message) {
        
        uint32_t const headerLength  = sizeof(uint32_t);
        uint32_t const messageLength = message.ByteSize();

        // Make sure we have enough space to accomodate the frame header
        // and the message body.
        extend(_size + headerLength + messageLength);

        // Serialize the message header carrying the length of the message
        *(reinterpret_cast<uint32_t*>(_data + _size)) = htonl(messageLength);
        _size += headerLength;

        // Serialize the message itself
        if (!message.SerializeToArray(_data + _size, _capacity - _size))
            throw FrameBufferError("FrameBuffer::serialize()  ** message serialization failed **");

        _size += messageLength;
    } 

private:

    /**
     * Ensure the buffer capacity is no less than the specified number of bytes.
     * Extend it otherwise. The previous contents (as per its 'size') of the buffer
     * as well as its size will be preserved.
     */
    void extend(size_t newCapacityBytes);

private:

    char* _data;      // start of the allocated buffer

    size_t _capacity;
    size_t _size;
};

}}} // namespace lsst::qserv::proto

#endif // LSST_QSERV_PROTO_FRAME_BUFFER_H
