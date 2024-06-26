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
#ifndef LSST_QSERV_REPLICA_PROTOCOLBUFFER_H
#define LSST_QSERV_REPLICA_PROTOCOLBUFFER_H

// System headers
#include <arpa/inet.h>
#include <atomic>
#include <cstdint>
#include <stdexcept>

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class ProtocolBuffer is a helper class encapsulating serialization,
 * de-serialization operations with Google Protobuf objects.
 */
class ProtocolBuffer {
public:
    /// Google Protobuf buffers are more efficient below this size (bytes)
    static size_t const DESIRED_LIMIT;

    /// The hard limit (bytes) for a single Google Protobuf buffer
    static size_t const HARD_LIMIT;

    /**
     * Construct the buffer of some initial capacity, which will be
     * extended later if needed to accommodate larger messages.
     *
     * @param capacity
     *   initial capacity (bytes) of the buffer
     */
    explicit ProtocolBuffer(size_t capacity);

    // Default construction and copy semantics are prohibited

    ProtocolBuffer() = delete;
    ProtocolBuffer(ProtocolBuffer const&) = delete;
    ProtocolBuffer& operator=(ProtocolBuffer const&) = delete;

    /// Destructor (non-trivial in order to free the memory)
    ~ProtocolBuffer();

    /// @return raw pointer to the data blob
    char* data() { return _data; }

    /// @return the maximum capacity (bytes) of the buffer
    size_t capacity() const { return _capacity; }

    /**
     * @return
     *   current meaningful size (bytes) of the buffer
     *
     * @note
     *   a value return by the method will never exceed the buffer's capacity.
     */
    size_t size() const { return _size; }

    /**
     * Set the size of the meaningful content of the buffer. If the buffer
     * capacity is insufficient to accommodate the requested size the buffer
     * will be extended. In the later case its previous content (if any) will
     * be preserved.
     *
     * @param newSizeBytes
     *   (optional) new size in bytes
     *
     * @throws std::overflow_error
     *   if the buffer doesn't have enough space to accommodate the request
     */
    void resize(size_t newSizeBytes = 0);

    /**
     * Add a message into the buffer. The message will be preceded
     * by a frame header carrying the length of the message.
     *
     * @param message
     *   Protobuf object to be serialized
     *
     * @throws std::overflow_error
     *   if the buffer doesn't have enough space to accommodate the data
     *
     * @throws std::runtime_error
     *   if the serialization failed
     */
    template <class T>
    void serialize(T const& message) {
        uint32_t const bytes = message.ByteSizeLong();

        // Make sure we have enough space to accommodate the frame length
        // and the message body.

        _extend(_size + sizeof(uint32_t) + bytes);

        // Serialize the message header carrying the length of the message

        *(reinterpret_cast<uint32_t*>(_data + _size)) = htonl(bytes);
        _size += sizeof(uint32_t);

        // Serialize the message itself

        if (not message.SerializeToArray(_data + _size, _capacity - _size)) {
            throw std::runtime_error("message serialization failed");
        }
        _size += bytes;
    }

    /**
     * Parse and de-serialize the length of a message from the frame header
     * assuming the header is stored at the very beginning of the data buffer.
     *
     * @return
     *   the length of a message
     *
     * @throws std::underflow_error
     *   if the buffer doesn't have enough data to be interpreted as
     *   the frame header
     */
    uint32_t parseLength() const;

    /*
     * Parse and de-serialize the message given the specified size of
     * the message as informed by a prior frame header. The message is
     * assumed to be stored at the very beginning of the data buffer.
     *
     * @param message
     *   Protobuf object to be initialized
     *
     * @param bytes
     *   the number of bytes to be consumed during the operation
     *
     * @throws std::underflow_error
     *   if the buffer doesn't have enough data to be interpreted as the message
     *   of the required size
     *
     * @throws std::runtime_error if the de-serialization failed
     */
    template <class T>
    void parse(T& message, uint32_t bytes) {
        if (_size != bytes) {
            throw std::underflow_error("not enough data to be interpreted as the message");
        }
        if (not message.ParseFromArray(_data, bytes)) {
            throw std::runtime_error("message de-serialization failed");
        }
    }

private:
    /**
     * Ensure the buffer capacity is no less than the specified number of bytes.
     * Extend it otherwise. The previous contents (as per its 'size') of the buffer
     * as well as its size will be preserved.
     *
     * @param newCapacityBytes
     *   the number of bytes to be set
     */
    void _extend(size_t newCapacityBytes);

    // Data members

    char* _data;

    size_t _capacity;
    size_t _size;

    // For memory usage monitoring.
    static std::atomic<size_t> _numInstances;
    static std::atomic<size_t> _numBytesAllocated;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_PROTOCOLBUFFER_H
