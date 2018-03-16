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
 *
 */
#ifndef LSST_QSERV_LOADER_BUFFERUDP_H_
#define LSST_QSERV_LOADER_BUFFERUDP_H_

// system headers
#include <arpa/inet.h>
#include <cstring>
#include <stdexcept>
#include <iostream> // &&&
#include <memory>
#include <sstream>
#include <string>

#include <boost/bind.hpp>
#include <boost/asio.hpp>

/// The absolute largest UDP message we would send.
/// Usually, they should be much smaller.
#define MAX_MSG_SIZE 6000


namespace lsst {
namespace qserv {
namespace loader {

class MsgElement;


/// A buffer for reading and writing. Nothing can be read from the buffer until
/// something has written to it.
class BufferUdp {
public:
    using Ptr = std::shared_ptr<BufferUdp>;

    BufferUdp() : BufferUdp(MAX_MSG_SIZE) {
        // std::cout << "&&& bufferUdp " << dump() << std::endl;
    }

    explicit BufferUdp(size_t length) : _length(length) {
        _buffer = new char[length];
        _ourBuffer = true;
        _setupBuffer();
    }

    /// If the buffer already contains valid data, advanceWriteCursor()
    /// must be called with the length of the valid data. Otherwise,
    /// BufferUdp thinks it has an empty buffer.
    BufferUdp(char* buf, size_t length) : _buffer(buf), _length(length) {
        _setupBuffer();
    }


    ~BufferUdp() {
        if (_ourBuffer) {
            delete[] _buffer;
        }
    }

    void reset() {
        _setupBuffer();
    }


    /// Return true only if this object owns the buffer.
    bool releaseOwnership() {
        if (_ourBuffer) {
            _ourBuffer = false;
            return true;
        }
        return false;
    }


    void makeOwnerOfBuffer() {
        _ourBuffer = true;
    }


    /// Return true if there's at least 'len' room left in the buffer.
    bool isAppendSafe(size_t len) const {
        return (_wCursor + len) <= _end;
    }


    bool append(const void* in, size_t len) {
        if (isAppendSafe(len)) {
            memcpy(_wCursor, in, len);
            _wCursor += len;
            return true;
        }
        return false;
    }


    void advanceWriteCursor(size_t len) {
        _wCursor += len;
        if (not isAppendSafe(0)) {
            throw new std::overflow_error("BufferUdp advanceWriteCursor beyond buffer len=" +
                    std::to_string(len));
        }
    }


    void advanceReadCursor(size_t len) {
        _rCursor += len;
        if (_rCursor > _end) {
            throw new std::overflow_error("BufferUdp advanceReadCursor beyond buffer len=" +
                                          std::to_string(len));
        }
    }


    /* &&&
    void setWriteCursor(size_t len) {
        _wCursor = _buffer + len;
        if (not isAppendSafe(0)) {
            throw new std::overflow_error("BufferUdp setCursor beyond buffer len=" + std::to_string(len));
        }
     }
     */


    std::shared_ptr<MsgElement> readFromSocket(boost::asio::ip::tcp::socket& socket, std::string const& note);


    size_t getMaxLength() const {
        return _length;
    }

    int getBytesLeftToRead() const {
        return _wCursor - _rCursor;
    }

    /// Returns the amount of room left in the buffer after the write cursor.
    size_t getAvailableWriteLength() const {
        return _end - _wCursor;
    }

    const char* getReadCursor() const { return _rCursor; }

    char* getWriteCursor() const { return _wCursor; }

    const char* begin() const { return _buffer; } // &&& kill

    char* getBuffer() const { return _buffer; } // &&& kill


    bool isRetrieveSafe(size_t len) const {
        auto newLen = (_rCursor + len);
        return (newLen <= _end && newLen <= _wCursor);
    }

    bool retrieve(void* out, size_t len) {
        if (isRetrieveSafe(len)) {
            memcpy(out, _rCursor, len);
            _rCursor += len;
            return true;
        }
        return false;
    }

    bool retrieveString(std::string& out, size_t len) {
        std::cout << "_rCursor + len=" << (long)(_rCursor + len) << " end=" << (long)_end << std::endl;
        if (isRetrieveSafe(len)) {
            const char* strEnd = _rCursor + len;
            std::string str(_rCursor, strEnd);
            _rCursor = strEnd;
            out = str;
            return true;
        }
        return false;
    }

    std::string dump(bool hexDump=true) { return dump(hexDump, false); }

    std::string dump(bool hexDump, bool charDump) const {
        std::stringstream os;
        os << "maxLength=" << _length;

        os <<   " buffer=" << (long)_buffer;
        os <<  " wCurLen=" << getAvailableWriteLength();
        os <<  " wCursor=" << (long)_wCursor;
        os <<  " rCurLen=" << getBytesLeftToRead();
        os <<  " rCursor=" << (long)_wCursor;
        os <<      " end=" << (long)_end;

        // hex dump
        if (hexDump) {
        os << "(";
        for (const char* j=_buffer; j < _wCursor; ++j) {
            os << std::hex << (int)*j << " ";
        }
        os << ")";
        }
        std::string str(os.str());

        // character dump
        if (charDump) {
            str += "(" + std::string(_buffer, _wCursor) + ")";
        }
        return str;
    }


private:
    void _setupBuffer() {
        _end = _buffer + _length;
        _wCursor = _buffer;
        _rCursor = _buffer;
    }


    /// If a message is not recovered, the buffer is left effectively unchanged.
    std::shared_ptr<MsgElement> _safeRetrieve();


    char* _buffer;
    size_t _length;  ///< Number of elements in the array (total capacity of array).
    char* _end;      ///< Immediately after the last element in the array.
    char* _wCursor;  ///< Where new elements will be appended to the array.
    const char* _rCursor; ///< Where data is read from the buffer.

    bool _ourBuffer{false}; ///< true if this class object is responsible for deleting the buffer.
};

}}} // namespace lsst:qserv:loader

#endif // LSST_QSERV_LOADER_BUFFERUDP_H_
