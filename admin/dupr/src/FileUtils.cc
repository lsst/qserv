/*
 * LSST Data Management System
 * Copyright 2013 LSST Corporation.
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

#include "FileUtils.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>

#include <stdexcept>

using std::runtime_error;

namespace fs = boost::filesystem;


namespace lsst {
namespace qserv {
namespace admin {
namespace dupr {

InputFile::InputFile(fs::path const & path) : _path(path), _fd(-1), _sz(-1) {
    char msg[1024];
    struct ::stat st;
    int fd = ::open(path.string().c_str(), O_RDONLY);
    if (fd == -1) {
        ::strerror_r(errno, msg, sizeof(msg));
        throw runtime_error("open() failed [" + path.string() + "]: " + msg);
    }
    if (::fstat(fd, &st) != 0) {
        ::strerror_r(errno, msg, sizeof(msg));
        ::close(fd);
        throw runtime_error("fstat() failed [" + path.string() + "]: " + msg);
    }
    _fd = fd;
    _sz = st.st_size;
}

InputFile::~InputFile() {
    char msg[1024];
    if (_fd != -1 && ::close(_fd) != 0) {
        ::snprintf(msg, sizeof(msg),  "close() failed [%s]",
                   _path.string().c_str());
        ::perror(msg);
        ::exit(EXIT_FAILURE);
    }
}

void InputFile::read(void * buf, off_t off, size_t sz) const {
    char msg[1024];
    uint8_t * cur = static_cast<uint8_t *>(buf);
    while (sz > 0) {
        ssize_t n = ::pread(_fd, cur, sz, off);
        if (n == 0) {
            throw runtime_error("pread() received EOF [" + _path.string() +
                                "]");
        } else if (n < 0 && errno != EINTR) {
            ::strerror_r(errno, msg, sizeof(msg));
            throw runtime_error("pread() failed [" + _path.string() +
                                "]: " + msg);
        } else if (n > 0) {
            sz -= static_cast<size_t>(n);
            off += n;
            cur += n;
        }
    }
}


OutputFile::OutputFile(fs::path const & path, bool truncate) :
    _path(path), _fd(-1)
{
    char msg[1024];
    int flags = O_CREAT | O_WRONLY;
    if (truncate) {
        flags |= O_TRUNC;
    }
    int fd = ::open(path.string().c_str(), flags,
                    S_IROTH | S_IRGRP | S_IRUSR | S_IWUSR);
    if (fd == -1) {
        ::strerror_r(errno, msg, sizeof(msg));
        throw runtime_error("open() failed [" + path.string() + "]: " + msg);
    }
    if (!truncate) {
        if (::lseek(fd, 0, SEEK_END) < 0) {
            ::strerror_r(errno, msg, sizeof(msg));
            close(fd);
            throw runtime_error("lseek() failed [" + path.string() +
                                "]: " + msg);
        }
    }
    _fd = fd;
}

OutputFile::~OutputFile() {
    char msg[1024];
    if (_fd != -1 && close(_fd) != 0) {
        ::snprintf(msg, sizeof(msg), "close() failed [%s]",
                   _path.string().c_str());
        ::perror(msg);
        ::exit(EXIT_FAILURE);
    }
}

void OutputFile::append(void const * buf, size_t sz) {
    char msg[1024];
    if (!buf || sz == 0) {
        return;
    }
    char const * b = static_cast<char const *>(buf);
    while (sz > 0) {
        ssize_t n = ::write(_fd, b, sz);
        if (n < 0) {
            if (errno != EINTR) {
                ::strerror_r(errno, msg, sizeof(msg));
                throw runtime_error("write() failed [" + _path.string() +
                                    "]: " + msg);
            }
        } else {
            sz -= static_cast<size_t>(n);
            b += n;
        }
    }
}


BufferedAppender::BufferedAppender(size_t blockSize) :
    _buf(0), _end(_buf + blockSize), _cur(0), _file(0) { }

BufferedAppender::~BufferedAppender() {
    close();
    free(_buf);
    _buf = 0;
    _end = 0;
    _cur = 0;
}

void BufferedAppender::append(void const * buf, size_t size) {
    if (!_file) {
        throw std::logic_error(std::string(
            "BufferedAppender: append() called after "
            "close() and/or before open().\n"));
    }
    uint8_t const * b = static_cast<uint8_t const *>(buf);
    while (size > 0) {
        size_t n = std::min(size, static_cast<size_t>(_end - _cur));
        ::memcpy(_cur, b, n);
        b += n;
        _cur += n;
        if (_cur == _end) {
            _file->append(_buf, static_cast<size_t>(_cur - _buf));
            _cur = _buf;
        }
        size -= n;
    }
}

void BufferedAppender::open(fs::path const & path, bool truncate) {
    close();
    OutputFile * f = new OutputFile(path, truncate);
    if (!_buf) {
        // Allocate buffer.
        size_t sz = static_cast<size_t>(_end - _buf);
        uint8_t * buf = static_cast<uint8_t *>(malloc(sz));
        if (!buf) {
            delete f;
            throw std::bad_alloc();
        }
        _buf = buf;
        _end = buf + sz;
        _cur = buf;
    }
    _file = f;
}

void BufferedAppender::close() {
    if (_file) {
        // Write out any buffered data.
        _file->append(_buf, static_cast<size_t>(_cur - _buf));
        _cur = _buf;
        delete _file;
        _file = 0;
    }
}

}}}} // namespace lsst::qserv::admin::dupr
