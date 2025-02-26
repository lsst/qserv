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

#include "partition/FileUtils.h"
#include "partition/ParquetInterface.h"

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

// LSST headers
#include "lsst/log/Log.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.partitionner");
}  // namespace

namespace fs = boost::filesystem;

namespace lsst::partition {

InputFile::InputFile(fs::path const &path) : _path(path), _fd(-1), _sz(-1) {
    struct ::stat st;
    int fd = ::open(path.string().c_str(), O_RDONLY);
    if (fd == -1) {
        ::strerror_r(errno, _msg, sizeof(_msg));
        throw std::runtime_error("InputFile::" + std::string(__func__) + " :open() failed [" + path.string() +
                                 "]: " + _msg);
    }
    if (::fstat(fd, &st) != 0) {
        ::strerror_r(errno, _msg, sizeof(_msg));
        ::close(fd);
        throw std::runtime_error("InputFile::" + std::string(__func__) + " :fstat() failed [" +
                                 path.string() + "]: " + _msg);
    }
    _fd = fd;
    _sz = st.st_size;
}

InputFile::~InputFile() {
    if (_fd != -1 && ::close(_fd) != 0) {
        ::snprintf(_msg, sizeof(_msg), "InputFile::~InputFile close() failed [%s]", _path.string().c_str());
        ::perror(_msg);
        LOGS(_log, LOG_LVL_WARN, _msg);
    }
}

void InputFile::read(void *buf, off_t off, size_t sz) const {
    uint8_t *cur = static_cast<uint8_t *>(buf);
    while (sz > 0) {
        ssize_t n = ::pread(_fd, cur, sz, off);
        if (n == 0) {
            throw std::runtime_error("InputFile::" + std::string(__func__) + ":received EOF [" +
                                     _path.string() + "]");
        } else if (n < 0 && errno != EINTR) {
            ::strerror_r(errno, _msg, sizeof(_msg));
            throw std::runtime_error("InputFile::" + std::string(__func__) + ":failed [" + _path.string() +
                                     "]: " + _msg);
        } else if (n > 0) {
            sz -= static_cast<size_t>(n);
            off += n;
            cur += n;
        }
    }
}

void InputFile::read(void *buf, off_t off, size_t sz, int & /*bufferSize*/,
                     ConfigParamArrow const & /*params*/) const {
    read(buf, off, sz);
}

InputFileArrow::InputFileArrow(fs::path const &path, off_t blockSize)
        : InputFile(path), _path(path), _fd(-1), _sz(-1) {
    struct ::stat st;

    _batchReader = std::make_unique<lsst::partition::ParquetFile>(path.string());
    _batchReader->setupBatchReader(blockSize);

    int fd = ::open(path.string().c_str(), O_RDONLY);

    if (fd == -1) {
        ::strerror_r(errno, _msg, sizeof(_msg));
        throw std::runtime_error("InputFileArrow::" + std::string(__func__) + ": open() failed [" +
                                 path.string() + "]: " + _msg);
    }
    if (::fstat(fd, &st) != 0) {
        ::strerror_r(errno, _msg, sizeof(_msg));
        ::close(fd);
        throw std::runtime_error("InputFileArrow::" + std::string(__func__) + ": fstat() failed [" +
                                 path.string() + "]: " + _msg);
    }
    _fd = fd;
    _sz = st.st_size;
}

InputFileArrow::~InputFileArrow() {
    if (_fd != -1 && ::close(_fd) != 0) {
        ::snprintf(_msg, sizeof(_msg), "InputFileArrow::~InputFileArrow : close() failed [%s]",
                   _path.string().c_str());
        ::perror(_msg);
        LOGS(_log, LOG_LVL_WARN, _msg);
    }
}

int InputFileArrow::getBatchNumber() const { return _batchReader->getTotalBatchNumber(); }

void InputFileArrow::read(void *buf, off_t off, size_t sz, int &csvBufferSize,
                          ConfigParamArrow const &params) const {
    uint8_t *cur = static_cast<uint8_t *>(buf);

    auto const success =
            _batchReader->readNextBatch_Table2CSV(cur, csvBufferSize, params.columns, params.optionalColumns,
                                                  params.str_null, params.str_delimiter, params.quote);
    if (success) {
        ssize_t n = csvBufferSize;
        if (n == 0) {
            throw std::runtime_error("InputFileArrow::" + std::string(__func__) + ": received EOF [" +
                                     _path.string() + "]");
        } else if (n < 0 && errno != EINTR) {
            ::strerror_r(errno, _msg, sizeof(_msg));
            throw std::runtime_error("InputFileArrow::" + std::string(__func__) + ": failed [" +
                                     _path.string() + "]: " + _msg);
        }
    }
}

OutputFile::OutputFile(fs::path const &path, bool truncate) : _path(path), _fd(-1) {
    int flags = O_CREAT | O_WRONLY;
    if (truncate) {
        flags |= O_TRUNC;
    }
    int fd = ::open(path.string().c_str(), flags, S_IROTH | S_IRGRP | S_IRUSR | S_IWUSR);
    if (fd == -1) {
        ::strerror_r(errno, _msg, sizeof(_msg));
        throw std::runtime_error("OutputFile::" + std::string(__func__) + ": open() failed [" +
                                 path.string() + "]: " + _msg);
    }
    if (!truncate) {
        if (::lseek(fd, 0, SEEK_END) < 0) {
            ::strerror_r(errno, _msg, sizeof(_msg));
            close(fd);
            throw std::runtime_error("OutputFile::" + std::string(__func__) + ": lseek() failed [" +
                                     path.string() + "]: " + _msg);
        }
    }
    _fd = fd;
}

OutputFile::~OutputFile() {
    if (_fd != -1 && close(_fd) != 0) {
        ::snprintf(_msg, sizeof(_msg), "OutputFile::~OutputFile : close() failed [%s]",
                   _path.string().c_str());
        ::perror(_msg);
        LOGS(_log, LOG_LVL_WARN, _msg);
    }
}

void OutputFile::append(void const *buf, size_t sz) {
    if (!buf || sz == 0) {
        return;
    }
    char const *b = static_cast<char const *>(buf);
    while (sz > 0) {
        ssize_t n = ::write(_fd, b, sz);
        if (n < 0) {
            if (errno != EINTR) {
                ::strerror_r(errno, _msg, sizeof(_msg));
                throw std::runtime_error("OutputFile::" + std::string(__func__) + ": write() failed [" +
                                         _path.string() + "]: " + _msg);
            }
        } else {
            sz -= static_cast<size_t>(n);
            b += n;
        }
    }
}

BufferedAppender::BufferedAppender(size_t blockSize) : _buf(0), _end(_buf + blockSize), _cur(0), _file(0) {}

BufferedAppender::~BufferedAppender() {
    close();
    free(_buf);
    _buf = 0;
    _end = 0;
    _cur = 0;
}

void BufferedAppender::append(void const *buf, size_t size) {
    if (!_file) {
        throw std::logic_error(
                std::string("BufferedAppender: append() called after "
                            "close() and/or before open().\n"));
    }
    uint8_t const *b = static_cast<uint8_t const *>(buf);
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

void BufferedAppender::open(fs::path const &path, bool truncate) {
    close();
    OutputFile *f = new OutputFile(path, truncate);
    if (!_buf) {
        // Allocate buffer.
        size_t sz = static_cast<size_t>(_end - _buf);
        uint8_t *buf = static_cast<uint8_t *>(malloc(sz));
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

}  // namespace lsst::partition
