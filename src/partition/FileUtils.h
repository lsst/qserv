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

/// \file
/// \brief Simple file access.

#ifndef LSST_PARTITION_FILEUTILS_H
#define LSST_PARTITION_FILEUTILS_H

// System headers
#include <memory>
#include <set>
#include <string>
#include <vector>
#include <sys/types.h>
#include <stdint.h>

// Third-party headers
#include "boost/filesystem.hpp"
#include "boost/static_assert.hpp"

namespace lsst::partition {

class ParquetFile;

struct ConfigParamArrow {
    std::vector<std::string> const columns;
    std::set<std::string> optionalColumns;
    std::string str_null;
    std::string str_delimiter;
    std::string str_escape;
    bool quote = false;

    ConfigParamArrow(std::vector<std::string> const &columns, std::set<std::string> const &optionalColumns,
                     std::string const &vnull, std::string const &vdelimiter, std::string const &vescape,
                     bool vquote)
            : columns(columns),
              optionalColumns(optionalColumns),
              str_null(vnull),
              str_delimiter(vdelimiter),
              str_escape(vescape),
              quote(vquote) {}

    ConfigParamArrow() = default;
    ConfigParamArrow(const ConfigParamArrow &v) = default;
    ConfigParamArrow &operator=(const ConfigParamArrow &) = delete;
};

typedef struct ConfigParamArrow ConfigParamArrow;

/// An input file. Safe for use from multiple threads.
class InputFile {
public:
    explicit InputFile(boost::filesystem::path const &path);
    virtual ~InputFile();

    // Disable copy construction and assignment.
    InputFile(InputFile const &) = delete;
    InputFile &operator=(InputFile const &) = delete;

    /// Return the size of the input file.
    off_t size() const { return _sz; }

    /// Return the path of the input file.
    boost::filesystem::path const &path() const { return _path; }

    // Needed in derived class InputFileArrow
    virtual int getBatchNumber() const { return -1; }

    /// Read a range of bytes into `buf`.
    void read(void *buf, off_t off, size_t sz) const;
    virtual void read(void *buf, off_t off, size_t sz, int &bufferSize, ConfigParamArrow const &params) const;

private:
    mutable char _msg[1024];

    boost::filesystem::path const _path;
    int _fd;
    off_t _sz;
};

class InputFileArrow : public InputFile {
public:
    InputFileArrow(boost::filesystem::path const &path, off_t blockSize);
    virtual ~InputFileArrow();

    // Disable copy construction and assignment.
    InputFileArrow(InputFileArrow const &) = delete;
    InputFileArrow &operator=(InputFileArrow const &) = delete;

    virtual int getBatchNumber() const override;

    /// Read a range of bytes into `buf`.
    virtual void read(void *buf, off_t off, size_t sz, int &bufferSize,
                      ConfigParamArrow const &params) const override;

private:
    mutable char _msg[1024];

    boost::filesystem::path const _path;
    int _fd;
    off_t _sz;

    std::unique_ptr<ParquetFile> _batchReader;
};

/// An output file that can only be appended to, and which should only be
/// used by a single thread at a time.
class OutputFile {
public:
    /// Open the given file for writing, creating it if necessary.
    /// Setting `truncate` to true will cause the file to be overwritten
    /// if it already exists.
    OutputFile(boost::filesystem::path const &path, bool truncate);
    ~OutputFile();

    // Disable copy construction and assignment.
    OutputFile(OutputFile const &) = delete;
    OutputFile &operator=(OutputFile const &) = delete;

    /// Return the path of the output file.
    boost::filesystem::path const &path() const { return _path; }

    /// Append `size` bytes from `buf` to the file.
    void append(void const *buf, size_t size);

private:
    mutable char _msg[1024];

    boost::filesystem::path const _path;
    int _fd;
};

/// A file writer which buffers data passed to append() in an attempt to
/// maximize the size of each actual write to disk. The file being appended
/// to must be specified via open(), and can be changed at any time.
class BufferedAppender {
public:
    explicit BufferedAppender(size_t blockSize);
    ~BufferedAppender();

    /// Append `size` bytes from `buf` to the currently open file.
    void append(void const *buf, size_t size);

    /// Is there a currently open file? If not, calling `append()` is forbidden.
    bool isOpen() const { return _file != 0; }

    /// Close the currently open file and open a new one.
    void open(boost::filesystem::path const &path, bool truncate);

    /// Write any buffered data to the currently open file and close it.
    void close();

private:
    // Disable copy-construction and assignment.
    BufferedAppender(BufferedAppender const &);
    BufferedAppender &operator=(BufferedAppender const &);

    uint8_t *_buf;
    uint8_t *_end;
    uint8_t *_cur;
    OutputFile *_file;
};

// TODO(smm): the functions below should be moved to their own header.

/// Encode a 32 bit integer as a little-endian sequence of 4 bytes. Return
/// `buf + 4`.
inline uint8_t *encode(uint8_t *buf, uint32_t x) {
    buf[0] = static_cast<uint8_t>(x & 0xff);
    buf[1] = static_cast<uint8_t>((x >> 8) & 0xff);
    buf[2] = static_cast<uint8_t>((x >> 16) & 0xff);
    buf[3] = static_cast<uint8_t>((x >> 24) & 0xff);
    return buf + 4;
}
/// Encode a 64 bit integer as a little-endian sequence of 8 bytes. Return
/// `buf + 8`.
inline uint8_t *encode(uint8_t *buf, uint64_t x) {
    buf[0] = static_cast<uint8_t>(x & 0xff);
    buf[1] = static_cast<uint8_t>((x >> 8) & 0xff);
    buf[2] = static_cast<uint8_t>((x >> 16) & 0xff);
    buf[3] = static_cast<uint8_t>((x >> 24) & 0xff);
    buf[4] = static_cast<uint8_t>((x >> 32) & 0xff);
    buf[5] = static_cast<uint8_t>((x >> 40) & 0xff);
    buf[6] = static_cast<uint8_t>((x >> 48) & 0xff);
    buf[7] = static_cast<uint8_t>((x >> 56) & 0xff);
    return buf + 8;
}

template <typename T>
inline T decode(uint8_t const *) {
    BOOST_STATIC_ASSERT(sizeof(T) == 0);
}
/// Decode a little-endian sequence of 4 bytes to a 32-bit integer.
template <>
inline uint32_t decode<uint32_t>(uint8_t const *buf) {
    return static_cast<uint32_t>(buf[0]) | (static_cast<uint32_t>(buf[1]) << 8) |
           (static_cast<uint32_t>(buf[2]) << 16) | (static_cast<uint32_t>(buf[3]) << 24);
}
/// Decode a little-endian sequence of 8 bytes to a 64-bit integer.
template <>
inline uint64_t decode<uint64_t>(uint8_t const *buf) {
    return static_cast<uint64_t>(buf[0]) | (static_cast<uint64_t>(buf[1]) << 8) |
           (static_cast<uint64_t>(buf[2]) << 16) | (static_cast<uint64_t>(buf[3]) << 24) |
           (static_cast<uint64_t>(buf[4]) << 32) | (static_cast<uint64_t>(buf[5]) << 40) |
           (static_cast<uint64_t>(buf[6]) << 48) | (static_cast<uint64_t>(buf[7]) << 56);
}

}  // namespace lsst::partition

#endif  // LSST_PARTITION_FILEUTILS_H
