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
#ifndef LSST_QSERV_ADMIN_DUPR_INPUTLINES_H
#define LSST_QSERV_ADMIN_DUPR_INPUTLINES_H

/// \file
/// \brief A class for reading lines from a set of text files in parallel.

#include <sys/types.h>
#include <stdint.h>
#include <utility>
#include <vector>

#include "boost/filesystem.hpp"
#include "boost/shared_ptr.hpp"


namespace lsst {
namespace qserv {
namespace admin {
namespace dupr {

/// The InputLines class reads lines from a list of input text files in an IO
/// efficient and parallel way. Each file is split up into blocks, where all
/// blocks except those at the end of a file have the same size. Files are read
/// exactly at block boundaries, without any prior seeking to ensure that reads
/// happen at line boundaries. Both file reads and opens can occur in parallel.
///
/// To use the class, a thread simply calls read() on a shared InputLines
/// instance with a suitably sized buffer (see getMinimumBufferCapacity()) when
/// it is ready for data. This class uses the "pimpl" idiom - copies are cheap
/// and shallow.
///
/// The maximum number of open file descriptors will be equal to the minimum
/// of the number of input files and the number of distinct threads calling
/// read(), so that thousands of input files can be used without having to
/// worry about per-process file descriptor limits.
///
/// Though disk reads happen exactly at block boundaries (and thus may split
/// lines), the range of characters returned by read() calls will always
/// correspond to some complete set of lines. The joining of line fragments
/// and assignment of full lines to reading threads is wait-free on platforms
/// that support atomic compare-and-swap of pointers.
///
/// Finally, as far as this class is concerned, a line is a sequence of no more
/// than MAX_LINE_SIZE bytes ending with LF, CR or CRLF.
class InputLines {
public:
    /// Corresponds to no input. Useless unless assigned to.
    InputLines() : _impl() { }

    /// Read lines from a list of files at the given granularity, optionally
    /// ignoring the first line in each file. The user is responsible for
    /// ensuring that the file list contains no empty or duplicate entries.
    /// Note that `blockSize` is clamped to lie between 1MiB and 1GiB.
    InputLines(std::vector<boost::filesystem::path> const & paths,
               size_t blockSize,
               bool skipFirstLine);

    ~InputLines() { }

    /// Return the IO read block size in bytes.
    size_t getBlockSize() const;

    /// Return the minimum capacity of a buffer passed to read().
    size_t getMinimumBufferCapacity() const;

    /// Has all the input been read?
    bool empty() const;

    /// Read consecutive lines of text into `buf`, and return a pointer range
    /// `[i,end)` identifying the bytes in `buf` containing valid data. The
    /// pointers returned will both be NULL if and only if there is no more
    /// input left to read. Note that `buf` must have a capacity of at least
    /// getMinimumBufferCapacity() bytes.
    std::pair<char *, char *> const read(char * buf);

private:
    class Impl;

    boost::shared_ptr<Impl> _impl;
};

}}}} // namespace lsst::qserv::admin::dupr

#endif // LSST_QSERV_ADMIN_DUPR_INPUTLINES_H
