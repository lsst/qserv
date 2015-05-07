// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2008, 2009, 2010 LSST Corporation.
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

// MmapFile.h:
// class MmapFile -- A light C++ wrapper for mmap'ed files.
// Initially used with TableMerger.
//

#ifndef LSST_QSERV_UTIL_MMAPFILE_H
#define LSST_QSERV_UTIL_MMAPFILE_H

// System headers
#include <memory>
#include <string>
#include <sys/stat.h>

#if !defined(_FILE_OFFSET_BITS) || (_FILE_OFFSET_BITS<64)
// 64-bit support is needed to handle files > 2GB.
#error "Please compile with -D_FILE_OFFSET_BITS=64 or higher."
#endif

namespace lsst {
namespace qserv {
namespace util {

class MmapFile {
public:
    static std::shared_ptr<MmapFile> newMap(std::string const& filename,
                                            bool read, bool write);
    ~MmapFile();
    bool isValid() const { return (_filename.length() > 0) && _fd && _buf; }
    void* getBuf() { return _buf; }
    void const* getBuf() const { return _buf; }
    ::off_t getSize() const { return _fstat.st_size; }
private:
    MmapFile() : _buf(0), _fd(0) {}
    MmapFile(MmapFile const&); // disable copy constructor

    void _init(std::string const& filename, bool read_, bool write_);

    struct ::stat _fstat;
    void* _buf;
    int _fd;
    std::string _filename;
};

}}} // namespace lsst::qserv::util

#endif // LSST_QSERV_UTIL_MMAPFILE_H
