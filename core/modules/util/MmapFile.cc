// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2008-2014 LSST Corporation.
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
// MmapFile is a lightweight wrapper for a mmap'd file.

#include "util/MmapFile.h"

// System headers
#include <assert.h>
#include <fcntl.h>
#include <iostream>
#include <sys/mman.h>

namespace lsst {
namespace qserv {
namespace util {

typedef std::shared_ptr<MmapFile> MmapPtr;

MmapPtr
MmapFile::newMap(std::string const& filename, bool read, bool write) {
    std::shared_ptr<MmapFile> m(new MmapFile());
    assert(m.get());
    m->_init(filename, read, write);
    if(!m->isValid()) {
        m.reset();
    }
    return m;
}

MmapFile::~MmapFile() {
    if(_buf) {
        if(-1 == ::munmap(_buf, _fstat.st_size)) {
            // LOGF_ERROR("Munmap failed (%1%, %2%). Memory corruption likely."
            //            % (void*)_buf % _fstat.st_size);
        }
        _buf = 0;
    }
    if(_fd > 0) {
        if(-1 == close(_fd)) {
            // LOGF_WARN("Warning, broken close of %1% (fd=%2%)" % _filename % _fd);
        }
        _fd = 0;
    }
}

void
MmapFile::_init(std::string const& filename, bool read_, bool write_) {
    _filename = filename;
    int openFlags = 0;
    int mapProt = 0;

    if(!(read_ || write_)) { return; } // refuse to init for no access

    if(read_ && write_) {
        openFlags |= O_RDWR;
        mapProt |= PROT_READ | PROT_WRITE;
    } else {
        openFlags |= read_ ? O_RDONLY : 0;
        openFlags |= write_ ? O_WRONLY : 0;
        mapProt |= read_ ? PROT_READ : 0;
        mapProt |= write_ ? PROT_WRITE : 0;
    }
    _fd = ::open(_filename.c_str(), openFlags);
    if(_fd == -1) {
        // LOGF_WARN("Error opening file.");
        _fd = 0;
    }
    if((-1 == ::fstat(_fd, &_fstat)) || // get filesize
       (MAP_FAILED == (_buf = ::mmap(0, _fstat.st_size,  // map file
                                     mapProt, MAP_SHARED, _fd, 0))
        )
       ) {
        if((MAP_FAILED == _buf) && _fstat.st_size > ((off_t)1ULL << 30)) {
            // LOGF_WARN("file too big? (mmap failed)");
        }
        _buf = 0; // reset buffer.
    }
    // _fd and _buf will get closed/munmapped as necessary in destructor.
}

}}} // namespace lsst::qserv::util
