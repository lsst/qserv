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

#include "lsst/qserv/master/MmapFile.h"
#include <iostream>
#include <fcntl.h>
#include <sys/mman.h>

namespace qMaster = lsst::qserv::master;

typedef boost::shared_ptr<qMaster::MmapFile> MmapPtr;

MmapPtr qMaster::MmapFile::newMap(std::string const& filename, 
                                  bool read, bool write) {
    boost::shared_ptr<MmapFile> m(new MmapFile());
    assert(m.get());
    m->_init(filename, read, write);
    if(!m->isValid()) {
        m.reset();
    }
    return m;
}

qMaster::MmapFile::~MmapFile() {
    if(_buf) {
        if(-1 == ::munmap(_buf, _fstat.st_size)) {
            std::cout << "Munmap failed (" << (void*)_buf
                      << ", " << _fstat.st_size 
                      << "). Memory corruption likely." << std::endl;
        }
        _buf = 0;
    }
    if(_fd > 0) {
        if(-1 == close(_fd)) {
            std::cout << "Warning, broken close of " << _filename
                      << " (fd=" << _fd << ")" << std::endl;
        }
        _fd = 0;
    }
}

void qMaster::MmapFile::_init(std::string const& filename,
                              bool read_, bool write_) {
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
        std::cout << "DEBUG: Error opening file." << std::endl;
        _fd = 0;
    }
    if((-1 == ::fstat(_fd, &_fstat)) || // get filesize
       (MAP_FAILED == (_buf = ::mmap(0, _fstat.st_size,  // map file
                                     mapProt, MAP_SHARED, _fd, 0))
        )
       ) {
        if((MAP_FAILED == _buf) && _fstat.st_size > ((off_t)1ULL << 30)) {
            std::cout << "DEBUG: file too big? (mmap failed) "
                      << std::endl;
        }
        _buf = 0; // reset buffer.
    }
    // _fd and _buf will get closed/munmapped as necessary in destructor.
}
