// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2015 AURA/LSST.
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

// Class header
#include "qdisp/TransactionSpec.h"

// System headers
#include <cassert>
#include <fcntl.h>
#include <fstream>
#include <sys/mman.h>

namespace {
int seekMagic(int start, char* buffer, int term) {
    // Find magic sequence
    const char m[] = "####"; // MAGIC!
    for(int p = start; p < term; ++p) {
        if(((term - p) > 4) &&
           (buffer[p+0] == m[0]) && (buffer[p+1] == m[1]) &&
           (buffer[p+2] == m[2]) && (buffer[p+3] == m[3])) {
            return p;
        }
    }
    return term;
}
} // anonymous namespace

namespace lsst {
namespace qserv {
namespace qdisp {

//////////////////////////////////////////////////////////////////////
// TransactionSpec
//////////////////////////////////////////////////////////////////////
TransactionSpec::Reader::Reader(std::string const& file) {
    _mmapFd = -1;
    _mmapChunk = 0;
    _rawContents = NULL;
    _setupMmap(file);
    //_readWholeFile(file);
    _pos = 0;
}

TransactionSpec::Reader::~Reader() {
    if(_rawContents != NULL) delete[] _rawContents; // cleanup
    _cleanupMmap();
}

void TransactionSpec::Reader::_readWholeFile(std::string const& file) {
    // Read the file into memory.  All of it.
    std::ifstream is;
    is.open(file.c_str(), std::ios::binary);

    // get length of file:
    is.seekg(0, std::ios::end);
    _rawLength = is.tellg();
    if(_rawLength <= 0) {
        _rawContents = NULL;
        return;
    }
    is.seekg(0, std::ios::beg);

    // allocate memory:
    _rawContents = new char[_rawLength];

    // read data as a block:
    is.read(_rawContents, _rawLength);
    is.close();
 }

void TransactionSpec::Reader::_setupMmap(std::string const& file) {
    { // get length
        std::ifstream is;
        is.open(file.c_str(), std::ios::binary);

        // get length of file:
        is.seekg(0, std::ios::end);
        _rawLength = is.tellg();
        is.close();
    }
    // 0x1000: 4K, 0x10000: 64K 0x100000:1M, 0x1000 000: 16M
    _mmapDefaultSize = 0x1000000; // 16M
    _mmapChunkSize = _mmapDefaultSize;
    _mmapMinimum = 0x40000; // 256K
    _mmapOffset = 0;
    _mmapFd = open(file.c_str(), O_RDONLY);
    _mmapChunk = NULL;
    _mmapChunk = (char*)mmap(NULL, _mmapDefaultSize, PROT_READ, MAP_PRIVATE,
                             _mmapFd, _mmapOffset);
    if(_mmapChunk == (void*)-1) {
        perror("error mmaping.");
    }
    assert(_mmapChunk != (void*)-1);
}

void TransactionSpec::Reader::_advanceMmap() {
    int distToEnd = _rawLength - _mmapOffset;
    if(distToEnd > _mmapDefaultSize) { // Non-last chunk?
        int posInChunk = _pos - _mmapOffset;
        int distToBorder = _mmapDefaultSize - posInChunk;
        if(distToBorder < _mmapMinimum) {
            munmap(_mmapChunk, _mmapDefaultSize);
            _mmapOffset += _mmapDefaultSize - _mmapMinimum;
            _mmapChunk = (char*)mmap(NULL, _mmapDefaultSize, PROT_READ,
                                     MAP_PRIVATE, _mmapFd, _mmapOffset);
            assert(_mmapChunk != (void*)-1);

            if((_rawLength - _mmapOffset) < _mmapDefaultSize) {
                // Chunk exceeds end of file.
                // Overwrite mmap chunk size
                _mmapChunkSize = _rawLength - _mmapOffset;
            }
        }
    }
}

void TransactionSpec::Reader::_cleanupMmap() {
    if(_mmapChunk != NULL) {
        munmap(_mmapChunk, _mmapChunkSize);
    }
    if(_mmapFd != -1) {
        close(_mmapFd);
    }
}

TransactionSpec TransactionSpec::Reader::getSpec() {
    int beginPath;
    int endPath;
    int beginQuery;
    int endQuery;
    //int bufEnd = _rawContents;
    int bufEnd = _mmapChunkSize;;

    const int magicLength=4;
    TransactionSpec ts;

    //beginPath = seekMagic(_pos, _rawContents, bufEnd);
    beginPath = seekMagic(_pos-_mmapOffset, _mmapChunk, bufEnd);
    if(beginPath == bufEnd) return ts;
    beginPath += magicLength; // Start after magic sequence.

    //endPath = seekMagic(beginPath, _rawContents, bufEnd);
    endPath = seekMagic(beginPath, _mmapChunk, bufEnd);
    if(endPath == bufEnd) return ts;
    beginQuery = endPath + magicLength;

    //endQuery = seekMagic(beginQuery, _rawContents, bufEnd);
    endQuery = seekMagic(beginQuery, _mmapChunk, bufEnd);
    if(endQuery == bufEnd) return ts;
    // ts.path = std::string(_rawContents + beginPath, endPath - beginPath);
    // ts.query = std::string(_rawContents + beginQuery, endQuery - beginQuery);
    ts.path = std::string(_mmapChunk + beginPath, endPath - beginPath);
    ts.query = std::string(_mmapChunk + beginQuery, endQuery - beginQuery);
    ts.savePath = "/dev/null";
    ts.bufferSize = 1024000;
    //_pos = endQuery + magicLength; // Advance past the detected marker.
    _pos = _mmapOffset + endQuery + magicLength; // Advance past the detected marker.
    _advanceMmap();
    return ts;
}


}}} // namespace lsst::qserv::qdisp
