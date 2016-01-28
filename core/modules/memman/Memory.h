// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2016 LSST Corporation.
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

#ifndef LSST_QSERV_MEMMAN_MEMORY_H
#define LSST_QSERV_MEMMAN_MEMORY_H

// System headers
#include <atomic>
#include <cstdint>
#include <string>
#include <unistd.h>

namespace lsst {
namespace qserv {
namespace memman {

//-----------------------------------------------------------------------------
//! @brief Memory information object describing memory requirements or errors.
//-----------------------------------------------------------------------------

class MemInfo {
public:
friend class Memory;

    //-----------------------------------------------------------------------------
    //! @brief Return reason why this object is not valid.
    //!
    //! @return >0 the errno describing the problem.
    //! @return =0 the object is valid there is no error.
    //-----------------------------------------------------------------------------

    int    errCode() {return (_memSize == 0 ? _errCode : 0);}

    //-----------------------------------------------------------------------------
    //! @brief Check if this object is valid.
    //!
    //! @return True if object is valid and false otherwise.
    //-----------------------------------------------------------------------------

    bool   isValid() {return _memSize != 0;}

    //-----------------------------------------------------------------------------
    //! @brief Set error code.
    //!
    //! @param  eNum   - The error code number.
    //-----------------------------------------------------------------------------

    void   setErrCode(int eNum) {_memSize = 0; _errCode = eNum;}

    //-----------------------------------------------------------------------------
    //! @brief Return size of the file.
    //!
    //! @return >0 the number of bytes corresponding to the file.
    //! @return =0 this object is not valid.
    //-----------------------------------------------------------------------------

    uint64_t size() {return _memSize;}

    MemInfo() : _memAddr((void *)-1), _memSize(0) {}
   ~MemInfo() {}

private:
union {void  *_memAddr; int _errCode;};
uint64_t      _memSize;  //!< If contains 0 then _errCode is valid.
};

//-----------------------------------------------------------------------------
//! @brief Physical memory manager
//-----------------------------------------------------------------------------

class Memory {
public:

    //-----------------------------------------------------------------------------
    //! Obtain number of bytes locked.
    //!
    //! @return The number of bytes locked.
    //-----------------------------------------------------------------------------

    uint64_t bytesLocked() {return _lokBytes;}

    //-----------------------------------------------------------------------------
    //! Obtain number of bytes in memory.
    //!
    //! @return The number of bytes in memory.
    //-----------------------------------------------------------------------------

    uint64_t bytesMax() {return _maxBytes;}

    //-----------------------------------------------------------------------------
    //! @brief Get file information.
    //!
    //! @param  fPath - File path for which information is obtained.
    //!
    //! @return A MemInfo object corresponding to the file. Use the MemInfo
    //!         methods to determine success or failure.
    //-----------------------------------------------------------------------------

    MemInfo fileInfo(std::string const& fPath);

    //-----------------------------------------------------------------------------
    //! @brief Generate a file path given directory, a table name and chunk.
    //!
    //! @param  dbTable    - The name of the table
    //! @param  chunk      - The chunk number in question
    //! @param  isIndex    - True to return the index file path. Otherwise,
    //!                      the file path to the data table is returned.
    //!
    //! @return File path to the desired file system object.
    //-----------------------------------------------------------------------------

    std::string filePath(std::string const& dbTable,
                         int chunk,
                         bool isIndex=false
                        );

    //-----------------------------------------------------------------------------
    //! Obtain and optionally update of flexible files that were locked.
    //!
    //! @return The number of flexible files that were locked.
    //-----------------------------------------------------------------------------

    uint32_t flexNum(uint32_t cnt=0) {
                    if (cnt != 0) _flexNum += cnt;
                    return _flexNum;
                    }

    //-----------------------------------------------------------------------------
    //! @brief Lock a database file in memory.
    //!
    //! @param  fPath  - Path of the database file to be locked in memory.
    //! @param  isFlex - When true this is a flexible file request.
    //!
    //! @return A MemInfo object corresponding to the file. Use the MemInfo
    //!         methods to determine if the file pages were actually locked.
    //-----------------------------------------------------------------------------

    MemInfo memLock(std::string const& fPath, bool isFlex=false);

    //-----------------------------------------------------------------------------
    //! @brief Unlock a memory object.
    //!
    //! @param  mInfo   - Memory MemInfo object returned by memLock().
    //-----------------------------------------------------------------------------

    void    memRel(MemInfo& mInfo);

    //-----------------------------------------------------------------------------
    //! Constructor
    //!
    //! @param  dbDir  - Directory path to where managed files reside.
    //! @param  memSZ  - Size of memory to manage.
    //-----------------------------------------------------------------------------

    Memory(std::string const& dbDir, uint64_t memSZ)
          : _dbDir(dbDir), _maxBytes(memSZ), _lokBytes(0), _flexNum(0) {}

    ~Memory() {}

private:

    std::string        _dbDir;
    uint64_t           _maxBytes;
    std::atomic_ullong _lokBytes;
    std::atomic_uint   _flexNum;
};
}}} // namespace lsst:qserv:memman
#endif  // LSST_QSERV_MEMMAN_MEMORY_H

