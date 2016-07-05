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
#include <mutex>
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
//!
//! This class is partially MT-safe. Inspection of single variables is MT-safe.
//! Compound variable inspection, while MT-safe may not yield an accurate value.
//! Methods that modify variables must be externally synchronized. All methods
//! are MT-safe.
//-----------------------------------------------------------------------------

class Memory {
public:

    //-----------------------------------------------------------------------------
    //! Obtain number of bytes free (this takes into account reserved bytes).
    //!
    //! @return The number of bytes free.
    //-----------------------------------------------------------------------------

    uint64_t bytesFree() {
        std::lock_guard<std::mutex> guard(_memMutex);
        return (_maxBytes <= _rsvBytes ? 0 : _maxBytes - _rsvBytes);
    }

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
    //! @brief Lock a database file in memory.
    //!
    //! @param  mInfo  - The memory mapping returned by mapFile().
    //! @param  isFlex - When true account for flexible files in the statistics.
    //!
    //! @return =0     - Memory was locked.
    //! @return !0     - Memory not locked, retuned value is the errno.
    //-----------------------------------------------------------------------------

    int     memLock(MemInfo mInfo, bool isFlex);

    //-----------------------------------------------------------------------------
    //! @brief Map a database file in memory.
    //!
    //! @param  fPath  - Path of the database file to be mapped in memory.
    //! @param  isFlex - When true this is a flexible file request.
    //!
    //! @return A MemInfo object corresponding to the file. Use the MemInfo
    //!         methods to determine if the file pages were actually mapped.
    //-----------------------------------------------------------------------------

    MemInfo mapFile(std::string const& fPath);

    //-----------------------------------------------------------------------------
    //! @brief Unlock a memory object.
    //!
    //! @param  mInfo   - Memory MemInfo object returned by memLock(). It is
    //!                   reset to an invalid state upon return.
    //! @param  islkd   - When true, update locked memory statistics.
    //-----------------------------------------------------------------------------

    void    memRel(MemInfo& mInfo, bool islkd);

    //-----------------------------------------------------------------------------
    //! @brief Reserve memory for future locking.
    //!
    //! @param  memSZ   - Bytes of memory to reserve.
    //-----------------------------------------------------------------------------

    void    memReserve(uint64_t memSZ) {
                       std::lock_guard<std::mutex> guard(_memMutex);
                       _rsvBytes += memSZ;
                      }

    //-----------------------------------------------------------------------------
    //! @brief Restore memory previously reserved.
    //! This method must be externally serialized, it is not MT-safe.
    //!
    //! @param  memSZ   - Bytes of memory to release.
    //-----------------------------------------------------------------------------

    void    memRestore(uint64_t memSZ) {
                       std::lock_guard<std::mutex> guard(_memMutex);
                       if (_rsvBytes <= memSZ) _rsvBytes = 0;
                          else _rsvBytes -= memSZ;
                      }

    //-----------------------------------------------------------------------------
    //! @bried Obtain memory statistics.
    //!
    //! @return A MemStats structure containing the statistics.
    //-----------------------------------------------------------------------------

    struct MemStats {
        uint64_t bytesMax;       //!< Maximum number of bytes being managed
        uint64_t bytesReserved;  //!< Number of bytes reserved
        uint64_t bytesLocked;    //!< Number of bytes locked
        uint32_t numMapErrors;   //!< Number of mmap()  calls that failed
        uint32_t numLokErrors;   //!< Number of mlock() calls that failed
        uint32_t numFlexFiles;   //!< Number of Flexible files encountered
    };

    MemStats Statistics() {
        MemStats mStats;
        mStats.bytesMax      = _maxBytes;
        _memMutex.lock();
        mStats.bytesReserved = _rsvBytes;
        mStats.bytesLocked   = _lokBytes;
        _memMutex.unlock();
        mStats.numMapErrors  = _numMapErrs;
        mStats.numLokErrors  = _numLokErrs;
        mStats.numFlexFiles  = _flexNum;
        return mStats;
    }

    //-----------------------------------------------------------------------------
    //! Constructor
    //!
    //! @param  dbDir  - Directory path to where managed files reside.
    //! @param  memSZ  - Size of memory to manage in bytes.
    //-----------------------------------------------------------------------------

    Memory(std::string const& dbDir, uint64_t memSZ)
          : _dbDir(dbDir), _maxBytes(memSZ), _lokBytes(0), _rsvBytes(0),
            _numMapErrs(0), _numLokErrs(0), _flexNum(0) {}

    ~Memory() {}

private:

    std::string        _dbDir;
    std::mutex         _memMutex;
    uint64_t           _maxBytes;    // Set at construction time
    uint64_t           _lokBytes;    // Protected by _memMutex
    uint64_t           _rsvBytes;    // Ditto
    std::atomic_uint   _numMapErrs;
    std::atomic_uint   _numLokErrs;
    std::atomic_uint   _flexNum;
};
}}} // namespace lsst:qserv:memman
#endif  // LSST_QSERV_MEMMAN_MEMORY_H

