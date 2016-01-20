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

// Class Header
#include "memman/Memory.h"

// System Headers
#include <errno.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>

namespace lsst {
namespace qserv {
namespace memman {

/******************************************************************************/
/*                              f i l e I n f o                               */
/******************************************************************************/
  
MemInfo Memory::fileInfo(std::string const& fPath) {

    MemInfo     fInfo;
    struct stat sBuff;

    // Simply issue a stat() to get the size
    //
    if (stat(fPath.c_str(), &sBuff)) {
        fInfo._errCode = errno;
    } else {
        if (sBuff.st_size > 0) {
            fInfo._memSize = static_cast<uint64_t>(sBuff.st_size);
        } else {
            fInfo._errCode = ESPIPE;
        }
    }

    // Return file information
    //
    return fInfo;
}

/******************************************************************************/
/*                              f i l e P a t h                               */
/******************************************************************************/

std::string Memory::filePath(std::string const& dbTable,
                             int chunk, bool isIndex) {

    std::string fPath;

    // Construct name and return it. The format here is DB-specific and may need 
    // to change if something other than mySQL is being used.
    //
    fPath.reserve(_dbDir.size() + dbTable.size() + 16);
    fPath  = _dbDir;
    fPath += '/';
    fPath += dbTable;
    fPath += '_';
    fPath += std::to_string(chunk);
    fPath += (isIndex ? ".MYI" : ".MYD");
    return fPath;
}

/******************************************************************************/
/*                               m e m L o c k                                */
/******************************************************************************/
  
MemInfo Memory::memLock(std::string const& fPath, bool isFlex) {

    MemInfo     mInfo;
    struct stat sBuff;
    int         fdNum;

    // We first open the file. we currently open this R/W because we want to
    // disable copy on write operations when we memory map the file.
    //
    fdNum = open(fPath.c_str(), O_RDWR);
    if (fdNum < 0 || fstat(fdNum, &sBuff)) {
        mInfo.setErrCode(errno);
        if (fdNum >= 0) close(fdNum);
        return mInfo;
    }

    // Verify the size of the file
    //
    if (sBuff.st_size > 0) {
        mInfo._memSize = static_cast<uint64_t>(sBuff.st_size);
    } else {
        close(fdNum);
        mInfo.setErrCode(ESPIPE);
        return mInfo;
    }

    // Map the file into memory
    //
    mInfo._memAddr = mmap(0, mInfo._memSize, PROT_WRITE, MAP_SHARED, fdNum, 0);

    // Diagnose any errors or update statistics. If succeeded, try locking it.
    //
    if (mInfo._memAddr == MAP_FAILED) {
        mInfo.setErrCode(errno);
    } else {
        if (!mlock(mInfo._memAddr, mInfo._memSize)) {
            _lokBytes += mInfo._memSize;
            if (isFlex) _flexNum++;
        } else {
            int rc = (errno == EAGAIN ? ENOMEM : errno);
            munmap( mInfo._memAddr, mInfo._memSize);
            mInfo.setErrCode(rc);
        }
    }

    // Close the file and return result
    //
    close(fdNum);
    return mInfo;
}

/******************************************************************************/
/*                                m e m R e l                                 */
/******************************************************************************/
  
void Memory::memRel(MemInfo& mInfo) {

    // If this is a valid object then unlock it (munmap does it for us).
    //
    if (mInfo._memSize > 0 && mInfo._memAddr != MAP_FAILED) {
        munmap(mInfo._memAddr, mInfo._memSize);
        _lokBytes -= mInfo._memSize;
        mInfo._memSize = 0;
    }
}
}}} // namespace lsst:qserv:memman

