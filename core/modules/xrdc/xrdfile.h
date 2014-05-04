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

/// xrdfile.h - a module created to cleanly export xrootd client
/// functions to the Python layer via SWIG.  These functions no longer
/// serve that purpose since xrootd management no longer happens
/// across SWIG.  Consider refactoring these, or folding them into a
/// coherent layer that abstracts the rest of qserv from
/// xrootd-specific semantics.

#ifndef LSST_QSERV_XRDC_XRDFILE_H
#define LSST_QSERV_XRDC_XRDFILE_H

// System headers
#include <string>

class XrdPosixCallBack; // Forward.

namespace lsst {
namespace qserv {
namespace xrdc {

struct XrdTransResult {
    int open;
    int queryWrite;
    int read;
    int localWrite;
    bool isSuccessful() const {
	return ((open > 0) && // Successful open
		(queryWrite > 0) && // Some bytes sent off
		(read >= 0) && // Some results read back
		(localWrite > 0)); // Saved some result bytes.
    }
};

int xrdOpen(const char *path, int oflag);
int xrdOpenAsync(const char* path, int oflag, XrdPosixCallBack *cbP);

long long xrdRead(int fildes, void *buf, unsigned long long nbyte);

long long xrdWrite(int fildes, const void *buf, unsigned long long nbyte);

int xrdClose(int fildes);

long long xrdLseekSet(int fildes, unsigned long long offset);

std::string xrdGetEndpoint(int fildes);

int xrdReadStr(int fildes, char *str, int len);

void xrdReadToLocalFile(int fildes, int fragmentSize,
                        const char* filename,
                        bool const* abortFlag,
                        int* write, int* read);

XrdTransResult xrdOpenWriteReadSaveClose(const char *path,
                                         const char* buf, int len,
                                         int fragmentSize,
                                         const char* outfile);
XrdTransResult xrdOpenWriteReadSave(const char *path,
                                    const char* buf, int len,
                                    int fragmentSize,
                                    const char* outfile);

}}} // namespace lsst::qserv::xrdc

#endif // #ifndef LSST_QSERV_XRDC_XRDFILE_H
