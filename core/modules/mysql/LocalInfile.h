// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014 LSST Corporation.
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
#ifndef LSST_QSERV_MYSQL_LOCALINFILE_H
#define LSST_QSERV_MYSQL_LOCALINFILE_H
// MySQL-dependent construction of schema. Separated from sql::*Schema
// to provide better isolation of sql module from mysql-isms.

// System headers
#include <map>
#include <sstream>
#include <string>

// Third-party headers
#include <boost/shared_ptr.hpp>
#include <mysql/mysql.h>

namespace lsst {
namespace qserv {
namespace mysql {

class RowBuffer; // Forward. Defined in LocalInfile.cc

/// LocalInfile : a virtual LOCAL INFILE handler for mysql to use.
/// Do not inherit. Used in mysql_set_local_infile_handler .
class LocalInfile {
public:
    class Mgr; // Helper for attaching to MYSQL*

    LocalInfile(char const* filename, MYSQL_RES* result);
    LocalInfile(char const* filename, boost::shared_ptr<RowBuffer> rowBuffer);
    ~LocalInfile();
    int read(char* buf, unsigned int bufLen);
    int getError(char* buf, unsigned int bufLen);
    inline bool isValid() const { return _rowBuffer; }

private:
    char* _buffer;
    int _bufferSize;
    char* _leftover;
    unsigned _leftoverSize;
    std::string _filename;
    boost::shared_ptr<RowBuffer> _rowBuffer;

};

/// Do not inherit. Used in mysql_set_local_infile_handler
/// Can only be attached to one MYSQL*
class LocalInfile::Mgr {
public:
    Mgr();

    // User interface //////////////////////////////////////////////////
    void attach(MYSQL* mysql);
    void detachReset(MYSQL* mysql);

    /// Prepare a local infile, specifying a filename
    void prepareSrc(std::string const& filename, MYSQL_RES* result);
    /// Prepare a local infile, using an auto-generated filename
    std::string prepareSrc(MYSQL_RES* result);
    std::string prepareSrc(boost::shared_ptr<RowBuffer> rowbuffer);


    // mysql_local_infile_handler interface ////////////////////////////////
    // These function pointers are needed to attach a handler
    static int local_infile_init(void **ptr, const char *filename,
                                 void *userdata);
    static int local_infile_read(void *ptr, char *buf, unsigned int buf_len);
    static void local_infile_end(void *ptr);

    static int local_infile_error(void *ptr,
                                  char *error_msg,
                                  unsigned int error_msg_len);

private:
    class Impl;
    std::auto_ptr<Impl> _impl;
};

}}} // namespace lsst::qserv::mysql
#endif // LSST_QSERV_MYSQL_LOCALINFILE_H
