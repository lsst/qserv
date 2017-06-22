// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015 LSST Corporation.
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

// System headers
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>

// Third-party headers
#include "boost/utility.hpp"
#include <mysql/mysql.h>

namespace lsst {
namespace qserv {
namespace mysql {

class RowBuffer; // Forward. Defined in LocalInfile.cc

/// LocalInfile : a virtual LOCAL INFILE handler for mysql to use.
/// Do not inherit. Used in mysql_set_local_infile_handler .
///
/// The purpose of this class is to provide an efficient means of
/// pushing rows into the czar's mysqld. LOAD DATA INFILE is currently
/// recognized as the highest-performing means of getting data rows
/// into a mysql table, short of directly writing data directly into
/// the mysqld's data directory (likely only possible with MyISAM
/// tables).
/// LocalInfile objects can obtain rows directly from a query result
/// via a MYSQL_RES* result handle, or via a RowBuffer object, which
/// is an abstract interface to a buffer of table rows (with constant
/// schema). In general, client code will not need to construct
/// LocalInfile objects directly: they instead use the
/// LocalInfile::Mgr interface that generates them and manages them
/// implicitly.
class LocalInfile : boost::noncopyable {
public:
    class Mgr; // Helper for attaching to MYSQL*

    LocalInfile(char const* filename, MYSQL_RES* result);
    LocalInfile(char const* filename, std::shared_ptr<RowBuffer> rowBuffer);
    ~LocalInfile();

    /// Read up to bufLen bytes of infile contents into buf.
    /// @return number of bytes filled.
    /// Filling less than bufLen does not necessarily indicate
    /// EOF. Returning 0 bytes filled indicates EOF.
    int read(char* buf, unsigned int bufLen);
    /// Fill a buffer with an NULL-terminated text error description.
    /// @return an error code if available
    int getError(char* buf, unsigned int bufLen);
    /// @return true if the instance is valid for usage.
    inline bool isValid() const { return static_cast<bool>(_rowBuffer); }

private:
    char* _buffer; ///< Internal buffer for passing to mysql
    int _bufferSize; ///< Allocated size of internal buffer
    char* _leftover; ///< Ptr to bytes not yet sent to mysql
    unsigned _leftoverSize; ///< Size of bytes not yet sent in _leftover
    std::string _filename; ///< virtual filename for mysql
    std::shared_ptr<RowBuffer> _rowBuffer; ///< Underlying row source
};

/// Do not inherit or copy. Used in mysql_set_local_infile_handler
/// Can only be attached to one MYSQL*
/// Client code should use this interface in nearly all cases rather
/// than managing LocalInfile instances manually.
/// See:
/// http://dev.mysql.com/doc/refman/5.5/en/mysql-set-local-infile-handler.html
/// for more information on the required interface.
class LocalInfile::Mgr : boost::noncopyable {
public:
    Mgr() {}
    ~Mgr() {}

    // User interface //////////////////////////////////////////////////
    /// Attach the handler to a mysql client connection
    void attach(MYSQL* mysql);
    /// Detach this handler from a mysql client connection
    void detachReset(MYSQL* mysql);

    /// Prepare a local infile, specifying a filename
    void prepareSrc(std::string const& filename, MYSQL_RES* result);

    /// Prepare a local infile from a MYSQL_RES* and link it to an
    /// auto-generated filename. A RowBuffer object is constructed and
    /// used internally.
    /// @return generated filename
    std::string prepareSrc(MYSQL_RES* result);

    /// Prepare a local infile from a RowBuffer and link it to an
    /// auto-generated filename.
    /// @return generated filename
    std::string prepareSrc(std::shared_ptr<RowBuffer> const& rowbuffer, std::string const& qId);

    // mysql_local_infile_handler interface ////////////////////////////////
    // These function pointers are needed to attach a handler
    static int local_infile_init(void **ptr, const char *filename, void *userdata);
    static int local_infile_read(void *ptr, char *buf, unsigned int buf_len);
    static void local_infile_end(void *ptr);

    static int local_infile_error(void *ptr, char *error_msg, unsigned int error_msg_len);

    std::string insertBuffer(std::shared_ptr<RowBuffer> const& rb);
    void setBuffer(std::string const& s, std::shared_ptr<RowBuffer> const& rb);
    std::shared_ptr<RowBuffer> get(std::string const& s);



private:
    /// @return next filename
    std::string _nextFilename();

    /// @return true if new element inserted
    bool _set(std::string const& s, std::shared_ptr<RowBuffer> const& rb);

    typedef std::map<std::string, std::shared_ptr<RowBuffer>> RowBufferMap;
    RowBufferMap _map;
    std::mutex _mapMutex;
};

}}} // namespace lsst::qserv::mysql

#endif // LSST_QSERV_MYSQL_LOCALINFILE_H
