// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2017 LSST Corporation.
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
#ifndef LSST_QSERV_RPROC_PROTOROWBUFFER_H
#define LSST_QSERV_RPROC_PROTOROWBUFFER_H

// Class header
#include "rproc/ProtoRowBuffer.h"

// System headers
#include <limits>


// Qserv headers
#include "mysql/RowBuffer.h"
#include "proto/worker.pb.h"
#include "sql/Schema.h"

namespace lsst {
namespace qserv {
namespace rproc {
/* &&&
/// Construct a RowBuffer with a Result message row source.
mysql::RowBuffer::Ptr newProtoRowBuffer(proto::Result& r);
*/


/// ProtoRowBuffer is an implementation of RowBuffer designed to allow a
/// LocalInfile object to use a Protobufs Result message as a row source
class ProtoRowBuffer : public mysql::RowBuffer {
public:
    ProtoRowBuffer(proto::Result& res);
    virtual unsigned fetch(char* buffer, unsigned bufLen);
    std::string dump() override;

    /// Escape a bytestring for LOAD DATA INFILE, as specified by MySQL doc:
    /// https://dev.mysql.com/doc/refman/5.1/en/load-data.html
    /// This is limited to:
    /// Character    Escape Sequence
    /// \0     An ASCII NUL (0x00) character
    /// \b     A backspace character
    /// \n     A newline (linefeed) character
    /// \r     A carriage return character
    /// \t     A tab character.
    /// \Z     ASCII 26 (Control+Z)
    /// \N     NULL
    ///
    /// @return the number of bytes written to dest
    template <typename Iter, typename CIter>
    static inline int escapeString(Iter destBegin, CIter srcBegin, CIter srcEnd) {
        //mysql_real_escape_string(_mysql, cursor, col, r.lengths[i]);
        // empty string isn't escaped
        if (srcEnd == srcBegin) return 0;
        assert(srcEnd - srcBegin > 0);
        assert(srcEnd - srcBegin < std::numeric_limits<int>::max() / 2);
        Iter destI = destBegin;
        for(CIter i = srcBegin; i != srcEnd; ++i) {
            switch(*i) {
              case '\0':   *destI++ = '\\'; *destI++ = '0'; break;
              case '\b':   *destI++ = '\\'; *destI++ = 'b'; break;
              case '\n':   *destI++ = '\\'; *destI++ = 'n'; break;
              case '\r':   *destI++ = '\\'; *destI++ = 'r'; break;
              case '\t':   *destI++ = '\\'; *destI++ = 't'; break;
              case '\032': *destI++ = '\\'; *destI++ = 'Z'; break;
              default: *destI++ = *i; break;
                // Null (\N) is not treated by escaping in this context.
            }
        }
        return destI - destBegin;
    }

    /// Copy a rawColumn to an STL container
    template <typename T>
    static inline int copyColumn(T& dest, std::string const& rawColumn) {
        int existingSize = dest.size();
        dest.resize(existingSize + 2 + 2 * rawColumn.size());
        dest[existingSize] = '\'';
        int valSize = escapeString(dest.begin() + existingSize + 1,
                                   rawColumn.begin(), rawColumn.end());
        dest[existingSize + 1 + valSize] = '\'';
        dest.resize(existingSize + 2 + valSize);
        return 2 + valSize;
    }

private:
    void _initCurrentRow();
    void _initSchema();
    void _readNextRow();
    // Copy a row bundle into a destination STL char container
    template <typename T>
    int _copyRowBundle(T& dest, proto::RowBundle const& rb) {
        int sizeBefore = dest.size();
        // Add jobId
        // dest.insert(dest.end(), _jobIdColName.begin(), _jobIdColName.end()) &&&
        for(int ci=0, ce=rb.column_size(); ci != ce; ++ci) {
            if (ci != 0) {
                dest.insert(dest.end(), _colSep.begin(), _colSep.end());
            }

            if (!rb.isnull(ci)) {
                copyColumn(dest, rb.column(ci));
            } else {
                dest.insert(dest.end(), _nullToken.begin(), _nullToken.end() );
            }
        }
        return dest.size() - sizeBefore;
    }


    std::string _colSep; ///< Column separator
    std::string _rowSep; ///< Row separator
    std::string _nullToken; ///< Null indicator (e.g. \N)
    proto::Result& _result; ///< Ref to Resultmessage

    std::string _jobIdColName{"queryJobId"};

    sql::Schema _schema; ///< Schema object
    int _rowIdx; ///< Row index
    int _rowTotal; ///< Total row count
    std::vector<char> _currentRow; ///< char buffer representing current row.
};



}}} // namespace lsst::qserv::rproc
#endif // LSST_QSERV_RPROC_PROTOROWBUFFER_H
