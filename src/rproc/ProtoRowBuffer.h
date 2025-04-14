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

// System headers
#include <string>
#include <vector>

// Qserv headers
#include "mysql/MySqlUtils.h"
#include "mysql/RowBuffer.h"
#include "proto/worker.pb.h"

namespace lsst::qserv::rproc {

/// ProtoRowBuffer is an implementation of RowBuffer designed to allow a
/// LocalInfile object to use a Protobufs Result message as a row source
class ProtoRowBuffer : public mysql::RowBuffer {
public:
    ProtoRowBuffer(proto::ResponseData const& res);
    unsigned fetch(char* buffer, unsigned bufLen) override;
    std::string dump() const override;

    /// Copy a rawColumn to an STL container
    template <typename T>
    static inline int copyColumn(T& dest, std::string const& rawColumn) {
        int existingSize = dest.size();
        dest.resize(existingSize + 2 + 2 * rawColumn.size());
        dest[existingSize] = '\'';
        int valSize =
                mysql::escapeString(dest.begin() + existingSize + 1, rawColumn.begin(), rawColumn.end());
        dest[existingSize + 1 + valSize] = '\'';
        dest.resize(existingSize + 2 + valSize);
        return 2 + valSize;
    }

private:
    void _initCurrentRow();
    void _readNextRow();
    // Copy a row bundle into a destination STL char container
    template <typename T>
    int _copyRowBundle(T& dest, proto::RowBundle const& rb) {
        int sizeBefore = dest.size();
        for (int ci = 0, ce = rb.column_size(); ci != ce; ++ci) {
            // Don't add column separator before the first column
            if (ci != 0) {
                dest.insert(dest.end(), _colSep.begin(), _colSep.end());
            }
            if (!rb.isnull(ci)) {
                copyColumn(dest, rb.column(ci));
            } else {
                dest.insert(dest.end(), _nullToken.begin(), _nullToken.end());
            }
        }
        return dest.size() - sizeBefore;
    }

    std::string _colSep;                 ///< Column separator
    std::string _rowSep;                 ///< Row separator
    std::string _nullToken;              ///< Null indicator (e.g. \N)
    proto::ResponseData const& _result;  ///< Ref to the ResponseData message

    int _rowIdx;                    ///< Row index
    int _rowTotal;                  ///< Total row count
    std::vector<char> _currentRow;  ///< char buffer representing current row.
};

}  // namespace lsst::qserv::rproc
#endif  // LSST_QSERV_RPROC_PROTOROWBUFFER_H
