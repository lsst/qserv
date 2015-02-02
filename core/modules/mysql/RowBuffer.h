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
#ifndef LSST_QSERV_MYSQL_ROWBUFFER_H
#define LSST_QSERV_MYSQL_ROWBUFFER_H

// System headers
#include <string>

// Third-party headers
#include "boost/shared_ptr.hpp"
#include <mysql/mysql.h>

namespace lsst {
namespace qserv {
namespace proto {
class Result;
}}}

namespace lsst {
namespace qserv {
namespace mysql {

/// Row is a mysql row abstraction that bundles field sizes and counts. Row is
/// shallow, and does not perform any memory management.
struct Row {
    Row() : row(NULL), lengths(NULL), numFields(-1) {}

    // Shallow copies all-around.
    Row(char** row_, unsigned long int* lengths_, int numFields_)
        : row(row_), lengths(lengths_), numFields(numFields_) {}

    unsigned int minRowSize() const {
        unsigned int sum = 0;
        for(int i=0; i < numFields; ++i) {
            sum += lengths[i];
        }
        return sum;
    }

    char** row;
    unsigned long int* lengths;
    int numFields;
};

/// RowBuffer: an buffer from which arbitrarily-sized buckets of bytes
/// can be read. The buffer represents a tab-separated-field,
/// line-delimited-tuple sequence of tuples.
class RowBuffer {
public:
    typedef boost::shared_ptr<RowBuffer> Ptr;

    virtual ~RowBuffer() {};

    /// Fetch a number of bytes into a buffer. Return the number of bytes
    /// fetched. Returning less than bufLen does NOT indicate EOF.
    virtual unsigned fetch(char* buffer, unsigned bufLen) = 0;

    /// Construct a RowBuffer tied to a MySQL query result
    static Ptr newResRowBuffer(MYSQL_RES* result);
};

}}} // lsst::qserv::mysql
#endif // LSST_QSERV_MYSQL_ROWBUFFER_H
