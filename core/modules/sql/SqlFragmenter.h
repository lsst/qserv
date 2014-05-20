// -*- LSST-C++ -*-
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
#ifndef LSST_QSERV_SQL_SQLFRAGMENTER_H
#define LSST_QSERV_SQL_SQLFRAGMENTER_H

// System headers
#include <string>

namespace lsst {
namespace qserv {
namespace sql {

// SqlFragmenter: Simple iteration over pieces of a longer batch
// of sql statements.
class SqlFragmenter {
public:
    typedef std::pair<char const*, int> Piece;

    SqlFragmenter(std::string const& query);

    bool isDone() const { return _pNext == _qEnd; }
    Piece const& getNextPiece();
    unsigned getCount() const { return _count; }
private:
    void _advance();

    // Constant
    static const std::string _delimiter;

    // Iteration state
    std::string _query;
    std::string::size_type _pNext;
    std::string::size_type _qEnd;
    std::string::size_type _sizeTarget;
    unsigned _count;
    Piece _current;
};

}}} // namespace lsst::qserv::sql

#endif // LSST_QSERV_SQL_SQLFRAGMENTER_H
