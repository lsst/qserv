// -*- LSST-C++ -*-
/* LSST Data Management System
 * Copyright 2014-2016 AURA/LSST.
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

// Third-party headers

// Qserv headers
#include "sql/MockSql.h"

namespace lsst {
namespace qserv {

typedef std::vector<StringVector> StringVectorVector;

namespace { // anonymous
StringVectorVector vec;
}

namespace sql {
// class MockSql Implementation

std::shared_ptr<SqlResultIter>
MockSql::getQueryIter(std::string const& query) {
    typedef StringVectorVector::const_iterator SubIter;
    std::shared_ptr<Iter<SubIter> > iter = std::make_shared<Iter<SubIter> >(vec.begin(), vec.end());
    return iter;
}

} // namespace sql
}} // namespace lsst::qserv
