// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2019 LSST Corporation.
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

#ifndef LSST_QSERV_QUERY_SUBSETHELPER_H
#define LSST_QSERV_QUERY_SUBSETHELPER_H

#include <memory>
#include <vector>

namespace lsst::qserv::query {

template <typename T>
bool isSubsetOf(std::vector<std::shared_ptr<T>> const& a, std::vector<std::shared_ptr<T>> const& b) {
    if (a.size() != b.size()) {
        return false;
    }
    size_t size = a.size();
    for (size_t i = 0; i < size; ++i) {
        if (not a[i]->isSubsetOf(
                    *b[i]))  // nptodo pass in function to call? rename to for_each_i or something?
            return false;
    }
    return true;
}

template <typename T>
bool isSubsetOf(std::vector<T> const& a, std::vector<T> const& b) {
    if (a.size() != b.size()) {
        return false;
    }
    size_t size = a.size();
    for (size_t i = 0; i < size; ++i) {
        if (not a[i].isSubsetOf(b[i]))  // nptodo pass in function to call? rename to for_each_i or something?
            return false;
    }
    return true;
}

}  // namespace lsst::qserv::query

#endif  // LSST_QSERV_QUERY_SUBSETHELPER_H
