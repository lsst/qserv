// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2018 AURA/LSST.
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
/**
 * @file
 *
 * @ingroup util
 *
 * @brief pointerCompare is a function for comparing the value of 2 pointers of the same type.
 * If both pointers are null it returns true.
 * If one pointer is null but the other is not it returns false.
 * Otherwise it checks for equality (operator==) on the objects pointed to by the pointers.
 *
 * @author Nathan Pease, SLAC
 */

#ifndef LSST_QSERV_UTIL_POINTER_COMPARE_H
#define LSST_QSERV_UTIL_POINTER_COMPARE_H

namespace lsst {
namespace qserv {
namespace util {


template <typename T>
bool pointerCompare(T lhs, T rhs) {
    if (lhs == nullptr) {
        if (rhs == nullptr) {
            return true;
        }
        return false;
    }
    if (rhs == nullptr && lhs != nullptr) {
        return false;
    }
    return *lhs == *rhs;
}


template <typename T>
bool vectorPointerCompare(T lhs, T rhs) {
    if (lhs.size() != rhs.size()) {
        return false;
    }
    for (unsigned int i=0; i<lhs.size(); ++i) {
        if (false == pointerCompare(lhs[i], rhs[i])) {
            return false;
        }
    }
    return true;
}


}}} // namespace lsst::qserv::util

#endif // LSST_QSERV_UTIL_POINTER_COMPARE_H
