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

#include <deque>
#include <vector>
#include <memory>

namespace lsst {
namespace qserv {
namespace util {


template <typename T>
bool ptrCompare(const std::shared_ptr<T> & lhs, const std::shared_ptr<T> & rhs) {
    if (lhs == nullptr && rhs == nullptr) {
        return true;
    }
    if (rhs == nullptr || lhs == nullptr) {
        return false;
    }
    if (*lhs == *rhs) {
        return true;
    }
    return false;
}


template <typename T>
bool vectorPtrCompare(const std::vector<std::shared_ptr<T>> & lhs,
                      const std::vector<std::shared_ptr<T>> & rhs) {
    if (lhs.size() != rhs.size()) {
        return false;
    }
    if (std::equal(lhs.begin(), lhs.end(), rhs.begin(), ptrCompare<T>)) {
        return true;
    }
    return false;
}


template <typename T>
bool ptrVectorCompare(const std::shared_ptr<std::vector<T>> & lhs,
                      const std::shared_ptr<std::vector<T>> & rhs) {
    if (nullptr == lhs && nullptr == rhs) {
        return true;
    } else if (nullptr == lhs || nullptr == rhs) {
        return false;
    }
    if (*lhs == *rhs) {
        return true;
    }
    return false;
}


template <typename T>
bool ptrVectorPtrCompare(const std::shared_ptr<std::vector<std::shared_ptr<T>>> & lhs,
                         const std::shared_ptr<std::vector<std::shared_ptr<T>>> & rhs) {
    if (nullptr == lhs && nullptr == rhs) {
        return true;
    } else if (nullptr == lhs || nullptr == rhs) {
        return false;
    }
    return vectorPtrCompare<T>(*lhs, *rhs);
}


template <typename T>
bool ptrDequeCompare(const std::shared_ptr<std::deque<T>> & lhs,
                     const std::shared_ptr<std::deque<T>> & rhs) {
    if (nullptr == lhs && nullptr == rhs) {
        return true;
    } else if (nullptr == lhs || nullptr == rhs) {
        return false;
    }
    if (*lhs == *rhs) {
        return true;
    }
    return false;
}


}}} // namespace lsst::qserv::util

#endif // LSST_QSERV_UTIL_POINTER_COMPARE_H
