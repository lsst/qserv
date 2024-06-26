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
 * @brief functions for comparing the value of 2 pointers of the same type, and containers of pointers, and
 * pointers to containers of objects, and pointers to containers of pointers, etc.
 * If both pointers are null the function evaluates to true.
 * If one pointer is null but the other is not it evaluates to false.
 * Otherwise it checks for equality (operator==) on the objects pointed to by the pointers.
 *
 * @author Nathan Pease, SLAC
 */

#ifndef LSST_QSERV_UTIL_POINTER_COMPARE_H
#define LSST_QSERV_UTIL_POINTER_COMPARE_H

#include <deque>
#include <vector>
#include <memory>

namespace lsst::qserv::util {

template <typename T>
bool ptrCompare(std::shared_ptr<T> const& lhs, std::shared_ptr<T> const& rhs) {
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
bool vectorPtrCompare(std::vector<std::shared_ptr<T>> const& lhs,
                      std::vector<std::shared_ptr<T>> const& rhs) {
    if (lhs.size() != rhs.size()) {
        return false;
    }
    if (std::equal(lhs.begin(), lhs.end(), rhs.begin(), ptrCompare<T>)) {
        return true;
    }
    return false;
}

template <typename T>
bool ptrVectorCompare(std::shared_ptr<std::vector<T>> const& lhs,
                      std::shared_ptr<std::vector<T>> const& rhs) {
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
bool ptrVectorPtrCompare(std::shared_ptr<std::vector<std::shared_ptr<T>>> const& lhs,
                         std::shared_ptr<std::vector<std::shared_ptr<T>>> const& rhs) {
    if (nullptr == lhs && nullptr == rhs) {
        return true;
    } else if (nullptr == lhs || nullptr == rhs) {
        return false;
    }
    return vectorPtrCompare<T>(*lhs, *rhs);
}

template <typename T>
bool ptrDequeCompare(std::shared_ptr<std::deque<T>> const& lhs, std::shared_ptr<std::deque<T>> const& rhs) {
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
struct Compare {
    using is_transparent = void;

    bool operator()(std::shared_ptr<T> const& a, std::shared_ptr<T> const& b) const { return *a < *b; }

    bool operator()(std::shared_ptr<T> const& a, int b) const { return *a < b; }

    bool operator()(int a, std::shared_ptr<T> const& b) const { return a < *b; }
};

}  // namespace lsst::qserv::util

#endif  // LSST_QSERV_UTIL_POINTER_COMPARE_H
