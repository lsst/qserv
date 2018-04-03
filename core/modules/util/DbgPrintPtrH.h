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
 * @brief Helper class for printing an object with a `dbgPrint` method to an ostream using the insertion
 *        operator inline. For example `myostream << DbgPrintHelper(myObject)`
 *        This is useful for cases where the ostream operator<< has been used for object serialization (as is
 *        the case with the query objects, where it writes the query IR back to an SQL query string using
 *        std::ostream& operator<<)
 *
 *        Note for efficiency the object does not keep its own copy of the passed-in shared pointer (thus
 *        eliminating the reference count change mechanics. DO NOT KEEP AN INSTANTIATED COPY OF THIS CLASS;
 *        the shared_ptr may be released. Use the DbgPrintHelper in the generation of on ostream and then
 *        throw it away.
 *
 *        A nullptr may be passed into the constructor, in that case 'nullptr' will be printed out.
 *
 * @author Nathan Pease, SLAC
 */

#ifndef LSST_QSERV_UTIL_DBG_PRINT_HELPER_H
#define LSST_QSERV_UTIL_DBG_PRINT_HELPER_H

#include <deque>
#include <memory>
#include <ostream>
#include <vector>

namespace lsst {
namespace qserv {
namespace util {


template <typename T>
class DbgPrintH {
public:
    DbgPrintH(const T& dumpee)
    : _dumpee(dumpee) {}

    const T& _dumpee;
};

template <typename T>
std::ostream& operator<<(std::ostream& os, const DbgPrintH<T>& helper) {
    helper._dumpee.dbgPrint(os);
    return os;
}


template <typename T>
class DbgPrintPtrH {
public:
    DbgPrintPtrH(const std::shared_ptr<T>& dumpee)
    : _dumpee(dumpee) {}

    const std::shared_ptr<T>& _dumpee;
};

template <typename T>
std::ostream& operator<<(std::ostream& os, DbgPrintPtrH<T> const& dbgPrintHelper) {
    if (dbgPrintHelper._dumpee != nullptr) {
        dbgPrintHelper._dumpee->dbgPrint(os);
    } else {
        os << "nullptr";
    }
    return os;
}


template <typename T>
class DbgPrintVectorH {
public:
    DbgPrintVectorH(const std::vector<T>& dumpees)
    : _dumpees(dumpees) {}

    const std::vector<T>& _dumpees;
};

template <typename T>
std::ostream& operator<<(std::ostream& os, DbgPrintVectorH<T> const& helper)  {
    os << "(";
    for (const T & dumpee : helper._dumpees) {
        os << dumpee;
        if (dumpee != helper._dumpees.back()) {
            os << ", ";
        }
    }
    os << ")";
    return os;
}


template <typename T>
class DbgPrintVectorPtrH {
public:
    DbgPrintVectorPtrH(const std::vector<std::shared_ptr<T>>& dumpees)
    : _dumpees(dumpees) {}

    const std::vector<std::shared_ptr<T>>& _dumpees;
};

template <typename T>
std::ostream& operator<<(std::ostream& os, DbgPrintVectorPtrH<T> const& dbgPrintVectorHelper)  {
    os << "(";
    for (const std::shared_ptr<T>& dumpee : dbgPrintVectorHelper._dumpees) {
        if (dumpee == nullptr) {
            os << "nullptr";
        } else {
            os << DbgPrintPtrH<T>(dumpee);
        }
        if (dumpee != dbgPrintVectorHelper._dumpees.back()) {
            os << ", ";
        }
    }
    os << ")";
    return os;
}


template <typename T>
class DbgPrintPtrVectorH {
public:
    DbgPrintPtrVectorH(const std::shared_ptr<std::vector<T>>& dumpees)
    : _dumpees(dumpees) {}

    const std::shared_ptr<std::vector<T>>& _dumpees;
};

template <typename T>
std::ostream& operator<<(std::ostream& os, DbgPrintPtrVectorH<T> const& helper)  {
    if (nullptr == helper._dumpees) {
        os << "nullptr";
    } else {
        os << "(";
        for (T & dumpee : *helper._dumpees) {
            os << DbgPrintH<T>(dumpee);
        }
    }
    os << ")";
    return os;
}


template <typename T>
class DbgPrintPtrDequeH {
public:
    DbgPrintPtrDequeH(const std::shared_ptr<std::deque<T>>& dumpees)
    : _dumpees(dumpees) {}

    const std::shared_ptr<std::deque<T>>& _dumpees;
};

template <typename T>
std::ostream& operator<<(std::ostream& os, DbgPrintPtrDequeH<T> const& helper)  {
    if (nullptr == helper._dumpees) {
        os << "nullptr";
    } else {
        os << "(";
        for (T & dumpee : *helper._dumpees) {
            os << DbgPrintH<T>(dumpee);
        }
    }
    os << ")";
    return os;
}


template <typename T>
class DbgPrintPtrVectorPtrH {
public:
    DbgPrintPtrVectorPtrH(const std::shared_ptr<std::vector<std::shared_ptr<T>>>& dumpees)
    : _dumpees(dumpees) {}

    const std::shared_ptr<std::vector<std::shared_ptr<T>>>& _dumpees;
};

template <typename T>
std::ostream& operator<<(std::ostream& os, DbgPrintPtrVectorPtrH<T> const& helper)  {
    if (nullptr == helper._dumpees) {
        os << "nullptr";
    } else {
        os << "(";
        for (const std::shared_ptr<T>& dumpee : *helper._dumpees) {
            if (dumpee == nullptr) {
                os << "nullptr";
            } else {
                os << DbgPrintPtrH<T>(dumpee);
            }
            if (dumpee != helper._dumpees->back()) {
                os << ", ";
            }
        }
        os << ")";
    }
    return os;
}

}}}

#endif

