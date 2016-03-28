// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015 LSST Corporation.
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
#ifndef LSST_QSERV_UTIL_CALLABLE_H
#define LSST_QSERV_UTIL_CALLABLE_H

// System headers
#include <memory>

// Qserv headers
#include "util/InstanceCount.h" // &&&

/// VoidCallable, UnaryCallable, and BinaryCallable are convenience templates
/// for declaring no-argument, unary-argument, and binary-argument function
/// objects. These classes are useful when defining interfaces.

namespace lsst {
namespace qserv {
namespace util {

template <typename Ret>
class VoidCallable {
public:
    typedef std::shared_ptr<VoidCallable<Ret> > Ptr;
    typedef Ret R;
    virtual ~VoidCallable() {}
    virtual Ret operator()() = 0;
    util::InstanceCount _instC{"VoidCallable&&&"};
};

template <typename Ret, typename Arg>
class UnaryCallable {
public:
    typedef std::shared_ptr<UnaryCallable<Ret,Arg> > Ptr;
    typedef Ret R;
    typedef Arg A;

    virtual ~UnaryCallable() {}
    virtual Ret operator()(Arg a) = 0;
    util::InstanceCount _instC{"UnaryCallable&&&"};
};

template <typename Ret, typename Arg1, typename Arg2>
class BinaryCallable {
public:
    typedef std::shared_ptr<BinaryCallable<Ret,Arg1,Arg2> > Ptr;
    typedef Ret R;
    typedef Arg1 A1;
    typedef Arg2 A2;

    virtual ~BinaryCallable() {}
    virtual Ret operator()(A1 a1, A2 a2) = 0;
    util::InstanceCount _instC{"BinaryCallable&&&"};
};

}}} // lsst::qserv::util
#endif // LSST_QSERV_UTIL_CALLABLE_H
