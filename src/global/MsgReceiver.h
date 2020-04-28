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
#ifndef LSST_QSERV_MSGRECEIVER_H
#define LSST_QSERV_MSGRECEIVER_H
/**
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <string>

namespace lsst {
namespace qserv {

/// MsgReceiver : a functor for receiving simple messages. Used to encapsulate
/// the most basic error reporting so that downstream objects can report errors
/// without directly depending on a logging or error management facility.
class MsgReceiver {
public:
    virtual ~MsgReceiver() {}
    virtual void operator()(int code, std::string const& msg) = 0;
};

}} // namespace lsst::qserv

#endif // LSST_QSERV_MSGRECEIVER_H
