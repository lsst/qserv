// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2014 LSST Corporation.
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
// Callback: generic callback interface class.
#ifndef LSST_QSERV_CCONTROL_CALLBACK_H
#define LSST_QSERV_CCONTROL_CALLBACK_H

// Third-party headers
#include "boost/shared_ptr.hpp"

namespace lsst {
namespace qserv {
namespace ccontrol {

class Callback {
public:
    typedef boost::shared_ptr<Callback> Ptr;
    virtual ~Callback() {}
    void operator()() {}
};

}}} // namespace lsst::qserv::ccontrol

#endif // LSST_QSERV_CCONTROL_CALLBACK_H
