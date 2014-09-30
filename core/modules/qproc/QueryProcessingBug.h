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
#ifndef LSST_QSERV_QPROC_QUERYPROCESSINGBUG_H
#define LSST_QSERV_QPROC_QUERYPROCESSINGBUG_H

// Qserv headers
#include "global/Bug.h"

namespace lsst {
namespace qserv {
namespace qproc {

/// QueryProcessingBug is a trivial exception that marks a bug in qproc
class QueryProcessingBug : public Bug {
public:
    explicit QueryProcessingBug(char const* msg) : Bug(msg) {}
    explicit QueryProcessingBug(std::string const& msg) : Bug(msg) {}
};
}}} // namespace lsst::qserv::qana

#endif // LSST_QSERV_QPROC_QUERYPROCESSINGBUG_H
