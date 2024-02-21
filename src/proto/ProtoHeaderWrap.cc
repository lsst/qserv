// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015-2016 LSST Corporation.
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

// System headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "proto/ProtoHeaderWrap.h"
#include "util/common.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.parser.ProtoHeaderWrap");
}

namespace lsst::qserv::proto {

// Google protobuffers are more efficient below 2MB, but xrootd is faster with larger limits.
// Reducing max to 2MB as it reduces the probablity of running out of memory.
const size_t ProtoHeaderWrap::PROTOBUFFER_DESIRED_LIMIT = 2000000;
// A single Google protobuffer can't be larger than this.
const size_t ProtoHeaderWrap::PROTOBUFFER_HARD_LIMIT = 64000000;

}  // namespace lsst::qserv::proto
