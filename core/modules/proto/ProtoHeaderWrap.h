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

#ifndef LSST_QSERV_PROTO_PROTO_HEADER_WRAP_H
#define LSST_QSERV_PROTO_PROTO_HEADER_WRAP_H
 /**
  * @file
  *
  * @brief Wrap the google protocol header in a fixed size container.
  *
  * @author John Gates, SLAC
  */

// System headers
#include <memory>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "proto/ProtoImporter.h"
#include "proto/WorkerResponse.h"

namespace lsst {
namespace qserv {
namespace proto {

class ProtoHeaderWrap {
public:
    static const size_t PROTO_HEADER_SIZE;
    static const size_t PROTOBUFFER_DESIRED_LIMIT;
    static const size_t PROTOBUFFER_HARD_LIMIT;
    ProtoHeaderWrap() {};
    virtual ~ProtoHeaderWrap() {};

    static std::string wrap(std::string& protoHeaderString);
    static bool unwrap(std::shared_ptr<WorkerResponse>& response, std::vector<char>& buffer);
};

}}} // end namespace

#endif
