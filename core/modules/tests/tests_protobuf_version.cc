/*
 * LSST Data Management System
 * Copyright 2009-2018 AURA/LSST.
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

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "proto/worker.pb.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.tests.tests_protobuf_version");
}

int main (int arc, char const* argv[]) {

    LOGS(_log, LOG_LVL_INFO, "testing a version of the Protobuf library");

    // Verify that the version of the library that we linked against is
    // compatible with the version of the headers we compiled against.

    GOOGLE_PROTOBUF_VERIFY_VERSION;

    LOGS(_log, LOG_LVL_INFO, "success");

    return 0;
}
