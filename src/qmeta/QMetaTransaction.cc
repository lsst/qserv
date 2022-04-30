/*
 * LSST Data Management System
 * Copyright 2015-2016 AURA/LSST.
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
 * see <https://www.lsstcorp.org/LegalNotices/>.
 */

// Class header
#include "QMetaTransaction.h"

// System headers

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "Exceptions.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.qmeta.QMetaTransaction");
}

namespace lsst::qserv::qmeta {

void QMetaTransaction::throwException(util::Issue::Context const& ctx, std::string const& msg) {
    LOGS(_log, LOG_LVL_WARN, " QMetaTransaction::throwException " + msg);
    throw SqlError(ctx, errObj);
}

}  // namespace lsst::qserv::qmeta
