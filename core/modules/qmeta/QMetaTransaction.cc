/*
 * LSST Data Management System
 * Copyright 2015 AURA/LSST.
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

// logger instance for this module
LOG_LOGGER _logger = LOG_GET("lsst.qserv.qmeta.QMetaTransaction");

}

namespace lsst {
namespace qserv {
namespace qmeta {

// Constructors
QMetaTransaction::QMetaTransaction(sql::SqlConnection& conn)
    : _errObj(), _trans(conn, _errObj) {
    if (_errObj.isSet()) {
        throw SqlError(ERR_LOC, _errObj);
    }
}

// Destructor
QMetaTransaction::~QMetaTransaction() {
    // instead of just destroying SqlTransaction instance we call abort and see
    // if error happens. We cannot throw here but we can print a message.
    if (_trans.isActive()) {
        if (not _trans.abort(_errObj)) {
            LOGF(_logger, LOG_LVL_ERROR, "Failed to abort transaction: mysql error: (%1%) %2%" %
                 _errObj.errNo() % _errObj.errMsg());
        }
    }
}

/// Explicitly commit transaction, throws SqlError for errors.
void
QMetaTransaction::commit() {
    if (not _trans.commit(_errObj)) {
        LOGF(_logger, LOG_LVL_ERROR, "Failed to commit transaction: mysql error: (%1%) %2%" %
             _errObj.errNo() % _errObj.errMsg());
        throw SqlError(ERR_LOC, _errObj);
    }
}

/// Explicitly abort transaction, throws SqlError for errors.
void
QMetaTransaction::abort() {
    if (not _trans.abort(_errObj)) {
        LOGF(_logger, LOG_LVL_ERROR, "Failed to abort transaction: mysql error: (%1%) %2%" %
             _errObj.errNo() % _errObj.errMsg());
        throw SqlError(ERR_LOC, _errObj);
    }
}

}}} // namespace lsst::qserv::qmeta
