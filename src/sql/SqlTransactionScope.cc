/*
 * This file is part of qserv.
 *
 * Developed for the LSST Data Management System.
 * This product includes software developed by the LSST Project
 * (https://www.lsst.org).
 * See the COPYRIGHT file at the top-level directory of this distribution
 * for details of code ownership.
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
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

// Class header
#include "sql/SqlTransactionScope.h"

#include "lsst/log/Log.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.util.SqlTransactionStd");
}

namespace lsst { namespace qserv { namespace sql {

void SqlTransactionScope::verify() {
    if (errObj.isSet()) {
        LOGS(_log, LOG_LVL_ERROR, "Constructor failed (" << errObj.errNo() << ") " << errObj.errMsg());
        throwException(ERR_LOC, "Constructor failed");
    }
}

SqlTransactionScope::~SqlTransactionScope() {
    // instead of just destroying SqlTransaction instance we call abort and see
    // if error happens. We cannot throw here but we can print a message.
    if (trans.isActive()) {
        if (not trans.abort(errObj)) {
            LOGS(_log, LOG_LVL_ERROR,
                 "Failed to abort transaction: mysql error: (" << errObj.errNo() << ") " << errObj.errMsg());
        }
    }
}

void SqlTransactionScope::throwException(util::Issue::Context const& ctx, std::string const& msg) {
    throw util::Issue(ctx, msg + " mysql(" + std::to_string(errObj.errNo()) + " " + errObj.errMsg() + ")");
}

void SqlTransactionScope::commit() {
    if (not trans.commit(errObj)) {
        LOGS(_log, LOG_LVL_ERROR,
             "Failed to commit transaction: mysql error: (" << errObj.errNo() << ") " << errObj.errMsg());
        throwException(ERR_LOC, "Failed to commit transaction");
    }
}

void SqlTransactionScope::abort() {
    if (not trans.abort(errObj)) {
        LOGS(_log, LOG_LVL_ERROR,
             "Failed to abort transaction: mysql error: (" << errObj.errNo() << ")" << errObj.errMsg());
        throwException(ERR_LOC, "Failed to abort transaction");
    }
}

}}}  // namespace lsst::qserv::sql
