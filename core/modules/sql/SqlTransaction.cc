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
#include "sql/SqlTransaction.h"

// System headers

// Third-party headers

// Qserv headers

namespace lsst {
namespace qserv {
namespace sql {

// Constructor
SqlTransaction::SqlTransaction(SqlConnection& conn, SqlErrorObject& errObj)
    : _conn(conn), _doCleanup(false) {
    _conn.runQuery("START TRANSACTION", errObj);
    _doCleanup = not errObj.isSet();
}

// Destructor
SqlTransaction::~SqlTransaction() {
    if (_doCleanup) {
        SqlErrorObject errObj;
        _conn.runQuery("ROLLBACK", errObj);
    }
}

// Explicitly commit transaction
void
SqlTransaction::commit(SqlErrorObject& errObj) {
    _conn.runQuery("COMMIT", errObj);
    _doCleanup = false;
}

// Explicitly abort transaction
void
SqlTransaction::abort(SqlErrorObject& errObj) {
    _conn.runQuery("ROLLBACK", errObj);
    _doCleanup = false;
}

}}} // namespace lsst::qserv::sql
