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
#ifndef LSST_QSERV_SQL_SQLTRANSACTION_H
#define LSST_QSERV_SQL_SQLTRANSACTION_H

// System headers

// Third-party headers

// Qserv headers
#include "SqlConnection.h"
#include "SqlErrorObject.h"

namespace lsst {
namespace qserv {
namespace sql {

/// @addtogroup sql

/**
 *  @ingroup sql
 *
 *  @brief Transaction management using RAII.
 *
 *  This class starts new transaction in constructor. Transaction
 *  is aborted in destructor and it can be committed by explicit
 *  call to commit() method.
 *
 *  Instances of this class are non-copyable, here is the typical example
 *  of code using transactions:
 *
 *    sql::SqlErrorObject errObj;
 *    SqlConnection conn(...);
 *    SqlTransaction trans(conn, errObj);
 *    if (errObj.isSet()) {
 *        throw SqlError(ERR_LOC, errObj);
 *    }
 *
 *    sql::SqlResults results;
 *    conn.runQuery(query, results, errObj);
 *    if (errObj.isSet()) {
 *        throw SqlError(ERR_LOC, errObj);
 *    }
 *    trans.commit();
 *    return;
 *
 */

class SqlTransaction {
public:

    /// Constructor takes connection instance
    explicit SqlTransaction(SqlConnection& conn,
                            SqlErrorObject& errObj);

    // Instances cannot be copied
    SqlTransaction(SqlTransaction const&) = delete;
    SqlTransaction& operator=(SqlTransaction const&) = delete;

    /// Destructor aborts transaction if it has not been committed
    ~SqlTransaction();

    /// Explicitly commit transaction
    void commit(SqlErrorObject& errObj);

    /// Explicitly abort transaction
    void abort(SqlErrorObject& errObj);

    /// Returns true if transaction is active (no explicit commit/abort was called).
    bool isActive() const { return _doCleanup; }

protected:

private:

    SqlConnection& _conn;
    bool _doCleanup;

};

}}} // namespace lsst::qserv::sql

#endif // LSST_QSERV_SQL_SQLTRANSACTION_H
