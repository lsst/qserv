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
#ifndef LSST_QSERV_QMETA_QMETATRANSACTION_H
#define LSST_QSERV_QMETA_QMETATRANSACTION_H

// System headers

// Third-party headers

// Qserv headers
#include "sql/SqlErrorObject.h"
#include "sql/SqlTransaction.h"


namespace lsst {
namespace qserv {
namespace qmeta {

/// @addtogroup qmeta

/**
 *  @ingroup qmeta
 *
 *  @brief High-level wrapper for SqlTransaction class.
 *
 *  This wrapper generates exceptions when errors happen
 *  during calls to SqlTransaction instance.
 */

class QMetaTransaction  {
public:

    /// Constructor takes connection instance. It starts transaction.
    /// trows exception if error happens.
    QMetaTransaction(sql::SqlConnection& conn);

    // Instances cannot be copied
    QMetaTransaction(QMetaTransaction const&) = delete;
    QMetaTransaction& operator=(QMetaTransaction const&) = delete;

    /// Destructor aborts transaction if it was not explicitly committed
    /// or aborted. If error happens then no exception is generated
    /// (destructors cannot throw).
    ~QMetaTransaction();

    /// Explicitly commit transaction, throws SqlError for errors.
    void commit();

    /// Explicitly abort transaction, throws SqlError for errors.
    void abort();

private:

    sql::SqlErrorObject _errObj; // this must be declared before _trans
    sql::SqlTransaction _trans;

};

}}} // namespace lsst::qserv::qmeta

#endif // LSST_QSERV_QMETA_QMETATRANSACTION_H
