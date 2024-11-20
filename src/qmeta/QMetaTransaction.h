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
#include "sql/SqlTransactionScope.h"

namespace lsst::qserv::qmeta {

/// @addtogroup qmeta

/**
 *  @ingroup qmeta
 *
 *  @brief High-level wrapper for SqlTransaction class.
 *
 *  This wrapper generates exceptions when errors happen
 *  during calls to SqlTransaction instance.
 */
class QMetaTransaction : public sql::SqlTransactionScope {
public:
    using Ptr = std::shared_ptr<QMetaTransaction>;

    static Ptr create(sql::SqlConnection& conn) {
        return sql::SqlTransactionScope::create<QMetaTransaction>(conn);
    }

    ~QMetaTransaction() override {};

    void throwException(util::Issue::Context const& ctx, std::string const& msg) override;

    friend sql::SqlTransactionScope;

private:
    QMetaTransaction(sql::SqlConnection& conn) : sql::SqlTransactionScope(conn) {}
};

}  // namespace lsst::qserv::qmeta

#endif  // LSST_QSERV_QMETA_QMETATRANSACTION_H
