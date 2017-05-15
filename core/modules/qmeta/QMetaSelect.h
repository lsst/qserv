/*
 * LSST Data Management System
 * Copyright 2017 AURA/LSST.
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
#ifndef LSST_QSERV_QMETA_QMETASELECT_H
#define LSST_QSERV_QMETA_QMETASELECT_H

// System headers
#include <string>
#include <vector>

// Third-party headers

// Qserv headers
#include "mysql/MySqlConfig.h"
#include "sql/SqlConnection.h"
#include "sql/SqlResults.h"

namespace lsst {
namespace qserv {
namespace qmeta {

/// @addtogroup qmeta

/**
 *  @ingroup qmeta
 *
 *  @brief Select interface for QMeta database.
 *
 *  This is somewhat special class to used for selecting data from
 *  QMeta database. Unlike QMeta this is not an abstract class as it
 *  is tied very much to SQL implementation and exposes its details.
 */

class QMetaSelect {
public:

    /**
     *  @param mysqlConf: Configuration object for mysql connection
     */
    QMetaSelect(mysql::MySqlConfig const& mysqlConf);

    // Instances cannot be copied
    QMetaSelect(QMetaSelect const&) = delete;
    QMetaSelect& operator=(QMetaSelect const&) = delete;

    // Destructor
    virtual ~QMetaSelect();

    /**
     *  @brief Run arbitrary select on a table or view.
     *
     *  This is a very low-level interface for selecting data from QMeta tables.
     *  Its primary purpose is to implements support for "SHOW PROCESSLIST" commands.
     *  For that it is best to use special views (see QueryMetadata.sql).
     *
     *  @param query:   Complete SQL query.
     *  @returns SqlResults instance
     *  @throws SqlError
     */
    virtual std::unique_ptr<sql::SqlResults> select(std::string const& query);

protected:

    sql::SqlConnection _conn;

};

}}} // namespace lsst::qserv::qmeta

#endif // LSST_QSERV_QMETA_QMETASELECT_H
