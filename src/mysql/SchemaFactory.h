// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014 LSST Corporation.
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
#ifndef LSST_QSERV_MYSQL_SCHEMAFACTORY_H
#define LSST_QSERV_MYSQL_SCHEMAFACTORY_H
// MySQL-dependent construction of schema objects. Separated from sql::*Schema
// to provide better isolation of sql module from mysql-isms.

// Third-party headers
#include <mysql/mysql.h>

// Qserv headers
#include "sql/Schema.h"

namespace lsst {
namespace qserv {
namespace mysql {
class SchemaFactory {
public:
    static sql::ColSchema newColSchema(MYSQL_FIELD const& f);
    static sql::Schema newFromResult(MYSQL_RES* result);
};

}}} // namespace lsst::qserv::mysql
#endif // LSST_QSERV_MYSQL_SCHEMAFACTORY_H
