/* 
 * LSST Data Management System
 * Copyright 2008, 2009, 2010 LSST Corporation.
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
 
#ifndef LSST_QSERV_SQLUTILS_H
#define LSST_QSERV_SQLUTILS_H

namespace lsst {
namespace qserv {

class SqlUtils {
public:
    bool dbExists(std::string const& dbName) {return _conn.dbExists(dbName);}
    bool createDb(std::string const& dbName, bool failIfExists=true);
    bool dropDb(std::string const& dbName);
    bool tableExists(std::string const& tableName, std::string const& dbName="");
    std::vector< std::string> listTables(std::string const& prefixed="",
                                         std::string const& dbName="");

private:
    SqlConnection _conn;
};

}} // namespace lsst::qserv

#endif // LSST_QSERV_SQLUTILS_H

