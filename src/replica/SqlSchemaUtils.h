/*
 * LSST Data Management System
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
#ifndef LSST_QSERV_REPLICA_SQLSCHEMAUTILS_H
#define LSST_QSERV_REPLICA_SQLSCHEMAUTILS_H

// System headers
#include <list>
#include <string>
#include <vector>

// Qserv headers
#include "replica/Common.h"

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * Utility class SqlSchemaUtils hosts tools for manipulating schema(s).
 */
class SqlSchemaUtils {
public:
    /**
     * Read column definitions from a text file. Each column is defined
     * on a separate line of a file. And the format of the file looks
     * like this:
     * 
     *   <column-name> <column-type-definition>
     *
     * @param fileName  the name of a file to be parsed
     *
     * @return a collection of column definitions representing the name of a column
     *   and its MySQL type definition
     *
     * @throws std::invalid_argument  if the file can't be open/read
     *   or if it has a non-valid format
     */
    static std::list<SqlColDef> readFromTextFile(std::string const& fileName);

    /**
     * Read column definitions of an index specification from a text file.
     * Each column is defined on a separate line of a file. And the format of
     * the file looks like this:
     * 
     *   <column-name> <length> <ascending-flag>
     *
     * Where:
     *   'column-name'    - the name of a column
     *   'length'         - the length of a sub-string used for an index
     *   'ascending-flag' - the numeric flag defining the sorting order ('1' - for ascending,
     *                      and '0' for descending).
     *
     * @param fileName  the name of a file to be parsed
     *
     * @return a collection of column definitions needed for creating an index
     *
     * @throws std::invalid_argument  if the file can't be open/read
     *   or if it has a non-valid format
     */
    static std::vector<SqlIndexColumn> readIndexSpecFromTextFile(std::string const& fileName);
};

}}} // namespace lsst::qserv::replica

#endif /* LSST_QSERV_REPLICA_SQLSCHEMAUTILS_H */
