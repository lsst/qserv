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

// Class header
#include "replica/SqlResultSet.h"

using namespace std;

namespace lsst {
namespace qserv {
namespace replica {

SqlResultSet::Field::Field(ProtocolResponseSqlField const& field)
    :   name(     field.name()),
        orgName(  field.org_name()),
        table(    field.table()),
        orgTable( field.org_table()),
        db(       field.db()),
        catalog(  field.catalog()),
        def(      field.def()),
        length(   field.length()),
        maxLength(field.max_length()),
        flags(    field.flags()),
        decimals( field.decimals()),
        type(     field.type()) {
}


SqlResultSet::Row::Row(ProtocolResponseSqlRow const& row) {
    for (int i = 0; i < row.cells_size(); ++i) {
        cells.push_back(row.cells(i));
        nulls.push_back(row.nulls(i));
    }
}


void SqlResultSet::set(ProtocolResponseSql const& message) {

    hasResult = message.has_result();

    // Translate fields
    fields.clear();
    for (int i = 0; i < message.fields_size(); ++i) {
        fields.emplace_back(message.fields(i));
    }

    // Translate rows
    rows.clear();
    for (int i = 0; i < message.rows_size(); ++i) {
        rows.emplace_back(message.rows(i));
    }
}    

}}} // namespace lsst::qserv::replica
