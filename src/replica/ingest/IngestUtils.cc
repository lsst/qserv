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
#include "replica/ingest/IngestUtils.h"

// Qserv headers
#include "http/RequestBodyJSON.h"
#include "replica/util/Csv.h"

using namespace std;

namespace lsst::qserv::replica {

csv::DialectInput parseDialectInput(http::RequestBodyJSON const& body) {
    csv::DialectInput dialectInput;

    // Allow an empty string in the input. Simply replace the one (if present) with
    // the corresponding default value of the parameter.
    auto const getDialectParam = [&](string const& param, string const& defaultValue) -> string {
        string val = body.optional<string>(param, defaultValue);
        if (val.empty()) val = defaultValue;
        return val;
    };
    dialectInput.fieldsTerminatedBy =
            getDialectParam("fields_terminated_by", csv::Dialect::defaultFieldsTerminatedBy);
    dialectInput.fieldsEnclosedBy =
            getDialectParam("fields_enclosed_by", csv::Dialect::defaultFieldsEnclosedBy);
    dialectInput.fieldsEscapedBy = getDialectParam("fields_escaped_by", csv::Dialect::defaultFieldsEscapedBy);
    dialectInput.linesTerminatedBy =
            getDialectParam("lines_terminated_by", csv::Dialect::defaultLinesTerminatedBy);

    return dialectInput;
}

}  // namespace lsst::qserv::replica