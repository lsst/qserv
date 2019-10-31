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
#include "replica/SqlSchemaUtils.h"

// System headers
#include <stdexcept>
#include <vector>

// Qserv headers
#include "util/File.h"

using namespace std;

namespace {

vector<string> splitByFirstSpace(const string& str) {
    vector<string> result;
    auto const pos = str.find(' ');
    if (pos != string::npos) {
        result.push_back(str.substr(0, pos));
        result.push_back(str.substr(pos+1));
    }
    return result;
}

} // namespace

namespace lsst {
namespace qserv {
namespace replica {

list<SqlColDef> SqlSchemaUtils::readFromTextFile(string const& fileName) {
    list<SqlColDef> columns;
    int lineNum = 0;
    for (auto&& line: util::File::getLines(fileName)) {
        ++lineNum;
        auto tokens = ::splitByFirstSpace(line);
        if (tokens.size() != 2 or tokens[0].empty() or tokens[1].empty()) {
            throw invalid_argument(
                    "SqlSchemaUtils::" + string(__func__) + "  invalid format at line: " +
                    to_string(lineNum) + " of file: " + fileName);
        }
        columns.emplace_back(tokens[0], tokens[1]);
    }
    return columns;
}

}}} // namespace lsst::qserv::replica
