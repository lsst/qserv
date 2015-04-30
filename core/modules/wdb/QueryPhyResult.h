// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2014 LSST Corporation.
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

#ifndef LSST_QSERV_WDB_QUERYPHYRESULT_H
#define LSST_QSERV_WDB_QUERYPHYRESULT_H

// System headers
#include <set>
#include <string>

// Third-party headers
#include "boost/shared_ptr.hpp"

// Local headers
#include "lsst/log/Log.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace sql {
    class SqlErrorObject;
}
namespace wbase {
    class SendChannel;
}}} // End of forward declarations

namespace lsst {
namespace qserv {
namespace wdb {

/// Management class for handling query results.
/// Dumps the specified result tables to a file or to a SendChannel
class QueryPhyResult {
public:
    typedef std::set<std::string> StringSet;

    std::string const& getDb() const { return _outDb; }
    void setDb(std::string const& d) { _outDb = d; }

    void addResultTable(std::string const& t);
    bool hasResultTable(std::string const& t) const;
    std::string getCommaResultTables() ;

    void reset();

    /// Dump results to a file
    bool performMysqldump(LOG_LOGGER const& log,
                          std::string const& user,
                          std::string const& dumpFile,
                          sql::SqlErrorObject&);

    /// Dump results to a SendChannel
    bool dumpToChannel(LOG_LOGGER const& log,
                       std::string const& user,
                       boost::shared_ptr<wbase::SendChannel> sc,
                       sql::SqlErrorObject&);

private:
    void _mkdirP(std::string const& filePath);
    std::string _getSpaceResultTables() const;
    std::string _computeTmpFileName() const;

    StringSet _resultTables;
    std::string _outDb;
};

}}} // namespace lsst::qserv::wdb

#endif // LSST_QSERV_WDB_QUERYPHYRESULT_H
