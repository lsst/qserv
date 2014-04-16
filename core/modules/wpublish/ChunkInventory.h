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
// ChunkInventory is a class that implements the capability to retrieve
// table publishing information from a qserv worker's associated mysqld.
// It includes helper functions for checking the resulting data structure for
// the existence of certain xrootd paths.

#ifndef LSST_QSERV_WPUBLISH_CHUNKINVENTORY_H
#define LSST_QSERV_WPUBLISH_CHUNKINVENTORY_H
#include <deque>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <boost/shared_ptr.hpp>

namespace lsst {
namespace qserv {

class SqlConnection;

namespace worker {
class WLogger; // Forward
} // namespace worker

namespace wpublish {

class ChunkInventory {
public:
    typedef std::deque<std::string> StringDeque;
    // These should be converted to unordered_* with C++11
    typedef std::set<std::string> StringSet;
    typedef std::map<int,StringSet> ChunkMap;
    typedef std::map<std::string, ChunkMap> ExistMap;

    ChunkInventory(std::string const& name, worker::WLogger& log);
    ChunkInventory(std::string const& name, worker::WLogger& log,
                   boost::shared_ptr<SqlConnection> sc);

    static inline std::string makeKey(std::string const& db, int chunk) {
        std::stringstream ss;
        ss << db << chunk << "**key";
        return std::string(ss.str());
    }
    bool has(std::string const& db, int chunk, std::string table=std::string());
    void dbgPrint(std::ostream& os);
private:
    void _init(SqlConnection& sc);
    void _fillDbChunks(ChunkInventory::StringSet& s);

    ExistMap _existMap;
    std::string _name;
    worker::WLogger& _log;
};
}}} // namespace lsst::qserv::wpublish

#endif // LSST_QSERV_WPUBLISH_CHUNKINVENTORY_H
