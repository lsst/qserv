// -*- LSST-C++ -*-
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

#ifndef LSST_QSERV_QDISP_CHUNKMETA_H
#define LSST_QSERV_QDISP_CHUNKMETA_H

// System headers
#include <string>
#include <vector>

// Qserv headers
#include "util/InstanceCount.h" // &&&

namespace lsst {
namespace qserv {
namespace qdisp {

class ChunkMetaEntry {
public:
    ChunkMetaEntry(std::string const& db_,
                   std::string const& table_,
                   int chunkLevel_)
        : db(db_), table(table_), chunkLevel(chunkLevel_)
        { }
    std::string getTable() const { return table; }
    int getChunkLevel() const { return chunkLevel; }
private:
    std::string db;
    std::string table;
    int chunkLevel;
    util::InstanceCount _instC{"ChunkMetaEntry&&&"};
};
// class ChunkMeta is a value class that is used to transfer db/table
// information from the python layer into the C++ layer
//
class ChunkMeta {
public:
    typedef std::vector<ChunkMetaEntry> EntryVector;

    // Mutators:

    // Add a table to the whitelist.
    // Database db, Table table, chunkLevel as
    // 0: not partitioned, 1: chunked, 2: subchunked.
    void add(std::string const& db, std::string const& table, int chunkLevel) {
        _entries.push_back(ChunkMetaEntry(db, table, chunkLevel));
    }
    void add(ChunkMetaEntry const& e) { _entries.push_back(e); }

    // Const access (to create chunk mapping setup TableNamer)
    EntryVector const& getEntries() const { return _entries; }
private:
    EntryVector _entries;
    util::InstanceCount _instC{"ChunkMeta&&&"};
};

}}} // namespace lsst::qserv::qdisp

#endif // LSST_QSERV_QDISP_CHUNKMETA_H
