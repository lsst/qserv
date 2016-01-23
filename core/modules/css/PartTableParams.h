/*
 * LSST Data Management System
 * Copyright 2015-2016 AURA/LSST.
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
#ifndef LSST_QSERV_CSS_PARTTABLEPARAMS_H
#define LSST_QSERV_CSS_PARTTABLEPARAMS_H

// System headers

// Third-party headers

// Qserv headers

namespace lsst {
namespace qserv {
namespace css {

/// @addtogroup css

/**
 *  @ingroup css
 *
 *  @brief A container for partitioned table metadata.
 *
 *  If this metadata corresponds to director table then dirTable should be set
 *  to table name itself; and dirColName, latColName, and lonColName must be set.
 *  If this metadata is for non-director table then latColName and lonColName
 *  may be set if director table does not exist.
 */
struct PartTableParams {

    PartTableParams() : overlap(0.0), partitioned(false), subChunks(false) {}
    PartTableParams(std::string const& dirDb_, std::string const& dirTable_,
                    std::string const& dirColName_, std::string const& latColName_,
                    std::string const& lonColName_, double overlap_, bool partitioned_,
                    bool subChunks_) :
                        dirDb(dirDb_), dirTable(dirTable_), dirColName(dirColName_),
                        latColName(latColName_), lonColName(lonColName_),
                        overlap(overlap_), partitioned(partitioned_), subChunks(subChunks_) {}
    std::string dirDb;       ///< Director database name.
    std::string dirTable;    ///< Director table name.
    std::string dirColName;  ///< Column in current table mapping to objectId column in director table.
    std::string latColName;  ///< Name for latitude column in this table, may be empty.
    std::string lonColName;  ///< Name for longitude column in this table, may be empty.
    double overlap;          ///< Per-table overlap value.
    bool partitioned;        ///< True if table is chunked/partitioned
    bool subChunks;          ///< True if table is sub-chunked

    /// Returns true if table is partitioned
    bool isPartitioned() const {
        return partitioned;
    }

    /// Returns true if table is chunked === partitioned
    bool isChunked() const {
        return partitioned;
    }

    /// Returns true if table is sub-chunked
    bool isSubChunked() const {
        return subChunks;
    }

    /// Returns chunk level for this table
    int chunkLevel() const {
        if (isSubChunked()) return 2;
        if (isChunked()) return 1;
        return 0;
    }

    /** Returns the partitioning columns for the given table. This is a
      * 3-element vector containing the longitude, latitude, and secondary
      * index column name for that table. An empty string indicates
      * that a column is not available.
      */
    std::vector<std::string> partitionCols() const {
        return std::vector<std::string>{lonColName, latColName, dirColName};
    }

    /// Returns the names of all secondary index columns for the given table.
    std::vector<std::string> secIndexColNames() const {
        std::vector<std::string> res;
        if (not dirColName.empty()) res.push_back(dirColName);
        return res;
    }
};

}}} // namespace lsst::qserv::css

#endif // LSST_QSERV_CSS_PARTTABLEPARAMS_H
