
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
#ifndef LSST_QSERV_REPLICA_CHUNKEDTABLE_H
#define LSST_QSERV_REPLICA_CHUNKEDTABLE_H

// System headers
#include <string>

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class ChunkedTable represents a utility for parsing and building
 * names of the chunked tables.
 */
class ChunkedTable {
public:
    // Default construction, copy and assignment semantics is needed
    // for storing objects of the class in the value-based containers.

    ChunkedTable() = default;
    ChunkedTable(ChunkedTable const&) = default;
    ChunkedTable& operator=(ChunkedTable const&) = default;

    /// Construct the object from the components
    ChunkedTable(std::string const& baseName, unsigned int chunk, bool overlap = false);

    /**
     * Construct the object from the full name of the table
     * @param name_ the full name of a table to be parsed
     * @throws std::invalid_argument if the specified name doesn't correspond
     *   to a valid name of a table.
     */
    explicit ChunkedTable(std::string const& name_);

    ~ChunkedTable() = default;

    bool operator==(ChunkedTable const& rhs) const { return _name == rhs._name; }
    bool operator!=(ChunkedTable const& rhs) const { return not operator==(rhs); }

    bool valid() const { return not _baseName.empty(); }

    /**
     * @throws std::invalid_argument if the object is not in the valid state
     * @return the base name of the table
     */
    std::string const& baseName() const;

    /**
     * @throws std::invalid_argument if the object is not in the valid state
     * @return the chunk number attribute of the table
     */
    unsigned int chunk() const;

    /**
     * @throws std::invalid_argument if the object is not in the valid state
     * @return the 'overlap' attribute of the table
     */
    bool overlap() const;

    /**
     * @throws std::invalid_argument if the object is not in the valid state
     * @return the full name of the table
     */
    std::string const& name() const;

private:
    /// @throws std::invalid_argument if the object is not in the valid state
    void _assertValid() const;

    std::string _baseName;
    unsigned int _chunk = 0;
    bool _overlap = false;
    std::string _name;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_CHUNKEDTABLE_H
