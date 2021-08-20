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
#ifndef LSST_QSERV_QCACHE_PAGE_H
#define LSST_QSERV_QCACHE_PAGE_H

// System headers
#include <cstddef>
#include <memory>

// Third party headers
#include <mysql/mysql.h>

// Qserv headers
#include "qcache/PageIface.h"

// This header declarations
namespace lsst {
namespace qserv {
namespace qcache {

/**
  * Class Page is ...
  */
class Page final: public PageIface {
public:
    Page() = delete;
    Page(Page const&) = delete;
    Page& operator=(Page const&) = delete;

    virtual ~Page() = default;

    char const* data() const override { return _data.get(); }
    std::size_t sizeBytes() const override { return _sizeBytes; }
    std::size_t sizeRows() const override { return _sizeRows; }

    /**
     * Store a row in the buffer. Update counters.
     *
     * @param numFields The number of fields returned by MySQL's function mysql_numFields()
     * @param row The row returned by the last call to MySQL's function mysql_fetch_row()
     * @param lengths The array returned by the last call to MySQL's function mysql_fetch_lengths()
     *   made after calling mysql_fetch_row().
     * @throw std::invalid_argument For zero values of the input parameters.
     * @throws PageOverflow If the page has reached its capacity, and the row can't
     *   be added.
     */
    void add(unsigned int numFields, char const** row, long const* lengths);

    /**
     * The overloaded version of the method for MySQL type MYSQL_ROW which is
     * defined as char**.
     */
    void add(unsigned int numFields, MYSQL_ROW row, long const* lengths) {
        add(numFields, const_cast<char const**>(row), lengths);
    }

    friend class Pool;

private:
    /**
     * Create the Page with the specified capacity.
     * @param pool The page pool where the page will be returned 
    static std::shared_ptr<Page> create(std::shared_ptr<Page> const& pool,
                                        std::size_t capacityBytes);

    Page(std::size_t capacityBytes);

    // Parameters
    std::size_t const _capacityBytes;

    std::unique_ptr<char[]> _data;
    std::size_t _sizeBytes = 0;
    std::size_t _sizeRows = 0;

    // Links to the neighboring pages in the double-linked list of the page Pool.
    std::shared_ptr<Page> _prev;
    std::shared_ptr<Page> _next;
};

}}} // namespace lsst::qserv::qcache

#endif // LSST_QSERV_QCACHE_PAGE_H
