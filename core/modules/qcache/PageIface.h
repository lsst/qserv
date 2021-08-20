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
#ifndef LSST_QSERV_QCACHE_PAGEIFACE_H
#define LSST_QSERV_QCACHE_PAGEIFACE_H

// System headers
#include <cstddef>
#include <memory>

// This header declarations
namespace lsst {
namespace qserv {
namespace qcache {

/**
  * Class PageIface is an interface to the page.
  */
class PageIface: public std::enable_shared_from_this<PageIface> {
public:
    PageIface(PageIface const&) = delete;
    PageIface& operator=(PageIface const&) = delete;

    virtual ~PageIface() = default;

    /// @return A pointer to the data
    virtual char const* data() const=0;

    /// @return The total number of bytes.
    virtual std::size_t sizeBytes() const=0;

    /// @return The total number of rows.
    virtual std::size_t sizeRows() const=0;

protected:
    PageIface() = default;
};

}}} // namespace lsst::qserv::qcache

#endif // LSST_QSERV_QCACHE_PAGEIFACE_H
