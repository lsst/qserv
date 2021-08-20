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
#ifndef LSST_QSERV_QCACHE_POOL_H
#define LSST_QSERV_QCACHE_POOL_H

// System headers
#include <cstddef>
#include <memory>

// Forward declarations
namespace lsst {
namespace qserv {
namespace qcache {
    class Page;
}}} // namespace lsst::qserv::qcache

// This header declarations
namespace lsst {
namespace qserv {
namespace qcache {

/**
  * Class Pool is ...
  */
class Pool: public std::enable_shared_from_this<Pool> {
public:
    Pool() = delete;
    Pool(Pool const&) = delete;
    Pool& operator=(Pool const&) = delete;

    virtual ~Pool() = default;

    /**
     * Create the pool with the specified number of pages. Pages will be allocated
     * as needed not to exceed the specified limit.
     */
    static std::shared_ptr<Pool> create(std::size_t pageCapacityBytes=1024*1024,
                                        std::size_t maxNumPages=1024);

    /**
     * Allocate a page. If no free pages are available the method would have to wait
     * before some other thread will release the one.
     */
    std::shared_ptr<Page> allocate();

    /**
     * Return a page back to the pool.
     */
    void release(std::shared_ptr<Page> const& page);

private:
    Pool(std::size_t pageCapacityBytes, std::size_t maxNumPages);

    // Parameters
    std::size_t const _pageCapacityBytes;
    std::size_t const _maxNumPages;

    std::size_t _numPages = 0;  ///< The number of allocated pages

    /// The bi-directional list of the free pages. Up to the _maxNumPages pages could
    /// exist in the list. Pages get removed from the list once they're allocated.
    std::shared_ptr<Page> _free;
};

}}} // namespace lsst::qserv::qcache

#endif // LSST_QSERV_QCACHE_POOL_H
