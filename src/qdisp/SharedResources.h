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

#ifndef LSST_QSERV_SHAREDRESOURCES_H
#define LSST_QSERV_SHAREDRESOURCES_H

// System headers
#include <memory>

namespace lsst::qserv::qdisp {

class QdispPool;

/// Put resources that all Executives need to share in one class to reduce
/// the number of arguments passed.
/// This class should be kept simple so it can easily be included in headers
/// without undue compiler performances problems.
class SharedResources {
public:
    using Ptr = std::shared_ptr<SharedResources>;

    static Ptr create(std::shared_ptr<qdisp::QdispPool> const& qdispPool) {
        return Ptr(new SharedResources(qdispPool));
    }

    SharedResources() = delete;
    SharedResources(SharedResources const&) = delete;
    SharedResources& operator=(SharedResources const&) = delete;
    ~SharedResources() = default;

    std::shared_ptr<qdisp::QdispPool> getQdispPool() { return _qdispPool; }

private:
    SharedResources(std::shared_ptr<qdisp::QdispPool> const& qdispPool) : _qdispPool(qdispPool) {}

    /// Thread pool for handling Responses from XrdSsi.
    std::shared_ptr<qdisp::QdispPool> _qdispPool;
};

}  // namespace lsst::qserv::qdisp

#endif  // LSST_QSERV_SHAREDRESOURCES_H
