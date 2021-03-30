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
#ifndef LSST_QSERV_WBASE_TRANSMITDATA_H
#define LSST_QSERV_WBASE_TRANSMITDATA_H

// System headers
#include <memory>
#include <string>

// Qserv headers
#include "proto/ProtoHeaderWrap.h"


namespace google {
namespace protobuf {
class Arena;
}}

// This header declarations
namespace lsst {
namespace qserv {
namespace wbase {

/// This class is used to store information needed for one transmit
class TransmitData {
public:
    using Ptr = std::shared_ptr<TransmitData>;

    TransmitData() = delete;
    TransmitData(TransmitData const&) = delete;
    TransmitData& operator=(TransmitData const&) = delete;

    static Ptr createTransmitData(std::shared_ptr<google::protobuf::Arena> const& arena);

    /// Create a header using our arena.
    proto::ProtoHeader* createHeader();

    /// Create a result using our arena
    proto::Result* createResult();

    // proto objects are instantiated as part of google protobuf arenas
    // and should not be deleted. They are deleted when the arena is deleted.
    proto::ProtoHeader* header = nullptr;
    proto::Result* result = nullptr;

    /// Serialized header
    std::string headerMsg;

    /// Serialized string for result that is appended with wrapped string for headerNext.
    std::string dataMsg;
    bool nextHeaderAppended = false;

    int czarId = -1;
    bool cancelled = false;
    bool erred = false;
    bool largeResult = false;

private:
    TransmitData(std::shared_ptr<google::protobuf::Arena> const& arena);

    std::shared_ptr<google::protobuf::Arena> _arena;
};

}}} // namespace lsst::qserv::wbase

#endif // LSST_QSERV_WBASE_TRANSMITDATA_H
