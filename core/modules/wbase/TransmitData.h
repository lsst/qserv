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
#include "qmeta/types.h"


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

    /// Create a transmitData object
    static Ptr createTransmitData(qmeta::CzarId const& czarId_);

    /// Create a header for an empty result using our arena.
    /// This does not set the 'header' member of this object as there is a
    /// case where an empty header is needed to append to the result.
    /// This should only be appended to the result of this->result as
    /// the '_arena' will be deallocated when this object is destroyed.
    /// &&& TODO: This can probably be made private with a public function
    /// &&&       to end/finalize a TransmitData object.
    proto::ProtoHeader* createHeader();

    // proto objects are instantiated as part of google protobuf arenas
    // and should not be deleted. They are deleted when the arena is deleted.
    proto::ProtoHeader* header = nullptr;
    proto::Result* result = nullptr;

    /// Serialized string for result that is appended with wrapped string for headerNext.
    std::string dataMsg;

    qmeta::CzarId const czarId;
    bool erred = false;
    bool largeResult = false;
    bool scanInteractive = false;

private:
    TransmitData(qmeta::CzarId const& czarId, std::shared_ptr<google::protobuf::Arena> const& arena);

    /// Create a result using our arena.
    /// This does not set the 'result' member of this object for consistency.
    proto::Result* _createResult();


    std::shared_ptr<google::protobuf::Arena> _arena;
};

}}} // namespace lsst::qserv::wbase

#endif // LSST_QSERV_WBASE_TRANSMITDATA_H
