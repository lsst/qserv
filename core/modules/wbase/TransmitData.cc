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

// Class header
#include "wbase/TransmitData.h"

// System headers

// Third-party headers
#include <google/protobuf/arena.h>

// Qserv headers


using namespace std;

namespace lsst {
namespace qserv {
namespace wbase {


TransmitData::TransmitData(shared_ptr<google::protobuf::Arena> const& arena) : _arena(arena) {
}


TransmitData::Ptr TransmitData::createTransmitData(shared_ptr<google::protobuf::Arena> const& arena) {
    return make_shared<TransmitData>(arena);
}


 proto::ProtoHeader* TransmitData::createHeader() {
     return google::protobuf::Arena::CreateMessage<proto::ProtoHeader>(_arena.get());
 }


 proto::Result* TransmitData::createResult() {
     return google::protobuf::Arena::CreateMessage<proto::Result>(_arena.get());
 }

}}} // namespace lsst::qserv::wbase

