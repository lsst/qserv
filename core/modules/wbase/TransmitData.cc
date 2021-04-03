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
#include "global/debugUtil.h"
#include "util/StringHash.h"

using namespace std;

namespace lsst {
namespace qserv {
namespace wbase {


TransmitData::TransmitData(qmeta::CzarId const& czarId_, shared_ptr<google::protobuf::Arena> const& arena)
    : czarId(czarId_), _arena(arena) {
    header = createHeader();
}


TransmitData::Ptr TransmitData::createTransmitData(qmeta::CzarId const& czarId_) {
    shared_ptr<google::protobuf::Arena> arena = make_shared<google::protobuf::Arena>();
    return shared_ptr<TransmitData>(new TransmitData(czarId_, arena));
}


proto::ProtoHeader* TransmitData::createHeader() {
    proto::ProtoHeader* hdr = google::protobuf::Arena::CreateMessage<proto::ProtoHeader>(_arena.get());
    hdr->set_protocol(2); // protocol 2: row-by-row message
    hdr->set_size(0);
    hdr->set_md5(util::StringHash::getMd5("", 0));
    hdr->set_wname(getHostname());
    hdr->set_largeresult(false);
    hdr->set_endnodata(true);
    return hdr;
}


proto::Result* TransmitData::createResult() {
    proto::Result* rst = google::protobuf::Arena::CreateMessage<proto::Result>(_arena.get());
    return rst;
}

}}} // namespace lsst::qserv::wbase

