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
#include "wbase/SendChannelShared.h"

namespace lsst::qserv::wbase {

SendChannelShared::Ptr SendChannelShared::create(std::shared_ptr<wbase::SendChannel> const& sendChannel,
                                                 std::shared_ptr<wcontrol::TransmitMgr> const& transmitMgr,
                                                 qmeta::CzarId czarId) {
    return std::shared_ptr<SendChannelShared>(new SendChannelShared(sendChannel, transmitMgr, czarId));
}

SendChannelShared::SendChannelShared(std::shared_ptr<wbase::SendChannel> const& sendChannel,
                                     std::shared_ptr<wcontrol::TransmitMgr> const& transmitMgr,
                                     qmeta::CzarId czarId)
        : ChannelShared(sendChannel, transmitMgr, czarId) {}

}  // namespace lsst::qserv::wbase
