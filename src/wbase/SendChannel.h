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

#ifndef LSST_QSERV_WBASE_SENDCHANNEL_H
#define LSST_QSERV_WBASE_SENDCHANNEL_H

// System headers
#include <atomic>
#include <functional>
#include <memory>
#include <string>

namespace lsst::qserv { namespace wbase {

/// SendChannel is used to send information about results
/// and errors back to the czar so that the czar can collect
/// the results or cancel the related data.
class SendChannel {
public:
    using Ptr = std::shared_ptr<SendChannel>;
    using Size = long long;

    SendChannel() {}  // Strictly for non-Request versions of this object.

    virtual ~SendChannel() {}

    /// Construct a new NopChannel that ignores everything it is asked to send
    static SendChannel::Ptr newNopChannel();

    /// Construct a StringChannel, which appends all it receives into a string
    /// provided by reference at construction.
    static SendChannel::Ptr newStringChannel(std::string& dest);

    /// Kill this SendChannel
    /// @ return the previous value of _dead
    bool kill(std::string const& note);

    /// Return true if this sendChannel cannot send data back to the czar.
    bool isDead();

    /// Set just before destorying this object to prevent pointless error messages.
    void setDestroying() { _destroying = true; }

private:
    std::atomic<bool> _dead{false};  ///< True if there were any failures using this SendChanel.
    std::atomic<bool> _destroying{false};
};

}}  // namespace lsst::qserv::wbase
#endif  // LSST_QSERV_WBASE_SENDCHANNEL_H
