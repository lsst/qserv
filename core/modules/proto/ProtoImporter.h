// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015-2017 LSST Corporation.
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

#ifndef LSST_QSERV_PROTO_PROTOIMPORTER_H
#define LSST_QSERV_PROTO_PROTOIMPORTER_H

// System headers
#include <memory>
#include <string>


namespace lsst {
namespace qserv {
namespace proto {

/// ProtoImporter
/// Minimal-copy import of an arbitrary proto msg from a raw buffer.
/// Example:
/// struct TaskMsgAcceptor : public ProtoImporter<TaskMsg> {
///  virtual void operator()(std::shared_ptr<TaskMsg> m) { ...}
/// };
/// ProtoImporter<TaskMsg> p(std::shared_ptr<TaskMsgAcceptor>());
/// p(data,size); // calls operator() defined above.
template <typename Msg>
class ProtoImporter {
public:
    ProtoImporter() {};

    bool messageAcceptable(std::string const& msg) {
        Msg m;
        return setMsgFrom(m, msg.data(), msg.size());
    }

    static bool setMsgFrom(Msg& m, char const* buf, int bufLen) {
        // For dev/debugging: accepts a partially-formed message
        // bool ok = m.ParsePartialFromArray(buf, bufLen);

        // Accept only complete, compliant messages.
        bool ok = m.ParseFromArray(buf, bufLen);
        return ok && m.IsInitialized();
    }
};

}}} // lsst::qserv::proto

#endif // #define LSST_QSERV_PROTO_PROTOIMPORTER_H
