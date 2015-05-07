// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015 LSST Corporation.
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

#ifndef LSST_QSERV_PROTO_FAKEPROTOCOLFIXTURE_H
#define LSST_QSERV_PROTO_FAKEPROTOCOLFIXTURE_H

// System headers
#include <memory>
#include <string>

// Qserv headers
#include "util/Callable.h"

namespace lsst {
namespace qserv {
namespace proto {

/// FakeProtocolFixture is a utility class containing code for making fake
/// versions of the protobufs messages used in Qserv. Its intent was
/// only to be used for test code.
class FakeProtocolFixture {
public:
    FakeProtocolFixture() : _counter(0) {}

    lsst::qserv::proto::TaskMsg* makeTaskMsg() {
        lsst::qserv::proto::TaskMsg* t;
        t = new lsst::qserv::proto::TaskMsg();
        t->set_session(123456);
        t->set_chunkid(20 + _counter);
        t->set_db("elephant");
        t->add_scantables("orange");
        t->add_scantables("plum");
        for(int i=0; i < 3; ++i) {
            lsst::qserv::proto::TaskMsg::Fragment* f = t->add_fragment();
            f->add_query("Hello, this is a query.");
            addSubChunk(*f, 100+i);
            f->set_resulttable("r_341");
        }
        ++_counter;
        return t;
    }

    void addSubChunk(lsst::qserv::proto::TaskMsg_Fragment& f, int scId) {
        lsst::qserv::proto::TaskMsg_Subchunk* s;
        if(!f.has_subchunks()) {
            lsst::qserv::proto::TaskMsg_Subchunk subc;
            // f.add_scgroup(); // How do I add optional objects?
            subc.set_database("subdatabase");
            subc.add_table("subtable");
            f.mutable_subchunks()->CopyFrom(subc);
            s = f.mutable_subchunks();
        } else {
            s = f.mutable_subchunks();
        }
        s->add_id(scId);
    }
    lsst::qserv::proto::ProtoHeader* makeProtoHeader() {
        lsst::qserv::proto::ProtoHeader* p(new lsst::qserv::proto::ProtoHeader());
        p->set_protocol(2);
        p->set_size(500);
        p->set_md5(std::string("1234567890abcdef0"));
        return p;
    }
private:
    int _counter;
};
}}} // lsst::qserv::proto

#endif // #define LSST_QSERV_PROTO_FAKEPROTOCOLFIXTURE_H

