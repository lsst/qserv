// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2011-2014 LSST Corporation.
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

// System headers
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <iostream>

// Third-party headers
#include "boost/scoped_ptr.hpp"
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

// Local headers
#include "proto/worker.pb.h"
#include "proto/TaskMsgDigest.h"
#include "proto/FakeProtocolFixture.h"

// Boost unit test header
#define BOOST_TEST_MODULE Protocol_1
#include "boost/test/included/unit_test.hpp"

namespace test = boost::test_tools;
namespace gio = google::protobuf::io;

struct ProtocolFixture : public lsst::qserv::proto::FakeProtocolFixture {
    ProtocolFixture(void) : FakeProtocolFixture() {}
    ~ProtocolFixture(void) {}

    bool compareTaskMsgs(lsst::qserv::proto::TaskMsg& t1, lsst::qserv::proto::TaskMsg& t2) {
        bool nonFragEq = (t1.session() == t2.session())
            && (t1.chunkid() == t2.chunkid())
            && (t1.db() == t2.db());

        bool sTablesEq = t1.scantables_size() == t2.scantables_size();
        for(int i=0; i < t1.scantables_size(); ++i) {
            sTablesEq = sTablesEq && (t1.scantables(i) == t2.scantables(i));
        }

        bool fEqual = (t1.fragment_size() == t2.fragment_size());
        for(int i=0; i < t1.fragment_size(); ++i) {
            fEqual = fEqual && compareFragment(t1.fragment(i),
                                               t2.fragment(i));
        }
        return nonFragEq && fEqual && sTablesEq;
    }

    bool compareSubchunk(lsst::qserv::proto::TaskMsg_Subchunk const& s1,
                         lsst::qserv::proto::TaskMsg_Subchunk const& s2) {
        if(s1.database() != s2.database()) { return false; }
        if(s1.table_size() != s2.table_size()) { return false; }
        for(int i=0; i < s1.table_size(); ++i) {
            if(s1.table(i) != s2.table(i)) return false;
        }
        if(s1.id_size() != s2.id_size()) { return false; }
        for(int i=0; i < s1.id_size(); ++i) {
            if(s1.id(i) != s2.id(i)) return false;
        }
        return true;
    }

    bool compareFragment(lsst::qserv::proto::TaskMsg_Fragment const& f1,
                         lsst::qserv::proto::TaskMsg_Fragment const& f2) {
        bool qEqual = true;
        if(f1.query_size() == f2.query_size()) {
            for(int i=0; i < f1.query_size(); ++i) {
                if(f1.query(i) != f2.query(i)) return false;
            }
        } else { return false; }
        bool sEqual = true;
        if(f1.has_subchunks()) {
            if(f2.has_subchunks()) {
                sEqual = sEqual && compareSubchunk(f1.subchunks(), f2.subchunks());
            } else { sEqual = false; }
        } else if(f2.has_subchunks()) { sEqual = false; }
        return qEqual && sEqual;
    }

    bool compareProtoHeaders(lsst::qserv::proto::ProtoHeader const& p1,
                              lsst::qserv::proto::ProtoHeader const& p2) {
        return ((p1.protocol() == p2.protocol())
                && (p1.size() == p2.size())
                && (p1.md5() == p2.md5()));
    }

    int counter;
};

BOOST_FIXTURE_TEST_SUITE(ProtocolTestSuite, ProtocolFixture)

BOOST_AUTO_TEST_CASE(TaskMsgMsgSanity) {
    GOOGLE_PROTOBUF_VERIFY_VERSION;
    std::stringstream ss;
    boost::scoped_ptr<lsst::qserv::proto::TaskMsg> t1(makeTaskMsg());
    BOOST_CHECK(t1.get());
    t1->SerializeToOstream(&ss);

    std::string blah = ss.str();
    std::stringstream ss2(blah);
    boost::scoped_ptr<lsst::qserv::proto::TaskMsg> t2(new lsst::qserv::proto::TaskMsg());
    BOOST_CHECK(t1.get());
    t2->ParseFromIstream(&ss2);
    BOOST_CHECK(compareTaskMsgs(*t1, *t2));
}

BOOST_AUTO_TEST_CASE(ResultMsgSanity) {
    std::stringstream ss;
    boost::scoped_ptr<lsst::qserv::proto::ProtoHeader> r1(makeProtoHeader());
    BOOST_CHECK(r1.get());
    r1->SerializeToOstream(&ss);

    std::string blah = ss.str();
    std::stringstream ss2(blah);
    boost::scoped_ptr<lsst::qserv::proto::ProtoHeader> r2(new lsst::qserv::proto::ProtoHeader());
    BOOST_CHECK(r1.get());
    r2->ParseFromIstream(&ss2);
    BOOST_CHECK(compareProtoHeaders(*r1, *r2));
}

BOOST_AUTO_TEST_CASE(MsgBuffer) {
    std::stringstream ss;
    boost::scoped_ptr<lsst::qserv::proto::ProtoHeader> r1(makeProtoHeader());
    BOOST_CHECK(r1.get());
    r1->SerializeToOstream(&ss);

    std::string raw(ss.str());
    gio::ArrayInputStream input(raw.data(),
                                raw.size());
    gio::CodedInputStream coded(&input);
    boost::scoped_ptr<lsst::qserv::proto::ProtoHeader> r2(new lsst::qserv::proto::ProtoHeader());
    BOOST_CHECK(r1.get());
    r2->MergePartialFromCodedStream(&coded);
    BOOST_CHECK(compareProtoHeaders(*r1, *r2));
}

BOOST_AUTO_TEST_CASE(ProtoHashDigest) {
    boost::scoped_ptr<lsst::qserv::proto::TaskMsg> t1(makeTaskMsg());
    std::string hash = hashTaskMsg(*t1);
    std::string expected = "4c6e5ad217891467addaa0db015eef80";
    BOOST_CHECK_EQUAL(hash, expected);
}

BOOST_AUTO_TEST_SUITE_END()
