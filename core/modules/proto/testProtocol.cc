// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2011-2016 AURA/LSST.
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
#include <memory>

// Third-party headers
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/io/coded_stream.h>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "proto/ProtoHeaderWrap.h"
#include "proto/ScanTableInfo.h"
#include "proto/TaskMsgDigest.h"
#include "proto/worker.pb.h"
#include "proto/WorkerResponse.h"

#include "proto/FakeProtocolFixture.h"

// Boost unit test header
#define BOOST_TEST_MODULE Protocol_1
#include "boost/test/included/unit_test.hpp"

namespace test = boost::test_tools;
namespace gio = google::protobuf::io;

using namespace lsst::qserv;

struct ProtocolFixture : public lsst::qserv::proto::FakeProtocolFixture {
    ProtocolFixture(void) : FakeProtocolFixture(), counter(0) {}
    ~ProtocolFixture(void) {}

    bool compareTaskMsgs(lsst::qserv::proto::TaskMsg& t1, lsst::qserv::proto::TaskMsg& t2) {
        bool nonFragEq = (t1.session() == t2.session())
            && (t1.chunkid() == t2.chunkid())
            && (t1.db() == t2.db());

        bool sTablesEq = t1.scantable_size() == t2.scantable_size();
        for(int i=0; i < t1.scantable_size(); ++i) {
            auto const& sTbl1 = t1.scantable(i);
            auto const& sTbl2 = t2.scantable(i);
            bool eq = (sTbl1.db().compare(sTbl2.db()) == 0
                       && sTbl1.table() == sTbl2.table()
                       && sTbl1.lockinmemory() == sTbl2.lockinmemory()
                       && sTbl1.scanrating() == sTbl2.scanrating());
            sTablesEq = sTablesEq && eq;
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
        if (s1.database() != s2.database()) { return false; }
        if (s1.table_size() != s2.table_size()) { return false; }
        for(int i=0; i < s1.table_size(); ++i) {
            if (s1.table(i) != s2.table(i)) return false;
        }
        if (s1.id_size() != s2.id_size()) { return false; }
        for(int i=0; i < s1.id_size(); ++i) {
            if (s1.id(i) != s2.id(i)) return false;
        }
        return true;
    }

    bool compareFragment(lsst::qserv::proto::TaskMsg_Fragment const& f1,
                         lsst::qserv::proto::TaskMsg_Fragment const& f2) {
        bool qEqual = true;
        if (f1.query_size() == f2.query_size()) {
            for(int i=0; i < f1.query_size(); ++i) {
                if (f1.query(i) != f2.query(i)) return false;
            }
        } else { return false; }
        bool sEqual = true;
        if (f1.has_subchunks()) {
            if (f2.has_subchunks()) {
                sEqual = sEqual && compareSubchunk(f1.subchunks(), f2.subchunks());
            } else { sEqual = false; }
        } else if (f2.has_subchunks()) { sEqual = false; }
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
    std::unique_ptr<lsst::qserv::proto::TaskMsg> t1(makeTaskMsg());
    BOOST_CHECK(t1.get());
    t1->SerializeToOstream(&ss);

    std::string blah = ss.str();
    std::stringstream ss2(blah);
    std::unique_ptr<lsst::qserv::proto::TaskMsg> t2(new lsst::qserv::proto::TaskMsg());
    BOOST_CHECK(t1.get());
    t2->ParseFromIstream(&ss2);
    BOOST_CHECK(compareTaskMsgs(*t1, *t2));
}

BOOST_AUTO_TEST_CASE(ResultMsgSanity) {
    std::stringstream ss;
    std::unique_ptr<lsst::qserv::proto::ProtoHeader> r1(makeProtoHeader());
    BOOST_CHECK(r1.get());
    r1->SerializeToOstream(&ss);

    std::string blah = ss.str();
    std::stringstream ss2(blah);
    std::unique_ptr<lsst::qserv::proto::ProtoHeader> r2(new lsst::qserv::proto::ProtoHeader());
    BOOST_CHECK(r1.get());
    r2->ParseFromIstream(&ss2);
    BOOST_CHECK(compareProtoHeaders(*r1, *r2));
}

BOOST_AUTO_TEST_CASE(MsgBuffer) {
    std::stringstream ss;
    std::unique_ptr<lsst::qserv::proto::ProtoHeader> r1(makeProtoHeader());
    BOOST_CHECK(r1.get());
    r1->SerializeToOstream(&ss);

    std::string raw(ss.str());
    gio::ArrayInputStream input(raw.data(),
                                raw.size());
    gio::CodedInputStream coded(&input);
    std::unique_ptr<lsst::qserv::proto::ProtoHeader> r2(new lsst::qserv::proto::ProtoHeader());
    BOOST_CHECK(r1.get());
    r2->MergePartialFromCodedStream(&coded);
    BOOST_CHECK(compareProtoHeaders(*r1, *r2));
}

BOOST_AUTO_TEST_CASE(ProtoHashDigest) {
    std::unique_ptr<lsst::qserv::proto::TaskMsg> t1(makeTaskMsg());
    std::string hash = hashTaskMsg(*t1);
    std::string expected = "f3890e99c399ab196c164eccf2e99369";
    BOOST_CHECK_EQUAL(hash, expected);
}

BOOST_AUTO_TEST_CASE(ProtoHeaderWrap) {
    std::unique_ptr<proto::ProtoHeader> ph(makeProtoHeader());
    std::string str;
    ph->SerializeToString(&str);
    LOGS_DEBUG("wrapping " << str.size());
    std::string msgBuf = proto::ProtoHeaderWrap::wrap(str);
    std::vector<char> msgVect;
    std::copy(msgBuf.begin(), msgBuf.end(), std::back_inserter(msgVect));
    std::shared_ptr<proto::WorkerResponse> response(new proto::WorkerResponse());
    response->headerSize = msgBuf.size();
    LOGS_DEBUG("unwrapping");
    bool worked = proto::ProtoHeaderWrap::unwrap(response, msgVect);
    BOOST_CHECK(worked);
    BOOST_CHECK(compareProtoHeaders(response->protoHeader, *ph));
}

BOOST_AUTO_TEST_CASE(ScanTableInfo) {
    lsst::qserv::proto::ScanTableInfo stiA{"dba", "fruit", false, 1};
    lsst::qserv::proto::ScanTableInfo stiB{"dba", "fruit", true, 1};
    BOOST_CHECK(stiA.compare(stiB) < 0);
    BOOST_CHECK(stiB.compare(stiA) > 0);
    BOOST_CHECK(stiA.compare(stiA) == 0);
    BOOST_CHECK(stiB.compare(stiB) == 0);

    lsst::qserv::proto::ScanTableInfo stiC{"dba", "fruit", true, 1};
    lsst::qserv::proto::ScanTableInfo stiD{"dba", "fruit", true, 2};
    BOOST_CHECK(stiC.compare(stiD) < 0);
    BOOST_CHECK(stiD.compare(stiC) > 0);
    BOOST_CHECK(stiC.compare(stiC) == 0);
    BOOST_CHECK(stiD.compare(stiD) == 0);

    lsst::qserv::proto::ScanTableInfo stiE{"dba", "fruit", true, 2};
    lsst::qserv::proto::ScanTableInfo stiF{"dbb", "fruit", true, 2};
    BOOST_CHECK(stiE.compare(stiF) < 0);
    BOOST_CHECK(stiF.compare(stiE) > 0);
    BOOST_CHECK(stiE.compare(stiE) == 0);
    BOOST_CHECK(stiF.compare(stiF) == 0);

    lsst::qserv::proto::ScanTableInfo stiG{"dbb", "fruit", true, 2};
    lsst::qserv::proto::ScanTableInfo stiH{"dbb", "veggie", true, 2};
    BOOST_CHECK(stiG.compare(stiH) < 0);
    BOOST_CHECK(stiH.compare(stiG) > 0);
    BOOST_CHECK(stiG.compare(stiG) == 0);
    BOOST_CHECK(stiH.compare(stiH) == 0);

    lsst::qserv::proto::ScanTableInfo::ListOf list = { stiE, stiH, stiC, stiD, stiB, stiA, stiG, stiF };
    lsst::qserv::proto::ScanInfo scanInfo;
    scanInfo.infoTables = list;
    scanInfo.sortTablesSlowestFirst();
    int j = 0;
    BOOST_CHECK(scanInfo.infoTables[j++].compare(stiH) == 0);
    BOOST_CHECK(scanInfo.infoTables[j++].compare(stiG) == 0);
    BOOST_CHECK(scanInfo.infoTables[j++].compare(stiF) == 0);
    BOOST_CHECK(scanInfo.infoTables[j++].compare(stiE) == 0);
    BOOST_CHECK(scanInfo.infoTables[j++].compare(stiD) == 0);
    BOOST_CHECK(scanInfo.infoTables[j++].compare(stiC) == 0);
    BOOST_CHECK(scanInfo.infoTables[j++].compare(stiB) == 0);
    BOOST_CHECK(scanInfo.infoTables[j++].compare(stiA) == 0);
}



BOOST_AUTO_TEST_SUITE_END()
