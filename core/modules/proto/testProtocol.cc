// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2011-2013 LSST Corporation.
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

// Boost unit test header
#define BOOST_TEST_MODULE Protocol_1
#include "boost/test/included/unit_test.hpp"

namespace test = boost::test_tools;
namespace gio = google::protobuf::io;


struct ProtocolFixture {
    ProtocolFixture(void) : counter(0){}
    ~ProtocolFixture(void) { };
    lsst::qserv::proto::TaskMsg* makeTaskMsg() {
        lsst::qserv::proto::TaskMsg* t;
        t = new lsst::qserv::proto::TaskMsg();
        t->set_session(123456);
        t->set_chunkid(20 + counter);
        t->set_db("elephant");
        t->add_scantables("orange");
        t->add_scantables("plum");
        for(int i=0; i < 3; ++i) {
            lsst::qserv::proto::TaskMsg::Fragment* f = t->add_fragment();
            f->add_query("Hello, this is a query.");
            addSubChunk(*f, 100+i);
            f->set_resulttable("r_341");
        }
        ++counter;
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

    lsst::qserv::proto::ResultHeader* makeResultHeader() {
        lsst::qserv::proto::ResultHeader* r(new lsst::qserv::proto::ResultHeader());
        r->set_session(256+counter);
        for(int i=0; i < 4; ++i) {
            lsst::qserv::proto::ResultHeader::Result* res = r->add_result();
            std::stringstream hash;
            while(hash.tellp() < 16) { hash << counter; }
            res->set_hash(hash.str().substr(0,16));
            res->set_resultsize(65536+counter);
            res->set_chunkid(100+i+counter);
        }
        ++counter;
        return r;
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

    bool compareResultHeaders(lsst::qserv::proto::ResultHeader const& r1,
                              lsst::qserv::proto::ResultHeader const& r2) {
        bool same = r1.session() == r2.session();
        same = same && (r1.result_size() == r2.result_size());
        for(int i=0; i < r1.result_size(); ++i) {
            same = same && compareResults(r1.result(i),
                                         r2.result(i));
        }
        return same;
    }

    bool compareResults(lsst::qserv::proto::ResultHeader_Result const& r1,
                        lsst::qserv::proto::ResultHeader_Result const& r2) {
        return (r1.hash() == r2.hash())
            && (r1.resultsize() == r2.resultsize())
            && (r1.chunkid() == r2.chunkid());
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
    boost::scoped_ptr<lsst::qserv::proto::ResultHeader> r1(makeResultHeader());
    BOOST_CHECK(r1.get());
    r1->SerializeToOstream(&ss);

    std::string blah = ss.str();
    std::stringstream ss2(blah);
    boost::scoped_ptr<lsst::qserv::proto::ResultHeader> r2(new lsst::qserv::proto::ResultHeader());
    BOOST_CHECK(r1.get());
    r2->ParseFromIstream(&ss2);
    BOOST_CHECK(compareResultHeaders(*r1, *r2));
}

BOOST_AUTO_TEST_CASE(MsgBuffer) {
    std::stringstream ss;
    boost::scoped_ptr<lsst::qserv::proto::ResultHeader> r1(makeResultHeader());
    BOOST_CHECK(r1.get());
    r1->SerializeToOstream(&ss);

    std::string raw(ss.str());
    gio::ArrayInputStream input(raw.data(),
                                raw.size());
    gio::CodedInputStream coded(&input);
    boost::scoped_ptr<lsst::qserv::proto::ResultHeader> r2(new lsst::qserv::proto::ResultHeader());
    BOOST_CHECK(r1.get());
    r2->MergePartialFromCodedStream(&coded);
    BOOST_CHECK(compareResultHeaders(*r1, *r2));
}

BOOST_AUTO_TEST_CASE(ProtoHashDigest) {
    boost::scoped_ptr<lsst::qserv::proto::TaskMsg> t1(makeTaskMsg());
    std::string hash = hashTaskMsg(*t1);
    std::string expected = "4c6e5ad217891467addaa0db015eef80";
    BOOST_CHECK_EQUAL(hash, expected);
}

BOOST_AUTO_TEST_SUITE_END()
