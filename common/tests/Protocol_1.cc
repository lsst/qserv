/* 
 * LSST Data Management System
 * Copyright 2008, 2009, 2010 LSST Corporation.
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
#define BOOST_TEST_MODULE Protocol_1
#include "boost/test/included/unit_test.hpp"
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include "boost/scoped_ptr.hpp"
#include "worker.pb.h"
#include "TaskMsgDigest.h"
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

namespace test = boost::test_tools;
namespace gio = google::protobuf::io;


struct ProtocolFixture {
    ProtocolFixture(void) : counter(0){}
    ~ProtocolFixture(void) { };
    lsst::qserv::TaskMsg* makeTaskMsg() {
        lsst::qserv::TaskMsg* t;
        t = new lsst::qserv::TaskMsg();
        t->set_session(123456);
        t->set_chunkid(20 + counter);
        for(int i=0; i < 3; ++i) {
            lsst::qserv::TaskMsg::Fragment* f = t->add_fragment();
            f->set_query("Hello, this is a query.");
            f->add_subchunk(100+i); 
            f->set_resulttable("r_341");
        }
        ++counter;
        return t;
    }

    bool compareTaskMsgs(lsst::qserv::TaskMsg& t1, lsst::qserv::TaskMsg& t2) {
        bool sessionAndChunk = (t1.session() == t2.session()) 
            && (t1.chunkid() == t2.chunkid());
        
        bool fEqual = (t1.fragment_size() == t2.fragment_size());
        for(int i=0; i < t1.fragment_size(); ++i) {
            fEqual = fEqual && compareFragment(t1.fragment(i), 
                                               t2.fragment(i));
        }
        return sessionAndChunk && fEqual;            
    }

    lsst::qserv::ResultHeader* makeResultHeader() {
        lsst::qserv::ResultHeader* r(new lsst::qserv::ResultHeader());
        r->set_session(256+counter);
        for(int i=0; i < 4; ++i) {
            lsst::qserv::ResultHeader::Result* res = r->add_result();
            std::stringstream hash;
            while(hash.tellp() < 16) { hash << counter; }            
            res->set_hash(hash.str().substr(0,16));
            res->set_resultsize(65536+counter);
            res->set_chunkid(100+i+counter); 
        }
        ++counter;
        return r;
    }    

    bool compareFragment(lsst::qserv::TaskMsg_Fragment const& f1, 
                         lsst::qserv::TaskMsg_Fragment const& f2) {
        bool qEqual = (f1.query() == f2.query());
        bool sEqual = (f1.subchunk_size() == f2.subchunk_size());
        for(int i=0; i < f1.subchunk_size(); ++i) {
            sEqual = sEqual && (f1.subchunk(i) == f2.subchunk(i));
        }
        return qEqual && sEqual;
    }

    bool compareResultHeaders(lsst::qserv::ResultHeader const& r1, 
                              lsst::qserv::ResultHeader const& r2) {
        bool same = r1.session() == r2.session();
        same = same && (r1.result_size() == r2.result_size());
        for(int i=0; i < r1.result_size(); ++i) {
            same = same && compareResults(r1.result(i), 
                                         r2.result(i));
        }
        return same;
    } 

    bool compareResults(lsst::qserv::ResultHeader_Result const& r1, 
                        lsst::qserv::ResultHeader_Result const& r2) {
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
    boost::scoped_ptr<lsst::qserv::TaskMsg> t1(makeTaskMsg());
    BOOST_CHECK(t1.get());
    t1->SerializeToOstream(&ss);

    std::string blah = ss.str();
    std::stringstream ss2(blah);
    boost::scoped_ptr<lsst::qserv::TaskMsg> t2(new lsst::qserv::TaskMsg());
    BOOST_CHECK(t1.get());
    t2->ParseFromIstream(&ss2);
    BOOST_CHECK(compareTaskMsgs(*t1, *t2));
}

BOOST_AUTO_TEST_CASE(ResultMsgSanity) {
    std::stringstream ss;
    boost::scoped_ptr<lsst::qserv::ResultHeader> r1(makeResultHeader());
    BOOST_CHECK(r1.get());
    r1->SerializeToOstream(&ss);

    std::string blah = ss.str();
    std::stringstream ss2(blah);
    boost::scoped_ptr<lsst::qserv::ResultHeader> r2(new lsst::qserv::ResultHeader());
    BOOST_CHECK(r1.get());
    r2->ParseFromIstream(&ss2);
    BOOST_CHECK(compareResultHeaders(*r1, *r2));    
}

BOOST_AUTO_TEST_CASE(MsgBuffer) {
    std::stringstream ss;
    boost::scoped_ptr<lsst::qserv::ResultHeader> r1(makeResultHeader());
    BOOST_CHECK(r1.get());
    r1->SerializeToOstream(&ss);
    
    std::string raw(ss.str());
    gio::ArrayInputStream input(raw.data(), 
                                raw.size());
    gio::CodedInputStream coded(&input);
    boost::scoped_ptr<lsst::qserv::ResultHeader> r2(new lsst::qserv::ResultHeader());
    BOOST_CHECK(r1.get());
    r2->MergePartialFromCodedStream(&coded);
    BOOST_CHECK(compareResultHeaders(*r1, *r2));
}

BOOST_AUTO_TEST_CASE(ProtoHashDigest) {
    boost::scoped_ptr<lsst::qserv::TaskMsg> t1(makeTaskMsg());
    std::string hash = hashTaskMsg(*t1);
    std::string expected = "ac6e91da94a922036a2e968d42209f36";
    BOOST_CHECK_EQUAL(hash, expected);
}

BOOST_AUTO_TEST_SUITE_END()
