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

namespace test = boost::test_tools;

struct ProtocolFixture {
    ProtocolFixture(void) : counter(0){}
    ~ProtocolFixture(void) { };
    Qserv::Task* makeTask() {
        Qserv::Task* t;
        t = new Qserv::Task();
        t->set_session(123456);
        t->set_chunkid(20 + counter);
        for(int i=0; i < 3; ++i) {
            Qserv::Task::Fragment* f = t->add_fragment();
            f->set_query("Hello, this is a query.");
            f->add_subchunk(100+i); 
        }
        ++counter;
        return t;
    }

    bool compareTasks(Qserv::Task& t1, Qserv::Task& t2) {
        bool sessionAndChunk = (t1.session() == t2.session()) 
            && (t1.chunkid() == t2.chunkid());
        
        bool fEqual = (t1.fragment_size() == t2.fragment_size());
        for(int i=0; i < t1.fragment_size(); ++i) {
            fEqual = fEqual && compareFragment(t1.fragment(i), 
                                               t2.fragment(i));
        }
        return sessionAndChunk && fEqual;            
    }

    Qserv::ResultHeader* makeResultHeader() {
        Qserv::ResultHeader* r(new Qserv::ResultHeader());
        r->set_session(256+counter);
        for(int i=0; i < 4; ++i) {
            Qserv::ResultHeader::Result* res = r->add_result();
            std::stringstream hash;
            while(hash.tellp() < 16) { hash << counter; }            
            res->set_hash(hash.str().substr(0,16));
            res->set_resultsize(65536+counter);
            res->set_chunkid(100+i+counter); 
        }
        ++counter;
        return r;
    }    

    bool compareFragment(Qserv::Task_Fragment const& f1, 
                         Qserv::Task_Fragment const& f2) {
        bool qEqual = (f1.query() == f2.query());
        bool sEqual = (f1.subchunk_size() == f2.subchunk_size());
        for(int i=0; i < f1.subchunk_size(); ++i) {
            sEqual = sEqual && (f1.subchunk(i) == f2.subchunk(i));
        }
        return qEqual && sEqual;
    }

    bool compareResultHeaders(Qserv::ResultHeader const& r1, 
                              Qserv::ResultHeader const& r2) {
        bool same = r1.session() == r2.session();
        same = same && (r1.result_size() == r2.result_size());
        for(int i=0; i < r1.result_size(); ++i) {
            same = same && compareResults(r1.result(i), 
                                         r2.result(i));
        }
        return same;
    } 

    bool compareResults(Qserv::ResultHeader_Result const& r1, 
                        Qserv::ResultHeader_Result const& r2) {
        return (r1.hash() == r2.hash()) 
            && (r1.resultsize() == r2.resultsize()) 
            && (r1.chunkid() == r2.chunkid());
    } 


    int counter;
};

BOOST_FIXTURE_TEST_SUITE(ProtocolTestSuite, ProtocolFixture)

BOOST_AUTO_TEST_CASE(TaskMsgSanity) {
    GOOGLE_PROTOBUF_VERIFY_VERSION;
    std::stringstream ss;
    boost::scoped_ptr<Qserv::Task> t1(makeTask());
    BOOST_CHECK(t1.get());
    t1->SerializeToOstream(&ss);

    std::string blah = ss.str();
    std::stringstream ss2(blah);
    boost::scoped_ptr<Qserv::Task> t2(new Qserv::Task());
    BOOST_CHECK(t1.get());
    t2->ParseFromIstream(&ss2);
    BOOST_CHECK(compareTasks(*t1, *t2));
}

BOOST_AUTO_TEST_CASE(ResultMsgSanity) {
    std::stringstream ss;
    boost::scoped_ptr<Qserv::ResultHeader> r1(makeResultHeader());
    BOOST_CHECK(r1.get());
    r1->SerializeToOstream(&ss);

    std::string blah = ss.str();
    std::stringstream ss2(blah);
    boost::scoped_ptr<Qserv::ResultHeader> r2(new Qserv::ResultHeader());
    BOOST_CHECK(r1.get());
    r2->ParseFromIstream(&ss2);
    BOOST_CHECK(compareResultHeaders(*r1, *r2));
    
    
}

BOOST_AUTO_TEST_SUITE_END()
