// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2008-2015 AURA/LSST.
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
#include "XrdSfs/XrdSfsInterface.hh"

// System headers
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <iostream>

// Third-party headers
#include "boost/make_shared.hpp"
#include "boost/scoped_ptr.hpp"
#include "boost/shared_ptr.hpp"
#include "XrdSys/XrdSysError.hh"
#include "XrdSys/XrdSysLogger.hh"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "util/StringHash.h"
#include "wcontrol/ResultTracker.h"
#include "wdb/QueryRunner.h"
#include "xrdfs/MySqlFsFile.h"

// Boost unit test header
#define BOOST_TEST_MODULE MySqlFs_2
#include "boost/test/included/unit_test.hpp"

namespace test = boost::test_tools;
using boost::make_shared;

LOG_LOGGER myLog = LOG_GET("test");

// For chunk 9980, subchunks 1,3 (tuson26 right now)
std::string queryNonMagic =
    "CREATE TABLE Result AS "
    "-- SUBCHUNKS: 1,3\n"
    "SELECT COUNT(*) FROM "
    "(SELECT * FROM Subchunks_9880.Object_9880_1 "
    "UNION "
    "SELECT * FROM Subchunks_9880.Object_9880_3) AS _Obj_Subchunks;";
//SELECT COUNT(*) FROM (SELECT * FROM Subchunks_9880.Object_9880_1 UNION SELECT * FROM Subchunks_9880.Object_9880_3) AS _Obj_Subchunks;

std::string query(queryNonMagic + std::string(4, '\0')); // Force magic EOF
std::string queryHash = lsst::qserv::util::StringHash::getMd5Hex(query.c_str(),
                                                                 query.size());
std::string queryResultPath = "/result/"+queryHash;

struct TrackerFixture {
    TrackerFixture() :
	invokeFile(myLog, "qsmaster", make_shared<AddCallbackFunc>()),
	resultFile(myLog, "qsmaster", make_shared<AddCallbackFunc>()) {}
    class StrCallable {
    public:
        StrCallable(std::string& s) : val(s), isNotified(false)  {}
        void operator()(std::string const& s) {
            isNotified = true;
            val.assign(s);
        }
        std::string& val;
        bool isNotified;
    };
    class Listener {
    public:
        Listener(std::string const& filename) :_filename(filename) {}
        virtual ~Listener() {}
        virtual void operator()(lsst::qserv::wcontrol::ResultError const& re) {
            std::cout << "notification received for file "
                      << _filename << std::endl;
        }
        std::string _filename;
    };
    class AddCallbackFunc : public lsst::qserv::xrdfs::AddCallbackFunction {
    public:
        typedef boost::shared_ptr<lsst::qserv::xrdfs::AddCallbackFunction> Ptr;
        AddCallbackFunc() {}

        virtual ~AddCallbackFunc() {}
        virtual void operator()(XrdSfsFile& caller,
                                std::string const& filename) {
            std::cout << "Will listen for " << filename << ".\n";
            lsst::qserv::wdb::QueryRunner::getTracker().listenOnce(
                filename, Listener(filename));
        }
    };

    lsst::qserv::wdb::QueryRunner::Tracker& getTracker() { // alias.
        return lsst::qserv::wdb::QueryRunner::getTracker();
    }

    void printNews() { //
        typedef lsst::qserv::wdb::QueryRunner::Tracker Tracker;
        Tracker& t = getTracker();
        Tracker::NewsMap& nm = t.debugGetNews();
        Tracker::NewsMap::iterator i = nm.begin();
        Tracker::NewsMap::iterator end = nm.end();
        std::cout << "dumping newsmap " << std::endl;
        for(; i != end; ++i) {
            std::cout << "str=" << i->first << " code="
                      << i->second.first << std:: endl;
        }
    }

    lsst::qserv::xrdfs::MySqlFsFile invokeFile;
    lsst::qserv::xrdfs::MySqlFsFile resultFile;
    int lastResult;
    lsst::qserv::wdb::QueryRunner::Tracker* debugTrackerPtr;
};


BOOST_FIXTURE_TEST_SUITE(ResultTrackerSuite, TrackerFixture)

BOOST_AUTO_TEST_CASE(IntKey) {
    lsst::qserv::wcontrol::ResultTracker<int, std::string> rt;
    BOOST_CHECK(rt.getSignalCount() == 0);
    BOOST_CHECK(rt.getNewsCount() == 0);
    std::string msg;
    StrCallable sc(msg);
    rt.listenOnce(5, sc);
    BOOST_CHECK(rt.getSignalCount() == 1);
    BOOST_CHECK(rt.getNewsCount() == 0);
    rt.notify(4,"no!");
    BOOST_CHECK(rt.getNewsCount() == 1);
    BOOST_CHECK(rt.getSignalCount() == 2);
    BOOST_CHECK(msg.size() == 0);
    rt.notify(5, "five");
    BOOST_CHECK(rt.getNewsCount() == 2);
    BOOST_CHECK(rt.getSignalCount() == 2);
    BOOST_CHECK(msg.size() == 4);
    std::string msg2;
    StrCallable sc2(msg2);
    rt.listenOnce(4, sc2);
    BOOST_CHECK(rt.getNewsCount() == 2);
    BOOST_CHECK(rt.getSignalCount() == 2);
    if(!sc2.isNotified) {
        usleep(1000); // Sleep for 1ms to let the notifier wakeup
    }
    BOOST_CHECK(msg2.size() == 3);
}

#if 0 // FIXME: needs to be rewritten to use two-file-transactions.
BOOST_AUTO_TEST_CASE(QueryAttemptCombo) {
    // params: filename, openMode(ignored), createMode(ignored),
    // clientSecEntity(ignored), opaque(ignored)

    lastResult = invokeFile.open("/query/9880",0,0,0,0);
    BOOST_CHECK_EQUAL(lastResult, SFS_OK);
    lastResult = invokeFile.write(0, query.c_str(), query.size());
    BOOST_CHECK_EQUAL((unsigned)lastResult, query.size());
    int pos = 0;
    const int blocksize = 1024;
    char contents[blocksize+1];
    while(1) {
	lastResult = invokeFile.read(pos, contents, blocksize);
	BOOST_CHECK(lastResult >= 0);
	if(lastResult >= 0) {
	    pos += blocksize;
	    contents[lastResult] = '\0';
	    std::cout << "recv("<< lastResult
		      << "):" << contents << std::endl;
	} else {
	    std::cout << "recv error("<< lastResult
		      << "):" << std::endl;
	}
	if(lastResult < blocksize)
	    break;
    }

    lastResult = invokeFile.close();
    BOOST_CHECK_EQUAL(lastResult, SFS_OK);
}

BOOST_AUTO_TEST_CASE(QueryAttemptTwo) {

    debugTrackerPtr = &getTracker();
    debugTrackerPtr->debugReset();
    lastResult = invokeFile.open("/query2/9880",0,0,0,0);
    BOOST_CHECK_EQUAL(lastResult, SFS_OK);
    lastResult = invokeFile.write(0, query.c_str(), query.size());
    BOOST_CHECK_EQUAL((unsigned)lastResult, query.size());
    lastResult = invokeFile.close();
    BOOST_CHECK_EQUAL(lastResult, SFS_OK);
    while(1) {
	std::cout << "attempting open of " << queryResultPath << "\n";
	lastResult = resultFile.open(queryResultPath.c_str(),0,0,0,0);
	if(lastResult == SFS_OK) {
	    break;
	} else if(lastResult == SFS_STARTED) {
        lsst::qserv::wcontrol::ResultErrorPtr p;
	    int i = 0;
	    while(i < 10) {
		p = getTracker().getNews(queryHash);
		if(p.get() != 0) break;
		printNews();
		sleep(1);
		++i;
	    }
	    continue; // Try opening again.
	} else {
	    BOOST_CHECK((lastResult == SFS_OK) || (lastResult == SFS_STARTED));
	    break;
	}
    }

    int pos = 0;
    const int blocksize = 1024;
    char contents[blocksize+1];
    while(1) {
	lastResult = resultFile.read(pos, contents, blocksize);
	BOOST_CHECK(lastResult >= 0);
	if(lastResult >= 0) {
	    pos += blocksize;
	    contents[lastResult] = '\0';
	    std::cout << "recv("<< lastResult
		      << "):" << contents << std::endl;
	} else {
	    std::cout << "recv error("<< lastResult
		      << "):" << std::endl;
	}
	if(lastResult < blocksize)
	    break;
    }

    lastResult = resultFile.close();
    BOOST_CHECK_EQUAL(lastResult, SFS_OK);

}
#endif
BOOST_AUTO_TEST_SUITE_END()
