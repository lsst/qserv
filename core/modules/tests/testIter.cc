// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2009-2015 AURA/LSST.
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
#include <cstring>
#include <list>
#include <map>
#include <string>

// Third-party headers
#include "boost/make_shared.hpp"

// Qserv headers
#include "rproc/SqlInsertIter.h"
//#include "util/PacketBuffer.h"
//#include "xrdc/XrdBufferSource.h"

// Boost unit test header
#define BOOST_TEST_MODULE testIter
#include "boost/test/included/unit_test.hpp"

namespace test = boost::test_tools;
using lsst::qserv::rproc::SqlInsertIter;
//using lsst::qserv::util::PacketBuffer;
//using lsst::qserv::xrdc::XrdBufferSource;
using std::string;


struct IterFixture {
    IterFixture(void) {
        dummyBlock = "/*!40101 SET @saved_cs_client     = @@character_set_client */;\n"
            "/*!40101 SET character_set_client = utf8 */;\n"
            "CREATE TABLE `r_4_1ff8f47beaf8909932_1003` (\n"
            "  `count(*)` bigint(21) NOT NULL DEFAULT '0'\n"
            ") ENGINE=MEMORY DEFAULT CHARSET=latin1;\n"
            "/*!40101 SET character_set_client = @saved_cs_client */;\n"
            "LOCK TABLES `r_4_1ff8f47beaf8909932_1003` WRITE;\n"

            "INSERT INTO `r_4_1ff8f47beaf8909932_1003` VALUES (1288372);\n"
            "INSERT INTO `r_4_1ff8f47beaf8909932_1003` VALUES (1288372);\n"
            "INSERT INTO `r_4_1ff8f47beaf8909932_1003` VALUES (1654621);\n"
            "INSERT INTO `r_4_1ff8f47beaf8909932_1003` VALUES (564072);\n"
            "INSERT INTO `r_4_1ff8f47beaf8909932_1003` VALUES (855877);\n"

            "INSERT INTO `r_4_1ff8f47beaf8909932_1003` VALUES (564352);\n"
            "INSERT INTO `r_4_1ff8f47beaf8909932_1003` VALUES (632303);\n"
            "INSERT INTO `r_4_1ff8f47beaf8909932_1003` VALUES (561991);\n"
            "INSERT INTO `r_4_1ff8f47beaf8909932_1003` VALUES (562435);\n"
            "INSERT INTO `r_4_1ff8f47beaf8909932_1003` VALUES (632559);\n"

            "INSERT INTO `r_4_1ff8f47beaf8909932_1003` VALUES (562871);\n"
            "INSERT INTO `r_4_1ff8f47beaf8909932_1003` VALUES (581626);\n"
            "INSERT INTO `r_4_1ff8f47beaf8909932_1003` VALUES (563283);\n"
            "INSERT INTO `r_4_1ff8f47beaf8909932_1003` VALUES (1451023);\n"
            "INSERT INTO `r_4_1ff8f47beaf8909932_1003` VALUES (1474794);\n"

            "INSERT INTO `r_4_1ff8f47beaf8909932_1003` VALUES (1545106);\n"
            "INSERT INTO `r_4_1ff8f47beaf8909932_1003` VALUES (6578574);\n"
            "INSERT INTO `r_4_1ff8f47beaf8909932_1003` VALUES (3938215);\n"
            "INSERT INTO `r_4_1ff8f47beaf8909932_1003` VALUES (3798854);\n"
            "INSERT INTO `r_4_1ff8f47beaf8909932_1003` VALUES (6601552);\n"

            "INSERT INTO `r_4_1ff8f47beaf8909932_1003` VALUES (1969958);\n"
            "INSERT INTO `r_4_1ff8f47beaf8909932_1003` VALUES (1916080);\n"
            "INSERT INTO `r_4_1ff8f47beaf8909932_1003` VALUES (1744053);\n"
            "INSERT INTO `r_4_1ff8f47beaf8909932_1003` VALUES (1732599);\n"
            "INSERT INTO `r_4_1ff8f47beaf8909932_1003` VALUES (424365);\n"

            "INSERT INTO `r_4_1ff8f47beaf8909932_1003` VALUES (1798521);\n"
            "INSERT INTO `r_4_1ff8f47beaf8909932_1003` VALUES (1821647);\n"
            "UNLOCK TABLES;\n";
        totalInserts = 27;
        dummyFilename = "/tmp/qservTestIterFile.dummy";
        tableName = "r_4_1ff8f47beaf8909932_1003";
        _setupDummy();
    }
    ~IterFixture(void) {}

    void _setupDummy() {
        dummyLen = strlen(dummyBlock);
        int fd = open(dummyFilename, O_CREAT | O_WRONLY,
                      S_IRWXU | S_IRWXG | S_IRWXO);
        assert(fd > 0);
        write(fd, dummyBlock, dummyLen);
        close(fd);
    }

    unsigned iterateInserts(SqlInsertIter& sii) {
        unsigned sCount = 0;
        for(; !sii.isDone(); ++sii) {
            char const* stmtBegin = sii->first;
            std::size_t stmtSize = sii->second - stmtBegin;
            BOOST_CHECK(stmtSize > 0);
            std::string q(stmtBegin, stmtSize);
            //std::cout << "Statement---" << q << std::endl;
            ++sCount;
        }
        return sCount;
    }


    char const* dummyBlock;
    int dummyLen;
    char const* dummyFilename;
    std::string tableName;
    unsigned totalInserts;
};

BOOST_FIXTURE_TEST_SUITE(IterTests, IterFixture)

// BOOST_AUTO_TEST_CASE(PlainIterTest) {
//     XrdBufferSource* bs = new XrdBufferSource(string(dummyFilename),
//                                               512,
//                                               true);
//     PacketBuffer::Ptr p = boost::make_shared<PacketBuffer>(bs);
//     char const* c = dummyBlock;
//     bool same = true;
//     for(; !p->isDone(); ++(*p)) {
//         PacketBuffer::Value const& v = **p;
//         // std::cout << "frag: " << std::string(v.first, v.second)
//         //           << std::endl;
//         for(unsigned i=0; i < v.second; ++i) {
//             same = same && (*c == v.first[i]);
//             ++c;
//         }
//         BOOST_CHECK(same);
//     }
// }

// BOOST_AUTO_TEST_CASE(pbtest) {
//     char const* c = dummyBlock;
//     bool same = true;
//     for(int fragSize=16; fragSize < 512; fragSize*=2) {
//         XrdBufferSource* bs =
//             new XrdBufferSource(string(dummyFilename),
//                                 fragSize,
//                                 true);

//         for(PacketBuffer::Ptr p = boost::make_shared<PacketBuffer>(bs);
//             !p->isDone(); ++(*p)) {
//             PacketBuffer::Value v = **p;
// #if 0
//             std::cout << "frag:"
//                       << (unsigned long long)v.first
//                       << " " << "sz=" << v.second << std::endl
//                       << std::string(v.first, v.second) << std::endl;
// #endif
//             for(unsigned i=0; i < v.second; ++i) {
//                 same = same && (*c == v.first[i]);
//                 ++c;
//             }

//         }
//     }
// }

// BOOST_AUTO_TEST_CASE(SqlIterTest) {
//     for(int fragSize=16; fragSize < 512; fragSize*=2) {
//         XrdBufferSource* bs =
//             new XrdBufferSource(string(dummyFilename),
//                                 fragSize,
//                                 true);
//         PacketBuffer::Ptr p = boost::make_shared<PacketBuffer>(bs);
//         SqlInsertIter sii(p, tableName, true);
//         unsigned sCount = iterateInserts(sii);
//         BOOST_CHECK_EQUAL(sCount, totalInserts);
//     }
// }
BOOST_AUTO_TEST_CASE(SqlIterTestPlain) {
    SqlInsertIter sii(dummyBlock, dummyLen, tableName, true);
    unsigned sCount = iterateInserts(sii);
    BOOST_CHECK_EQUAL(sCount, totalInserts);
}


BOOST_AUTO_TEST_SUITE_END()

