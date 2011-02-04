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
 
#define BOOST_TEST_MODULE testIter
#include "boost/test/included/unit_test.hpp"
#include <list>
#include <map>
#include <string>
#include <cstring>
#include "lsst/qserv/master/SqlInsertIter.h"
#include "lsst/qserv/master/PacketIter.h"

namespace test = boost::test_tools;
using lsst::qserv::master::PacketIter;
using std::string;
namespace {


} // anonymous namespace

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
            "INSERT INTO `r_4_1ff8f47beaf8909932_1127` VALUES (1654621);\n"
            "INSERT INTO `r_4_1ff8f47beaf8909932_1208` VALUES (564072);\n"
            "INSERT INTO `r_4_1ff8f47beaf8909932_121` VALUES (855877);\n"
            "INSERT INTO `r_4_1ff8f47beaf8909932_1211` VALUES (564352);\n"
            "INSERT INTO `r_4_1ff8f47beaf8909932_1248` VALUES (632303);\n"
            "INSERT INTO `r_4_1ff8f47beaf8909932_1249` VALUES (561991);\n"
            "INSERT INTO `r_4_1ff8f47beaf8909932_1252` VALUES (562435);\n"
            "INSERT INTO `r_4_1ff8f47beaf8909932_1254` VALUES (632559);\n"
            "INSERT INTO `r_4_1ff8f47beaf8909932_1255` VALUES (562871);\n"
            "INSERT INTO `r_4_1ff8f47beaf8909932_1256` VALUES (581626);\n"
            "INSERT INTO `r_4_1ff8f47beaf8909932_1258` VALUES (563283);\n"
            "INSERT INTO `r_4_1ff8f47beaf8909932_1322` VALUES (1451023);\n"
            "INSERT INTO `r_4_1ff8f47beaf8909932_1327` VALUES (1474794);\n"
            "INSERT INTO `r_4_1ff8f47beaf8909932_1329` VALUES (1545106);\n"
            "INSERT INTO `r_4_1ff8f47beaf8909932_240` VALUES (6578574);\n"
            "INSERT INTO `r_4_1ff8f47beaf8909932_242` VALUES (3938215);\n"
            "INSERT INTO `r_4_1ff8f47beaf8909932_249` VALUES (3798854);\n"
            "INSERT INTO `r_4_1ff8f47beaf8909932_251` VALUES (6601552);\n"
            "INSERT INTO `r_4_1ff8f47beaf8909932_361` VALUES (1969958);\n"
            "INSERT INTO `r_4_1ff8f47beaf8909932_362` VALUES (1916080);\n"
            "INSERT INTO `r_4_1ff8f47beaf8909932_363` VALUES (1744053);\n"
            "INSERT INTO `r_4_1ff8f47beaf8909932_374` VALUES (1732599);\n"
            "INSERT INTO `r_4_1ff8f47beaf8909932_603` VALUES (424365);\n"
            "INSERT INTO `r_4_1ff8f47beaf8909932_630` VALUES (1798521);\n"
            "INSERT INTO `r_4_1ff8f47beaf8909932_721` VALUES (1821647);\n"
            "UNLOCK TABLES;\n";
        dummyFilename = "/tmp/qservTestIterFile.dummy";
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
    
    char const* dummyBlock;
    int dummyLen;
    char const* dummyFilename;
};

BOOST_FIXTURE_TEST_SUITE(IterTests, IterFixture)

BOOST_AUTO_TEST_CASE(PlainIter) {
    PacketIter::Ptr p(new PacketIter(string(dummyFilename), 512, true));
    for(; !p->isDone(); ++(*p)) {
        std::cout << "frag: " << std::string((*p)->first, (*p)->second) 
                  << std::endl;
    }

}


BOOST_AUTO_TEST_SUITE_END()

