//#define BOOST_TEST_MODULE testCppParser_1
//#include "boost/test/included/unit_test.hpp"
#include <list>
#include <map>
#include <string>
#include "lsst/qserv/master/parser.h"


//namespace test = boost::test_tools;


struct ParserFixture {
    ParserFixture(void) {
	cMapping.addChunkKey("Source");
	cMapping.addSubChunkKey("Object");
    };
    ~ParserFixture(void) { };
    ChunkMapping cMapping;
};

//BOOST_FIXTURE_TEST_SUITE(QservSuite, ParserFixture)

//BOOST_AUTO_TEST_CASE(SqlSubstitution) {
void trySubstitute() {
    std::list<std::string> names;
    names.push_front("Object");
    names.push_front("Source");
    std::map<std::string,std::string> m;
    m["Object"] = "Object_24_35";
    m["Source"] = "Source_24_35";

    std::string stmt = "select * from LSST.Object as o1, LSST.Source where o1.id = 4 and LSST.Source.flux > 4 and ra < 5 and dista(ra,decl,ra,decl) < 1; select * from Temp;";


    SqlSubstitution s(stmt, m);
    std::cout << "Plain transform " << s.transform(m) << std::endl;
    m["Object"] = "Object_10_22";
    m["Source"] = "Source_10_22";
    std::cout << "Next transform " << s.transform(m) << std::endl;
}

//BOOST_AUTO_TEST_CASE(SqlSubstitution) {
void tryAutoSubstitute() {
    std::string stmt = "select * from LSST.Object as o1, LSST.Source where o1.id = 4 and LSST.Source.flux > 4 and ra < 5 and dista(ra,decl,ra,decl) < 1; select * from Temp;";
    ChunkMapping c;
    c.addChunkKey("Source");
    c.addSubChunkKey("Object");
    SqlSubstitution ss(stmt, c.getMapping(32,53432));
    for(int i = 4; i < 6; ++i) {
	std::cout << "--" << ss.transform(c.getMapping(i,3)) << std::endl;
    }
}

void tryNnSubstitute() {
    std::string stmt = "select * from LSST.Object as o1, LSST.Object as o2 where o1.id != o2.id and dista(o1.ra,o1.decl,o2.ra,o2.decl) < 1;";
    ChunkMapping c;
    c.addChunkKey("Source");
    c.addSubChunkKey("Object");
    SqlSubstitution ss(stmt, c.getMapping(32,53432));
    for(int i = 4; i < 6; ++i) {
	std::cout << "--" << ss.transform(c.getMapping(i,3)) << std::endl;
    }
}

void tryTriple() {
    std::string stmt = "select * from LSST.Object as o1, LSST.Object as o2, LSST.Source where o1.id != o2.id and dista(o1.ra,o1.decl,o2.ra,o2.decl) < 1 and Source.oid=o1.id;";
    ChunkMapping c;
    c.addChunkKey("Source");
    c.addSubChunkKey("Object");
    c.addSubChunkKey("ObjectSub");
    SqlSubstitution ss(stmt, c.getMapping(32,53432));
    for(int i = 4; i < 6; ++i) {
	std::cout << "--" << ss.transform(c.getMapping(i,3)) << std::endl;
    }
}

void tryAggregate() {
    std::string stmt = "select sum(pm_declErr),sum(bMagF), sum(bMagF2) bmf2 from LSST.Object where bMagF > 20.0;";
    std::string stmt2 = "select sum(pm_declErr),chunkId, sum(bMagF2) bmf2 from LSST.Object where bMagF > 20.0;";
    ChunkMapping c;
    c.addChunkKey("Source");
    c.addSubChunkKey("Object");
    SqlSubstitution ss(stmt, c.getMapping(32,53432));
    for(int i = 4; i < 6; ++i) {
	std::cout << "--" << ss.transform(c.getMapping(i,3)) << std::endl;
    }
    SqlSubstitution ss2(stmt2, c.getMapping(32,4352));
    std::cout << "--" << ss2.transform(c.getMapping(24,3)) << std::endl;
    
}



int main(int, char**) {
    tryAutoSubstitute();
    //tryNnSubstitute();
    //tryTriple();
    tryAggregate();
    return 0;
}
