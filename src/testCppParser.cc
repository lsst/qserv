//#define BOOST_TEST_MODULE testCppParser_1
//#include "boost/test/included/unit_test.hpp"
#include <list>
#include <map>
#include <string>
#include "parser.h"


//namespace test = boost::test_tools;


struct ParserFixture {
    ParserFixture(void) {
	// empty
    };
    ~ParserFixture(void) { };
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

int main(int, char**) {
    tryAutoSubstitute();
    return 0;
}
