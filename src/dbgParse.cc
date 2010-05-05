#include <map>
#include "lsst/qserv/master/parserBase.h"
#include "lsst/qserv/master/parseTreeUtil.h"

namespace qMaster = lsst::qserv::master;

class ColumnHandler : public VoidFourRefFunc {
public:    
    virtual ~ColumnHandler() {}
    virtual void operator()(antlr::RefAST a, antlr::RefAST b, 
			    antlr::RefAST c, antlr::RefAST d) {
	using lsst::qserv::master::tokenText;
	std::cout << "col _" << tokenText(a) 
		  << "_ _" << tokenText(b) 
		  << "_ _" << tokenText(c) 
		  << "_ _" << tokenText(d) 
		  << "_ "; 
	a->setText("AWESOMECOLUMN");
    }

};

class TableHandler : public VoidThreeRefFunc {
public: 
    virtual ~TableHandler() {}
    virtual void operator()(antlr::RefAST a, antlr::RefAST b,
			    antlr::RefAST c)  {
	using lsst::qserv::master::tokenText;
	std::cout << "qualname " << tokenText(a) 
		  << " " << tokenText(b) << " " 
		  << tokenText(c) << " "; 
	a->setText("AwesomeTable");
    }
};


class TestAliasHandler : public VoidTwoRefFunc {
public: 
    virtual ~TestAliasHandler() {}
    virtual void operator()(antlr::RefAST a, antlr::RefAST b)  {
	if(b.get()) {
	    std::cout << "Alias " << qMaster::tokenText(a) 
		      << " = " << qMaster::tokenText(b) << std::endl;
	}
    }
};

class TestSelectListHandler : public VoidOneRefFunc {
public: 
    virtual ~TestSelectListHandler() {}
    virtual void operator()(antlr::RefAST a)  {
	antlr::RefAST bound = qMaster::getLastSibling(a);
	std::cout << "SelectList " << qMaster::walkTreeString(a) 
		  << "--From " << a << " to " << bound << std::endl;
    }
    
};


class TestSetFuncHandler : public VoidOneRefFunc {
public: 
    typedef std::map<std::string, int> Map;
    typedef Map::const_iterator MapConstIter;
    typedef Map::iterator MapIter;

    TestSetFuncHandler() {
	_map["count"] = 1;
	_map["avg"] = 1;
	_map["max"] = 1;
	_map["min"] = 1;
	_map["sum"] = 1;
    }
    virtual ~TestSetFuncHandler() {}
    virtual void operator()(antlr::RefAST a) {
	std::cout << "Got setfunc " << qMaster::walkTreeString(a)
		  << std::endl;
	//verify aggregation cmd.
	std::string origAgg = qMaster::tokenText(a);
	MapConstIter i = _map.find(origAgg); // case-sensitivity?
	if(i == _map.end()) {
	    std::cout << origAgg << " is not an aggregate." << std::endl;
	    return; // Skip.  Actually, this would be an parser bug.
	}
	// Extract meaning and label parts.
	// meaning is function + arguments
	// label is aliased name, if available, or function+arguments text otherwise.
	//std::string label = 
      
    }
    Map _map;
};
