// Standard
#include <iostream>
#include <sstream>
#include <list>
#include <deque>

// Boost
#include "boost/shared_ptr.hpp"

// ANTLR
#include "antlr/AST.hpp"
#include "antlr/CommonAST.hpp"

// Package
#include "lsst/qserv/master/parser.h"

// Local (placed in src/)
#include "SqlSQL2Parser.hpp"
#include "SqlSQL2Lexer.hpp"


namespace { // Anonymous

std::string tokenText(RefAST r) {
    if(r.get()) {
	return r->getText();
    } else return std::string();
}

class ColumnHandler : public VoidFourRefFunc {
public:    
    virtual ~ColumnHandler() {}
    virtual void operator()(RefAST a, RefAST b, RefAST c, RefAST d) {
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
    virtual void operator()(RefAST a, RefAST b, RefAST c)  {
	std::cout << "qualname " << tokenText(a) 
		  << " " << tokenText(b) << " " 
		  << tokenText(c) << " "; 
	a->setText("AwesomeTable");
    }
};

template <typename AnAst>
struct TrivialCheckTerm {
    bool operator()(AnAst r, int depth) {return false; }
};

template <typename AnAst>
struct ParenCheckTerm {
    bool operator()(AnAst r, int depth) {
	return (depth == 0) && (tokenText(r) == ")");
    }
};

template <typename AnAst>
struct SibCheckTerm {
    SibCheckTerm(AnAst lastSib_) : lastSib(lastSib_) {}
    bool operator()(AnAst r, int depth) {
	return (depth == 0) && (r == lastSib);
    }
    AnAst lastSib;
};

template <typename AnAst>
struct PrintVisitor {
public:
    void operator()(AnAst a) {
	if(!result.empty()) {
	    result += " " + a->getText();
	} else {
	    result = a->getText();
	}
    }
    std::string result;
};

template <typename AnAst>
struct CompactPrintVisitor {
public:
    void operator()(AnAst a) {
	std::string s = a->getText();
	if(!s.empty() && !result.empty()) {
	    int last = result[result.size()-1];
	    int next = s[0];
	    if(shouldSeparate(last,next)) {
		result += " ";
	    } 
	}
	result += s;
    }
    bool shouldSeparate(int last, int next) {
	return (isalnum(last) && isalnum(next)) // adjoining alnums
	    || ((last == '*') && isalnum(next)) // *saf
	    || ((next == '*') && isalnum(last)) // saf*
	    || ((last == ')') && isalnum(next)) // )asdf
	    ;
    }
    std::string result;
};

template <typename AnAst>
std::string walkTree(AnAst r) {
    //DFS walk
    // Print child (child will print siblings)
    std::string result;
    RefAST c = r->getFirstChild();
    if(c.get()) {
	result = walkTree(c);
    }
    // Now print sibling(s)
    RefAST s = r->getNextSibling();
    if(s.get()) {
	if(!result.empty()) result += " ";
	result += walkTree(s);
    }
    if(!result.empty()) result = " " + result;
    return r->getText() + result;
	
}


template <typename AnAst, typename Visitor, typename CheckTerm>
void walkTreeVisit(AnAst r, Visitor& v, CheckTerm& ct, int depth=0) {
    //DFS walk?
    v(r);
    //if(ct(r,depth)) return; // On terminal, visit only.
    RefAST c = r->getFirstChild();
    if(c.get()) {
	std::cout << "Child: " << tokenText(r) << "----" << tokenText(c) 
		  << std::endl;
	walkTreeVisit(c, v, ct, depth+1);
    }
    // Now print sibling(s)
    RefAST s = r->getNextSibling();
    if(s.get() && !ct(r,depth)) {
	//	std::cout << "Sib: " << tokenText(r) << "----" << tokenText(s) 
	//		  << std::endl;
	walkTreeVisit(s, v, ct, depth);
    }
	
}

template <typename AnAst, typename Visitor>
void walkTreeVisit(AnAst r, Visitor& v) {
    TrivialCheckTerm<AnAst> t;
    walkTreeVisit(r, v, t);
}

template <typename AnAst>
std::string walkTreeString(AnAst r) {
    CompactPrintVisitor<AnAst> p;
    TrivialCheckTerm<AnAst> t;
    walkTreeVisit(r, p, t);
    return p.result;
}

template <typename AnAst>
std::string walkBoundedTreeString(AnAst r, AnAst lastSib) {
    CompactPrintVisitor<AnAst> p;
    SibCheckTerm<AnAst> t(lastSib);
    walkTreeVisit(r, p, t);
    return p.result;
}


template <typename AnAst>
std::string getFuncString(AnAst r) {
    CompactPrintVisitor<AnAst> p;
    ParenCheckTerm<AnAst> t;
    walkTreeVisit(r, p, t);
    return p.result;
}

template <typename AnAst>
AnAst getLastSibling(AnAst r) {
    AnAst last;
    do {
	last = r;
	r = r->getNextSibling();
    } while(r.get());
    return last;
}

RefAST collapseNodeRange(RefAST start, RefAST bound) {
    // Destroy a node's siblings stopping (but including) a bound
    // This is useful in patching up an AST, substituting one parse 
    // element for another. 
    // @return the missing fragment so the caller can save it.

    // Example:
    // RefAST someList;
    // RefAST listBound = getLastSibling(someList)
    // someList->setTokenText("Something new")
    // collapseNodeRange(someList, listBound)
    
    // This is a simple linked-list ranged delete.
    assert(start.get());
    assert(bound.get());
    RefAST dead = start->getNextSibling();
    start->setNextSibling(bound);
    return dead;
}

class TestAliasHandler : public VoidTwoRefFunc {
public: 
    virtual ~TestAliasHandler() {}
    virtual void operator()(RefAST a, RefAST b)  {
	if(b.get()) {
	    std::cout << "Alias " << tokenText(a) 
		      << " = " << tokenText(b) << std::endl;
	}
    }
};

class TestSelectListHandler : public VoidOneRefFunc {
public: 
    virtual ~TestSelectListHandler() {}
    virtual void operator()(RefAST a)  {
	RefAST bound = getLastSibling(a);
	std::cout << "SelectList " << walkTreeString(a) 
		  << "--From " << a << " to " << bound << std::endl;
    }
    
};


class TestSetFuncHandler : public VoidOneRefFunc {
public: 
    typedef map<std::string, int> Map;
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
    virtual void operator()(RefAST a) {
	std::cout << "Got setfunc " << walkTreeString(a) << std::endl;
	//verify aggregation cmd.
	std::string origAgg = tokenText(a);
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

class AggregateMgr {
public:
    typedef std::pair<RefAST, RefAST> NodeBound;
    class AliasVal {
    public:
	AliasVal(RefAST lbl_, RefAST meaning_) : lbl(lbl_), meaning(meaning_){}
	RefAST lbl;
	RefAST meaning;
    };
    class AggregateRecord {
    public:
	NodeBound lbl;
	NodeBound meaning;
	std::string orig; // Original
	std::string pass; // Subquery
	std::string fixup; // Merging/fixup
	std::ostream& printTo(std::ostream& os) {
	    os << "Aggregate orig=" << orig << std::endl 
	       << "pass=" << pass  << std::endl
	       << "fixup=" << fixup;
	}
    };
    typedef std::map<RefAST, AggregateRecord> AggMap;
    class AggBuilderIf {
    public:
	typedef boost::shared_ptr<AggBuilderIf> Ptr;
	virtual AggregateRecord operator()(NodeBound const& lbl,
					   NodeBound const& meaning) = 0;
    };
    class EasyAggBuilder : public AggBuilderIf {
    public:
	virtual AggregateRecord operator()(NodeBound const& lbl,
					   NodeBound const& meaning) {
	    AggregateRecord a;
	    a.lbl = lbl;
	    a.meaning = meaning;
	    if(lbl.first != meaning.first) {
		assert(lbl.second.get()); // must have bound.
		a.orig = walkBoundedTreeString(meaning.first, lbl.second);
	    } else {
		a.orig = walkBoundedTreeString(meaning.first, meaning.second);
	    }
	    a.pass = a.orig;
	    a.fixup = computeFixup(meaning, lbl);
	    return a;
	}
	std::string computeFixup(NodeBound meaning, NodeBound lbl) {
	    std::string agg = tokenText(meaning.first);
	    RefAST lparen = meaning.first->getNextSibling();
	    assert(lparen.get());
	    RefAST paramAst = lparen->getNextSibling();
	    assert(paramAst.get());
	    std::string param = getFuncString(paramAst);
	    std::string lblText = walkBoundedTreeString(lbl.first, lbl.second);
	    // Orig: agg ( param ) lbl
	    // Fixup: agg ( quoted-lbl) 
	    return agg + "(`" + lblText + "`) AS `" + lblText + "`";
	}
    };
    class CountAggBuilder {
    };
    class AvgAggBuilder {
    };
    
    class SetFuncHandler : public VoidOneRefFunc {
    public: 
	typedef map<std::string, AggBuilderIf::Ptr> Map;
	typedef Map::const_iterator MapConstIter;
	typedef Map::iterator MapIter;

	typedef std::deque<NodeBound> Deque;
	typedef Deque::const_iterator DequeConstIter;
	typedef Deque::iterator DequeIterator;
	
	SetFuncHandler() {
	    _map["count"].reset();
	    _map["avg"].reset();
	    _map["max"].reset(new EasyAggBuilder());
	    _map["min"].reset(new EasyAggBuilder());
	    _map["sum"].reset(new EasyAggBuilder());
	}
	virtual ~SetFuncHandler() {}
	virtual void operator()(RefAST a) {
	    //std::cout << "---- SETFUNC: ----" << walkTreeString(a) << std::endl;
	    std::string origAgg = tokenText(a);
	    MapConstIter i = _map.find(origAgg); // case-sensitivity?
	    assert(i != _map.end());
	    _aggs.push_back(NodeBound(a, getLastSibling(a)));
	}
	Deque const& getAggs() const { return _aggs; }
	Map& getProcs() { return _map; }
    private:
	Deque _aggs;
	Map _map;
    }; // class SetFuncHandler

    class AliasHandler : public VoidTwoRefFunc {
    public: 
	typedef std::map<RefAST, NodeBound> Map;
	typedef Map::const_iterator MapConstIter;
	typedef Map::iterator MapIter;

	AliasHandler() {}
	virtual ~AliasHandler() {}
	virtual void operator()(RefAST a, RefAST b)  {
	    if(b.get()) {
		_map[a] = NodeBound(b, getLastSibling(a));
	    }
	}
	Map const& getInvAliases() const { return _map; }
    private:
	Map _map;
    }; // class AliasHandler

    class SelectListHandler : public VoidOneRefFunc {
    public: 
	class SelectStarHandler : public VoidVoidFunc {
	public: 
	    SelectStarHandler(SelectListHandler& h) : handler(h) {}
	    virtual ~SelectStarHandler() {}
	    virtual void operator()() { handler.handleSelectStar(); }
	    SelectListHandler& handler;
	};
	typedef std::deque<RefAST> SelectList;
	typedef SelectList::const_iterator SelectListConstIter;
	typedef std::deque<SelectList> Deque;
	SelectListHandler() : isStarFirst(false) {}
	virtual ~SelectListHandler() {}
	virtual void operator()(RefAST a)  {
	    if(selectLists.size() == 0) {
		firstSelectBound.first = a;
		firstSelectBound.second = getLastSibling(a);
	    }
	    selectLists.push_back(SelectList());
	    SelectList& sl = selectLists.back();
	    //std::cout << "sl ";
	    for(RefAST i = a; i.get(); 
		i = i->getNextSibling()) { 
		sl.push_back(i);
		//std::cout << tokenText(i) << " ";
	    }
	    std::cout << std::endl;
	    
	}
	void handleSelectStar() {
	    if(selectLists.empty()) {
		isStarFirst = true;
	    }
	}
	boost::shared_ptr<SelectStarHandler> getSelectStarHandler() {
	    typedef boost::shared_ptr<SelectStarHandler> Ptr;
	    return Ptr(new SelectStarHandler(*this));
	}
	Deque selectLists;
	NodeBound firstSelectBound;
	bool isStarFirst;
    }; // class SelectListHandler

    AggregateMgr() : _aliaser(new AliasHandler()),
		     _setFuncer(new SetFuncHandler()),
		     _selectLister(new SelectListHandler()) {
    }
    
    void postprocess() {
	AliasHandler::Map const& aMap = _aliaser->getInvAliases();
	AliasHandler::MapConstIter aEnd = aMap.end();
	SetFuncHandler::Deque const& aggd = _setFuncer->getAggs();

	for(SetFuncHandler::DequeConstIter i = aggd.begin(); 
	    i != aggd.end(); ++i) {
	    AliasHandler::MapConstIter f = aMap.find(i->first);
	    std::string agg = tokenText(i->first);
	    if(f != aEnd) {
		//std::cout << agg << " aliased as " 
		//<< tokenText(f->second.first) << std::endl;
		SetFuncHandler::Map& m = _setFuncer->getProcs();
		AggregateRecord a = (*m[agg])(f->second, *i);
		//a.printTo(std::cout) << std::endl;
		_aggRecords[i->first] = a;
		
	    } else {
		SetFuncHandler::Map& m = _setFuncer->getProcs();
		AggregateRecord a = (*m[agg])(*i, *i);
		//a.printTo(std::cout) << std::endl;
		_aggRecords[i->first] = a;
	    }
	}
    }
    void applyAggPass() {
	std::string passText = getPassSelect();
	if(passText == "*") {
	    // SELECT * means we don't have to fix anything.
	    return;
	}
	NodeBound const& nb = _selectLister->firstSelectBound;
	RefAST orphans = collapseNodeRange(nb.first, nb.second);
	nb.first->setText(passText);
    }
    std::string getPassSelect() {
	if(_passSelect.empty()) {
	    _computeSelects();
	}
	return _passSelect;
    }
    std::string getFixupSelect() {
	if(_fixupSelect.empty()) {
	    _computeSelects();
	}
	return _fixupSelect;
	
    }
    void _computeSelects() {
	// passSelect = "".join(map(lambda s: s.pass, selectList))
	// fixupSelect = "".join(map(lambda s: s.fixup, selectList))
	if(_selectLister->isStarFirst) {
	    _passSelect = "*";
	    _fixupSelect = "*";
	    return;
	}
	SelectListHandler::Deque& d = _selectLister->selectLists;
	assert(!d.empty());
	if(d.size() > 1) {
	    std::cout << "Warning, multiple select lists->subqueries?" 
		      << std::endl; // FIXME. Should be sterner?
	}
	std::stringstream ps;
	std::stringstream fs;
	SelectListHandler::SelectList& sl = d[0];
	bool written = false;
	for(SelectListHandler::SelectListConstIter i=sl.begin();
	    i != sl.end(); ++i) {
	    // get the pass record.
	    if(_aggRecords.find(*i) != _aggRecords.end()) {
		if(written) {
		    ps << ", ";
		    fs << ", ";
		}
		ps << _aggRecords[*i].pass;
		fs << _aggRecords[*i].fixup;
		written = true;
	    }
	} 
	_passSelect = ps.str();
	_fixupSelect = fs.str();
	
    
    }

    boost::shared_ptr<VoidTwoRefFunc> getAliasHandler() {return _aliaser;}
    boost::shared_ptr<VoidOneRefFunc> getSetFuncHandler() {return _setFuncer;}
    boost::shared_ptr<VoidOneRefFunc> getSelectListHandler() {return _selectLister;}
    boost::shared_ptr<VoidVoidFunc> getSelectStarHandler() {
	return _selectLister->getSelectStarHandler();
    }
    std::string _passSelect;
    std::string _fixupSelect;
    
private:
    boost::shared_ptr<AliasHandler> _aliaser;
    boost::shared_ptr<SetFuncHandler> _setFuncer;
    boost::shared_ptr<SelectListHandler> _selectLister;
    AggMap _aggRecords;
}; // class AggregateMgr

class Templater {
public:
    class ColumnHandler : public VoidFourRefFunc {
    public:    
	ColumnHandler(Templater& t) : _templater(t) {}
	virtual ~ColumnHandler() {}
	virtual void operator()(RefAST a, RefAST b, RefAST c, RefAST d) {
	    if(d.get()) {
		_templater._processName(c);
	    } else if(c.get()) {
		_templater._processName(b);
	    } else if(b.get()) {
		_templater._processName(a);
	    }
	    // std::cout << "col _" << tokenText(a) 
	    // 	      << "_ _" << tokenText(b) 
	    // 	      << "_ _" << tokenText(c) 
	    // 	      << "_ _" << tokenText(d) 
	    // 	      << "_ "; 
	    // a->setText("AWESOMECOLUMN");
	}
    private:
	Templater& _templater;
	
    };
    
    class TableHandler : public VoidThreeRefFunc {
    public: 
	TableHandler(Templater& t) : _templater(t) {}
	virtual ~TableHandler() {}
	virtual void operator()(RefAST a, RefAST b, RefAST c)  {
	    // right-most is the table name.
	    if(c.get()) {
		_templater._processName(c);
	    } else if(b.get()) {
		_templater._processName(b);
	    } else if(a.get()) {
		_templater._processName(a);
	    }
	    // std::cout << "qualname " << tokenText(a) 
	    // 	      << " " << tokenText(b) << " " 
	    // 	      << tokenText(c) << " "; 
	}
    private:
	Templater& _templater;
    };
    struct TypeVisitor {
    public:
	void operator()(RefAST& a) {
	    std::cout << "(" << a->getText() << " " 
		      << a->getType() << " " 
		      << a->typeName()
		      << ") " << std::endl;
	}
    };
    class JoinVisitor {
    public:
	JoinVisitor(std::string delim, std::string subPrefix) 
	    : _delim(delim), _subPrefix(subPrefix),
	      _hasChunks(false), _hasSubChunks(false) 
	{}
	void operator()(RefAST& a) {
	    if(_isDelimited(a->getText())) {
		_addRef(a);
		_hasChunks = true;
	    }
	}
	void applySubChunkRule() {
	    RefMapIter e = _map.end();
	    for(RefMapIter i = _map.begin(); i != e; ++i) {
		if(i->second.size() > 1) {
		    _reassignRefs(i->second);
		    _hasSubChunks = true;
		}
	    }
	}
	bool getHasChunks() const { return _hasChunks; }
	bool getHasSubChunks() const { return _hasSubChunks; }
    private:
	typedef std::deque<RefAST> RefList;
	typedef RefList::iterator RefListIter;
	typedef std::map<std::string, RefList> RefMap;
	typedef RefMap::value_type RefMapValue;
	typedef RefMap::iterator RefMapIter;

	void _addRef(RefAST& a) {
	    std::string key(a->getText());
	    if(_map.find(key) == _map.end()) {
		_map.insert(RefMap::value_type(key,RefList()));
	    }
	    _map[key].push_back(a);
	}
	bool _isDelimited(std::string const& s) {
	    std::string::size_type lpos = s.find(_delim);
	    if(std::string::npos != lpos && lpos == 0) {
		std::string::size_type rpos = s.rfind(_delim);
		if((std::string::npos != rpos) 
		   && (rpos == s.size()-_delim.size())) {
		    return true;
		}
	    }
	    return false;
	}
	void _reassignRefs(RefList& l) {
	    RefListIter e = l.end();
	    int num=1;
	    for(RefListIter i = l.begin(); i != e; ++i) {
		RefAST& r = *i;
		std::string orig = r->getText();
		stringstream specss; 
		specss << _subPrefix << num++;
		orig.insert(orig.rfind(_delim), specss.str());
		r->setText(orig);
	    }
	}
	RefMap _map;
	std::string _delim;
	std::string _subPrefix;
	// May want to store chunk table names and subchunk table names.
	bool _hasChunks;
	bool _hasSubChunks;
    };

    class TableListHandler : public VoidTwoRefFunc {
    public: 
	TableListHandler(Templater& t) : _templater(t) {}
	virtual ~TableListHandler() {}
	virtual void operator()(RefAST a, RefAST b)  {
	    TypeVisitor t;
	    JoinVisitor j("*?*", "_sc");
	    walkTreeVisit(a,j);
	    j.applySubChunkRule();
	    _hasChunks = j.getHasChunks();
	    _hasSubChunks = j.getHasSubChunks();
	}
	bool getHasChunks() const { return _hasChunks; }
	bool getHasSubChunks() const { return _hasSubChunks; }
    private:
	Templater& _templater;
	bool _hasChunks;
	bool _hasSubChunks;
    };

    typedef std::map<std::string, char> ReMap;
    
    Templater(std::string const& delimiter="*?*") 
	: _delimiter(delimiter) {
    }
    ~Templater() {
    }
    template <typename Iter>
    void setKeynames(Iter begin, Iter end) {
	// Clear the map, then fill it.
	_map.clear();
	for(Iter i = begin; i != end; ++i) {
	    _map[*i] = true;
	}
    }

    std::string mungeName(std::string const& name) {
	return _delimiter + name + _delimiter;
    }

    bool isSpecial(std::string const& s) {
	return _map.find(s) != _map.end();
    }
    boost::shared_ptr<TableHandler> getTableHandler() {
	return boost::shared_ptr<TableHandler>(new TableHandler(*this));
    }
    boost::shared_ptr<ColumnHandler> getColumnHandler() {
	return boost::shared_ptr<ColumnHandler>(new ColumnHandler(*this));
    }
    boost::shared_ptr<TableListHandler> getTableListHandler() {
	return boost::shared_ptr<TableListHandler>(new TableListHandler(*this));
    }
private:
    void _processName(RefAST n) {
	if(isSpecial(n->getText())) {
	    n->setText(mungeName(n->getText()));
	}
    }

    ReMap _map;
    std::string _delimiter;

    friend class Templater::TableHandler;
    friend class Templater::ColumnHandler;
};


class SqlParseRunner {
public:
    SqlParseRunner(std::string const& statement, std::string const& delimiter) :
	_statement(statement),
	_stream(statement, stringstream::in | stringstream::out),
	_lexer(_stream),
	_parser(_lexer),
	_delimiter(delimiter),
	_templater(_delimiter)
    { }

    void setup(std::list<std::string> const& names) {
	_templater.setKeynames(names.begin(), names.end());
	_parser._columnRefHandler = _templater.getColumnHandler();
	_parser._qualifiedNameHandler = _templater.getTableHandler();
	_tableListHandler = _templater.getTableListHandler();
	_parser._tableListHandler = _tableListHandler;
	_parser._setFctSpecHandler = _aggMgr.getSetFuncHandler();
	_parser._aliasHandler = _aggMgr.getAliasHandler();
	_parser._selectListHandler = _aggMgr.getSelectListHandler();
	_parser._selectStarHandler = _aggMgr.getSelectStarHandler();
    }

    std::string getParseResult() {
	if(_errorMsg.empty() && _parseResult.empty()) {
	    _computeParseResult();
	}
	return _parseResult;
    }
    std::string getAggParseResult() {
	if(_errorMsg.empty() && _aggParseResult.empty()) {
	    _computeParseResult();
	}
	return _aggParseResult;
    }
    void _computeParseResult() {
	try {
	    _parser.initializeASTFactory(_factory);
	    _parser.setASTFactory(&_factory);
	    _parser.sql_stmt();
	    _aggMgr.postprocess();
	    RefAST ast = _parser.getAST();
	    if (ast) {
		//std::cout << "fixupSelect " << getFixupSelect();
		//std::cout << "passSelect " << getPassSelect();
		// ";" is not in the AST, so add it back.
		_parseResult = walkTreeString(ast) + ";";
		_aggMgr.applyAggPass();
		_aggParseResult = walkTreeString(ast) + ";";
	    } else {
		_errorMsg = "Error: no AST from parse";
	    }
	} catch( ANTLRException& e ) {
	    _errorMsg =  "Parse exception: " + e.getMessage();
	} catch( exception& e ) {
	    _errorMsg = std::string("General exception: ") + e.what();
	}
	return; // Error.
    }
    bool getHasChunks() const { 
	return _tableListHandler->getHasChunks();
    }
    bool getHasSubChunks() const { 
	return _tableListHandler->getHasSubChunks();
    }
    bool getNeedsFixup() const {
	return false; // FIXME
    }
    std::string getFixupSelect() {
	return _aggMgr.getFixupSelect();
    }
    std::string getPassSelect() {
	return _aggMgr.getPassSelect();
    }
    std::string const& getError() const {
	return _errorMsg;
    }
private:
    std::string _statement;
    std::stringstream _stream;
    ASTFactory _factory;
    SqlSQL2Lexer _lexer;
    SqlSQL2Parser _parser;
    std::string _delimiter;
    Templater _templater;
    AggregateMgr _aggMgr;
    boost::shared_ptr<Templater::TableListHandler>  _tableListHandler;

    std::string _parseResult;
    std::string _aggParseResult;
    std::string _errorMsg;

};
} // Anonymous namespace

///////////////////////////////////////////////////////////////////////////
// class Substitution
///////////////////////////////////////////////////////////////////////////
Substitution::Substitution(std::string template_, std::string const& delim) 
    : _template(template_) {
    _build(delim);
}
    
std::string Substitution::transform(Mapping const& m) {
    // This can be made more efficient by pre-sizing the result buffer
    // copying directly into it, rather than creating
    // intermediate string objects and appending.
    //
    unsigned pos = 0;
    std::string result;
    // No re-allocations if transformations are constant-size.
    result.reserve(_template.size()); 

    for(std::vector<Item>::const_iterator i = _index.begin();
	i != _index.end(); ++i) {
	// Copy bits since last match
	result += _template.substr(pos, i->position - pos);
	// Copy substitution
	Mapping::const_iterator s = m.find(i->name);
	if(s == m.end()) {
	    result += i->name; // passthrough.
	} else {
	    result += s->second; // perform substitution
	}
	// Update position
	pos = i->position + i->length;
    }
    // Copy remaining.
    if(pos < _template.length()) {
	result += _template.substr(pos);
    }
    return result;
}

// Let delim = ***
// blah blah ***Name*** blah blah
//           |         |
//         pos       endpos
//           |-length-|
//        name = Name
void Substitution::_build(std::string const& delim) {
    //int maxLength = _max(names.begin(), names.end());
    int delimLength = delim.length();
    for(unsigned pos=_template.find(delim); 
	pos < _template.length(); 
	pos = _template.find(delim, pos+1)) {
	unsigned endpos = _template.find(delim, pos + delimLength);
	Item newItem;
	newItem.position = pos;
	newItem.length = (endpos - pos) + delimLength;
	newItem.name.assign(_template, pos + delimLength,
			    newItem.length - delimLength - delimLength);
	// Note: length includes two delimiters.
	_index.push_back(newItem);
	pos = endpos;

	// Sanity check:
	// Check to see if the name is in names.
	    
    }

}

///////////////////////////////////////////////////////////////////////////
// class SqlSubstitution
///////////////////////////////////////////////////////////////////////////
SqlSubstitution::SqlSubstitution(std::string const& sqlStatement, 
				 Mapping const& mapping) 
    : _delimiter("*?*") {
    _build(sqlStatement, mapping);
    //
}
    
std::string SqlSubstitution::transform(Mapping const& m) {
    return _substitution->transform(m);
}

void SqlSubstitution::_build(std::string const& sqlStatement, 
			     Mapping const& mapping) {
    // 
    std::string template_;

    Mapping::const_iterator end = mapping.end();
    std::list<std::string> names;
    for(Mapping::const_iterator i=mapping.begin(); i != end; ++i) {
	names.push_back(i->first);
    }
    SqlParseRunner spr(sqlStatement, _delimiter);
    spr.setup(names);
    if(spr.getNeedsFixup()) {
	template_ = spr.getAggParseResult();
    } else {
	template_ = spr.getParseResult();
    } 
    _computeChunkLevel(spr.getHasChunks(), spr.getHasSubChunks());

    if(template_.empty()) {
	_errorMsg = spr.getError();
    }
    _substitution = SubstPtr(new Substitution(template_, _delimiter));
	
}

void SqlSubstitution::_computeChunkLevel(bool hasChunks, bool hasSubChunks) {
    // SqlParseRunner's TableList handler will know if it applied 
    // any subchunk rules, or if it detected any chunked tables.

    if(hasChunks) {
	if(hasSubChunks) {
	    _chunkLevel = 2;
	} else {
	    _chunkLevel = 1;
	}
    } else {
	_chunkLevel = 0;
    }
}

///////////////////////////////////////////////////////////////////////////
//class ChunkMapping
///////////////////////////////////////////////////////////////////////////
ChunkMapping::Map ChunkMapping::getMapping(int chunk, int subChunk) {
    Map m;
    ModeMap::const_iterator end = _map.end();
    std::string chunkStr = _toString(chunk);
    std::string subChunkStr = _toString(subChunk);
    static const std::string one("1");
    static const std::string two("2");
    // Insert mapping for: plainchunk, plainsubchunk1, plainsubchunk2
    for(ModeMap::const_iterator i = _map.begin(); i != end; ++i) {
	if(i->second == CHUNK) {
	    m.insert(MapValue(i->first, i->first + "_" + chunkStr));
	} else if (i->second == CHUNK_WITH_SUB) {
	    // Object --> Object_chunk
	    // Object_s1 --> Object_chunk_subchunk
	    // Object_s2 --> Object_chunk_subchunk (FIXME: add overlap)
	    m.insert(MapValue(i->first, i->first + "_" + chunkStr));
	    m.insert(MapValue(i->first + _subPrefix + one, 
			      i->first + "_" + chunkStr + "_" + subChunkStr));
	    m.insert(MapValue(i->first + _subPrefix + two, 
			      i->first + "_" + chunkStr + "_" + subChunkStr));
	}
    }
    return m;
}

ChunkMapping::Map const& ChunkMapping::getMapReference(int chunk, int subChunk) {
    _instanceMap = getMapping(chunk, subChunk);
    return _instanceMap;
}
