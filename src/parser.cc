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
std::string walkTree(AnAst r) {
    //DFS walk?
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

template <typename AnAst, typename Visitor>
void walkTreeVisit(AnAst r, Visitor& v) {
    //DFS walk?
    v(r);
    RefAST c = r->getFirstChild();
    if(c.get()) {
	walkTreeVisit(c, v);
    }
    // Now print sibling(s)
    RefAST s = r->getNextSibling();
    if(s.get()) {
	walkTreeVisit(s, v);
    }
	
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
	std::cout << "Got setfunc " << walkTree(a) << std::endl;
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
	_parser._setFctSpecHandler 
	    = boost::shared_ptr<TestSetFuncHandler>(new TestSetFuncHandler());
	_parser._aliasHandler 
	    = boost::shared_ptr<TestAliasHandler>(new TestAliasHandler());
    }

    std::string getParseResult() {
	try {
	    _parser.initializeASTFactory(_factory);
	    _parser.setASTFactory(&_factory);
	    _parser.sql_stmt();
	    RefCommonAST ast = RefCommonAST(_parser.getAST());
	    
	    if (ast) {
		// ";" is not in the AST, so add it back.
		return ast->toStringList() +";"; 
	    } else {
		_errorMsg = "Error: no AST from parse";
	    }
	} catch( ANTLRException& e ) {
	    _errorMsg =  "Parse exception: " + e.getMessage();
	} catch( exception& e ) {
	    _errorMsg = std::string("General exception: ") + e.what();
	}
	return std::string(); // Error.

    }
    bool getHasChunks() const { 
	return _tableListHandler->getHasChunks();
    }
    bool getHasSubChunks() const { 
	return _tableListHandler->getHasSubChunks();
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
    boost::shared_ptr<Templater::TableListHandler>  _tableListHandler;

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
    Mapping::const_iterator end = mapping.end();
    std::list<std::string> names;
    for(Mapping::const_iterator i=mapping.begin(); i != end; ++i) {
	names.push_back(i->first);
    }
    SqlParseRunner spr(sqlStatement, _delimiter);
    spr.setup(names);
    std::string template_ = spr.getParseResult();

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
